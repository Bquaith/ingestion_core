from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from ingestion_core.oidc_sts import (
    OIDCClientCredentialsConfig,
    OIDCSTSExchangeError,
    WebIdentitySTSConfig,
    exchange_client_credentials_for_sts,
)


@dataclass
class FakeResponse:
    status_code: int
    text: str
    json_body: dict[str, Any] | None = None

    def json(self) -> Any:
        if self.json_body is None:
            raise ValueError("invalid json")
        return self.json_body


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    def post(self, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"url": url, **kwargs})
        return self.responses.pop(0)


def test_exchange_client_credentials_for_sts_returns_temp_credentials() -> None:
    session = FakeSession(
        [
            FakeResponse(
                status_code=200,
                text='{"access_token":"jwt"}',
                json_body={"access_token": "jwt"},
            ),
            FakeResponse(
                status_code=200,
                text="""
                <AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
                  <AssumeRoleWithWebIdentityResult>
                    <Credentials>
                      <AccessKeyId>ACCESS</AccessKeyId>
                      <SecretAccessKey>SECRET</SecretAccessKey>
                      <SessionToken>TOKEN</SessionToken>
                      <Expiration>2026-03-26T12:00:00Z</Expiration>
                    </Credentials>
                  </AssumeRoleWithWebIdentityResult>
                </AssumeRoleWithWebIdentityResponse>
                """.strip(),
            ),
        ]
    )

    credentials = exchange_client_credentials_for_sts(
        oidc_config=OIDCClientCredentialsConfig(
            token_url="http://keycloak/token",
            client_id="airflow-minio-sts",
            client_secret="secret",
        ),
        sts_config=WebIdentitySTSConfig(endpoint_url="http://minio:9000"),
        session=session,  # type: ignore[arg-type]
    )

    assert credentials.access_key_id == "ACCESS"
    assert credentials.secret_access_key == "SECRET"
    assert credentials.session_token == "TOKEN"
    assert len(session.calls) == 2
    assert session.calls[1]["url"].startswith("http://minio:9000?Action=AssumeRoleWithWebIdentity")


def test_exchange_client_credentials_for_sts_fails_on_missing_access_token() -> None:
    session = FakeSession(
        [
            FakeResponse(
                status_code=200,
                text='{"token_type":"Bearer"}',
                json_body={"token_type": "Bearer"},
            )
        ]
    )

    with pytest.raises(OIDCSTSExchangeError, match="access_token"):
        exchange_client_credentials_for_sts(
            oidc_config=OIDCClientCredentialsConfig(
                token_url="http://keycloak/token",
                client_id="airflow-minio-sts",
                client_secret="secret",
            ),
            sts_config=WebIdentitySTSConfig(endpoint_url="http://minio:9000"),
            session=session,  # type: ignore[arg-type]
        )
