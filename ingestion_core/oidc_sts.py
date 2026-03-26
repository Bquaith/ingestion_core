from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import urlencode
import xml.etree.ElementTree as ET

import requests


class OIDCSTSExchangeError(RuntimeError):
    """Raised when OIDC or STS credential exchange fails."""


@dataclass(frozen=True)
class OIDCClientCredentialsConfig:
    token_url: str
    client_id: str
    client_secret: str
    timeout_seconds: int = 30
    verify_ssl: bool = True
    scope: str | None = None


@dataclass(frozen=True)
class WebIdentitySTSConfig:
    endpoint_url: str
    duration_seconds: int = 3600
    timeout_seconds: int = 30
    verify_ssl: bool = True


@dataclass(frozen=True)
class TemporaryObjectStoreCredentials:
    access_key_id: str
    secret_access_key: str
    session_token: str
    expiration: datetime | None


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    return datetime.fromisoformat(normalized)


def request_oidc_access_token(
    config: OIDCClientCredentialsConfig,
    session: requests.Session | None = None,
) -> str:
    http = session or requests.Session()
    payload: dict[str, str] = {
        "grant_type": "client_credentials",
        "client_id": config.client_id,
        "client_secret": config.client_secret,
    }
    if config.scope:
        payload["scope"] = config.scope

    try:
        response = http.post(
            config.token_url,
            data=payload,
            timeout=config.timeout_seconds,
            verify=config.verify_ssl,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
    except requests.RequestException as exc:
        raise OIDCSTSExchangeError(f"Failed to request OIDC access token: {exc}") from exc

    if response.status_code >= 400:
        raise OIDCSTSExchangeError(
            f"OIDC token endpoint returned HTTP {response.status_code}: {response.text[:500]}"
        )

    try:
        body = response.json()
    except ValueError as exc:
        raise OIDCSTSExchangeError("OIDC token endpoint returned non-JSON response") from exc

    access_token = body.get("access_token")
    if not isinstance(access_token, str) or not access_token.strip():
        raise OIDCSTSExchangeError("OIDC token response did not include access_token")
    return access_token


def assume_role_with_web_identity(
    config: WebIdentitySTSConfig,
    web_identity_token: str,
    session: requests.Session | None = None,
) -> TemporaryObjectStoreCredentials:
    http = session or requests.Session()
    query = urlencode(
        {
            "Action": "AssumeRoleWithWebIdentity",
            "Version": "2011-06-15",
            "DurationSeconds": str(config.duration_seconds),
            "WebIdentityToken": web_identity_token,
        }
    )
    url = config.endpoint_url.rstrip("/")
    request_url = f"{url}?{query}"

    try:
        response = http.post(
            request_url,
            timeout=config.timeout_seconds,
            verify=config.verify_ssl,
        )
    except requests.RequestException as exc:
        raise OIDCSTSExchangeError(f"Failed to request STS credentials: {exc}") from exc

    if response.status_code >= 400:
        raise OIDCSTSExchangeError(
            f"STS endpoint returned HTTP {response.status_code}: {response.text[:500]}"
        )

    try:
        root = ET.fromstring(response.text)
    except ET.ParseError as exc:
        raise OIDCSTSExchangeError("STS endpoint returned malformed XML response") from exc

    namespace = {"sts": "https://sts.amazonaws.com/doc/2011-06-15/"}

    def _read(path: str) -> str | None:
        node = root.find(path, namespaces=namespace)
        if node is None or node.text is None:
            return None
        return node.text.strip()

    access_key_id = _read(".//sts:Credentials/sts:AccessKeyId")
    secret_access_key = _read(".//sts:Credentials/sts:SecretAccessKey")
    session_token = _read(".//sts:Credentials/sts:SessionToken")
    expiration = _parse_iso_datetime(_read(".//sts:Credentials/sts:Expiration"))

    if not access_key_id or not secret_access_key or not session_token:
        raise OIDCSTSExchangeError("STS response did not include temporary credentials")

    return TemporaryObjectStoreCredentials(
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        session_token=session_token,
        expiration=expiration,
    )


def exchange_client_credentials_for_sts(
    oidc_config: OIDCClientCredentialsConfig,
    sts_config: WebIdentitySTSConfig,
    session: requests.Session | None = None,
) -> TemporaryObjectStoreCredentials:
    http = session or requests.Session()
    access_token = request_oidc_access_token(oidc_config, session=http)
    return assume_role_with_web_identity(sts_config, access_token, session=http)
