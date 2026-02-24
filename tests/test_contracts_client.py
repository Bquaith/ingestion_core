from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest
import requests

from ingestion_platform.contracts_client import (
    ContractPayloadError,
    ContractRegistryClient,
    ContractRegistryHTTPError,
)


@dataclass
class FakeResponse:
    status_code: int
    payload: dict[str, Any] | None = None
    text: str = ""

    def json(self) -> Any:
        if self.payload is None:
            raise ValueError("invalid json")
        return self.payload


class FakeSession:
    def __init__(self, responses: list[Any]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, timeout: int, headers: dict[str, str]) -> Any:
        self.calls.append({"url": url, "timeout": timeout, "headers": headers})
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _contract_payload() -> dict[str, Any]:
    return {
        "contract": {"id": "orders-contract", "target_layer": "curated"},
        "version": {
            "version": "1",
            "checksum": "abc123",
            "schema_json": {
                "fields": [
                    {"name": "id", "type": "bigint"},
                    {"name": "status", "type": "string"},
                ],
                "keys": {
                    "primary": ["id"],
                    "business": [],
                    "hash_keys": ["status"],
                },
            },
        },
    }


def test_fetch_contract_retries_on_transient_status() -> None:
    session = FakeSession(
        [
            FakeResponse(status_code=503, payload={"message": "temp unavailable"}, text="temp unavailable"),
            FakeResponse(status_code=200, payload=_contract_payload(), text="ok"),
        ]
    )
    sleeps: list[float] = []

    client = ContractRegistryClient(
        base_url="http://contracts.local",
        max_retries=2,
        retry_backoff_seconds=0.1,
        session=session,  # type: ignore[arg-type]
        sleep_func=sleeps.append,
    )

    payload = client.fetch_contract(namespace="sales", name="orders")

    assert payload.contract_id == "orders-contract"
    assert payload.field_types == {"id": "bigint", "status": "string"}
    assert len(session.calls) == 2
    assert sleeps == [0.1]


def test_fetch_contract_retries_on_timeout() -> None:
    session = FakeSession(
        [
            requests.Timeout("timeout"),
            FakeResponse(status_code=200, payload=_contract_payload(), text="ok"),
        ]
    )
    sleeps: list[float] = []

    client = ContractRegistryClient(
        base_url="http://contracts.local",
        max_retries=1,
        retry_backoff_seconds=0.2,
        session=session,  # type: ignore[arg-type]
        sleep_func=sleeps.append,
    )

    payload = client.fetch_contract(namespace="sales", name="orders")

    assert payload.version == "1"
    assert len(session.calls) == 2
    assert sleeps == [0.2]


def test_fetch_contract_does_not_retry_on_not_found() -> None:
    session = FakeSession([FakeResponse(status_code=404, payload={"error": "not found"}, text="not found")])

    client = ContractRegistryClient(
        base_url="http://contracts.local",
        max_retries=3,
        session=session,  # type: ignore[arg-type]
        sleep_func=lambda _: None,
    )

    with pytest.raises(ContractRegistryHTTPError) as exc_info:
        client.fetch_contract(namespace="sales", name="orders")

    assert exc_info.value.status_code == 404
    assert len(session.calls) == 1


def test_parse_payload_validates_required_fields() -> None:
    session = FakeSession([])
    client = ContractRegistryClient(base_url="http://contracts.local", session=session, sleep_func=lambda _: None)  # type: ignore[arg-type]

    with pytest.raises(ContractPayloadError, match="contract.id"):
        client._parse_payload(
            {
                "contract": {"id": "", "target_layer": "curated"},
                "version": {
                    "version": "1",
                    "checksum": "abc",
                    "schema_json": {
                        "fields": [{"name": "id"}],
                        "keys": {"primary": ["id"]},
                    },
                },
            }
        )
