from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable

import requests


class ContractRegistryError(RuntimeError):
    """Base error for contract registry integration."""


class ContractRegistryRequestError(ContractRegistryError):
    """Raised when request transport fails before receiving a valid response."""


class ContractRegistryHTTPError(ContractRegistryError):
    """Raised when registry returns a non-success HTTP status code."""

    def __init__(self, message: str, status_code: int, endpoint: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.endpoint = endpoint


class ContractPayloadError(ContractRegistryError):
    """Raised when response payload does not match expected contract shape."""


@dataclass(frozen=True)
class ContractPayload:
    contract_id: str
    target_layer: str
    version: str
    checksum: str
    fields: list[str]
    primary_keys: list[str]
    business_keys: list[str]
    hash_keys: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract_id": self.contract_id,
            "target_layer": self.target_layer,
            "version": self.version,
            "checksum": self.checksum,
            "fields": self.fields,
            "primary_keys": self.primary_keys,
            "business_keys": self.business_keys,
            "hash_keys": self.hash_keys,
        }


class ContractRegistryClient:
    RETRIABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}

    def __init__(
        self,
        base_url: str,
        timeout_seconds: int = 30,
        max_retries: int = 3,
        retry_backoff_seconds: float = 0.5,
        max_backoff_seconds: float = 8.0,
        session: requests.Session | None = None,
        sleep_func: Callable[[float], None] | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds
        self.max_backoff_seconds = max_backoff_seconds
        self.session = session or requests.Session()
        self.sleep_func = sleep_func or time.sleep

    def fetch_contract(self, namespace: str, name: str, version: str | None = None) -> ContractPayload:
        if version:
            endpoint = f"/contracts/{namespace}/{name}/version/{version}"
        else:
            endpoint = f"/contracts/{namespace}/{name}/active"

        payload = self._get_json_with_retry(endpoint)
        return self._parse_payload(payload)

    def _required_str(self, value: Any, field_name: str) -> str:
        text_value = str(value or "").strip()
        if not text_value:
            raise ContractPayloadError(f"Contract payload field '{field_name}' is required")
        return text_value

    def _is_retriable_status(self, status_code: int) -> bool:
        return status_code in self.RETRIABLE_STATUS_CODES

    def _sleep_before_retry(self, attempt: int) -> None:
        backoff = min(self.retry_backoff_seconds * (2 ** (attempt - 1)), self.max_backoff_seconds)
        self.sleep_func(backoff)

    def _get_json_with_retry(self, endpoint: str) -> dict[str, Any]:
        url = f"{self.base_url}{endpoint}"
        total_attempts = self.max_retries + 1

        for attempt in range(1, total_attempts + 1):
            try:
                response = self.session.get(
                    url,
                    timeout=self.timeout_seconds,
                    headers={"Accept": "application/json"},
                )
            except (requests.Timeout, requests.ConnectionError) as exc:
                if attempt == total_attempts:
                    raise ContractRegistryRequestError(
                        f"Failed to request contract registry endpoint {endpoint}: {exc}"
                    ) from exc
                self._sleep_before_retry(attempt)
                continue
            except requests.RequestException as exc:
                raise ContractRegistryRequestError(
                    f"Request exception while calling contract registry endpoint {endpoint}: {exc}"
                ) from exc

            if response.status_code >= 400:
                error = ContractRegistryHTTPError(
                    message=(
                        f"Contract registry returned HTTP {response.status_code} for endpoint {endpoint}: "
                        f"{response.text[:500]}"
                    ),
                    status_code=response.status_code,
                    endpoint=endpoint,
                )
                if self._is_retriable_status(response.status_code) and attempt < total_attempts:
                    self._sleep_before_retry(attempt)
                    continue
                raise error

            try:
                body = response.json()
            except ValueError as exc:
                raise ContractPayloadError(
                    f"Contract registry endpoint {endpoint} returned non-JSON response"
                ) from exc

            if not isinstance(body, dict):
                raise ContractPayloadError(
                    f"Contract registry endpoint {endpoint} returned unexpected payload type: {type(body)!r}"
                )
            return body

        raise ContractRegistryRequestError(f"Failed to request contract registry endpoint {endpoint}")

    def _parse_payload(self, payload: dict[str, Any]) -> ContractPayload:
        data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
        if not isinstance(data, dict):
            raise ContractPayloadError("Contract payload root must be an object")

        contract = data.get("contract") if isinstance(data.get("contract"), dict) else data
        version = data.get("version") if isinstance(data.get("version"), dict) else {}
        if not isinstance(contract, dict):
            raise ContractPayloadError("Contract payload must include contract object")
        if not isinstance(version, dict):
            raise ContractPayloadError("Contract payload must include version object")

        schema_json = version.get("schema_json") if isinstance(version.get("schema_json"), dict) else {}
        if not schema_json and isinstance(data.get("schema"), dict):
            # Compatibility fallback for simplified contract shape.
            schema_json = data.get("schema")  # type: ignore[assignment]
        if not isinstance(schema_json, dict):
            raise ContractPayloadError("Contract payload must include schema_json object")

        fields_raw = schema_json.get("fields") if isinstance(schema_json.get("fields"), list) else []

        fields: list[str] = []
        for item in fields_raw:
            if isinstance(item, str):
                fields.append(item)
                continue
            if isinstance(item, dict) and item.get("name"):
                fields.append(str(item["name"]))
        if not fields:
            raise ContractPayloadError("Contract payload must include at least one schema field")

        keys = schema_json.get("keys") if isinstance(schema_json.get("keys"), dict) else {}

        primary_keys = [str(v) for v in (keys.get("primary") or [])]
        if not primary_keys:
            primary_keys = [str(v) for v in (schema_json.get("primary_key") or [])]
        business_keys = [str(v) for v in (keys.get("business") or [])]
        hash_keys = [str(v) for v in (keys.get("hash_keys") or [])]

        return ContractPayload(
            contract_id=self._required_str(contract.get("id"), "contract.id"),
            target_layer=self._required_str(contract.get("target_layer"), "contract.target_layer"),
            version=self._required_str(version.get("version"), "version.version"),
            checksum=self._required_str(version.get("checksum"), "version.checksum"),
            fields=fields,
            primary_keys=primary_keys,
            business_keys=business_keys,
            hash_keys=hash_keys,
        )
