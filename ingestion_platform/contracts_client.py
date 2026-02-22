from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests


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
    def __init__(self, base_url: str, timeout_seconds: int = 30) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def fetch_contract(self, namespace: str, name: str, version: str | None = None) -> ContractPayload:
        if version:
            endpoint = f"/contracts/{namespace}/{name}/version/{version}"
        else:
            endpoint = f"/contracts/{namespace}/{name}/active"

        response = requests.get(
            f"{self.base_url}{endpoint}",
            timeout=self.timeout_seconds,
            headers={"Accept": "application/json"},
        )
        response.raise_for_status()
        return self._parse_payload(response.json())

    def _parse_payload(self, payload: dict[str, Any]) -> ContractPayload:
        data = payload.get("data") if isinstance(payload.get("data"), dict) else payload

        contract = data.get("contract") if isinstance(data.get("contract"), dict) else data
        version = data.get("version") if isinstance(data.get("version"), dict) else {}

        schema_json = version.get("schema_json") if isinstance(version.get("schema_json"), dict) else {}
        if not schema_json and isinstance(data.get("schema"), dict):
            # Compatibility fallback for simplified contract shape.
            schema_json = data.get("schema")  # type: ignore[assignment]
        fields_raw = schema_json.get("fields") if isinstance(schema_json.get("fields"), list) else []

        fields: list[str] = []
        for item in fields_raw:
            if isinstance(item, str):
                fields.append(item)
                continue
            if isinstance(item, dict) and item.get("name"):
                fields.append(str(item["name"]))

        keys = schema_json.get("keys") if isinstance(schema_json.get("keys"), dict) else {}

        primary_keys = [str(v) for v in (keys.get("primary") or [])]
        if not primary_keys:
            primary_keys = [str(v) for v in (schema_json.get("primary_key") or [])]
        business_keys = [str(v) for v in (keys.get("business") or [])]
        hash_keys = [str(v) for v in (keys.get("hash_keys") or [])]

        return ContractPayload(
            contract_id=str(contract.get("id", "")),
            target_layer=str(contract.get("target_layer", "")),
            version=str(version.get("version", "")),
            checksum=str(version.get("checksum", "")),
            fields=fields,
            primary_keys=primary_keys,
            business_keys=business_keys,
            hash_keys=hash_keys,
        )
