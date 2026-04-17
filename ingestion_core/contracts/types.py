from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class ContractDefinition:
    contract_id: str
    target_layer: str
    version: str
    checksum: str
    schema_json: dict[str, Any]
    fields: list[str]
    field_types: dict[str, str]
    required_fields: list[str]
    primary_keys: list[str]
    business_keys: list[str]
    hash_keys: list[str]

    @property
    def key_fields(self) -> list[str]:
        if self.primary_keys:
            return self.primary_keys
        if self.business_keys:
            return self.business_keys
        return []

    @property
    def effective_hash_fields(self) -> list[str]:
        if self.hash_keys:
            return self.hash_keys
        return self.fields

    def validate(self) -> None:
        if not self.contract_id:
            raise ValueError("contract.id is required")
        if not self.fields:
            raise ValueError("version.schema_json.fields must not be empty")
        if not self.key_fields:
            raise ValueError("Contract keys are empty: provide keys.primary or keys.business")
        if not isinstance(self.schema_json, dict):
            raise ValueError("version.schema_json must be an object")

        missing_keys = [key for key in self.key_fields if key not in self.fields]
        if missing_keys:
            raise ValueError(f"Key fields are absent in fields: {missing_keys}")

        missing_hash_fields = [key for key in self.effective_hash_fields if key not in self.fields]
        if missing_hash_fields:
            raise ValueError(f"Hash fields are absent in fields: {missing_hash_fields}")

        unknown_typed_fields = [field for field in self.field_types if field not in self.fields]
        if unknown_typed_fields:
            raise ValueError(f"Typed fields are absent in fields: {unknown_typed_fields}")

        unknown_required_fields = [field for field in self.required_fields if field not in self.fields]
        if unknown_required_fields:
            raise ValueError(f"Required fields are absent in fields: {unknown_required_fields}")

    @classmethod
    def from_registry_payload(cls, payload: Mapping[str, Any]) -> "ContractDefinition":
        contract = cls(
            contract_id=str(payload.get("contract_id", "")),
            target_layer=str(payload.get("target_layer", "")),
            version=str(payload.get("version", "")),
            checksum=str(payload.get("checksum", "")),
            schema_json=dict(payload.get("schema_json") or {}),
            fields=[str(v) for v in (payload.get("fields") or [])],
            field_types={str(k): str(v) for k, v in dict(payload.get("field_types") or {}).items()},
            required_fields=[str(v) for v in (payload.get("required_fields") or [])],
            primary_keys=[str(v) for v in (payload.get("primary_keys") or [])],
            business_keys=[str(v) for v in (payload.get("business_keys") or [])],
            hash_keys=[str(v) for v in (payload.get("hash_keys") or [])],
        )
        contract.validate()
        return contract
