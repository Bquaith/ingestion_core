from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
import ipaddress
import json
import math
import re
from typing import Any, Mapping, Sequence
from urllib.parse import urlsplit
from uuid import UUID

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_HOSTNAME_LABEL_RE = re.compile(r"^[A-Za-z0-9-]{1,63}$")
_DURATION_RE = re.compile(
    r"^P(?=.)(\d+Y)?(\d+M)?(\d+D)?(T(?=.)(\d+H)?(\d+M)?(\d+(\.\d+)?S)?)?$"
)


@dataclass(frozen=True)
class ContractSchemaViolation:
    code: str
    message: str
    path: tuple[str | int, ...]
    constraint: str | None = None
    actual_value: str | None = None
    contract_title: str | None = None
    contract_description: str | None = None

    @property
    def field(self) -> str:
        if not self.path:
            return "$"

        chunks: list[str] = []
        for index, part in enumerate(self.path):
            if isinstance(part, int):
                chunks.append(f"[{part}]")
                continue
            if index == 0:
                chunks.append(part)
            else:
                chunks.append(f".{part}")
        return "".join(chunks)


def prepare_instance_for_validation(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): prepare_instance_for_validation(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [prepare_instance_for_validation(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, time):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")
    return value


def validate_instance_against_schema(
    schema: Mapping[str, Any] | None,
    instance: Any,
) -> list[ContractSchemaViolation]:
    if not isinstance(schema, Mapping):
        return []
    if schema.get("type") != "object":
        return []
    if not isinstance(schema.get("properties"), Mapping):
        return []

    prepared_instance = prepare_instance_for_validation(instance)
    return _validate_node(schema, prepared_instance, ())


def _make_violation(
    *,
    code: str,
    schema: Mapping[str, Any],
    path: tuple[str | int, ...],
    constraint: str,
    reason: str,
    actual_value: Any | None = None,
) -> ContractSchemaViolation:
    contract_title = _extract_schema_text(schema.get("title"))
    contract_description = _extract_schema_text(schema.get("description"))
    actual_value_text = _format_error_value(actual_value) if actual_value is not None else None

    message_parts = [f"Constraint: {constraint}.", f"Reason: {reason}."]
    if actual_value_text is not None:
        message_parts.append(f"Actual value: {actual_value_text}.")
    if contract_title:
        message_parts.append(f"Contract title: {contract_title}.")
    if contract_description:
        message_parts.append(f"Contract description: {contract_description}.")

    return ContractSchemaViolation(
        code=code,
        message=" ".join(message_parts),
        path=path,
        constraint=constraint,
        actual_value=actual_value_text,
        contract_title=contract_title,
        contract_description=contract_description,
    )


def _validate_node(
    schema: Mapping[str, Any],
    instance: Any,
    path: tuple[str | int, ...],
) -> list[ContractSchemaViolation]:
    violations: list[ContractSchemaViolation] = []

    expected_type = schema.get("type")
    if isinstance(expected_type, str) and not _matches_type(expected_type, instance):
        violations.append(
            _make_violation(
                code="schema.type",
                schema=schema,
                path=path,
                constraint=f'type="{expected_type}"',
                reason=f"expected {_describe_instance_type_label(expected_type)}, got {_describe_instance_type(instance)}",
                actual_value=instance,
            )
        )
        return violations

    const_value = schema.get("const")
    if "const" in schema and not _json_values_equal(instance, const_value):
        violations.append(
            _make_violation(
                code="schema.const",
                schema=schema,
                path=path,
                constraint=f"const={_format_error_value(const_value)}",
                reason="value does not exactly match the contract constant",
                actual_value=instance,
            )
        )

    enum_values = schema.get("enum")
    if isinstance(enum_values, Sequence) and not isinstance(enum_values, (str, bytes, bytearray)):
        if not any(_json_values_equal(instance, candidate) for candidate in enum_values):
            violations.append(
                _make_violation(
                    code="schema.enum",
                    schema=schema,
                    path=path,
                    constraint=f"enum={_format_error_value(list(enum_values))}",
                    reason="value is not included in the allowed set",
                    actual_value=instance,
                )
            )

    if expected_type == "string":
        violations.extend(_validate_string_keywords(schema, instance, path))
    elif expected_type in {"integer", "number"}:
        violations.extend(_validate_numeric_keywords(schema, instance, path))
    elif expected_type == "object":
        violations.extend(_validate_object_keywords(schema, instance, path))
    elif expected_type == "array":
        violations.extend(_validate_array_keywords(schema, instance, path))

    return violations


def _validate_string_keywords(
    schema: Mapping[str, Any],
    instance: Any,
    path: tuple[str | int, ...],
) -> list[ContractSchemaViolation]:
    if not isinstance(instance, str):
        return []

    violations: list[ContractSchemaViolation] = []
    min_length = schema.get("minLength")
    if isinstance(min_length, int) and len(instance) < min_length:
        violations.append(
            _make_violation(
                code="schema.min_length",
                schema=schema,
                path=path,
                constraint=f"minLength={min_length}",
                reason=f"string length {len(instance)} is shorter than the allowed minimum {min_length}",
                actual_value=instance,
            )
        )

    max_length = schema.get("maxLength")
    if isinstance(max_length, int) and len(instance) > max_length:
        violations.append(
            _make_violation(
                code="schema.max_length",
                schema=schema,
                path=path,
                constraint=f"maxLength={max_length}",
                reason=f"string length {len(instance)} exceeds the allowed maximum {max_length}",
                actual_value=instance,
            )
        )

    pattern = schema.get("pattern")
    if isinstance(pattern, str):
        try:
            if re.search(pattern, instance) is None:
                violations.append(
                    _make_violation(
                        code="schema.pattern",
                        schema=schema,
                        path=path,
                        constraint=f"pattern={_format_error_value(pattern)}",
                        reason="string does not match the required regular expression",
                        actual_value=instance,
                    )
                )
        except re.error as exc:
            raise ValueError(f"Contract schema contains invalid regex pattern '{pattern}': {exc}") from exc

    format_name = schema.get("format")
    if isinstance(format_name, str) and not _matches_format(format_name, instance):
        violations.append(
            _make_violation(
                code="schema.format",
                schema=schema,
                path=path,
                constraint=f"format={_format_error_value(format_name)}",
                reason=f"string does not satisfy the declared format {format_name}",
                actual_value=instance,
            )
        )

    return violations


def _validate_numeric_keywords(
    schema: Mapping[str, Any],
    instance: Any,
    path: tuple[str | int, ...],
) -> list[ContractSchemaViolation]:
    if not _matches_type("number", instance):
        return []

    current_value = _to_decimal(instance)
    violations: list[ContractSchemaViolation] = []

    minimum = schema.get("minimum")
    if minimum is not None and current_value < _to_decimal(minimum):
        violations.append(
            _make_violation(
                code="schema.minimum",
                schema=schema,
                path=path,
                constraint=f"minimum={_format_error_value(minimum)}",
                reason=f"numeric value {current_value} is smaller than the allowed minimum {minimum}",
                actual_value=instance,
            )
        )

    maximum = schema.get("maximum")
    if maximum is not None and current_value > _to_decimal(maximum):
        violations.append(
            _make_violation(
                code="schema.maximum",
                schema=schema,
                path=path,
                constraint=f"maximum={_format_error_value(maximum)}",
                reason=f"numeric value {current_value} is greater than the allowed maximum {maximum}",
                actual_value=instance,
            )
        )

    exclusive_minimum = schema.get("exclusiveMinimum")
    if exclusive_minimum is not None and current_value <= _to_decimal(exclusive_minimum):
        violations.append(
            _make_violation(
                code="schema.exclusive_minimum",
                schema=schema,
                path=path,
                constraint=f"exclusiveMinimum={_format_error_value(exclusive_minimum)}",
                reason=f"numeric value {current_value} must be strictly greater than {exclusive_minimum}",
                actual_value=instance,
            )
        )

    exclusive_maximum = schema.get("exclusiveMaximum")
    if exclusive_maximum is not None and current_value >= _to_decimal(exclusive_maximum):
        violations.append(
            _make_violation(
                code="schema.exclusive_maximum",
                schema=schema,
                path=path,
                constraint=f"exclusiveMaximum={_format_error_value(exclusive_maximum)}",
                reason=f"numeric value {current_value} must be strictly less than {exclusive_maximum}",
                actual_value=instance,
            )
        )

    return violations


def _validate_object_keywords(
    schema: Mapping[str, Any],
    instance: Any,
    path: tuple[str | int, ...],
) -> list[ContractSchemaViolation]:
    if not isinstance(instance, dict):
        return []

    violations: list[ContractSchemaViolation] = []
    properties = schema.get("properties")
    properties_map = dict(properties) if isinstance(properties, Mapping) else {}

    required_fields = schema.get("required")
    if isinstance(required_fields, Sequence) and not isinstance(required_fields, (str, bytes, bytearray)):
        for field_name in required_fields:
            if isinstance(field_name, str) and field_name not in instance:
                violations.append(
                    _make_violation(
                        code="schema.required",
                        schema=schema,
                        path=path + (field_name,),
                        constraint=f"required contains {_format_error_value(field_name)}",
                        reason=f"required property '{field_name}' is absent",
                        actual_value="<missing>",
                    )
                )

    additional_properties = schema.get("additionalProperties", True)
    if additional_properties is False:
        extra_fields = sorted(set(instance) - set(properties_map))
        for field_name in extra_fields:
            violations.append(
                _make_violation(
                    code="schema.additional_properties",
                    schema=schema,
                    path=path + (field_name,),
                    constraint="additionalProperties=false",
                    reason=f"property '{field_name}' is not declared in contract properties",
                    actual_value=field_name,
                )
            )

    for field_name, property_schema in properties_map.items():
        if field_name not in instance:
            continue
        if not isinstance(property_schema, Mapping):
            continue
        violations.extend(_validate_node(property_schema, instance[field_name], path + (field_name,)))

    return violations


def _validate_array_keywords(
    schema: Mapping[str, Any],
    instance: Any,
    path: tuple[str | int, ...],
) -> list[ContractSchemaViolation]:
    if not isinstance(instance, list):
        return []

    violations: list[ContractSchemaViolation] = []
    min_items = schema.get("minItems")
    if isinstance(min_items, int) and len(instance) < min_items:
        violations.append(
            _make_violation(
                code="schema.min_items",
                schema=schema,
                path=path,
                constraint=f"minItems={min_items}",
                reason=f"array length {len(instance)} is smaller than the allowed minimum {min_items}",
                actual_value=instance,
            )
        )

    max_items = schema.get("maxItems")
    if isinstance(max_items, int) and len(instance) > max_items:
        violations.append(
            _make_violation(
                code="schema.max_items",
                schema=schema,
                path=path,
                constraint=f"maxItems={max_items}",
                reason=f"array length {len(instance)} exceeds the allowed maximum {max_items}",
                actual_value=instance,
            )
        )

    items_schema = schema.get("items")
    if isinstance(items_schema, Mapping):
        for index, item in enumerate(instance):
            violations.extend(_validate_node(items_schema, item, path + (index,)))

    return violations


def _matches_type(expected_type: str, instance: Any) -> bool:
    if expected_type == "null":
        return instance is None
    if expected_type == "boolean":
        return isinstance(instance, bool)
    if expected_type == "string":
        return isinstance(instance, str)
    if expected_type == "integer":
        return isinstance(instance, int) and not isinstance(instance, bool)
    if expected_type == "number":
        if isinstance(instance, bool):
            return False
        if isinstance(instance, Decimal):
            return instance.is_finite()
        if isinstance(instance, int):
            return True
        if isinstance(instance, float):
            return math.isfinite(instance)
        return False
    if expected_type == "object":
        return isinstance(instance, dict)
    if expected_type == "array":
        return isinstance(instance, list)
    return True


def _describe_instance_type(instance: Any) -> str:
    if instance is None:
        return "null"
    if isinstance(instance, bool):
        return "boolean"
    if isinstance(instance, str):
        return "string"
    if isinstance(instance, int):
        return "integer"
    if isinstance(instance, (float, Decimal)):
        return "number"
    if isinstance(instance, dict):
        return "object"
    if isinstance(instance, list):
        return "array"
    return type(instance).__name__


def _describe_instance_type_label(expected_type: str) -> str:
    if expected_type == "null":
        return "null"
    if expected_type == "boolean":
        return "boolean"
    if expected_type == "string":
        return "string"
    if expected_type == "integer":
        return "integer"
    if expected_type == "number":
        return "number"
    if expected_type == "object":
        return "object"
    if expected_type == "array":
        return "array"
    return expected_type


def _to_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("Non-finite Decimal is not supported for contract validation")
        return value
    if isinstance(value, bool):
        raise ValueError("Boolean is not a numeric contract value")
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("Non-finite float is not supported for contract validation")
        return Decimal(str(value))
    raise ValueError(f"Value {value!r} is not numeric")


def _json_values_equal(left: Any, right: Any) -> bool:
    return _normalize_json_value(left) == _normalize_json_value(right)


def _normalize_json_value(value: Any) -> Any:
    prepared_value = prepare_instance_for_validation(value)

    if prepared_value is None or isinstance(prepared_value, (bool, str)):
        return prepared_value
    if isinstance(prepared_value, Decimal):
        return ("number", _normalize_decimal(prepared_value))
    if isinstance(prepared_value, int) and not isinstance(prepared_value, bool):
        return ("number", _normalize_decimal(Decimal(prepared_value)))
    if isinstance(prepared_value, float):
        return ("number", _normalize_decimal(Decimal(str(prepared_value))))
    if isinstance(prepared_value, list):
        return ("array", tuple(_normalize_json_value(item) for item in prepared_value))
    if isinstance(prepared_value, dict):
        return (
            "object",
            tuple(
                (key, _normalize_json_value(item))
                for key, item in sorted(prepared_value.items(), key=lambda pair: pair[0])
            ),
        )
    return prepared_value


def _normalize_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == normalized.to_integral():
        return str(normalized.quantize(Decimal("1")))
    return format(normalized, "f")


def _extract_schema_text(value: Any) -> str | None:
    if isinstance(value, str):
        normalized = value.strip()
        if normalized:
            return normalized
    return None


def _format_error_value(value: Any) -> str:
    if value == "<missing>":
        return "<missing>"

    prepared = prepare_instance_for_validation(value)
    if isinstance(prepared, Decimal):
        prepared = _normalize_decimal(prepared)

    try:
        return json.dumps(prepared, ensure_ascii=True, sort_keys=True)
    except TypeError:
        return repr(prepared)


def _matches_format(format_name: str, value: str) -> bool:
    normalized = format_name.strip().lower()
    if not normalized:
        return True
    if normalized == "date-time":
        return _is_valid_datetime(value)
    if normalized == "date":
        return _is_valid_date(value)
    if normalized == "time":
        return _is_valid_time(value)
    if normalized == "uuid":
        return _is_valid_uuid(value)
    if normalized == "email":
        return bool(_EMAIL_RE.fullmatch(value))
    if normalized == "hostname":
        return _is_valid_hostname(value)
    if normalized in {"ipv4", "ipv6"}:
        return _is_valid_ip(value, normalized)
    if normalized in {"uri", "uri-reference"}:
        return _is_valid_uri(value, normalized == "uri")
    if normalized == "regex":
        return _is_valid_regex(value)
    if normalized == "duration":
        return bool(_DURATION_RE.fullmatch(value))
    return True


def _is_valid_datetime(value: str) -> bool:
    trimmed = value.strip()
    if trimmed.endswith("Z"):
        trimmed = f"{trimmed[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(trimmed)
    except ValueError:
        return False
    return parsed.tzinfo is not None


def _is_valid_date(value: str) -> bool:
    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return True


def _is_valid_time(value: str) -> bool:
    try:
        time.fromisoformat(value)
    except ValueError:
        return False
    return True


def _is_valid_uuid(value: str) -> bool:
    try:
        UUID(value)
    except ValueError:
        return False
    return True


def _is_valid_hostname(value: str) -> bool:
    if not value or len(value) > 253:
        return False
    labels = value.rstrip(".").split(".")
    if not labels:
        return False
    for label in labels:
        if not label or label.startswith("-") or label.endswith("-"):
            return False
        if not _HOSTNAME_LABEL_RE.fullmatch(label):
            return False
    return True


def _is_valid_ip(value: str, version: str) -> bool:
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        return False
    if version == "ipv4":
        return isinstance(address, ipaddress.IPv4Address)
    return isinstance(address, ipaddress.IPv6Address)


def _is_valid_uri(value: str, require_scheme: bool) -> bool:
    parsed = urlsplit(value)
    if require_scheme and not parsed.scheme:
        return False
    if require_scheme and not (parsed.netloc or parsed.path):
        return False
    return any([parsed.scheme, parsed.netloc, parsed.path, parsed.query, parsed.fragment])


def _is_valid_regex(value: str) -> bool:
    try:
        re.compile(value)
    except re.error:
        return False
    return True
