from __future__ import annotations

import argparse
from collections.abc import Mapping
import json
from pathlib import Path
from typing import Any

from ingestion_core.adapters.oidc_sts import OIDCClientCredentialsConfig, request_oidc_access_token
from ingestion_core.contracts import ContractDefinition, ContractRegistryClient


class CLIError(RuntimeError):
    """Raised when CLI input cannot be resolved into a valid setup request."""


def add_json_output_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output.",
    )


def add_contract_source_arguments(parser: argparse.ArgumentParser) -> None:
    group = parser.add_argument_group("Contract Source")
    group.add_argument(
        "--contract-file",
        help="Path to a JSON file containing either a published contract payload or a normalized contract payload.",
    )
    group.add_argument(
        "--contracts-service-url",
        help="Base URL of the contract registry, for example http://contracts.local.",
    )
    group.add_argument("--namespace", help="Contract namespace when loading from registry.")
    group.add_argument("--name", help="Contract name when loading from registry.")
    group.add_argument("--contract-version", help="Optional explicit contract version from the registry.")
    group.add_argument("--contracts-access-token", help="Static bearer token for the contract registry.")
    group.add_argument(
        "--contracts-oidc-token-url",
        help="OIDC token endpoint for client-credentials auth to the contract registry.",
    )
    group.add_argument(
        "--contracts-oidc-client-id",
        help="OIDC client id for contract-registry auth.",
    )
    group.add_argument(
        "--contracts-oidc-client-secret",
        help="OIDC client secret for contract-registry auth.",
    )
    group.add_argument(
        "--contracts-oidc-scope",
        help="Optional OIDC scope for contract-registry auth.",
    )
    group.set_defaults(contracts_oidc_verify_ssl=True)
    group.add_argument(
        "--contracts-oidc-no-verify-ssl",
        dest="contracts_oidc_verify_ssl",
        action="store_false",
        help="Disable TLS verification for OIDC token retrieval.",
    )


def write_json_payload(
    payload: Mapping[str, Any],
    *,
    pretty: bool,
    stdout,
) -> None:
    if pretty:
        stdout.write(json.dumps(dict(payload), ensure_ascii=True, indent=2, sort_keys=True))
    else:
        stdout.write(json.dumps(dict(payload), ensure_ascii=True, sort_keys=True))
    stdout.write("\n")


def load_contract_definition_from_args(args: argparse.Namespace) -> ContractDefinition:
    contract_file = _optional_text(getattr(args, "contract_file", None))
    using_registry = any(
        _optional_text(getattr(args, field_name, None)) is not None
        for field_name in (
            "contracts_service_url",
            "namespace",
            "name",
            "contract_version",
            "contracts_access_token",
            "contracts_oidc_token_url",
            "contracts_oidc_client_id",
            "contracts_oidc_client_secret",
            "contracts_oidc_scope",
        )
    )

    if contract_file and using_registry:
        raise CLIError(
            "Use either --contract-file or registry arguments "
            "(--contracts-service-url/--namespace/--name), not both"
        )
    if contract_file:
        return load_contract_definition_from_file(contract_file)
    if not using_registry:
        raise CLIError(
            "Contract is required. Provide --contract-file or "
            "--contracts-service-url with --namespace and --name"
        )

    contracts_service_url = _required_text(
        getattr(args, "contracts_service_url", None),
        "--contracts-service-url",
    )
    namespace = _required_text(getattr(args, "namespace", None), "--namespace")
    name = _required_text(getattr(args, "name", None), "--name")
    contract_version = _optional_text(getattr(args, "contract_version", None))
    access_token, token_provider = _resolve_contract_registry_auth(args)

    client = ContractRegistryClient(
        base_url=contracts_service_url,
        access_token=access_token,
        token_provider=token_provider,
    )
    payload = client.fetch_contract(namespace=namespace, name=name, version=contract_version)
    return ContractDefinition.from_registry_payload(payload.to_dict())


def load_contract_definition_from_file(contract_file: str) -> ContractDefinition:
    contract_path = Path(contract_file)
    try:
        payload = json.loads(contract_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise CLIError(f"Contract file was not found: {contract_path}") from exc
    except OSError as exc:
        raise CLIError(f"Failed to read contract file {contract_path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise CLIError(f"Contract file is not valid JSON: {contract_path}") from exc

    if not isinstance(payload, Mapping):
        raise CLIError(f"Contract file payload must be a JSON object: {contract_path}")

    if "contract_id" in payload and "schema_json" in payload:
        return ContractDefinition.from_registry_payload(payload)

    parser_client = ContractRegistryClient(base_url="http://contracts.local")
    try:
        parsed_payload = parser_client._parse_payload(dict(payload))
    except Exception as exc:
        raise CLIError(f"Contract file has unsupported payload shape: {exc}") from exc

    return ContractDefinition.from_registry_payload(parsed_payload.to_dict())


def _resolve_contract_registry_auth(args: argparse.Namespace) -> tuple[str | None, Any]:
    access_token = _optional_text(getattr(args, "contracts_access_token", None))
    token_url = _optional_text(getattr(args, "contracts_oidc_token_url", None))
    client_id = _optional_text(getattr(args, "contracts_oidc_client_id", None))
    client_secret = _optional_text(getattr(args, "contracts_oidc_client_secret", None))
    scope = _optional_text(getattr(args, "contracts_oidc_scope", None))
    verify_ssl = bool(getattr(args, "contracts_oidc_verify_ssl", True))

    if access_token and any(value is not None for value in (token_url, client_id, client_secret, scope)):
        raise CLIError(
            "Use either --contracts-access-token or OIDC client-credentials arguments, not both"
        )

    if not any(value is not None for value in (token_url, client_id, client_secret, scope)):
        return access_token, None

    missing = [
        option
        for option, value in (
            ("--contracts-oidc-token-url", token_url),
            ("--contracts-oidc-client-id", client_id),
            ("--contracts-oidc-client-secret", client_secret),
        )
        if value is None
    ]
    if missing:
        raise CLIError(
            "OIDC auth requires all of: " + ", ".join(missing)
        )

    def _provider() -> str:
        return request_oidc_access_token(
            OIDCClientCredentialsConfig(
                token_url=str(token_url),
                client_id=str(client_id),
                client_secret=str(client_secret),
                verify_ssl=verify_ssl,
                scope=scope,
            )
        )

    return None, _provider


def _required_text(value: Any, option_name: str) -> str:
    normalized = _optional_text(value)
    if normalized is None:
        raise CLIError(f"Missing required option: {option_name}")
    return normalized


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None
