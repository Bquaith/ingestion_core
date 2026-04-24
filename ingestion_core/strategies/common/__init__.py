from ingestion_core.strategies.common.change_detection import (
    build_row_key,
    chunk_rows,
    classify_changes,
    read_existing_hashes_for_keys,
)
from ingestion_core.strategies.common.delta_apply import (
    DELTA_OP_DELETE,
    DELTA_OP_UPSERT,
    DeltaApplyResult,
    DeltaMetadataColumn,
    ParsedDeltaEvent,
    apply_delta_artifact_to_curated,
    qualify_table,
    quote_identifier,
)
from ingestion_core.strategies.common.source import (
    normalize_contract_type,
    normalize_sqlalchemy_type,
    validate_source_columns,
)
from ingestion_core.strategies.common.target import (
    delete_rows_by_keys,
    ensure_hash_state_table,
    ensure_target_table_from_contract,
    make_hash_state_table_name,
    upsert_changed_rows,
)

__all__ = [
    "build_row_key",
    "chunk_rows",
    "classify_changes",
    "DELTA_OP_DELETE",
    "DELTA_OP_UPSERT",
    "delete_rows_by_keys",
    "DeltaApplyResult",
    "DeltaMetadataColumn",
    "ensure_hash_state_table",
    "ensure_target_table_from_contract",
    "make_hash_state_table_name",
    "normalize_contract_type",
    "normalize_sqlalchemy_type",
    "ParsedDeltaEvent",
    "apply_delta_artifact_to_curated",
    "qualify_table",
    "quote_identifier",
    "read_existing_hashes_for_keys",
    "upsert_changed_rows",
    "validate_source_columns",
]
