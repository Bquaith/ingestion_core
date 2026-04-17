# ingestion-core

Переиспользуемое Python-ядро для инкрементальной загрузки данных.

Репозиторий содержит:
- клиент contract registry (`data-contracts-service`)
- детерминированное хеширование строк
- engine синхронизации PostgreSQL source/target
- staged hash-diff pipeline `extract_validate_land -> merge_curated`
- S3/MinIO object store client для landing-зоны
- аудит запусков и состояние пайплайна
- unit и integration тесты ядра

Airflow orchestration и Docker runtime вынесены в отдельный репозиторий `ingestion-airflow`.

## Структура пакета

Код разнесен по зонам ответственности:

- `ingestion_core.contracts`:
  модель контракта, runtime-нормализация и валидация строк, client для contract registry
- `ingestion_core.strategies.hash_diff`:
  snapshot/hash-diff engine и pipeline-функции
- `ingestion_core.adapters`:
  интеграции с PostgreSQL, S3/MinIO и OIDC/STS
- `ingestion_core.utils`:
  низкоуровневые утилиты, например детерминированное хеширование


## Установка

```bash
pip install -r requirements.txt
pip install -r requirements-test.txt
```

Editable install:

```bash
pip install -e .
```

Dev tooling:

```bash
make install-dev
```

## Пример contract payload

Поддерживаются ответы registry с эндпоинтов:
- `GET /contracts/{namespace}/{name}/active`
- `GET /contracts/{namespace}/{name}/version/{version}`

Минимальный пример полезной части payload:

```json
{
  "contract": {
    "id": "orders-contract",
    "target_layer": "curated"
  },
  "version": {
    "version": "1",
    "checksum": "sha256:3f6c...",
    "schema_json": {
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "type": "object",
      "properties": {
        "order_id": {"type": "integer"},
        "customer_id": {"type": "integer"},
        "amount": {"type": "number"},
        "status": {"type": "string"},
        "updated_at": {"type": "string", "format": "date-time"}
      },
      "required": ["order_id"],
      "additionalProperties": false,
      "x-primaryKey": ["order_id"],
      "x-businessKey": []
    }
  }
}
```

## Staged Hash-Diff Pipeline

Для orchestration через Airflow в пакете есть stage-функции:
- `extract_validate_land_snapshot`
- `merge_accepted_snapshot_to_curated`

Они используются DAG-ом `ingest_contract_hashdiff` в репозитории `ingestion-airflow` и реализуют полный цикл:

```text
source PostgreSQL
  -> extract + validate against contract
  -> land accepted snapshot to MinIO/S3
  -> load accepted snapshot into short-lived merge staging table
  -> merge into curated PostgreSQL
```

Для повторной загрузки от уже сохраненного `accepted_snapshot` в репозитории
`ingestion-airflow` реализован отдельный DAG `replay_contract_hashdiff_from_minio`.
Replay использует только шаги:

```text
accepted snapshot in MinIO/S3
  -> load accepted snapshot into short-lived merge staging table
  -> merge into curated PostgreSQL
```

## Тесты

Unit:

```bash
tox -e unit
```

Integration:

```bash
export TEST_SOURCE_DSN='postgresql+psycopg2://source_user:source_pass@localhost:5433/source_db'
export TEST_TARGET_DSN='postgresql+psycopg2://target_user:target_pass@localhost:5434/target_db'
tox -e integration
```

## Build And Migrations

Build:

```bash
make build
```
