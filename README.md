# ingestion-platform

Airflow-ориентированная платформа инкрементальной загрузки данных

Реализовано:
- оркестрация через DAG `ingest_contract_hashdiff`
- интеграция с `data-contracts-service` (registry)
- retry/backoff при временных ошибках registry и валидация payload контракта
- вычисление `row_hash` (SHA-256) по `keys.hash_keys` или всем полям контракта
- batched обработка изменений и batched UPSERT в curated-таблицу без физического удаления строк
- аудит запусков в `ingestion_meta.pipeline_state` и `ingestion_meta.run_audit`
- DB lock пайплайна + checkpoint в `ingestion_meta.pipeline_checkpoint`

- любое удаление данных при отсутствии строки в source

## Структура

```text
ingestion-platform/
  dags/
    ingest_contract_hashdiff.py
  ingestion_platform/
    __init__.py
    config.py
    contracts_client.py
    hashing.py
    postgres.py
    hash_diff.py
    audit.py
  docker/
    Dockerfile.airflow
    requirements.txt
    docker-compose.yml
    initdb/
      source.sql
  tests/
    test_hashing.py
    test_hashdiff_unit.py
    test_integration_hashdiff.py
  README.md
```

## 1. Запуск Docker Compose

```bash
cd ingestion-platform/docker
docker compose up --build -d
```

Сервисы:
- `postgres_source` (порт `5433`)
- `postgres_target` (порт `5434`)
- `airflow-init`
- `airflow-webserver` (UI: `http://localhost:8088`, логин/пароль: `airflow/airflow`)
- `airflow-scheduler`

Переменная для registry:
- `CONTRACTS_SERVICE_URL` (по умолчанию `http://host.docker.internal:8081`)

Пример запуска с явным registry:

```bash
CONTRACTS_SERVICE_URL=http://host.docker.internal:8081 docker compose up --build -d
```

## 2. Пример контракта для registry

Ниже пример JSON версии контракта, который должен быть доступен через:
- `GET /contracts/{namespace}/{name}/active`
- `GET /contracts/{namespace}/{name}/version/{version}`

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
      "fields": [
        {"name": "order_id"},
        {"name": "customer_id"},
        {"name": "amount"},
        {"name": "status"},
        {"name": "updated_at"}
      ],
      "keys": {
        "primary": ["order_id"],
        "business": [],
        "hash_keys": ["customer_id", "amount", "status", "updated_at"]
      }
    }
  }
}
```

Если `hash_keys` пустой, используются все поля `fields`.
Если `primary` пустой, используются `business`.
Если оба пусты, пайплайн завершается ошибкой.

## 3. Запуск DAG

Вариант через Airflow REST API:

```bash
curl -u airflow:airflow -X POST "http://localhost:8088/api/v1/dags/ingest_contract_hashdiff/dagRuns" \
  -H "Content-Type: application/json" \
  -d '{
    "dag_run_id": "manual-orders-1",
    "conf": {
      "contracts_service_url": "http://host.docker.internal:8081",
      "namespace": "sales",
      "name": "orders",
      "contract_version": "1",
      "source_dsn": "postgresql+psycopg2://source_user:source_pass@postgres_source:5432/source_db",
      "source_table": "public.orders",
      "target_dsn": "postgresql+psycopg2://target_user:target_pass@postgres_target:5432/target_db",
      "target_table_curated": "curated.orders",
      "source_batch_size": 1000,
      "upsert_batch_size": 1000
    }
  }'
```

`contract_version` можно не передавать, тогда используется `active` версия.
`source_batch_size` и `upsert_batch_size` опциональны (по умолчанию `1000`).

## 4. Ожидаемый результат синхронизации

Первый запуск:
- все строки из source попадают в `INSERT`
- `update_count = 0`, `unchanged_count = 0`

Второй запуск (после изменения части source-строк):
- новые ключи -> `INSERT`
- существующие ключи с другим хэшем -> `UPDATE`
- строки с тем же хэшем -> `UNCHANGED`
- отсутствующие в source строки не удаляются из target

Аудит:
- `ingestion_meta.run_audit` содержит детальные метрики запуска
- `ingestion_meta.pipeline_state` содержит последний статус пайплайна
- `ingestion_meta.pipeline_checkpoint` хранит последнюю успешную контрольную точку

## 5. Тесты

Unit-тесты:

```bash
cd ingestion-platform
pytest tests/test_hashing.py tests/test_hashdiff_unit.py
```

Integration-тест:

```bash
cd ingestion-platform
export TEST_SOURCE_DSN='postgresql+psycopg2://source_user:source_pass@localhost:5433/source_db'
export TEST_TARGET_DSN='postgresql+psycopg2://target_user:target_pass@localhost:5434/target_db'
pytest tests/test_integration_hashdiff.py -m integration
```

