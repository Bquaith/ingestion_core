# ingestion-core

Переиспользуемое Python-ядро для инкрементальной загрузки данных.

Репозиторий содержит:
- клиент contract registry (`data-contracts-service`)
- детерминированное хеширование строк
- engine синхронизации PostgreSQL source/target
- аудит запусков и состояние пайплайна
- unit и integration тесты ядра

Airflow orchestration и Docker runtime вынесены в отдельный репозиторий `ingestion-airflow`.


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
      "fields": [
        {"name": "order_id", "type": "bigint"},
        {"name": "customer_id", "type": "bigint"},
        {"name": "amount", "type": "decimal"},
        {"name": "status", "type": "string"},
        {"name": "updated_at", "type": "timestamp"}
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

## Пример использования

```python
from ingestion_core.contracts_client import ContractRegistryClient
from ingestion_core.hash_diff import ContractDefinition, run_hash_diff

client = ContractRegistryClient("http://contracts.local")
payload = client.fetch_contract(namespace="sales", name="orders")
contract = ContractDefinition.from_registry_payload(payload.to_dict())

result = run_hash_diff(
    source_dsn="postgresql+psycopg2://source_user:source_pass@localhost:5433/source_db",
    source_table="public.orders",
    target_dsn="postgresql+psycopg2://target_user:target_pass@localhost:5434/target_db",
    target_table_curated="curated.orders",
    contract=contract,
)

print(result)
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

