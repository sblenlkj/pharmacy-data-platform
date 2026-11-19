# dwh/ — dbt-проект DDS ДЗ-2

Слои (ADR-009/010/011, dwh_mapping.md §9–10):

```
raw.cdc_events (DDL, не dbt)      ctl.load_batches · kafka_positions · receipt_pharmacy
        │                                      ▲ пишет                 │
        ▼                                      │                       │ pharmacy_bk
staging (view): stg_cdc_events                │                       ▼
        │        — дедуп доставки at-least-once│             stg_pos_receipt (join)
        ▼                                      │                       │
staging (view): stg_<svc>_<table>              │                       │
        │        — 23 обёртки топиков: разбор payload, окно порции    │
        │          (batch_filter); справочники-спутники — без окна     │
        ▼          (filtered=false)            │                       │
dds (incremental, merge)                       │                       │
        │        — 8 dim + 3 fact, схема dwh_detailed, партиции        │
        │          фактов month(...), ключи merge — ADR-011            │
        ▼          ────────────────────────────────────────────────────┘
contract (view): 13 представлений contract_dwh (hw02_contract.yaml);
        v_cdc_events читает raw.cdc_events, v_load_batches — ctl.load_batches
```

## Загрузка: `scripts/load_dds`

Весь рабочий путь raw → DDS идёт загрузчиком порций:

- диапазоны `[kafka_positions, max(raw))` → `--vars {load_batch_id,
 batch_ranges}` → staging фильтруется окном, dds merge-ится по
 естественным ключам (`unique_key` в config моделей; `_load_batch_id`
 при merge не перезаписывается — `merge_exclude_columns`);
- выбор моделей — только затронутые топики (`dbt run --select stg_x+ …`);
- первая порция (нет успешных записей в `ctl.load_batches`) идёт с
 `--full-refresh` — таблицы строятся с нуля по всему окну;
- `--batch-id ID --replay` применяет сохранённые диапазоны без
 продвижения позиций и перезаписи строки порции;
- `ctl.receipt_pharmacy` (обогащение `pharmacy_bk`, см. ниже)
 пересинхронизируется из `pos.contract.v_receipt`, когда затронут
 `pos.public.receipt` или справочник пуст.

Запуск — в контейнере `tools` (dbt-trino в образе, in-process через
dbtRunner; target/log в /tmp — репозиторий смонтирован read-only):

```bash
docker compose run --rm -T --no-deps tools python scripts/load_dds
docker compose run --rm -T --no-deps tools python scripts/load_dds --batch-id ID --replay
```

Начальная загрузка при старте стенда — сервис compose `dwh-init`
(маркер `INITIAL DDS LOAD DONE`, затем обслуживание таблиц Iceberg и
маркер `ICEBERG MAINTENANCE DONE` — см. корневой README и журнал загрузки (docs/load_journal.md).

## Ручной запуск dbt (отладка; не рабочий путь)

profiles.yml — env (`TRINO_HOST/PORT/USER/CATALOG`, target по умолчанию
`main` — основной стенд; `DBT_TARGET=spike1` — стенд спайка). Без vars
порции (`batch_ranges`) staging не фильтруется окном — merge по полному
состоянию маршрутов:

```bash
docker compose run --rm -T --no-deps tools bash -c \
 "dbt run --project-dir dwh --profiles-dir dwh \
 --target-path /tmp/dwh-dbt-target --log-path /tmp/dwh-dbt-logs"
```

## Самопроверка

Официальный чекер (статика + сценарий) —
`hws_descriptions/hw2/hw02_check.py`, команды — в корневом README.

## Особенности реализации

- **Инкрементальность** — merge по естественным ключам (dbt-trino:
 `MERGE INTO`, Iceberg). Повтор той же порции = 0 изменений (счётчик
 вставок/обновлений; no-op MERGE всё равно создаёт снапшот Iceberg —
 обслуживается scripts/hw2/maintain_iceberg.py при старте стенда, см. ADR-011.
- **Составные факты** — заголовок join-ится в staging внутри окна порции
 (receipt и receipt_line / purchase_order и purchase_line пишутся одной
 транзакцией, FK parent-first → заголовок всегда в окне). Атрибуты,
 которые старше окна (sku движения, received_at закупки), — fallback на
 уже загруженный `dim_batch` (DAG dbt гарантирует порядок).
- **`pharmacy_bk` чека** — в источнике аптека
 достижима только через `receipt → shift → cash_register → pharmacy`,
 исключённые из репликации таблицы (dwh_mapping.md §9). Решение —
 справочник `ctl.receipt_pharmacy`: `scripts/load_dds` читает
 `pos.contract.v_receipt` (та же формула join, что у эталона сверки
 чекера) и полностью пересинхронизирует справочник; `stg_pos_receipt`
 join-ит его без окна. Варианты «включить топики в репликацию» и
 «обогатить envelope в ingest» отвергнуты: меняют конфигурацию CDC и
 ingest-сервиса.
- **Payload Debezium** (подтверждено реальным снапшотом): timestamptz —
 ISO-8601 строка с µs; **date — дни от epoch числом** (макрос
 `after_date` конвертирует оба вида); numeric — строка
 (`decimal.handling.mode=string`); boolean — JSON-boolean.
- **Партиционирование** — через `properties={'partitioning': …}`:
 dbt-trino 1.12 не поддерживает `partition_by`. Sort order /
 `write.distribution-mode` — TODO после первых прогонов.
- Ручной `dbt run` без порций записывает `_load_batch_id='manual'`
 (var по умолчанию) — такие строки видны отбором по `_load_batch_id`.
