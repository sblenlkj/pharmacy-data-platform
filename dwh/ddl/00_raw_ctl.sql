-- Инфраструктурный слой raw/ctl каталога iceberg. Применяется
-- автоматически: cdc-ingest при старте и scripts/load_dds перед порцией
-- (идемпотентно, IF NOT EXISTS); dbt этими таблицами не владеет —
-- raw.cdc_events пишет ingest-сервис, ctl.* — scripts/load_dds
-- (ADR-010, dwh_mapping.md §9–10).

CREATE SCHEMA IF NOT EXISTS iceberg.raw;

-- Append-only журнал доставки at-least-once: физические дубли координат
-- (topic, partition_id, kafka_offset) допускаются, снимаются представлением
-- v_cdc_events (row_number) — контракт ДЗ-2. Партиция day(ingested_at).
CREATE TABLE IF NOT EXISTS iceberg.raw.cdc_events (
    topic          varchar,                      -- <сервис>.<schema>.<таблица>
    partition_id   integer,
    kafka_offset   integer,
    source_system  varchar,                      -- crm | catalog | pos | wms
    source_table   varchar,                      -- schema.table источника
    event_key      varchar,                      -- JSON-ключ Kafka (PK таблицы)
    op             varchar,                      -- r | c | u | d
    payload        varchar,                      -- JSON envelope: before/after/op
    ingested_at    timestamp(6) with time zone
)
WITH (partitioning = ARRAY['day(ingested_at)']);

CREATE SCHEMA IF NOT EXISTS iceberg.ctl;

-- Журнал порций загрузчика; одна строка на batch_id.
CREATE TABLE IF NOT EXISTS iceberg.ctl.load_batches (
    batch_id     varchar,
    offsets_json varchar,                        -- [{"topic","partition","start_offset","end_offset"}]
    status       varchar                         -- success | failed
);

-- Позиция обработки; продвигается только после успешной загрузки порции.
CREATE TABLE IF NOT EXISTS iceberg.ctl.kafka_positions (
    topic         varchar,
    partition_id  integer,
    next_offset   integer
);

-- Справочник обогащения заголовка чека: pharmacy_bk достижим в источнике
-- только через receipt → shift → cash_register → pharmacy, а
-- shift/cash_register исключены из репликации (dwh_mapping.md §9).
-- Полностью пересинхронизируется scripts/load_dds из pos.contract.v_receipt
-- (когда затронут топик pos.public.receipt или справочник пуст); staging
-- stg_pos_receipt join-ит его без окна порции.
CREATE TABLE IF NOT EXISTS iceberg.ctl.receipt_pharmacy (
    receipt_bk   varchar,
    pharmacy_bk  varchar
);
