# ADR 010: OLAP-движок, каталог Iceberg, физическая организация DDS

**Статус:** принято (стек и каталог Iceberg подтверждены spike 1, 2026-11-15/16;
замеры производительности дополняются по мере прогонов —
[журнал загрузок](../load_journal.md)).

## Контекст

ДЗ-2 требует выбрать Greenplum, StarRocks или S3/MinIO + Trino + Iceberg.
Выбранный стек команды — **MinIO (S3) + Trino + Iceberg** (бонус +2):
raw хранится в S3 и читается через Trino, DDS материализован в Iceberg,
контрактные представления читаются через Trino.

Требования к физической организации: загрузчик пишет в таблицы схемы
`dwh_detailed` iceberg-каталога; представления контракта — схема
`contract_dwh` того же каталога; для каждой таблицы DDS указано
партиционирование либо обоснование его отсутствия.

## Рассмотренные альтернативы движка

1. **StarRocks** (allin1, PRIMARY KEY таблицы, встроенный routine load).
   Отвергнут: не даёт бонуса; JOIN-движок и upsert-семантика не критичны
   при наших объёмах.
2. **Greenplum.** Отвергнут: нет надёжного официального docker-образа,
   высокий инфраструктурный риск в WSL2 + docker.exe.
3. **Trino + Iceberg + MinIO.** Выбрано: бонус +2, raw и DDS на одном
   S3-хранилище, открытый формат для ДЗ-3, dbt-trino поддерживает
   incremental merge в Iceberg.

## Каталог Iceberg

Варианты: Hive Metastore / JDBC-каталог (переиспользовать Postgres стенда) /
REST (Nessie).

**Выбран Hive Metastore** (spike 1, 2026-11-15/16). Порядок перебора был
hive-metastore → nessie → jdbc-каталог; по условию спайка первый кандидат,
прошедший приёмку, прекращает перебор. HMS прошёл все 6 обязательных
критериев и опционный dbt-критерий:

1. схемы `dwh_detailed` + `contract_dwh`;
2. типы varchar / integer / decimal(18,2) / boolean / date /
   timestamp(6) with time zone + INSERT — микросекунды (`.123456`) и зона
   (Europe/Moscow → UTC) сохраняются;
3. `CREATE VIEW` в `contract_dwh` + SELECT;
4. оба запроса чекера из `<catalog>.information_schema.views/columns`:
   `data_type` = `timestamp(6) with time zone`, `decimal(18,2)` и т.д.
   (чекер срезает скобки регуляркой и сравнивает с допустимыми типами);
5. персистентность: `docker restart` trino и полное
   `docker compose down && up -d` — view и таблицы живы, данные идентичны
   (метаданные — в postgres-бэкенде HMS, данные — в named volume MinIO);
6. `MERGE INTO` по составному ключу (2 varchar + integer); повтор той же
   порции — no-op (0 rows), дублей нет.

Опционный критерий (dbt): dbt-trino 1.12.0 — view-модель + incremental
merge-модель, PASS=2; повторный `dbt run` — merge без изменения данных;
view виден в `information_schema`.

Nessie и JDBC-каталог не испытывались — перебор прекращён первым работающим
кандидатом; сравнительных замеров между каталогами нет, оба остаются
задокументированными fallback-ами.

Данные физически лежат в S3: `$files` → `s3://lakehouse/dwh_detailed.db/…parquet`.

### Зафиксированные версии (spike 1, все образы запинены)

| Компонент | Версия |
|---|---|
| Trino | `trinodb/trino:484` (каталог `iceberg`, коннектор iceberg + `iceberg.catalog.type=HIVE_METASTORE`) |
| HMS | `apache/hive:4.0.1` (standalone metastore) + jar в `/opt/hive/lib`: postgresql 42.7.14, hadoop-aws 3.3.6, aws-java-sdk-bundle 1.12.367 |
| БД HMS | `postgres:16.16-alpine3.23` |
| MinIO (S3) | `bitnamilegacy/minio:2025.7.23-debian-12-r5` — официальный `minio/minio` удалён из Docker Hub в сентябре 2026 (pull access denied, quay закрыт); план Б — собственный образ из бинарника dl.min.io с версией в ARG |
| dbt | `dbt-trino==1.12.0` (dbt-core 1.12.7), python:3.12-slim |

### Конфигурация каталога Trino

`/etc/trino/catalog/iceberg.properties`:

```properties
connector.name=iceberg
iceberg.catalog.type=HIVE_METASTORE
hive.metastore.uri=thrift://hive-metastore:9083
fs.s3.enabled=true
s3.endpoint=http://minio:9000
s3.path-style-access=true
s3.region=us-east-1
s3.aws-access-key=minioadmin
s3.aws-secret-key=minioadmin
iceberg.file-format=PARQUET
```

HMS: env `SERVICE_NAME=metastore`, `DB_DRIVER=postgres`; warehouse
`s3://lakehouse/` задаётся на стороне HMS.

### Ограничения и грабли (проверено spike-ом, обязательно при переносе)

1. `iceberg.catalog.warehouse` **не существует** для HMS-каталога (ошибка
   старта Trino «property was not used»); warehouse задаётся в HMS.
2. `fs.native-s3.enabled` в Trino 484 переименован в **`fs.s3.enabled`**.
3. В apache/hive:4.0.1 нет JDBC-драйверов, кроме Derby — postgres-jar
   докладываем в `/opt/hive/lib` сами.
4. HMS обязан физически создавать каталог БД на S3 — иначе
   `create_database` падает с пустым IOException. Лечение: hadoop-aws +
   aws-java-sdk-bundle (296 МБ) в `/opt/hive/lib` и warehouse `s3://lakehouse/`.
5. Конфигурация HMS — **только через `hive-site.xml`**: `-D` из
   `SERVICE_OPTS` не долетают до Hadoop Configuration (FileSystem-резолвер и
   s3a-провайдер их не видят; javax.jdo через HiveConf при этом работают).
6. Схему `s3://` алиасим на S3AFileSystem (`fs.s3.impl`), чтобы пути в
   метасторе оставались `s3://…` — их читает Trino напрямую (`fs.s3.enabled`);
   обходимся без двойной схемы s3 (Trino) + s3a (HMS).
7. Entry point apache/hive сам выполняет `schematool -initOrUpgradeSchema`
   (идемпотентно в Hive 4) — отдельный init-контейнер не нужен и дал бы гонку.
8. В dbt-trino для iceberg-каталога **не** указываем `table_format`
   (`WITH (type='iceberg')` — идиома hive-коннектора, в iceberg-каталоге
   свойства нет): таблицы и так Iceberg.
9. bitnami-minio хранит данные в **`/bitnami/minio/data`**, а не `/data`:
   named volume, смонтированный в `/data`, молча не используется — данные
   уходят в анонимный том, теряемый при `compose down` (симптом:
   `Metadata not found in metadata location`; ловится только полным
   down/up-тестом, рестарт Trino не показывает).

## Схемы и таблицы

Каталог `iceberg` (имя зафиксировано spike 1), схемы:

- `raw` — `cdc_events` (append-only): `topic, partition_id, kafka_offset,
  source_system, source_table, event_key, op, payload, ingested_at`;
  координаты `(topic, partition_id, kafka_offset)` уникальны логически,
  дедупликация at-least-once доставки — в представлении `v_cdc_events`;
- `dwh_detailed` — таблицы DDS из [ADR 009](009-dds-model.md);
- `contract_dwh` — SQL-представления контракта;
- `ctl` — служебные: `load_batches` (batch_id, offsets_json, status),
  `kafka_positions` (topic, partition_id, next_offset — позиция обработки,
  продвигается после успешной загрузки).

## Изменение после ревью 24.11.2026

Перед DDS добавлена Iceberg-таблица `dwh_staging.stg_source_state`:
одна строка на физический ключ Kafka внутри topic/partition. Она
партиционирована по `topic` (25 небольших маршрутов), чтобы разделять
сервисы при чтении текущего состояния и MERGE. Raw остаётся append-only,
а таблицы dim/fact и их партиции не меняют модель. Новый код ещё не
прогонялся на Trino; измерения ниже относятся к версии команды.

## Партиционирование и распределение

Эквивалент ключей сегментации в Iceberg — сочетание партиционирования
(spec transforms), свойств записи (`write.distribution-mode`) и sort order.

| Таблица | Партиционирование | Обоснование |
|---|---|---|
| `fact_sale_line` | `month(receipt_dt)` | рост по времени, отсечение партиций в ДЗ-3 и при повторной обработке диапазонов |
| `fact_purchase_line` | `month(ordered_at)` | аналогично |
| `fact_movement` | `month(moved_at)` | аналогично |
| `dim_sku`, `dim_customer`, `dim_pharmacy`, `dim_batch`, `dim_supplier`, `dim_distribution_center` | без партиций | тысячи строк, перезапись merge целиком дешевле управления партициями |
| `dim_sku_version`, `dim_price` | без партиций (TODO: пересмотреть при >10⁶ версий) | объём ≈ десятки тысяч строк; сортировка по ключу версии |
| `raw.cdc_events` | `day(ingested_at)` | изоляция порций, повторная обработка диапазонов и ретеншн |

TODO (после первых прогонов): зафиксировать `write.distribution-mode`
(hash по ключу факта) и sort order для фактов; измерить время начальной
загрузки и дельты в [журнале](../load_journal.md).

Физическую раскладку поддерживает обслуживание Iceberg
(`scripts/hw2/maintain_iceberg.py`, ADR-011): MERGE-загрузки оставляют
мелкие файлы и positional-delete файлы (половина записей в файлах фактов —
«мёртвые» под delete-масками); `optimize` переписывает партиции до ~1
файла на партицию без delete-файлов, `expire_snapshots` +
`remove_orphan_files` снимают снапшоты no-op MERGE и потерянные файлы
(замер 17.11: 427+57 файлов → 49, 13.0 → 8.8 МБ, replay начальной порции
59.5 → 41.5 с).

## Цена выбора

- Больше сервисов в стенде (MinIO, HMS + его postgres, Trino) — выше сложность
  `docker compose up -d`; компенсируется одной точкой оркестрации
  `dwh-init`; рабочий режим по docker stats (spike 1): trino 1.18 GiB,
  hive-metastore 0.55 GiB, postgres 70 MiB, minio 70 MiB — итого ≈1.85 GiB,
  укладывается в WSL2-бюджет параллельно со стендом ДЗ-1;
- формат timestamp в Iceberg — timestamptz в UTC; точность выбираем
  `timestamp(6)`, чтобы сохранить время источника без потерь;
- MERGE INTO в Trino/Iceberg дороже, чем upsert в StarRocks, — на наших
  объёмах приемлемо (подтвердить journal-замерами).
