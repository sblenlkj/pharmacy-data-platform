# Журнал загрузки DDS

Движок и версия: Trino `trinodb/trino:484` + Iceberg (HMS) + MinIO. Модель:
dimensional (SCD2), ADR 009. Код загрузки: dbt-trino 1.12.0 (dbt-core
1.12.7) поверх `raw.cdc_events`, порции — `scripts/load_dds` (ADR 011).
Исходный seed: `--seed 42` (как в ДЗ-1).

| Прогон | batch_id | Диапазоны Kafka offsets | Входных событий | Вставлено / обновлено в DDS | Время |
|---|---|---|---:|---|---:|
| Начальная загрузка — этап raw | — | все 23 топика, offset 0…end (200 233) | 200233 | raw: 200 233 (см. ниже) | 321 с; приёмочный чистый прогон 17.11: 211 с |
| Начальная загрузка — этап DDS | `87acfa94-943c-442c-b152-af7351f295d7` (приёмочный чистый прогон 17.11; первый прогон 16.11 — `54245b1b-aed6-4a98-b625-b170ef035f08`) | все 23 топика, offset 0…end (200 233) | 200233 | 169 511 вставлено (таблицы построены с нуля, merge) | 109.0 с (16.11: 51.0 с) |
| Малая дельта | `task3-delta-1` | 19 топиков, [end₀…end₁) | 12063 | 9 752 новых ключа, 0 обновлений существующих | 45.4 с |
| Повтор той же дельты | `task3-delta-1` (--replay) | те же 19 диапазонов (из ctl.load_batches) | 12063 | 0 изменений (контрольные суммы идентичны) | 32.0 с |
| Повтор начальной порции | `54245b1b…` (--replay) | те же 23 диапазона | 200233 | 0 изменений (контрольные суммы идентичны) | 39.4 с |

## Начальная загрузка

Этап raw (доставка Kafka → `raw.cdc_events`, сервис `cdc-ingest`): снапшот
200 233 события (23 топика) загружен за **320.6 с** (41 батч по 5000 строк,
INSERT-чанки по 700 КБ текста, средняя ~625 строк/с). Маркер в логе:
`CDC RAW CAUGHT UP: total_rows=200233 … lag=0`. Сверка с источниками —
`scripts/hw2/check_raw_snapshot.py` (cdc.*.snapshot, полнота по топикам,
ключевые таблицы, дедуп): 5/5 pass. Рестарты сервиса потерь и дублей в
`v_cdc_events` не дают (SIGKILL на 85k → перепоставка ≤1 батча, снята
дедупом; SIGTERM — чистый resume за 2 c).

Этап DDS (16.11.2026, стенд hw2; тот же вход — `scripts/load_dds` —
использует и сервис dwh-init при старте стенда):

Команда:

```text
docker compose run --rm -T --no-deps tools python scripts/load_dds
```

Сырой вывод / запрос к журналу:

```text
load_dds: порция 54245b1b-aed6-4a98-b625-b170ef035f08: 23 диапазонов,
  200233 событий, затронуто топиков: 23, первая загрузка (--full-refresh)
load_dds: ctl.receipt_pharmacy synced: 21525 receipts
load_dds: dbt run --select 'stg_catalog_*+ stg_crm_*+ stg_pos_*+ stg_wms_*+
  v_load_batches v_cdc_events' --full-refresh
  (batch_id=54245b1b…, 23 диапазонов)
21:08:17  OK created sql incremental model dwh_detailed.dim_sku_version ... [CREATE TABLE (687 rows) in 2.00s]
21:08:17  OK created sql incremental model dwh_detailed.dim_price ......... [CREATE TABLE (1_650 rows) in 2.40s]
21:08:17  OK created sql incremental model dwh_detailed.dim_sku ............ [CREATE TABLE (550 rows) in 2.61s]
21:08:18  OK created sql incremental model dwh_detailed.dim_customer ....... [CREATE TABLE (2_200 rows) in 2.68s]
21:08:18  OK created sql incremental model dwh_detailed.dim_pharmacy ....... [CREATE TABLE (36 rows) in 1.28s]
21:08:19  OK created sql incremental model dwh_detailed.dim_batch .......... [CREATE TABLE (3_300 rows) in 1.67s]
21:08:20  OK created sql incremental model dwh_detailed.dim_supplier ....... [CREATE TABLE (15 rows) in 1.74s]
21:08:20  OK created sql incremental model dwh_detailed.dim_distribution_center [CREATE TABLE (4 rows) in 2.44s]
21:08:25  OK created sql incremental model dwh_detailed.fact_purchase_line . [CREATE TABLE (5_400 rows) in 4.45s]
21:08:25  OK created sql incremental model dwh_detailed.fact_sale_line ..... [CREATE TABLE (63_525 rows) in 8.43s]
21:08:28  OK created sql incremental model dwh_detailed.fact_movement ..... [CREATE TABLE (92_144 rows) in 7.92s]
21:08:28  Done. PASS=47 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=47
load_dds: порция 54245b1b… success: kafka_positions продвинуты; впервые
  записано строк: 169511
load_dds: готово за 51.0 с
```

Вывод: вход — весь снапшот (200 233 события, 23 топика, окно порции с
offset 0); затронуты все ветки DAG (47 моделей: 23 staging-вью + 11
dim/fact + 13 представлений контракта); dim/fact построены merge-стратегией
с нуля (`--full-refresh`: до успешных порций нет — таблицы пересоздаются
по всему окну). Вставлено 169 511 строк: fact_sale_line 63 525,
fact_movement 92 144, fact_purchase_line 5 400, dim_customer 2 200
(все, включая soft-deleted — скрываются представлением), dim_batch 3 300,
dim_price 1 650, dim_sku_version 687, dim_sku 550, dim_pharmacy 36,
dim_supplier 15, dim_distribution_center 4. Позиции `ctl.kafka_positions`
продвинуты до концов диапазонов; строка порции в `v_load_batches` —
status=success. Длительность 51.0 с (включая DDL ctl, синхронизацию
справочника receipt→pharmacy, 21 525 строк, и холодный запуск dbt).

### Приёмочный чистый прогон (17.11.2026)

`docker compose down -v` → `up -d --build` на вытертых томах одним
запуском, без ручных команд: снапшот 200 233 события успевает внутри окна
`up -d` (маркер CDC SNAPSHOT DONE в 11:04, `up` вернулся в 11:04:08),
raw догоняет за 211.1 с (41 батч), порция DDS
`87acfa94-943c-442c-b152-af7351f295d7` — те же 169 511 строк за 109.0 с
(PASS=48: к 47 моделям добавился stg_cdc_events — с 17.11 он включён в
выборку каждой порции, чтобы прогон был самодостаточен на пустом
каталоге). Итоговый признак готовности `INITIAL DDS LOAD DONE` — через
**8м02с** после запуска `up -d` (включая сборку образов). Проверки
сетевого сида одинаковы детерминированно: события и строки совпадают с
прогоном 16.11 один в один, отличаются только тайминги. Чекер на этом
стенде: static 6/13, сценарий 13/13 (4м10с).

## Малая дельта

Строка «Малая дельта — 12 063 события» — это **не** fixture сценария чекера
(тот пишет 11 событий, см. ниже отдельные строки «Малая дельта — сценарий
чекера»). 12 063 события — результат отдельного эксперимента:
дельта-доводка штатным генератором ДЗ-1 в `--delta`-режиме со свежим
namespace поверх базового сида 42 (`scripts/infra/seed_delta.py`).
Генератор создал новую порцию данных целиком (300 клиентов, 120 SKU,
2723 строки продаж, 4459 движений и т.д.) — каждое insert/update этой
порции и стало CDC-событием. «Хвоста» событий начальной загрузки в дельте
нет: базовый сид завершается до регистрации коннекторов, поэтому весь
сид попадает в снапшот (200 233 события, op="r"), а потоковые события
после снапшота — только те, что сгенерированы явно.

Команда:

```text
# тестовая догенерация (генератор ДЗ-1, delta-режим, свежий namespace):
docker compose run --rm -T --no-deps tools python scripts/infra/seed_delta.py
# → 12 063 события CDC (19 топиков) доставлены в raw (маркер CDC RAW CAUGHT UP,
#   total_rows=212296)

time docker compose run --rm -T --no-deps tools \
  python scripts/load_dds --batch-id task3-delta-1
```

Границы входа (offsets по topic/partition) и затронутые ключи/партиции DDS:

```text
load_dds: порция task3-delta-1: 19 диапазонов, 12063 событий,
  затронуто топиков: 19
load_dds: dbt run --select 'stg_catalog_price_version+ stg_catalog_sku+
  stg_catalog_sku_version+ stg_crm_customer+ stg_crm_customer_profile_version+
  stg_pos_pharmacy+ stg_pos_receipt+ stg_pos_receipt_line+ stg_wms_*+
  v_load_batches v_cdc_events'   # инкрементально, без --full-refresh
21:26:56  Done. PASS=43 WARN=0 ERROR=0 SKIP=0 TOTAL=43
load_dds: порция task3-delta-1 success: kafka_positions продвинуты;
  впервые записано строк: 9752
  dim_sku: 120, dim_sku_version: 150, dim_price: 360, dim_customer: 300,
  dim_pharmacy: 4, dim_batch: 720, dim_supplier: 12, dim_distribution_center: 4,
  fact_sale_line: 2723, fact_purchase_line: 900, fact_movement: 4459
real	0m47.208s
```

Вывод: обработаны только 19 затронутых топиков и их ветки DAG (43 модели
вместо 47 — 4 справочных маршрута без событий не выбираются); staging
отфильтрован окном порции, merge дописал 9 752 новых ключа (дельта-namespace
генератора), существующие строки не тронуты. Полное время команды 45.4 с —
в бюджете таймаута чекера (90 с на команду `load`).

## Повтор той же дельты

Команда:

```text
docker compose run --rm -T --no-deps tools \
  python scripts/load_dds --batch-id task3-delta-1 --replay
```

Тот же `batch_id`, те же границы; сравнение бизнес-строк и версий до/после:

```text
load_dds: replay порции task3-delta-1 (status=success, 19 активных
  диапазонов); позиции не двигаются
21:28:12  Done. PASS=43 WARN=0 ERROR=0 SKIP=0 TOTAL=43
load_dds: replay … success: данные воспроизведены (строк с
  _load_batch_id=task3-de…: 9752, записей порций: 2); позиции на месте
load_dds: готово за 32.0 с

# контрольные суммы (scripts/hw2/warehouse_checksum.py, sha256 по всем
# 11 представлениям данных + распределение _load_batch_id) — diff до/после:
[ok] Files are identical
```

Вывод: вставок/обновлений — 0 (контрольные суммы 11 представлений
идентичны до/после, строки остались размечены исходной порцией),
границы порции не изменились (`ctl.load_batches` — по-прежнему 2 строки,
offsets_json не перезаписан), `kafka_positions` не двигались. Аналогично
повтор начальной порции (39.4 с) — суммы идентичны.

## Обслуживание Iceberg (expire/optimize/orphans, ADR-011)

`scripts/hw2/maintain_iceberg.py` по 11 таблицам dwh_detailed + raw.cdc_events;
запускается из dwh-init после `INITIAL DDS LOAD DONE` (маркер
`ICEBERG MAINTENANCE DONE`) — вне тайм-пути команд load чекера. Замеры
17.11 на приёмочном стенде:

| Прогон | Снапшоты | Файлы (данные+delete) | Размер | Время |
|---|---|---|---|---:|
| Штатный retention 7d | 358 → 370 | 427+57 → 49+0 | 13.0 → 8.8 МБ | 12.0 с |
| Демонстрация --retention 0d | 370 → 12 | — | −68.4 МБ в S3 (orphan-файлы raw) | 8.4+5.6 с |
| Повтор штатного (идемпотентность) | 12 → 12 | 49 → 49 | без изменений | 4.8 с |

Влияние на тайминги загрузок (тот же стенд, replay начальной порции
87acfa94…, 200 233 события, до/после обслуживания): **59.5 с → 41.5 с** —
не деградация, а ускорение (raw сжат с 316 файлов до 1, delete-файлы
MERGE устранены); контрольные суммы 11 представлений данных контракта
идентичны байт-в-байт до/после каждого прогона обслуживания.

## Сценарий чекера ДЗ-2 (`--scenario`)

Полный прогон `hw02_check.py --scenario` 17.11.2026: **13/13
автоматических баллов** (dds.\*, cdc.\*.snapshot, cdc.\*.delta,
incremental.delta/replay/batch_log, history.versions/privacy — все pass). Повторен на приёмочном чистом прогоне
того же дня: 13/13 за 4м10с на стенде, поднятом
с нуля одним `up -d`. Тестовые данные пишет
`scripts/check_fixture` (контракт commands.fixture), загрузки порций —
тем же `scripts/load_dds` по команде чекера.

| Прогон | batch_id | Диапазоны Kafka offsets | Входных событий | Вставлено / обновлено в DDS | Время |
|---|---|---|---:|---|---:|
| Малая дельта — сценарий чекера | `7435f5cd1a0a4adbb85db725c6f74d40-delta` (приёмочный прогон 17.11; первый — `a7402efbb23b4f99ba237871f2e6d007-delta`, batch_id генерируется чекером заново на каждый прогон) | 11 топиков, [end₀…end₁) | 11 | 8 новых ключей (по 1 в dim_sku, dim_sku_version, dim_price, dim_customer, dim_batch, fact_sale_line, fact_purchase_line, fact_movement), 0 обновлений существующих | загрузка командой чекера (в бюджете 90 с; полный сценарий 13/13 за 4м10с) |
| Повтор той же дельты — сценарий чекера | `…-delta` (--replay) | те же 11 диапазонов (из ctl.load_batches) | 11 | 0 изменений (инварианты чекера incremental.replay/batch_log — pass; контрольные суммы идентичны) | командой чекера |
| History-порция сценария | `…-history` | 2 топика | 4 | dim_sku_version +1, dim_price +1 (старые версии закрыты событиями u) | командой чекера |
| Privacy-порция сценария | `…-privacy` | 1 топик | 1 | 0 вставок; dim_customer обновлён (deleted_at) — клиент скрыт представлениями | командой чекера |

Выбор моделей (`--select`) по затронутым топикам окна — доказательство
incremental.scope (заголовок «dbt run --select» из вывода load_dds;
строки «N of M OK» — только ветки затронутых маршрутов):

```text
# дельта (11 топиков → 29 моделей из 47):
load_dds: dbt run --select 'stg_catalog_price_version+ stg_catalog_sku+
  stg_catalog_sku_version+ stg_crm_customer+ stg_crm_customer_profile_version+
  stg_pos_receipt+ stg_pos_receipt_line+ stg_wms_batch+ stg_wms_movement+
  stg_wms_purchase_line+ stg_wms_purchase_order+ v_load_batches v_cdc_events'
Done. PASS=29 WARN=0 ERROR=0 SKIP=0 TOTAL=29

# history (2 топика → 9 моделей):
load_dds: dbt run --select 'stg_catalog_price_version+ stg_catalog_sku_version+
  v_load_batches v_cdc_events'
Done. PASS=9  TOTAL=9

# privacy (1 топик → 7 моделей):
load_dds: dbt run --select 'stg_crm_customer+ v_load_batches v_cdc_events'
Done. PASS=7  TOTAL=7
```

Партиции Iceberg не пересобираются: факты партиционированы
month(receipt_dt/ordered_at/moved_at) (ADR-010), merge пишет только
затронутые строки — новые дельта-строки ложатся в парции 2026-09/2026-10,
существующие порции не читаются и не перезаписываются.

Замечание о порядке повторов (наблюдение, не дефект): повтор порции
воспроизводит её окно, поэтому внеочередной `--replay` старой порции
поверх более новых состояний честно откатывает SCD1-атрибуты к состоянию
окна (проверено: replay дельта-порции ПОСЛЕ history/privacy вернул
клиенту deleted_at IS NULL и открыл старую цену; повтор history- и
privacy-порций по порядку восстановил состояние байт-в-байт — контрольные суммы идентичны). Чекер повторяет порцию сразу после её
загрузки (incremental.replay — pass), семантика ADR-011 «порции
последовательны» соблюдена.

Повтор полного сценария после внедрения обслуживания Iceberg
(2026-11-17, предсдачная валидация; стенд после прогонов обслуживания и
replay): **13/13 за 3м43с** — быстрее
приёмочных 4м10с; тайминги команд load в бюджете таймаута 90 с,
деградации нет.
