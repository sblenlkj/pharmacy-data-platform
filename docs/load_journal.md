> **Исторический журнал версии команды, до исправлений 24.11.2026.**
> Здесь 23 топика и прежняя логика загрузчика; новая версия имеет 25 топиков.
> Эти замеры и отчёт 13/13 не подтверждают исправленный код.
> Для новой версии выполнены только offline-регрессии и dbt parse.
> После запуска на Docker Desktop добавьте новый раздел с сырыми логами
> и отчётом hw02-scenario-reviewed.json, не перезаписывая старые измерения.

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

Дефект прежней реализации, исправленный после ревью 24.11.2026: повтор порции
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




















## Проверка исправленной версии после ревью — 26.11.2026

### Цель и условия проверки

Проверена версия после исправлений ревью: восстановление аптеки чека
через CDC таблиц POS `shift` и `cash_register`, материализация
обработанного состояния источников, пересчёт DDS по изменениям зависимостей
и защита от отката более свежих данных при позднем replay.

Среда: Windows, Docker Desktop, PowerShell.
Compose project: `pharmacy-hw2-review`.

Использован отдельный проект с новыми томами. Инициализация источников,
начальный seed, настройка CDC и первая загрузка DDS выполнялись
автоматически. В CDC включены 25 физических таблиц четырёх источников.

Предыдущие разделы журнала описывают версию команды до ревью.
Приведённые ниже результаты относятся к исправленной версии.

### Команды запуска и проверки

```powershell
docker compose -p pharmacy-hw2-review up -d --build

docker compose -p pharmacy-hw2-review logs -f --tail=200 dwh-init

docker compose -p pharmacy-hw2-review ps -a

docker compose -p pharmacy-hw2-review run --rm -T --no-deps tools python hws_descriptions/hw2/hw02_check.py --submission ./hw02_submission.container.yaml --json /reports/hw02-static-reviewed.json

docker compose -p pharmacy-hw2-review run --rm -T --no-deps tools python hws_descriptions/hw2/hw02_check.py --submission ./hw02_submission.container.yaml --scenario --json /reports/hw02-scenario-reviewed.json

docker compose -p pharmacy-hw2-review run --rm -T --no-deps tools python scripts/hw2/check_late_replay.py

docker compose -p pharmacy-hw2-review run --rm -T --no-deps tools dbt test --project-dir dwh --profiles-dir dwh --target-path /tmp/dwh-dbt-target --log-path /tmp/dwh-dbt-logs

docker compose -p pharmacy-hw2-review exec trino trino --execute "SELECT batch_id, status, offsets_json FROM iceberg.contract_dwh.v_load_batches"

docker compose -p pharmacy-hw2-review exec trino trino --execute "SELECT batch_id, count(*) AS n FROM iceberg.contract_dwh.v_load_batches GROUP BY batch_id HAVING count(*) > 1"

docker compose -p pharmacy-hw2-review logs --no-color db-init cdc-init cdc-ingest dwh-init | Out-File -Encoding utf8 .\reports\hw02-startup-reviewed.log
```

### Начальная доставка и загрузка DDS

Начальный snapshot содержит 200 305 событий в 25 топиках:

| Источник | Snapshot-событий в raw |
|---|---:|
| CRM | 8 952 |
| Каталог | 3 532 |
| POS | 85 158 |
| WMS | 102 663 |
| **Всего** | **200 305** |

Границы первой порции: по каждому из 25 топиков от offset 0
до сохранённого `end_offset`, правая граница не включается.
Полный набор диапазонов сохранён в `v_load_batches.offsets_json`.

Идентификатор первой порции:

`baf995bb-8d33-49e4-a28a-16e254cf5606`

Доставка raw достигла `lag=0` через 244,6 с от запуска ingest-процесса.
Это время этапа доставки, а не полного запуска стенда.

Выдержки из сырого вывода:

```text
cdc-init-1 | CDC SNAPSHOT DONE: 25 topics, 200305 records total

cdc-ingest-1 | CDC RAW CAUGHT UP: total_rows=200305 batches=41 skipped=0 elapsed=244.6s lag=0

dwh-init-1 | Finished running 12 incremental models, 39 view models in 0 hours 1 minutes and 53.32 seconds (113.32s).

dwh-init-1 | Done. PASS=51 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=51

dwh-init-1 | load_dds: порция baf995bb-8d33-49e4-a28a-16e254cf5606 success; строк, впервые записанных этой порцией: 169511; позиции сохранены

dwh-init-1 | load_dds: готово за 128.3 с (baf995bb-8d33-49e4-a28a-16e254cf5606)
```

Первая загрузка создала 169 511 строк в 11 таблицах DDS:

| Таблица | Строк |
|---|---:|
| fact_sale_line | 63 525 |
| fact_purchase_line | 5 400 |
| fact_movement | 92 144 |
| dim_customer | 2 200 |
| dim_batch | 3 300 |
| dim_price | 1 650 |
| dim_sku_version | 687 |
| dim_sku | 550 |
| dim_pharmacy | 36 |
| dim_supplier | 15 |
| dim_distribution_center | 4 |
| **Всего** | **169 511** |

Дополнительно материализована таблица обработанного состояния
`dwh_staging.stg_source_state`, содержащая после snapshot 200 305 строк.
Она является техническим слоем и не включена в число строк DDS выше.

Полная команда загрузчика заняла 128,3 с, включая dbt-прогон
длительностью 113,32 с. После неё автоматически выполнены 24 dbt-теста
и обслуживание Iceberg.

Инициализация завершилась маркерами:

```text
INITIAL DDS LOAD DONE
ICEBERG MAINTENANCE DONE
```

В выводе `docker compose ps -a` задачи `db-init`, `cdc-init` и `dwh-init`
имеют статус `Exited (0)`. Постоянные сервисы продолжили работу.

Вывод: начальная доставка и наполнение DDS выполнены автоматически;
загрузчик завершился успешно и сохранил позиции обработки.

### Новая порция и немедленный replay

Официальный сценарий создал порцию:

`18efef14a2744c2eaddcf5cc04e31f3b-delta`

В Kafka и raw поступили 11 ожидаемых событий четырёх источников:

| Источник | Ожидалось | Найдено в Kafka | Найдено в raw |
|---|---:|---:|---:|
| CRM | 2 | 2 | 2 |
| Каталог | 3 | 3 | 3 |
| POS | 2 | 2 | 2 |
| WMS | 4 | 4 | 4 |
| **Всего** | **11** | **11** | **11** |

Границы порции, сохранённые в журнале.
Для всех строк `partition = 0`, интервал — `[start_offset, end_offset)`:

| Топик | start_offset | end_offset |
|---|---:|---:|
| catalog.public.price_version | 1650 | 1651 |
| catalog.public.sku | 550 | 551 |
| catalog.public.sku_version | 687 | 688 |
| crm.public.customer | 2200 | 2201 |
| crm.public.customer_profile_version | 2566 | 2567 |
| pos.public.receipt | 21525 | 21526 |
| pos.public.receipt_line | 63525 | 63526 |
| wms.public.batch | 3300 | 3301 |
| wms.public.movement | 92144 | 92145 |
| wms.public.purchase_line | 5400 | 5401 |
| wms.public.purchase_order | 1800 | 1801 |

Чекер дождался доставки событий в raw, вызвал загрузчик с этим batch_id,
а затем повторил ту же порцию с `--replay`.

Результат:

```text
pass    incremental.delta            1/1  Новые ключи совпали с источниками, связи проверены
pass    incremental.replay           1/1  Данные и версии не изменились
pass    incremental.batch_log        1/1  Границы порции покрывают тестовые события и не изменились при повторе
```

Вывод: новая порция загрузилась в DDS; немедленный replay сохранил
данные и версии, а также исходные границы порции.

Отдельные длительности загрузки дельты и replay и число затронутых строк
каждого MERGE в представленном выводе не раскрыты. Эти показатели
не измерялись отдельно. Сценарий завершился без увеличения стандартного
тайм-аута чекера.

### История товара, цены и soft delete клиента

После дельты официальный сценарий выполнил две порции:

| batch_id | Диапазонов | Событий по границам offsets | Статус |
|---|---:|---:|---|
| 18efef14a2744c2eaddcf5cc04e31f3b-history | 2 | 4 | success |
| 18efef14a2744c2eaddcf5cc04e31f3b-privacy | 1 | 1 | success |

Результат проверок:

```text
pass    history.versions             1/1  Старые и новые версии доступны, снимок продажи прежний
pass    history.privacy              1/1  Клиент скрыт, позиции чеков и суммы сохранены
```

Вывод: изменения товара и цены сохранили предыдущие версии и снимок
старой продажи. Soft delete скрыл клиента из аналитического контракта,
не удалив позиции чеков и их суммы.

### Дополнительная регрессия позднего replay

Выполнен отдельный тест с последовательностью:

`delta → history → privacy → replay исходной delta`

Идентификаторы его порций:

| batch_id | Диапазонов | Событий по границам offsets | Статус |
|---|---:|---:|---|
| late-02fdfaa83fa4424e88e2964d2005d15f-delta | 11 | 11 | success |
| late-02fdfaa83fa4424e88e2964d2005d15f-history | 2 | 4 | success |
| late-02fdfaa83fa4424e88e2964d2005d15f-privacy | 1 | 1 | success |

После history/privacy сохранены все 11 бизнес-представлений,
затем повторена старая delta и выполнено сравнение результатов.

Сырой вывод:

```text
PASS: old delta replay preserved all 11 business views; customer remains hidden
```

Вывод: поздний replay сохранил все бизнес-представления, включая историю
цен и видимость клиентов. Более старые события не отменили последующие
изменения. Это подтверждает исправление дефекта прежней реализации.

### Повторная проверка целостности DDS

После сценариев повторно выполнены dbt-тесты ключей, связей,
истории и lineage:

```text
Finished running 24 data tests in 0 hours 0 minutes and 2.33 seconds (2.33s).

Completed successfully
Done. PASS=24 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=24
```

Все 24 теста прошли. Время выполнения самих тестов — 2,33 с;
это не полная длительность команды с запуском и разбором проекта dbt.

### Проверка журнала порций

После всех сценариев в `v_load_batches` присутствуют семь порций:
начальная, три порции официального сценария и три порции дополнительного
теста. Все имеют статус `success`.

Запрос:

```sql
SELECT batch_id, count(*) AS n
FROM iceberg.contract_dwh.v_load_batches
GROUP BY batch_id
HAVING count(*) > 1;
```

вернул пустой результат. Повторы не создали дополнительных строк
журнала с тем же batch_id.

### Итог и подтверждающие материалы

- Статическая проверка: **6/13**, остальные сценарные пункты пропущены.
- Полный официальный сценарий: **13/13 автоматических баллов**.
- Дополнительная регрессия позднего replay: **PASS**.
- Повторные dbt-тесты: **24/24**.
- Автоматическая инициализация: завершена, одноразовые задачи `Exited (0)`.
- Дубли batch_id в журнале: не обнаружены.

Материалы прогона:

- [Полный консольный вывод](../reports/hw02-console-reviewed.txt).
- [Логи автоматической инициализации](../reports/hw02-startup-reviewed.log).
- [Статический JSON-отчёт](../reports/hw02-static-reviewed.json).
- [Сценарный JSON-отчёт](../reports/hw02-scenario-reviewed.json).

В логах присутствует предупреждение dbt о настройках проверки
SSL-сертификатов. Оно не привело к ошибке выполнения; показатель
`WARN=0` в итоговой строке относится к результатам тестов.

Исправленная версия прошла функциональную проверку на Trino/Iceberg.
Проектирование модели, ограниченность обработки, полнота журнала
и бонусные критерии остаются предметом ручной оценки преподавателя.
Отдельные длительности сценарных загрузок и физические планы выполнения
в этом прогоне не зафиксированы.



## Подтверждение на реальном стенде — 26.11.2026

Исправленная версия проверена на Docker Desktop с Trino/Iceberg.
Автоматическая инициализация завершилась успешно; официальный сценарий
получил 13/13, дополнительный тест позднего replay прошёл, повторные
dbt-тесты — 24/24. Подробности и материалы нового прогона приведены
в разделе от 26.11.2026 файла docs/load_journal.md.