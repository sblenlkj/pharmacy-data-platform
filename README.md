# Pharmacy data platform

ДЗ-2 — текущая сдача (ветка `hw2`): детальный слой хранилища — CDC четырёх
PostgreSQL-источников → raw (Iceberg) → DDS (dbt, dimensional) → представления
контракта; стек MinIO + Trino + Hive Metastore + Kafka/Debezium.

ДЗ-1 — четыре PostgreSQL-источника аптечной сети и учебный HA-стенд,
раздел ниже.

Участники: Кауров Александр, Каневский Дмитрий, Бичурина Елизавета, Ляхов Артем.

## ДЗ-2: лейкхаус MinIO + Trino + Iceberg + CDC (ветка `hw2`)

### Выбранный стек (подтверждён spike 1 и рабочим стендом, 2026-11-15/16)

| Компонент | Версия | Роль |
|---|---|---|
| Trino | `trinodb/trino:484`, каталог `iceberg` | OLAP-движок |
| Каталог Iceberg | **Hive Metastore**: `apache/hive:4.0.1` (standalone metastore, jar'ы докладываются в образ) + backing-БД `postgres:16.16-alpine3.23` | метаданные Iceberg |
| S3 | MinIO `bitnamilegacy/minio:2025.7.23-debian-12-r5` (официальный `minio/minio` удалён из Docker Hub в сентябре 2026) | object storage, бакет `lakehouse` |
| Kafka | `apache/kafka:4.3.1`, один брокер, KRaft (combined broker+controller), listeners: `kafka:29092` internal / `localhost:9092` external | шина CDC |
| Debezium Connect | `quay.io/debezium/connect:3.7.0.Final` (Kafka client 4.3.1; после 3.0.0.Final Debezium не публикуется в Docker Hub — официальный реестр quay.io) | 4 Postgres-коннектора, `pgoutput` |
| Наполнение | dbt: `dbt-trino==1.12.0` (dbt-core 1.12.7) — в образе `tools`, запуск in-process | dbt-проект dwh/ |
| Доставка raw | `cdc-ingest` (собственный образ `python:3.12-slim`, `confluent-kafka==2.16.1` + `trino==0.340.0`) | Kafka → `raw.cdc_events` |
| Загрузка DDS | `scripts/load_dds` (python; dbt через `dbtRunner` in-process, `--select` только затронутых веток) | raw → DDS порциями с merge-идемпотентностью |

Драйверы HMS (`postgresql`, `hadoop-aws`, `aws-java-sdk-bundle`, 296 МБ) в
репозитории не хранятся: `infra/hive-metastore/Dockerfile` скачивает их с
Maven Central при сборке с проверкой sha256 (бит-в-бит те же файлы, что
прошли spike 1 и рабочий прогон). Для сборки нужна сеть — как и для `FROM`-образов.

Все образы запинены. Решения и ограничения переноса:
[ADR 010](docs/adr/010-lakehouse-engine-partitioning.md),
[ADR 011](docs/adr/011-history-replay-idempotency.md).

### Что делает `docker compose up -d`

Никаких ручных команд, кроме одной. На чистых volumes:

1. поднимается стенд ДЗ-1 (Patroni×3, HAProxy, PgBouncer) и OLAP-стек ДЗ-2
   (`minio`, `hive-metastore-db`, `hive-metastore`, `trino`; порты
   9000/9001/8080);
2. **db-init** (tools) применяет миграции, создаёт роль `debezium`
   (`LOGIN REPLICATION`, `GRANT CONNECT/SELECT` на 4 БД и таблицы CDC),
   создаёт publication'ы `pub_cdc_{crm,catalog,pos,wms}` по
   [dwh_mapping.md §9](docs/dwh_mapping.md) (единый источник —
   `scripts/infra/cdc_sources.json`, 23 таблицы) и, если источники пусты,
   запускает базовый сид генератора (`DATA_SEED`, по умолчанию 42);
3. **cdc-init** (one-shot, тот же образ, что Connect) регистрирует через
   Kafka Connect REST 4 коннектора `cdc-{crm,catalog,pos,wms}`
   (`pgoutput`, `snapshot.mode=initial`, `topic.prefix` = имя сервиса,
   `table.include.list` из §9, подключение к лидеру через `haproxy:5000`),
   ждёт, пока каждый коннектор/tasks = RUNNING и end-offsets всех 23 топиков
   `{svc}.public.{table}` станут ненулевыми и стабильными, и пишет в лог
   маркер **`CDC SNAPSHOT DONE`** — начальный снимок источников уже в Kafka;
4. **cdc-ingest** (python, confluent-kafka + trino) читает 23 топика группы
   `cdc-ingest-raw` с earliest, батчами по 5000 строк (или 5 с) пишет
   `raw.cdc_events` (payload — полный envelope Debezium для переобработки),
   offsets коммитит строго после успешного INSERT — доставка at-least-once,
   дедуп по `(topic, partition_id, kafka_offset)` на чтении (ADR-011). По
   догону до lag=0 пишет в лог маркер **`CDC RAW CAUGHT UP`** (снапшот
   200 233 события, 211–321 с по журналам). DDL raw/ctl применяет сам при старте
   (идемпотентно);
5. **dwh-init** (one-shot, образ `tools` с dbt-trino) — начальная загрузка
   DDS: ждёт, пока cdc-ingest догонит Kafka (условие маркера
   `CDC RAW CAUGHT UP` — водяные знаки Kafka против максимумов offsets в
   raw, `scripts/infra/wait_raw_caught_up.py`), затем запускает
   `scripts/load_dds` (порция без batch-id, UUID) и пишет в лог маркер
   **`INITIAL DDS LOAD DONE`**. Представления контракта (включая
   `v_cdc_events`, `v_load_batches`) разворачивает тот же dbt-прогон.
   После маркера — обслуживание таблиц Iceberg
   (`scripts/hw2/maintain_iceberg.py`: expire_snapshots / optimize /
   remove_orphan_files, ADR-011) и финальный маркер
   **`ICEBERG MAINTENANCE DONE`** — чекер запускать после него.

**Признак завершения начальной загрузки** (после `up -d`, вернувшегося
раньше): строка первой порции со статусом `success` в
`contract_dwh.v_load_batches` + маркер `INITIAL DDS LOAD DONE` (и затем
`ICEBERG MAINTENANCE DONE`) в логе dwh-init. Замер на чистых томах
(приёмочный прогон 2026-11-17):
**~8 минут** от запуска `docker compose up -d --build` до маркера
(включая сборку образов и весь снапшот; снапшот успевает внутри окна
`up -d`, дальше ~5.5 мин — догрузка raw, порция DDS и обслуживание):

```bash
docker compose logs dwh-init | grep -e "INITIAL DDS LOAD DONE" -e "ICEBERG MAINTENANCE DONE"
docker compose exec trino trino --execute \
  "SELECT batch_id, status, json_extract_scalar(offsets_json,'$[0].topic') AS first_topic
   FROM iceberg.contract_dwh.v_load_batches"
```

Повторный `up -d` на живом стенде идемпотентен: миграции скипаются по ledger,
сид не запускается (источники непустые), конфиги коннекторов перезаписываются
те же (Connect не перезапускает их при неизменном конфиге), offsets в Kafka
сохраняются — повторного снапшота нет. dwh-init перезапускается, но его гейт
«raw впереди позиций» сворачивает загрузку в no-op (`kafka_positions` уже
покрывают raw → «новых событий нет»), обслуживание Iceberg повторно — тоже
no-op (optimize переписывает 0 файлов, снапшот не создаётся), маркеры
пишутся всё равно. Если на
момент `up -d` в raw накопились недогруженные события (например, delta),
dwh-init честно загружает их новой порцией.

### Запуск чекера с хоста (альтернатива контейнерному режиму)

Основной режим — в контейнере tools (WSL2 не видит host-порты). На обычном
Linux-хосте работает прямой вызов:

```bash
python3 -m pip install -r hws_descriptions/hw2/requirements.txt
export HW02_CRM_DSN='postgresql://checker:checker@localhost:6432/crm_service_db'  # + catalog/pos/wms
python3 hws_descriptions/hw2/hw02_check.py --submission hw02_submission.yaml --scenario
```

Манифест `hw02_submission.yaml` адресует хост (localhost:6432/8080/9092);
`commands.load`/`commands.fixture` исполняются при этом на хосте и требуют
локального python с зависимостями стенда (dbt-trino, psycopg2,
confluent-kafka) — поэтому рекомендованный путь остаётся контейнерным.

### Переменные окружения

| Переменная | Назначение | Значение стенда |
|---|---|---|
| `DATA_SEED` | namespace автоматического базового сида (db-init) | `42` |
| `DEBEZIUM_PASSWORD` | пароль роли `debezium` | `debezium` |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | учётка MinIO | `minioadmin` / `minioadmin` (константа в compose) |
| `MINIO_DEFAULT_BUCKETS` | бакет, создаваемый при старте | `lakehouse` |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | backing-БД HMS | `hive` / `hivepass` / `hive_metastore` |
| `DB_DRIVER` / `SERVICE_NAME` | режим контейнера HMS | `postgres` / `metastore` |

S3-параметры (endpoint, ключи, region, path-style) задаются не через env,
а в `iceberg.properties` Trino и `hive-site.xml` HMS — так надёжно
(причины — ADR 010).

### Как убедиться, что коннекторы заснапшотили

```bash
# 1) маркер в логе cdc-init (ждёт снапшоты сам, up уже вернулся раньше)
docker compose logs cdc-init | grep "CDC SNAPSHOT DONE"

# 2) все 23 топика непустые: сумма end-offsets
docker compose exec kafka-connect bash -c \
  'for t in $(/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list | grep public | sort); do \
     printf "%-40s %s\n" "$t" "$(/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:29092 --topic "$t" | cut -d: -f3 | awk "{s+=\$1} END {print s}")"; \
   done'

# 3) envelope: op="r" (snapshot-записи) и source.schema/source.table
docker compose exec kafka-connect bash -c \
  '/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 \
    --topic crm.public.customer --from-beginning --max-messages 1 --timeout-ms 10000'

# 4) снапшот доставлен в raw: маркер ingest + сверка с источниками
docker compose logs cdc-ingest | grep "CDC RAW CAUGHT UP"
python3 scripts/hw2/check_raw_snapshot.py   # 5/5 pass = cdc.*.snapshot чекера
```

Ожидаемый envelope (schemaless JSON, `VALUE_CONVERTER_SCHEMAS_ENABLE=false`):
`{"before":null,"after":{...},"op":"r","source":{"schema":"public","table":"customer",...},...}`;
ключ записи — PK таблицы. Порты для хоста: Kafka `localhost:9092`,
Connect REST `localhost:8083`, Trino `localhost:8080`, MinIO `localhost:9000/9001`.

### Как проверить загрузку DDS

```bash
# 1) признак завершения начальной загрузки и обслуживания Iceberg
#    (см. выше — раздел «Что делает up -d»)
docker compose logs dwh-init | grep -e "INITIAL DDS LOAD DONE" -e "ICEBERG MAINTENANCE DONE"

# 2) официальный чекер, статические проверки (dds.*, cdc.*.snapshot):
#    метаданные 13 представлений, значения, интервалы версий
docker compose run --rm -T --no-deps tools \
  python hws_descriptions/hw2/hw02_check.py \
    --submission ./hw02_submission.container.yaml
# ожидание: 6/13 автоматически (остальное — режим --scenario и ручные пункты)

# 3) dbt-тесты DDS: ключи (уникальность/NOT NULL, составные — сингулярными),
#    связи на dim_*, инварианты истории (интервалы без пересечений, одна
#    открытая версия) и lineage (_load_batch_id)
docker compose run --rm -T --no-deps tools bash -c \
  "dbt test --project-dir dwh --profiles-dir dwh \
   --target-path /tmp/dwh-dbt-target --log-path /tmp/dwh-dbt-logs --no-use-colors"

# 4) контрольные суммы DDS для теста идемпотентности повтора порции
docker compose run --rm -T --no-deps tools \
  python scripts/hw2/warehouse_checksum.py

# 5) обслуживание таблиц Iceberg (expire_snapshots/optimize/orphans) —
#    тот же запуск, что в dwh-init после начальной загрузки (ADR-011)
docker compose run --rm -T --no-deps tools \
  python scripts/hw2/maintain_iceberg.py
```

### Самопроверка ДЗ-2: официальный чекер (static + scenario)

Обе проверки запускаются из контейнера `tools` (DSN источников и Trino
заданы в environment сервиса, манифест — сетевой, с именами сервисов).
JSON-отчёты пишутся в примонтированный `./reports`:

```bash
# статические проверки (dds.*, cdc.*.snapshot): ожидание 6/13
docker compose run --rm -T --no-deps tools \
  python hws_descriptions/hw2/hw02_check.py \
    --submission ./hw02_submission.container.yaml \
    --json /reports/hw02-static.json

# полный сценарий (fixture → load → replay → history → privacy): 13/13
docker compose run --rm -T --no-deps tools \
  python hws_descriptions/hw2/hw02_check.py \
    --submission ./hw02_submission.container.yaml \
    --scenario \
    --json /reports/hw02-scenario.json
```

Сценарий занимает ~4 мин: чекер сам пишет тестовые данные командой
`scripts/check_fixture`, загружает порции `scripts/load_dds` (контракты
`commands.fixture`/`commands.load` манифеста) и сверяет сырьё, DDS,
журнал порций, версии и приватность. Итог сверяется со строкой
`Подтверждено автоматически: 13/13`; ручные пункты (model.*, engine.*,
incremental.scope, journal) проверяет преподаватель.

### Отказоустойчивость CDC: рестарт PG-кластера

Коннекторы подключаются к лидеру через `haproxy:5000`. Логические слоты
`cdc_{crm,catalog,pos,wms}` переживают рестарт кластера и смену лидера без
повторного снапшота — двумя настройками:

- **`permanent_slots` у Patroni** (`infra/patroni/patroni.yml`,
  `bootstrap.dcs.permanent_slots` — в Patroni 4.1 ключ именно верхнего
  уровня динамической конфигурации): Patroni держит слоты на лидере,
  синхронизирует их с репликами и пересоздаёт на новом лидере после
  рестарта/перевыборов. Важно: ключ в секции `postgresql` Patroni 4.1
  игнорирует (сверено с кодом 4.1.0 в образе).
- **`offset.mismatch.strategy=trust_slot` у Debezium** (прописан в
  `scripts/infra/cdc_init.py`): слот считается источником истины, оффсет
  коннектора догоняет позицию пересозданного слота; проверка «stored LSN
  недоступен» отключается самой стратегией. Без неё задача коннектора после
  пересоздания слота падает «LSN no longer available» и не поднимается.

Проверено 2026-11-17: полный
stop/start postgres1–3 — коннекторы восстанавливаются сами за ~30 с
(задачи даже не уходят в FAILED); принудительный дроп всех четырёх слотов —
Patroni пересоздаёт их за ~10 c, задачи Debezium переподнимаются сами
(внутренние ретраи) за ~20 c, повторного снапшота нет, новые события
доезжают до raw за секунды.

Остальные особенности CDC: heartbeat отключён — при долгом отсутствии
изменений WAL на источнике удерживается слотом (на объёмах ДЗ некритично);
`decimal.handling.mode=string`, `tombstones.on.delete=false` — чтобы
JSON-envelope было удобно парсить загрузчику DDS.

---

## ДЗ-1: источники и HA-стенд

### Статус

Основной HA-стенд собран и проверен локально.

Официальный checker подтверждает:

- contract, keys, volume, C01–C23, X01–X10;
- I1–I12 + P1/P2;
- async replica + PgBouncer;
- CDC automated checks;
- realism 5/5.

HA проходит 5/6 автоматических проверок. Единственный красный пункт связан с тем,
что checker требует `state=running` для всех Patroni members, тогда как здоровые
replicas возвращаются Patroni как `state=streaming`.

Подробности: [docs/infra/checker-compatibility.md](docs/infra/checker-compatibility.md).

### Быстрый запуск

Рекомендуемый путь — полностью через Docker:

```bash
docker compose up -d --build
./scripts/wait_ready.sh
./scripts/check.sh -v
```

`up -d` — единственная необходимая команда: db-init сам применяет миграции,
настраивает CDC-роль/publications и засеивает источники детерминированным
генератором (`DATA_SEED`, по умолчанию 42), если они пусты; cdc-init
регистрирует CDC-коннекторы и ждёт их снапшоты, dwh-init выполняет
начальную загрузку DDS (см. раздел ДЗ-2).
Дельта-довалка поверх базового сида для экспериментов HW1 — по-прежнему
`./scripts/seed.sh --seed <n> --delta`.

На хосте нужны только Docker, Docker Compose и shell. Python/uv для основного
сценария не требуются: checker и generator запускаются внутри контейнера `tools`.

Для JSON-отчёта:

```bash
./scripts/check.sh --json /reports/hw01-full.json -v
```

Остановить стенд без удаления volumes:

```bash
docker compose down
```

### Структура репозитория

```text
migrations/         SQL migrations четырёх source databases
dwh/                dbt-проект DDS ДЗ-2 (staging → dds → contract, DDL raw/ctl)
infra/              Patroni, HAProxy, PgBouncer и tools images
scripts/            shell entrypoints и infra orchestration
src/pharmacy_dwh/   детерминированный генератор данных
tests/              invariant tests и contract fixtures
docs/               итоговая документация HW1
notes/              рабочие design notes и open questions
hws_descriptions/   исходные материалы ДЗ-1/ДЗ-2 и официальные чекеры
reports/            generated checker reports
```

Что делает каждый script: [scripts/README.md](scripts/README.md).

### Два docker-compose

`docker-compose.yml` — основной HA/submission-стенд:

- 3 PostgreSQL под Patroni;
- 3 etcd;
- HAProxy;
- PgBouncer RW/RO;
- automatic bootstrap/migrations;
- tools container.

`docker-compose.dev.yml` — упрощённый single-Postgres dev-стенд. Он использовался
для ранней отладки schema/contract/seed без HA и сейчас нужен только как быстрый
fallback.

### Submission manifests

- `hw01_submission.yaml` — checker запускается **с хоста**, DSN через `localhost`;
- `hw01_submission.container.yaml` — checker запускается **в Docker network**;
  именно его использует `./scripts/check.sh`;
- `hw01_submission.dev.yaml` — только для упрощённого dev-стенда.

Для обычной проверки ничего выбирать не нужно:

```text
./scripts/check.sh
    -> hw01_submission.container.yaml
```

Альтернативно checker можно запустить с хоста:

```bash
uv sync
uv run python hws_descriptions/hw1/hw01_check.py \
  --submission hw01_submission.yaml \
  -v
```

### Допущения по предметной области

- **Календарный день — UTC.** Даты чека и приёмки для просрочки (I8/I9)
  вычисляются как `(timestamp AT TIME ZONE 'UTC')::date` и не зависят от
  `TimeZone` сессии.
- **Формат business key** — текстовый namespaced `<ENTITY>-<NAMESPACE>-<SEQ>`;
  у маленьких справочников BK выводится из имени. Обоснование и отвергнутые
  альтернативы: [ADR 008](docs/adr/008-business-key-format.md).
- **Карты лояльности:** за жизнь у клиента может быть несколько карт, активна
  ровно одна; перевыпуск = закрытие старой карты и выпуск новой (seed
  демонстрирует перевыпуски).
- **Согласия — события** `issued_at`/`revoked_at`; активное согласие одного
  типа одно; повторная выдача после отзыва допустима (seed демонстрирует
  отзыв и повторную выдачу маркетингового согласия).
- **Soft delete:** клиент по запросу на удаление исчезает из
  `contract.v_customer`, физическая строка остаётся, контакты обезличиваются;
  остальные справочники остаются в contract с `is_active=false`, чтобы не
  ломать исторические факты ([ADR 007](docs/adr/007-soft-delete.md)).
- **Смены:** чек действует внутри окна своей смены `[opened_at, closed_at]`
  (проверяется триггером в POS). Seed создаёт одну длинную смену на аптеку —
  осознанное упрощение, отражённое в open questions.
- **Цены:** интервалы `[valid_from, valid_to)` в области действия
  chain/region/pharmacy; более узкая область перекрывает более широкую;
  смена версии автоматизирована триггером (новая текущая версия закрывает
  предыдущую).
- **Продажа** всегда ссылается на конкретную партию и действующую на момент
  чека цену; значения зафиксированы snapshot-полями позиции.
- **Возврат** ссылается на исходный чек и суммарно не превышает проданное;
  обратное складское движение при возврате не создаётся (осознанное
  упрощение).
- **Движения WMS неизменяемы:** исправление ошибки — компенсирующее движение.
- **Анонимная покупка:** `customer_bk` в чеке NULL; в WMS списание идёт
  на получателя `ANONYMOUS`.
- **Деньги:** все суммы `numeric(18,2)`, валюта везде `RUB`, цена строго > 0.

### Бонусы

### +1: реалистичные данные

Бонус за реалистичность данных уже получен автоматически: официальный checker
показывает `realism 5/5`.

Генератор проходит все проверяемые признаки реалистичности:

- недельная сезонность продаж;
- long-tail по SKU;
- неравномерность продаж по аптекам;
- доля покупок без карты в допустимом диапазоне;
- доля возвратов в допустимом диапазоне.

То есть этот бонус подтверждён самим checker и не требует отдельной ручной проверки.

### +1: журнал отказоустойчивости

Бонусный журнал Э1–Э7 полностью выполнен на реальном HA-стенде.

Основной документ:

- [docs/failover_journal.md](docs/failover_journal.md) — команды, измеренные RTO/TPS/latency, raw excerpts и выводы.

Сырые доказательства:

- [reports/failover/](reports/failover/) — JSONL write-loop, summaries, Patroni/etcd logs, topology snapshots и exact commands;
- [reports/failover/README.md](reports/failover/README.md) — какие прогоны считаются валидными и какие диагностические прогоны были отвергнуты.

Ключевые измерения:

- emergency failover RTO: 27.579 с;
- planned switchover RTO: 3.419 с;
- возвращённый узел снова стал streaming-replica через 23.116 с;
- PgBouncer обслужил 250/250 клиентов, тогда как прямой PostgreSQL дал 25 ошибок из 250.

### +1: кворумный etcd

Стенд использует 3 etcd member, quorum = 2/3. Отдельная демонстрация выполнена в эксперименте Э6.

Где смотреть доказательство:

- [docs/failover_journal.md — Э6](docs/failover_journal.md) — итог и интерпретация;
- `reports/failover/e6-one-etcd-down/` — один etcd недоступен, quorum сохраняется, запись продолжается;
- `reports/failover/e6-quorum-loss/` — доступны только 1 из 3 etcd, quorum теряется;
- `reports/failover/e6-etcd-quorum/` — служебные проверки и итоговое восстановление.

Фактически продемонстрировано:

```text
3/3 etcd healthy
   ↓ stop 1
2/3 available -> quorum сохраняется -> 728/728 writes succeed
   ↓ restore
3/3 healthy
   ↓ stop 2
1/3 available -> quorum lost -> Patroni demotes leader -> read-only
   ↓ restore
3/3 healthy, 1 leader + 2 streaming replicas
```

После экспериментов `./scripts/wait_ready.sh` снова проходит, `synchronous_mode=false` восстановлен.

### Документация для проверки

Основные submission-документы лежат в `docs/`:

- [ERD четырёх источников](docs/erd/) — физическая модель, PK/FK и cross-service BK;
- [ADR](docs/adr/) — архитектурные решения и отвергнутые альтернативы;
- [DWH mapping](docs/dwh_mapping.md) — карта склейки источников для следующих ДЗ;
- [Failover journal](docs/failover_journal.md) — выполненные Э1–Э7 с реальными измерениями;
- [Failover evidence](reports/failover/) — raw logs, write events, summaries и доказательство quorum etcd;
- [Sources](docs/sources.md) — внешние источники и документация;
- [Infrastructure notes](docs/infra/README.md) — состав стенда, порты и детали запуска.

Рабочие проектные заметки и незакрытые вопросы:

- [notes/hw1/plan.md](notes/hw1/plan.md)
- [notes/hw1/open_questions.md](notes/hw1/open_questions.md)

### Connection strings

Через PgBouncer:

```text
RW checker: postgresql://checker:checker@localhost:6432/<service>_service_db
RO checker: postgresql://checker:checker@localhost:6433/<service>_service_db
RW app:     postgresql://app:app@localhost:6432/<service>_service_db
```

HAProxy напрямую:

```text
RW admin:   postgresql://postgres:postgres@localhost:5000/<service>_service_db
RO:         postgresql://postgres:postgres@localhost:5001/<service>_service_db
```

Patroni API (узлы postgres1/2/3, endpoints `/cluster`, `/primary`, `/replica`):

```text
http://localhost:8008
http://localhost:8009
http://localhost:8010
```

Service names:

```text
crm
catalog
pos
wms
```

Оба PgBouncer-пула (6432 RW и 6433 RO) работают в режиме **session pooling**
(`pool_mode=session`): чекер выставляет параметры сессии отдельными SET-запросами,
которые при transaction pooling могли бы потеряться. Подробности — в
[docs/infra/README.md](docs/infra/README.md).

Например: `pos_service_db`.

### Если добавили новую migration

Не переписывайте уже применённые migrations. Добавляйте новый нумерованный SQL-файл
и запускайте:

```bash
./scripts/migrate.sh
```

### Что ещё требует ручной проверки

Автоматический checker не закрывает всё задание. Перед сдачей нужно глазами проверить:

- читаемость ERD;
- минимум 5 ADR с реальными trade-off;
- полноту `docs/dwh_mapping.md`;
- корректность `README.md` и connection strings;
- `docs/sources.md`;
- сопоставить итоговые числа bonus-экспериментов с raw evidence из
  `reports/failover/` перед сдачей.

Известные предметные edge cases, которые не блокируют текущий checker, собраны в
[notes/hw1/open_questions.md](notes/hw1/open_questions.md).
