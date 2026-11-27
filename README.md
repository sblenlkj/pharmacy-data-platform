# Pharmacy data platform

ДЗ-2: четыре PostgreSQL → Debezium → Kafka → raw Iceberg → обработанное
состояние источников → DDS dbt → contract_dwh.

Участники: Кауров Александр, Каневский Дмитрий, Бичурина Елизавета, Ляхов Артем.

## Состояние после ревью 24.11.2026

Исправлены обогащение чека из контрактного VIEW PostgreSQL, потеря связей
между порциями, отсутствие пересчёта по изменениям зависимых таблиц и
откат приватности при позднем replay. Изменения и проверка:
[docs/hw2_review.md](docs/hw2_review.md).

**Новая версия ещё не запускалась в Docker/Trino.** Старые 13/13 в
reports/hw02-scenario-final.json и замеры в docs/load_journal.md относятся
к версии команды. После исправлений нужен новый чистый прогон;
его результаты нельзя подменять прежним отчётом.

## Стек ДЗ-2

| Компонент | Версия |
|---|---|
| Trino, каталог iceberg | trinodb/trino:484 |
| Iceberg, Hive Metastore | apache/hive:4.0.1 + драйверы из Maven с проверкой SHA256 |
| База метаданных HMS | postgres:16.16-alpine3.23 |
| S3 | bitnamilegacy/minio:2025.7.23-debian-12-r5, бакет lakehouse |
| Kafka | apache/kafka:4.3.1, KRaft, один брокер и одна партиция на топик |
| Debezium | quay.io/debezium/connect:3.7.0.Final |
| dbt | dbt-trino==1.12.0, dbt-core==1.12.7 |

Модель: dimensional, 8 dim + 3 fact. ERD:
[бизнес-слой](docs/erd/dds.mmd), [технический путь](docs/erd/dds_tech.mmd).
Партиции фактов — месяц события; состояние источников — по topic;
обоснование — ADR 009–011.

## Запуск на чистом стенде

Из корня репозитория, Docker Desktop запущен и использует Linux containers:

```bash
docker compose up -d
docker compose logs -f dwh-init
```

Compose автоматически применит миграции ДЗ-1, выполнит seed, зарегистрирует
4 коннектора с 25 физическими таблицами (включая POS shift/cash_register),
доставит raw и выполнит первую загрузку и dbt-тесты. Окончательный маркер:
**ICEBERG MAINTENANCE DONE**. Статус первой порции в v_load_batches —
success. Ctrl+C при просмотре logs закрывает только просмотр.

Если на этом компьютере остался стенд ДЗ-1, остановите его без удаления
данных. Для независимых новых томов добавляйте `-p pharmacy-hw2-review`
**ко всем** командам compose ниже. Для пересборки имеющихся локальных
образов используйте `up -d --build`. Одна лишь смена имени проекта
не освобождает занятые старым стендом порты.

После изменения списка CDC-таблиц проверяйте эту версию на новых томах:
обычное обновление уже завершившего snapshot коннектора не создаёт
начальные события для добавленных shift/cash_register. Автоматическая
миграция старого DDS в новую схему состояния не реализована.
`down -v` удаляет данные выбранного проекта; для проверки удобнее
выбрать новое имя проекта и сохранить старые тома.

Первый запуск скачивает образы, Python-пакеты и HMS-драйверы из Maven.
Замер прежней версии — около 8 минут; время новой версии ещё не измерено.

## Переменные окружения

Копировать .env.example необязательно, значения по умолчанию работают.

| Переменная | Значение по умолчанию | Назначение |
|---|---|---|
| POSTGRES_PASSWORD | postgres | администратор PostgreSQL |
| REPLICATION_PASSWORD | replicator | физическая репликация |
| APP_PASSWORD | app | запись тестовых данных |
| CHECKER_PASSWORD | checker | чтение контрактов источников |
| PATRONI_API_PASSWORD | patroni | Patroni REST |
| DATA_SEED | 42 | namespace начального генератора |
| DEBEZIUM_PASSWORD | debezium | CDC-роль |
| COMPOSE_PROJECT_NAME | pharmacy-hw1 | существующее имя проекта; -p имеет приоритет |

Trino/Kafka endpoints, MinIO minioadmin/minioadmin и HMS hive/hivepass —
учебные константы в compose и конфигурации. Список всех подставляемых
переменных — в .env.example. При изменении CHECKER_PASSWORD меняйте
DSN хостового чекера соответственно. Контейнер tools получает DSN сам.

Порты хоста: Trino 8080, Kafka 9092, Connect 8083, MinIO 9000/9001,
PostgreSQL через PgBouncer RW 6432. Python/dbt на хосте для основного
пути ниже не нужны.

## Проверка в Docker Desktop

После финального маркера запустите официальный чекер:

```bash
docker compose run --rm -T --no-deps tools python hws_descriptions/hw2/hw02_check.py --submission ./hw02_submission.container.yaml --scenario --json /reports/hw02-scenario-reviewed.json
```

Ожидаемый результат — 13/13, **ещё не подтверждён для этой версии**.
Сценарий создаёт данные и выполняет soft delete тестового клиента.
Без --scenario проверка только читает данные: ожидается 6/13.
При тайм-ауте на слабой машине есть --timeout 180; для сравнения
с приёмкой сначала проверяйте стандартные 90 секунд.

Отдельная проверка исходного дефекта позднего replay:
```bash
docker compose run --rm -T --no-deps tools python scripts/hw2/check_late_replay.py
```
Она также пишет тестовые данные. Порядок: delta → history → privacy →
replay старой delta, затем сравнение всех 11 бизнес-представлений.
Не запускайте её одновременно с официальным сценарием.

Отдельные dbt-тесты и диагностика:
```bash
docker compose run --rm -T --no-deps tools dbt test --project-dir dwh --profiles-dir dwh --target-path /tmp/dwh-dbt-target --log-path /tmp/dwh-dbt-logs
docker compose ps -a
docker compose logs --tail=100 db-init cdc-init cdc-ingest dwh-init
```

Exited (0) для одноразовых задач db-init/cdc-init/dwh-init — нормально.
Остановить с сохранением данных: `docker compose down`.

## Хостовый чекер преподавателя

Контейнерный манифест адресует сервисы Compose. Основной
hw02_submission.yaml адресует localhost; команды fixture/load в нём
теперь сами запускают tools через Docker, с передачей stdin JSON.
Хосту нужны только зависимости официального чекера, а не локальный dbt.

Пример для Bash, после старта стенда:
```bash
python3 -m pip install -r hws_descriptions/hw2/requirements.txt
export HW02_CRM_DSN='postgresql://checker:checker@localhost:6432/crm_service_db'
export HW02_CATALOG_DSN='postgresql://checker:checker@localhost:6432/catalog_service_db'
export HW02_POS_DSN='postgresql://checker:checker@localhost:6432/pos_service_db'
export HW02_WMS_DSN='postgresql://checker:checker@localhost:6432/wms_service_db'
python3 hws_descriptions/hw2/hw02_check.py --submission ./hw02_submission.yaml --scenario --json reports/hw02-host-reviewed.json
```
Если использовали нестандартное имя проекта, экспортируйте соответствующий
COMPOSE_PROJECT_NAME, чтобы вложенные команды манифеста адресовали тот же
стенд. В PowerShell для переменных используется `$env:ИМЯ = 'значение'`.

## Порции, replay и восстановление

```bash
docker compose run --rm -T --no-deps tools python scripts/load_dds
docker compose run --rm -T --no-deps tools python scripts/load_dds --batch-id my-batch
docker compose run --rm -T --no-deps tools python scripts/load_dds --batch-id my-batch --replay
```

Повторный batch_id не обозначает новую порцию. При ошибке или остановке
смотрите сохранённый ID в журнале/логе и повторяйте именно его.
Новая порция заблокирована до восстановления предыдущей.
Подробности: [dwh/README.md](dwh/README.md), [ADR 011](docs/adr/011-history-replay-idempotency.md).

Результаты нового запуска допишите в [docs/load_journal.md](docs/load_journal.md):
команды, сырые логи, batch_id, реальные границы, длительность,
число новых/изменённых строк, результат replay. Не переносите туда старые
тайминги как измерения исправленного кода.

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
