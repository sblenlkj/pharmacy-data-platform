# Scripts

Эта папка содержит пользовательские entrypoints и Python orchestration для ДЗ-1 и ДЗ-2.

Идея разделения простая:

```text
scripts/*.sh
    -> короткие команды, которые запускает человек или официальный checker

scripts/infra/*.py
    -> реализация инфраструктурной логики этих команд

src/pharmacy_dwh/
    -> бизнес-логика детерминированного генератора данных
```

То есть shell-скрипты — это внешний CLI проекта, а Python в `scripts/infra` — служебный код для HA/bootstrap/readiness/seed routing.

---

## Пользовательские shell entrypoints

### `check.sh`

Запускает **неизменённый официальный checker** внутри контейнера `tools`.

Использует:

```text
hw01_submission.container.yaml
```

потому что checker работает внутри Docker network и подключается по именам сервисов вроде `pgbouncer-rw` и `haproxy`.

Пример:

```bash
./scripts/check.sh -v
./scripts/check.sh --sections cdc,repl,ha -v
```

---

### `wait_ready.sh`

Ждёт готовность полного HA-стенда.

Внутри запускает:

```text
scripts/infra/wait_ready.py
```

Проверяет:

- 3 Patroni member;
- ровно одного leader;
- RW PgBouncer ведёт на leader;
- RO PgBouncer ведёт на replica;
- contract views доступны во всех 4 БД;
- обе физические реплики догнали WAL.

---

### `seed.sh`

Публичный entrypoint генератора данных.

Внутри запускает:

```text
scripts/infra/seed.py
```

а тот вызывает настоящий генератор из:

```text
src/pharmacy_dwh/seed/
```

Через wrapper подключения перенаправляются в основной HA-стенд через RW PgBouncer.

Пример:

```bash
./scripts/seed.sh --seed 42
```

---

### `seed_delta.sh`

Добавляет новую детерминированную порцию данных поверх существующего dataset.

Используется главным образом официальным checker в секции replication:

```text
seed_delta
-> новая порция данных
-> ожидание replica
-> сравнение master/replica contract digests
```

Внутри запускает:

```text
scripts/infra/seed_delta.py
```

---

### `migrate.sh`

Повторно запускает `db-init` для применения новых нумерованных migrations.

Используется после добавления нового SQL-файла в:

```text
migrations/<service>/
```

Сам migration runner находится в:

```text
scripts/infra/bootstrap.py
```

---

### `test_invariants.sh`

Локальный helper для быстрого прогона SQL invariant tests:

```text
tests/invariants/I01...I12
tests/invariants/P01...P02
```

Проверяет ожидаемые SQLSTATE у негативных тестов и `ok = true` у положительных.

Официальный checker запускает эти tests самостоятельно; этот shell-скрипт нужен только для удобной локальной проверки.

---

### `test_contracts.sh`

Локальный helper для проверки contract views и C01–C23.

Запускает:

```text
scripts/test_contracts.rb
```

Ruby используется только как dev-tool для чтения YAML contract и проверки PostgreSQL views. Это не часть runtime архитектуры.

---

### `init_dev_postgres.sh`

Bootstrap для упрощённого:

```text
docker-compose.dev.yml
```

Создаёт четыре service DB в одном PostgreSQL и применяет migrations.

Основной HA-стенд этот файл **не использует**. В основном стенде bootstrap делает `scripts/infra/bootstrap.py`.

---

## Failover experiments: `scripts/failover/`

Явные команды для Э1–Э7:

```bash
./scripts/failover/run.sh e1
./scripts/failover/run.sh e2-e3
./scripts/failover/run.sh e4
./scripts/failover/run.sh e5
./scripts/failover/run.sh e6
./scripts/failover/run.sh e7
```

`e2-e3`, `e4`, `e5` и `e6` намеренно нарушают работу кластера. Каждая такая
команда вызывает `wait_ready.sh` до эксперимента, восстанавливает изменённые
services/config и требует полную readiness после него. `e5` меняет Patroni DCS
config через `patronictl` и возвращает `synchronous_mode=false`.

`write_load.py` пишет только в техническую таблицу
`pos_service_db.failover_bench.write_probe`; `connection_pressure.py` создаёт
ограниченные ступени concurrent connections; `topology.py` читает Patroni REST.
Команды, JSON summaries, JSONL events и raw logs сохраняются в
`reports/failover/`. Детали и интерпретация — в `docs/failover_journal.md`.

---

## Python orchestration: `scripts/infra/`

### `common.py`

Общие функции подключения к инфраструктуре:

- HAProxy RW/RO;
- PgBouncer RW/RO;
- ожидание доступного leader.

Используется другими Python infra-helpers.

---

### `bootstrap.py`

Основной migration/bootstrap runner полного HA-стенда.

Запускается Docker service:

```text
db-init
```

Делает:

1. создаёт роли `app`, `checker` и `debezium` (LOGIN REPLICATION);
2. создаёт 4 service DB;
3. применяет `migrations/<service>/*.sql`;
4. ведёт ledger применённых migrations с SHA-256;
5. настраивает права (включая SELECT для роли `debezium` на таблицы CDC);
6. создаёт CDC publication `hw01_all_tables` + publication'ы `pub_cdc_*`
   по `scripts/infra/cdc_sources.json` (= dwh_mapping §9);
7. настраивает `REPLICA IDENTITY`;
8. проверяет наличие contract views;
9. если источники пусты — запускает базовый сид генератора (`DATA_SEED`,
   по умолчанию 42) напрямую через `haproxy:5000`.

Это основной bootstrap production-like стенда HW1/HW2.

---

### `cdc_init.py`

One-shot оркестратор CDC (ДЗ-2), сервис `cdc-init` (образ тот же,
что у Kafka Connect: внутри есть python3-stdlib и kafka-cli).

1. ждёт Kafka Connect REST;
2. регистрирует (idempotent PUT) 4 коннектора Debezium `cdc-{svc}`:
   `pgoutput`, `snapshot.mode=initial`, `topic.prefix` = сервис,
   `table.include.list` из `cdc_sources.json`, лидер через `haproxy:5000`;
3. ждёт RUNNING у connector/tasks и стабильные ненулевые end-offsets
   всех 23 топиков `{svc}.public.{table}`;
4. пишет в лог маркер `CDC SNAPSHOT DONE`.

---

### `wait_raw_caught_up.py` и `load_dds` (загрузчик DDS)

Сервис `dwh-init` (образ `tools`, dbt-trino in-process) — начальная
загрузка DDS после CDC SNAPSHOT DONE:

- `wait_raw_caught_up.py` ждёт эквивалент маркера cdc-ingest
  `CDC RAW CAUGHT UP`: водяные знаки Kafka (confluent-kafka) совпадают
  с максимумами offsets в `raw.cdc_events` по всем 23 топикам;
- `load_dds` (контракт `commands.load` чекера ДЗ-2, запуск и человеком):
  - без аргументов — новая порция (batch_id=UUID), диапазоны
    `[ctl.kafka_positions, max(raw))`;
  - `--batch-id ID` — то же с переданным идентификатором;
  - `--batch-id ID --replay` — применить сохранённые в `ctl.load_batches`
    диапазоны; позиции не двигаются, строка порции не перезаписывается;
  - затронутые топики → затронутые staging-модели → `dbt run --select
    stg_x+ …` (in-process dbtRunner, target/log в /tmp — репозиторий ro);
    первая порция (`ctl.load_batches` пуст) идёт с `--full-refresh`;
  - успех → строка `ctl.load_batches(status=success)` + продвижение
    `ctl.kafka_positions`; также пересинхронизирует `ctl.receipt_pharmacy`
    (обогащение `pharmacy_bk` чека — маршрут shift → cash_register →
    pharmacy вне репликации) из `pos.contract.v_receipt`.

Механика порций и идемпотентность — ADR-011.

---

### `wait_ready.py`

Реализация readiness-проверки.

Проверяет topology, RW/RO routing, contract views и replay position обеих replicas.

Вызывается через:

```bash
./scripts/wait_ready.sh
```

---

### `seed.py`

Тонкий adapter между HA-инфраструктурой и генератором.

Он не генерирует данные сам. Вместо этого:

```text
scripts/infra/seed.py
    -> добавляет host/port/user/password для RW PgBouncer
    -> вызывает src/pharmacy_dwh/seed/cli.py
```

---

### `seed_delta.py`

Выделяет новый deterministic seed namespace через sequence:

```text
infra.delta_seed_seq
```

после чего запускает generator в `--delta` mode.

Это позволяет официальному replication checker каждый раз добавить действительно новую порцию данных.

---

### `submission.py`

Генерирует два HA submission manifest:

```text
hw01_submission.yaml
hw01_submission.container.yaml
```

Разница между ними только в сетевом контексте:

- host manifest использует `localhost`;
- container manifest использует Docker service names.

Runtime проекта от этого файла не зависит после того, как YAML уже сгенерированы, но он помогает не поддерживать два почти одинаковых manifest вручную.

---

## Где находится настоящая логика генератора

Сам generator живёт не в `scripts/infra`, а в:

```text
src/pharmacy_dwh/seed/
```

Там находятся:

- CLI;
- config;
- connection helpers;
- generation context;
- deterministic generation Catalog/CRM/WMS/POS;
- base/delta logic.

Таким образом:

```text
scripts/
    -> как запустить

scripts/infra/
    -> как связать запуск с HA-инфраструктурой

src/pharmacy_dwh/
    -> что именно генерируется
```
