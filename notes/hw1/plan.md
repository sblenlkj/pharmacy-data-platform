# HW1 — текущее состояние, структура проекта и план

Этот файл — рабочая карта HW1. Он нужен, чтобы быстро понять:

1. что от нас вообще требует задание;
2. что мы уже сделали;
3. зачем в репозитории лежат текущие SQL / shell / Ruby-файлы;
4. что ещё осталось;
5. какой следующий логичный шаг.

Исходные материалы преподавателя остаются источником истины:

```text
hws_descriptions/hw1/
├── README.md
├── invariants.md
├── hw01_contract.yaml
├── hw01_check.py
├── dwh_mapping.template.md
├── failover_journal.template.md
└── hw01_submission.example.yaml
```

Наши notes не заменяют эти файлы, а объясняют принятые нами решения.

---

# 1. Что вообще делаем в HW1

Есть четыре независимых исходных сервиса:

```text
CRM      -> crm_service_db
Catalog  -> catalog_service_db
POS      -> pos_service_db
WMS      -> wms_service_db
```

Нужно не просто придумать таблицы, а получить реальные PostgreSQL-источники, которые затем будут использоваться в следующих домашних заданиях для DWH.

Большие части HW1:

1. спроектировать четыре физические модели;
2. реализовать DB-level invariants;
3. реализовать обязательный внешний `contract.*`;
4. сгенерировать достаточно большой согласованный набор данных;
5. подготовить базы к CDC;
6. сделать локальный стенд с репликацией и PgBouncer;
7. заполнить DWH mapping и submission;
8. опционально/для полной версии — Patroni + etcd + HAProxy и failover experiments.

---

# 2. Где мы находимся сейчас

На текущий момент завершены первые три больших слоя:

```text
Domain design
      ↓
Physical PostgreSQL schema
      ↓
DB invariants I1–I12
      ↓
contract.* views + C01–C23
```

То есть основная логическая модель источников уже практически собрана.

## Уже сделано

### A. Спроектированы четыре базы

Наши подробные design notes:

```text
notes/hw1/dbs_schemas/
├── crm.md
├── catalog.md
├── pos.md
└── wms.md
```

Рядом лежат Mermaid ER-схемы `*.mmd`.

В этих Markdown-файлах описаны:

- таблицы;
- поля;
- ключи;
- история;
- soft delete;
- snapshots;
- ограничения;
- mapping в contract views;
- рассмотренные и отвергнутые альтернативы.

### B. Реализованы физические PostgreSQL-схемы

```text
db/migrations/
├── crm/
│   ├── 001_initial_schema.sql
│   ├── 002_invariants.sql
│   └── 003_contract_views.sql
├── catalog/
│   ├── 001_initial_schema.sql
│   ├── 002_invariants.sql
│   └── 003_contract_views.sql
├── pos/
│   ├── 001_initial_schema.sql
│   ├── 002_invariants.sql
│   └── 003_contract_views.sql
└── wms/
    ├── 001_initial_schema.sql
    ├── 002_invariants.sql
    └── 003_contract_views.sql
```

Смысл нумерации:

- `001` — физические таблицы, PK/FK, простые CHECK/UNIQUE, timestamps;
- `002` — сложные DB invariants и triggers;
- `003` — внешний интерфейс `contract.*`.

### C. Поднят минимальный Docker-стенд

Сейчас один PostgreSQL-контейнер содержит четыре отдельные базы:

```text
PostgreSQL container
├── crm_service_db
├── catalog_service_db
├── pos_service_db
└── wms_service_db
```

Это разрешено условиями HW1.

Файлы:

```text
docker-compose.yml
docker/postgres/init/00-create-service-databases.sh
```

Init-скрипт:

1. создаёт четыре DB;
2. последовательно применяет migrations для каждого сервиса.

Важно: PostgreSQL `docker-entrypoint-initdb.d` выполняется только на пустом volume.

Поэтому для проверки всех миграций с нуля используем:

```bash
docker compose down -v
docker compose up -d --wait
```

Позже можно решить, нужен ли отдельный migration runner. Пока для учебного стенда clean recreation достаточно.

---

# 3. Что такое tests/invariants

```text
tests/invariants/
├── I01_...
├── I02_...
├── ...
├── I12_...
├── P01_updated_at.sql
└── P02_customer_soft_delete.sql
```

Это не обычные unit tests.

Преподаватель требует для каждого обязательного invariant отдельный SQL-файл.

Негативный тест специально пытается записать запрещённое состояние и **обязан упасть**.

Например:

```text
I5 -> пытаемся создать пересекающиеся интервалы цены
    -> PostgreSQL должен вернуть exclusion_violation

I10 -> пытаемся продать Rx без prescription
     -> constraint trigger должен отклонить transaction
```

Именно SQLSTATE ошибки потом указывается в submission manifest.

## scripts/test_invariants.sh

```text
scripts/test_invariants.sh
```

Это наш локальный helper.

Он:

1. берёт каждый SQL invariant test;
2. запускает его в Docker PostgreSQL;
3. ожидает ошибку;
4. вытаскивает реальный SQLSTATE;
5. сверяет его с ожидаемым;
6. отдельно запускает P1/P2, которые наоборот должны вернуть `ok = true`.

Он не является требованием преподавателя сам по себе. Это удобный способ быстро проверить все обязательные SQL tests.

Запуск:

```bash
./scripts/test_invariants.sh
```

Сейчас I1–I12 и P1/P2 проходят.

---

# 4. Какие invariants уже реализованы

Обязательные I1–I12 закрыты на уровне PostgreSQL.

Ключевые решения:

### I1 — unique BK

PK / UNIQUE.

### I2 — immutable BK

Trigger запрещает изменение business key через UPDATE.

### I3 — receipt total = сумма строк

Deferred constraint trigger.

Физического `line_amount` нет:

```text
line_amount =
quantity * unit_price - line_discount
```

При проверке receipt row блокируется через `FOR UPDATE`, чтобы конкурентные изменения одного чека сериализовались.

### I4 — арифметика receipt line

CHECK:

- quantity > 0;
- unit_price >= 0;
- line_discount >= 0;
- discount <= gross amount.

### I5 — price interval overlap

```text
EXCLUDE USING gist
```

по:

```text
sku + price_scope + scope_bk + [valid_from, valid_to)
```

### I6 — одна current version

Partial UNIQUE indexes.

### I7 — stock неотрицательный

Movement автоматически изменяет materialized stock в той же DB transaction.

Внутренние stock locations:

```text
dc
pharmacy
```

Внешние endpoints:

```text
supplier
customer
NULL
```

После INSERT movement неизменяем.

UPDATE/DELETE movement запрещены; исправление делается compensating movement.

### I8 — нельзя продать expired batch

POS хранит локальный snapshot:

```text
batch_expiry_date_snapshot
```

Это позволяет POS самостоятельно проверить invariant без запроса в WMS.

### I9 — нельзя принять expired batch

WMS CHECK по `expiry_date` и `received_at`.

### I10 — Rx без prescription

POS использует локальный:

```text
is_rx_snapshot
```

и deferred constraint trigger.

### I11 — refund <= sold

Refund — обычный receipt:

```text
doc_type = refund
parent_receipt_bk -> исходный sale
```

Его собственные receipt lines показывают, что именно вернули.

Проверяется cumulative returned quantity по SKU с учётом предыдущих refunds.

### I12 — payments = receipt total

Deferred constraint trigger.

Payment amount всегда положительный.

Направление определяется receipt type:

```text
sale   -> деньги получили
refund -> деньги вернули
```

Receipt row также блокируется через `FOR UPDATE` перед агрегированием payments.

---

# 5. Что такое contract views

Преподаватель не хочет зависеть от нашей внутренней физической модели.

Поэтому каждая DB обязана предоставить стабильный interface:

```text
contract.*
```

Его описание находится в:

```text
hws_descriptions/hw1/hw01_contract.yaml
```

Сейчас реализованы все 14 required views:

## CRM

```text
contract.v_customer
contract.v_loyalty_card
```

## Catalog

```text
contract.v_sku
contract.v_price
```

## POS

```text
contract.v_pharmacy
contract.v_receipt
contract.v_receipt_line
contract.v_payment
```

## WMS

```text
contract.v_supplier
contract.v_distribution_center
contract.v_batch
contract.v_purchase_line
contract.v_movement
contract.v_stock
```

---

# 6. Почему часть полей есть только во views

Мы специально не стали физически хранить вычисляемые значения.

## POS

Не храним:

```text
receipt_line.line_amount
receipt.discount_amount
```

В views:

```text
line_amount =
quantity * unit_price - line_discount

discount_amount =
SUM(line_discount)
```

## WMS

Не храним:

```text
purchase_line.line_cost
purchase_line.received_at
```

Во view:

```text
line_cost =
quantity * unit_cost

received_at =
batch.received_at
```

Это уменьшает риск рассинхронизации вычисляемых данных.

---

# 7. Зачем появился Ruby

Сейчас есть:

```text
scripts/test_contracts.sh
scripts/test_contracts.rb
```

Ruby **не является частью архитектуры HW и не используется приложением**.

Это только локальный validation helper.

Причина его появления простая: Codex понадобился маленький скрипт, который напрямую читает:

```text
hw01_contract.yaml
```

и автоматически сравнивает его с реальными PostgreSQL views.

Ruby выбран потому, что в стандартной библиотеке есть YAML parser и для этого не пришлось добавлять ещё одну Python dependency.

## scripts/test_contracts.sh

Это тонкая shell-обёртка:

```bash
./scripts/test_contracts.sh
```

она просто запускает Ruby validator.

## scripts/test_contracts.rb

Он проверяет:

1. что все contract views существуют;
2. что имена колонок совпадают;
3. что порядок колонок совпадает;
4. что PostgreSQL types совместимы с YAML;
5. что обязательные поля не NULL на test fixtures;
6. что нет cross-DB dependency;
7. что во view нет volatile `now()/random()/clock_timestamp()`;
8. что C01–C23 возвращают 0 violations.

Это **наш dev helper**, а не обязательная часть сдачи.

Позже можем:

- оставить его;
- переписать на Python;
- удалить, если он больше не нужен.

Сейчас он полезен, потому что автоматически ловит несовпадение contract с YAML.

---

# 8. Что такое tests/contracts

```text
tests/contracts/
```

Это маленькие validation fixtures.

Они нужны потому, что проверить view на пустой БД недостаточно.

Например пустой `contract.v_receipt` может успешно SELECT-иться даже если JOIN написан неправильно.

Поэтому fixtures временно создают несколько связанных строк и проверяют реальные:

- JOIN;
- aggregates;
- snapshots;
- computed fields;
- current rows;
- refund shape;
- received vs pending purchase line.

Fixtures выполняются внутри transaction и откатываются.

Это **не seed generator**.

---

# 9. Что уже проверено

После task 003 была полностью пересоздана DB:

```bash
docker compose down -v
docker compose up -d --wait
```

После этого прошли:

```bash
./scripts/test_invariants.sh
./scripts/test_contracts.sh
```

Проверено:

- migrations 001 + 002 + 003;
- I1–I12;
- P1;
- P2 уже против настоящего `contract.v_customer`;
- все 14 contract views;
- column names/order/types;
- representative non-empty fixtures;
- C01–C23;
- отсутствие физически возвращённых derived columns.

На текущем маленьком validation dataset:

```text
C01–C23 = 0 violations
```

---

# 10. Важные design decisions, которые уже приняли

## Business keys

Стабильные human-readable BK являются integration identity.

Где это удобно, BK используется прямо как PK.

## Cross-service references

Никаких FK между databases.

Например:

```text
POS.receipt_line.sku
        ↓ logical BK
Catalog.sku.sku_bk
```

## Catalog history

Историзируем явно меняющиеся:

```text
sku_name
category
is_rx
```

через `sku_version`.

Price history — полноценные интервалы.

## SKU ingredients

M:N:

```text
sku <-> sku_active_ingredient <-> active_ingredient
```

Во внешний contract несколько МНН агрегируются в один deterministic string.

## Customer profile

История:

```text
city
region
loyalty_level
```

через `customer_profile_version`.

## Contacts

Не историзируем значения PII.

При customer soft delete контакты обезличиваются.

## Movement

Одна polymorphic movement table.

Мы рассматривали subtype tables, но отказались.

## Stock

Materialized table, которую movement изменяет атомарно.

## POS snapshots

На момент продажи физически фиксируются:

```text
unit_price
sku_name_snapshot
is_rx_snapshot
batch_expiry_date_snapshot
```

Последний snapshot — наше дополнительное поле для локального enforcement I8.

---

# 11. Известное наблюдение, которое пока не блокирует работу

Soft delete обычных справочников ещё требует окончательного осмысления для historical analytics.

Например сейчас:

```text
contract.v_pharmacy
WHERE deleted_at IS NULL
```

а `v_receipt` получает pharmacy через JOIN на текущую pharmacy.

Теоретически после soft delete pharmacy исторические receipts могут исчезнуть из contract view.

Для customer это намеренно по заданию.

Для pharmacy/supplier/DC это не так явно.

Пока checker и текущая модель это не ломают, поэтому не переделываем сейчас. Вернёмся к вопросу при seed / DWH mapping, если он станет практически значимым.

---

# 12. Артефакты задач Codex

Каждая существенная repository task хранится отдельно:

```text
.artifacts/
├── tasks/
│   ├── 001-initial-database-ddl.md
│   ├── 002-database-invariants.md
│   └── 003-contract-views.md
└── reports/
    ├── 001-initial-database-ddl-report.md
    ├── 002-database-invariants-report.md
    └── 003-contract-views-report.md
```

Task = что Codex должен был сделать.

Report = что фактически сделал и чем проверил.

Это удобно использовать при review и продолжении работы.

---

# 13. Что ещё НЕ сделано

Теперь начинается следующая большая половина HW1.

## A. Финальный seed generator

Нужно:

```text
scripts/seed.sh
+
Python code в src/pharmacy_dwh
```

Требования:

- deterministic: одинаковый `--seed N` -> одинаковые данные;
- idempotent при повторном запуске;
- умеет догенерировать новые данные;
- cross-service consistency;
- минимум 12 месяцев history;
- последний месяц current;
- required minimum row counts.

Минимальные объёмы из README:

```text
customers             >= 2 000
loyalty cards         >= 1 500
SKU                   >=   500
price versions        >= 1 500
pharmacies            >=    30
batches               >= 3 000
receipts              >=20 000
receipt lines         >=60 000
payments              >=20 000
purchase lines        >= 5 000
movements             >=20 000
stock rows            >= 5 000
suppliers             >=    10
distribution centers  >=     3
```

Это ближайший крупный технический этап.

## B. Cross-service checks X01–X10

После seed нужно проверять настоящую согласованность между DB:

- sold SKU exists in Catalog;
- purchased SKU exists in Catalog;
- batch SKU exists in Catalog;
- receipt customer exists in CRM;
- receipt batch exists in WMS;
- movement pharmacy exists in POS;
- expired batch not sold;
- valid price existed at sale time;
- receipt SKU == batch SKU;
- Rx snapshot consistency.

Эти проверки делает преподавательский checker уже через разные databases.

## C. ER diagrams в docs/erd

У нас исходные `*.mmd` уже есть в notes.

Нужно перенести/оформить итоговые diagrams в required submission location:

```text
docs/erd/
```

и убедиться, что там видны:

- keys;
- types;
- nullability;
- logical cross-service BK references.

## D. ADR

Нужно минимум 5 ADR под:

```text
docs/adr/
```

Обязательные темы:

1. business key format;
2. price history representation;
3. materialized stock vs computed stock;
4. I10 / is_rx snapshot;
5. наша дополнительная design choice.

Хороший пятый кандидат:

```text
single polymorphic movement
vs
typed movement subtype tables
```

У нас reasoning уже записан в notes, поэтому ADR можно собрать из существующих решений.

## E. CDC readiness

Для всех четырёх сервисов:

- `wal_level = logical`;
- publication всех base tables;
- корректный `REPLICA IDENTITY`;
- updated_at;
- delete tracking.

## F. Async physical replica

Базовая обязательная часть:

- replication slot;
- streaming;
- replica реально находится в receiving/streaming state.

## G. PgBouncer

Checker/application должны подключаться через pooler.

## H. DWH mapping

Нужно заполнить:

```text
docs/dwh_mapping.md
```

по template.

Это также хороший финальный sanity check модели перед DWH HW2.

## I. Submission manifest

Нужно собрать:

```text
hw01_submission.yaml
```

включая:

- connections;
- invariant test files;
- expected SQLSTATE;
- остальные required submission fields.

## J. Full course checker

Запустить:

```text
hws_descriptions/hw1/hw01_check.py
```

уже на настоящем assignment-scale dataset и инфраструктуре.

## K. Полная HA-часть

Для полного основного балла:

```text
Patroni
etcd
HAProxy
3 PostgreSQL nodes
```

И bonus:

```text
docs/failover_journal.md
```

с настоящими измерениями и raw logs.

---

# 14. Рекомендуемый порядок оставшейся работы

Теперь последовательность немного меняется по сравнению с первоначальным plan.

## Этап 4 — Seed generator

Сначала сделать большой согласованный dataset.

Почему сейчас:

- физическая модель уже стабильна;
- invariants готовы;
- contract готов;
- seed сразу начнёт находить реальные проблемы модели.

После seed обязательно прогнать:

```text
I1–I12
C01–C23
X01–X10
volume checks
```

## Этап 5 — ERD + ADR + DWH mapping

Когда seed подтвердит модель:

- финализировать diagrams;
- оформить ADR;
- заполнить mapping.

## Этап 6 — CDC + replica + PgBouncer

После стабильных данных и схемы:

- logical WAL settings;
- publications;
- replica identity;
- async physical replica;
- PgBouncer.

## Этап 7 — submission + full checker

Собрать manifest и прогнать преподавательский checker целиком.

## Этап 8 — Patroni / HA / bonus

Только после зелёной базовой части.

---

# 15. Ближайший следующий шаг

Следующий крупный task для Codex должен быть:

```text
deterministic cross-service seed generator
```

Но перед запуском стоит отдельно 10–15 минут обсудить стратегию генерации.

Нам надо решить:

- в каком порядке генерируются сервисы;
- как гарантировать одинаковые BK между DB;
- как строить 12 месяцев истории;
- как генерировать movements так, чтобы stock всегда был валиден;
- как связать POS sale с реально существующим WMS batch;
- как выбирать действующую на дату продажи price;
- как делать refunds;
- сколько данных генерировать за один batch;
- как реализовать idempotency и append.

После этого можно оформить task 004 для Codex.

---

# 16. Быстрая шпаргалка — что запускать сейчас

Полный clean start:

```bash
docker compose down -v
docker compose up -d --wait
```

Посмотреть состояние:

```bash
docker compose ps
```

Проверить DB invariants:

```bash
./scripts/test_invariants.sh
```

Проверить contract views:

```bash
./scripts/test_contracts.sh
```

Если обе команды зелёные, текущий реализованный слой HW1 находится в ожидаемом состоянии.

---

# 17. Точка остановки на сегодня

На сегодняшний момент разумно считать завершёнными:

- дизайн четырёх source DB;
- PostgreSQL DDL;
- минимальный Docker runtime;
- mandatory DB invariants;
- invariant SQL tests;
- contract schema/views;
- contract shape validation;
- C01–C23;
- P1/P2.

Следующая сессия начинается с **проектирования seed generator**, а не с возврата к таблицам с нуля.
