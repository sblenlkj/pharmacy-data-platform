# Источники кода и рецептов

Лог внешних источников по обеим частям проекта: доменной (схемы, инварианты,
контрактные представления, генератор данных) и инфраструктурной (Patroni,
etcd, HAProxy, PgBouncer, Compose). Заимствованных готовых compose-рецептов и
чужих фрагментов кода нет; ниже перечислены документация и материалы, по
которым команда сверялась.

## Доменная часть (migrations, src/pharmacy_dwh, tests)

Написана командой с нуля по материалам курса. Использованная документация
PostgreSQL 16:

- EXCLUDE-ограничения и btree_gist (I5, интервалы цен):
  https://www.postgresql.org/docs/16/ddl-constraints.html#DDL-CONSTRAINTS-EXCLUSION
  https://www.postgresql.org/docs/16/btree-gist.html
- DEFERRABLE constraint triggers (I3, I12):
  https://www.postgresql.org/docs/16/sql-createtrigger.html
- Partial unique indexes (одна активная карта/согласие/текущая версия):
  https://www.postgresql.org/docs/16/indexes-partial.html
- Generated columns (BK справочников, ADR 008):
  https://www.postgresql.org/docs/16/ddl-generated-columns.html
- Range types и функции для интервалов:
  https://www.postgresql.org/docs/16/functions-range.html
- Регулярные выражения в SQL (нормализация имён для BK):
  https://www.postgresql.org/docs/16/functions-matching.html#FUNCTIONS-POSIX-REGEXP
- Материалы курса (задание, контракт, чекер):
  https://github.com/mgcrp/hse_ftda_dwh_course_2026/tree/master/hw01

Генератор использует стандартную библиотеку `random` с фиксированными seed и
`psycopg2` (`execute_values`) без сторонних рецептов.

## Инфраструктурная часть (infra, scripts, Compose)

Инфраструктура подготовлена с помощью ChatGPT/Codex: обсуждение топологии,
генерация черновиков конфигов и их ревью. Официальные материалы, по которым
сверялись настройки:

- Patroni YAML, bootstrap, etcd3, pg_rewind:
  https://patroni.readthedocs.io/en/latest/yaml_configuration.html
- Patroni API, /primary, /replica, /cluster:
  https://patroni.readthedocs.io/en/latest/rest_api.html
- Установка Patroni и выбор зависимостей:
  https://patroni.readthedocs.io/en/latest/installation.html
- Закреплённый пакет Patroni 4.1.0:
  https://pypi.org/project/patroni/4.1.0/
- Запуск etcd в контейнерах, официальный registry и тег 3.5.21:
  https://etcd.io/docs/v3.5/op-guide/container/
- PgBouncer, session pooling и хранение паролей:
  https://www.pgbouncer.org/config.html
- HAProxy health checks:
  https://www.haproxy.com/documentation/haproxy-configuration-tutorials/reliability/health-checks/
- PostgreSQL 16 predefined roles (pg_monitor):
  https://www.postgresql.org/docs/16/predefined-roles.html
- PostgreSQL 16 publications:
  https://www.postgresql.org/docs/16/logical-replication-publication.html

## Инфраструктурная часть ДЗ-2 (лейкхаус, CDC, загрузка DDS)

Также сверялись с официальной документацией (готовых рецептов не заимствовали;
конфиги писали сами, по ссылкам — сверка свойств и ограничений):

- dbt-trino 1.12 (адаптер Trino для dbt-core 1.12; инкрементальные модели,
  merge-стратегия, profiles для Trino):
  https://github.com/dbt-labs/dbt-trino
  https://docs.getdbt.com/docs/connect-adapters/connect-trino
- Debezium 3.7 PostgreSQL-коннектор: слоты, publication'ы, snapshot.mode,
  `offset.mismatch.strategy=trust_slot` (возобновление с позиции слота после
  его пересоздания — без повторного снапшота; свойство изучено по исходникам
  тега v3.7.0.Final):
  https://debezium.io/documentation/reference/3.7/connectors/postgresql.html
  https://github.com/debezium/debezium/tree/v3.7.0.Final
- Официальный реестр образов Debezium после ухода из Docker Hub:
  https://quay.io/repository/debezium/connect
- Trino 484, коннектор Iceberg: каталог через Hive Metastore + S3 (MinIO),
  MERGE, партиционирование:
  https://trino.io/docs/current/connector/iceberg.html
- Образ apache/hive 4.0.1 (standalone metastore; добавление JDBC/S3-jar'ов
  в образ, hive-site.xml):
  https://hub.docker.com/r/apache/hive
- Образ bitnamilegacy/minio (официальный minio/minio удалён из Docker Hub
  в сентябре 2026; bitnami-legacy поддерживает актуальные теги, данные
  кладёт в /bitnami/minio/data):
  https://hub.docker.com/r/bitnamilegacy/minio
- Образ apache/kafka 4.3.1 (KRaft single-node, переменные окружения для
  комбинированного broker+controller):
  https://hub.docker.com/r/apache/kafka
- Patroni `permanent_slots` — постоянные репликационные слоты (пересоздание
  логических слотов CDC на новом лидере; в Patroni 4.1 ключ динамической
  конфигурации читается с верхнего уровня — сверено с установленным кодом
  4.1.0 в образе):
  https://patroni.readthedocs.io/en/latest/yaml_configuration.html

## Статус проверки

Сборка образов, запуск кластера, миграции, генерация данных, SQL-права,
репликация и failover проверены на практике: эксперименты Э1–Э7 выполнены
2026-10-18 на реальном стенде, сырые логи — в
[reports/failover/](../reports/failover/), измерения и выводы — в
[docs/failover_journal.md](failover_journal.md). Официальный чекер на этом
стенде — 17/21, единственное расхождение `ha.running` задокументировано в
[docs/infra/checker-compatibility.md](infra/checker-compatibility.md).

## Исправления после ревью 24.11.2026

Использовались официальные справочники синтаксиса и конфигурации:
- Trino MERGE: https://trino.io/docs/484/sql/merge.html
- dbt-trino incremental models: https://docs.getdbt.com/reference/resource-configs/trino-configs
- Debezium PostgreSQL connector: https://debezium.io/documentation/reference/stable/connectors/postgresql.html

Схема processed-state, фильтры зависимостей и регрессионные тесты добавлены
при доработке с LLM. Docker-прогон исправленной версии ещё не выполнен;
старые результаты выше относятся к предыдущим версиям.
