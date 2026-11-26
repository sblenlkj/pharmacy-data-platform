# Загрузка DDS

Рабочая точка входа — scripts/load_dds. Она не читает бизнес-таблицы или
контрактные VIEW PostgreSQL. PostgreSQL используется fixture для записи
тестовых данных и официальным чекером для сверки.

Путь: raw.cdc_events → dwh_staging.stg_source_state → typed staging VIEW
→ 11 таблиц dim/fact → 13 представлений contract_dwh.

Исходный raw append-only, содержит payload Debezium и координаты Kafka.
stg_source_state — одна последняя обработанная запись на физический PK
внутри topic/partition. В таблицах версий каждый source id остаётся
отдельной записью, поэтому история не схлопывается в один SKU.

## Алгоритм

1. Получить lock на общем reports.
2. Прочитать или создать неизменяемые диапазоны порции в ctl.load_batches.
3. Обновить состояние только из этих диапазонов; старые/equal offsets
   не заменяют обработанные строки.
4. Выполнить затронутые ветки dbt DAG. JOIN использует полное обработанное
   состояние, WHERE в dim/fact оставляет ключи, затронутые строкой или
   её зависимостями. Строка старого чека соединяется со старым заголовком.
5. После успеха сохранить watermark и success. После сбоя — failed;
   повторить тот же ID. Iceberg-коммиты разных таблиц не атомарны.

Первый запуск создаёт все модели. Только он использует --full-refresh;
дельта и replay выполняют merge. Ручной dbt run без batch vars не является
рабочим способом загрузки: он может пересчитать все строки вне журнала.

## Команды

Из корня репозитория:
```bash
docker compose run --rm -T --no-deps tools python scripts/load_dds
docker compose run --rm -T --no-deps tools python scripts/load_dds --batch-id ID
docker compose run --rm -T --no-deps tools python scripts/load_dds --batch-id ID --replay
```

В successful replay позиции и диапазоны не меняются. Старые события
снова читаются, но не могут воскресить удалённого клиента или открыть
закрытую более новой порцией цену. Replay не восстанавливает DDS в
прошлый момент времени.

Нет физического удаления DDS: физический DELETE источника вызывает
ошибку порции. Требуемый заданием soft delete клиента — обычный UPDATE.

## Проверка

- Официальный сценарий и Docker Desktop: корневой README.
- Дополнительный живой тест: scripts/hw2/check_late_replay.py.
- Offline-регрессии: установить tests/hw2/requirements.txt, затем
  python -m unittest discover -s tests/hw2 -v.
  SQL моделей исполняется в DuckDB после преобразования Trino SELECT;
  тесты не проверяют Docker, Kafka, Iceberg и совместимость MERGE Trino.
- dbt parse проверяет структуру проекта без запуска стенда.

Ограничения, физическая работа и альтернативы — ADR 011.
