#!/usr/bin/env python3
"""Ожидание догонной доставки raw (сервис dwh-init, ДЗ-2).

Эквивалент маркера cdc-ingest «CDC RAW CAUGHT UP» (lag=0, буфер пуст):
для каждого из 23 CDC-топиков конец журнала Kafka (watermark) совпадает
с максимумом offset в raw.cdc_events + 1, т.е. всё опубликованное в Kafka
уже доставлено в raw. Чтение чужого лога из соседнего контейнера
невозможно (нет docker-сокета), поэтому условие маркера вычисляется
напрямую: Kafka — confluent-kafka (водяные знаки), Trino — максимумы
offsets. Пока условие не сошлось — ждём: cdc-ingest догоняет снапшот
(~5.5 мин на 200k событий); новые события, опубликованные за время
ожидания (например, delta), тоже учитываются.

Переменные: KAFKA_BOOTSTRAP (kafka:29092), CDC_SOURCES
(scripts/infra/cdc_sources.json), TRINO_HOST/PORT/USER/CATALOG,
RAW_WAIT_TIMEOUT (1800 c), RAW_WAIT_POLL (15 c).
"""
import json
import os
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
CDC_SOURCES = Path(os.environ.get("CDC_SOURCES", REPO / "scripts/infra/cdc_sources.json"))
TIMEOUT = int(os.environ.get("RAW_WAIT_TIMEOUT", "1800"))
POLL = float(os.environ.get("RAW_WAIT_POLL", "15"))

TRINO = {
    "host": os.environ.get("TRINO_HOST", "trino"),
    "port": int(os.environ.get("TRINO_PORT", "8080")),
    "user": os.environ.get("TRINO_USER", "admin"),
    "catalog": os.environ.get("TRINO_CATALOG", "iceberg"),
}


def topics():
    sources = json.loads(CDC_SOURCES.read_text(encoding="utf-8"))
    return [f"{service}.public.{table}"
            for service, tables in sources.items() for table in tables]


def kafka_ends(consumer, topic_list):
    """{topic: {partition: high watermark}} — конец журнала Kafka."""
    from confluent_kafka import TopicPartition

    metadata = consumer.list_topics(timeout=30)
    ends = {}
    for topic in topic_list:
        if topic not in metadata.topics or metadata.topics[topic].error:
            continue  # топик ещё не создан — событий нет, raw тоже пуст
        ends[topic] = {}
        for partition in metadata.topics[topic].partitions:
            _, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=30, cached=False)
            ends[topic][partition] = high
    return ends


def raw_ends(conn, topic_list):
    """{topic: {partition: max(offset)+1}} — максимум доставленного в raw.

    Схема raw создаётся сервисом cdc-ingest при старте; dwh-init может
    опросить Trino раньше (depends_on: service_started, не healthy), поэтому
    отсутствие схемы/таблицы трактуем как «raw ещё пуст» — это лишь частный
    случай «не догнал», ожидание продолжится до таймаута.
    """
    import trino.exceptions

    catalog = TRINO["catalog"]
    placeholders = ", ".join(f"'{t}'" for t in topic_list)
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT topic, partition_id, max(kafka_offset) + 1 AS end_offset "
            f"FROM {catalog}.raw.cdc_events WHERE topic IN ({placeholders}) "
            f"GROUP BY topic, partition_id")
    except trino.exceptions.TrinoUserError as error:
        if error.error_name in ("SCHEMA_NOT_FOUND", "TABLE_NOT_FOUND"):
            return {}
        raise
    ends = {}
    for topic, partition, end in cur.fetchall():
        ends.setdefault(topic, {})[partition] = end
    return ends


def caught_up(kafka, raw):
    """True, когда каждый watermark покрыт raw (дубли доставки не мешают:
    максимум offset не зависит от повторов строк)."""
    missing = []
    for topic, partitions in kafka.items():
        for partition, high in partitions.items():
            got = raw.get(topic, {}).get(partition, 0)
            if got < high:
                missing.append(f"{topic}[{partition}]: {got}/{high}")
    return missing


def main():
    import trino.dbapi
    from confluent_kafka import Consumer

    topic_list = topics()
    print(f"waiting for raw to catch up with Kafka: {len(topic_list)} topics", flush=True)
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "dwh-init-wait",        # водяные знаки, группа не участвует
        "enable.auto.commit": False,
    })
    conn = trino.dbapi.connect(
        host=TRINO["host"], port=TRINO["port"], user=TRINO["user"],
        catalog=TRINO["catalog"], timezone="UTC")

    deadline = time.monotonic() + TIMEOUT
    while True:
        kafka = kafka_ends(consumer, topic_list)
        raw = raw_ends(conn, topic_list)
        missing = caught_up(kafka, raw)
        total_kafka = sum(sum(p.values()) for p in kafka.values())
        total_raw = sum(sum(p.values()) for p in raw.values())
        if not missing:
            print(f"CDC RAW CAUGHT UP (confirmed): raw покрывает водяные знаки "
                  f"Kafka по всем топикам (end-offsets: kafka={total_kafka}, "
                  f"raw={total_raw})", flush=True)
            return 0
        if time.monotonic() > deadline:
            print(f"timeout waiting for raw (lag {len(missing)} partition(s): "
                  f"{missing[:5]}); kafka={total_kafka}, raw={total_raw}",
                  file=sys.stderr, flush=True)
            return 1
        print(f"  raw {total_raw}/{total_kafka} offsets, lag in "
              f"{len(missing)} partition(s); retry in {POLL:.0f}s", flush=True)
        time.sleep(POLL)


if __name__ == "__main__":
    sys.exit(main())
