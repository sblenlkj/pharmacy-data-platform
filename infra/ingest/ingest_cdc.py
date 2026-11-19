#!/usr/bin/env python3
"""CDC ingest-сервис ДЗ-2: Kafka (Debezium) → iceberg.raw.cdc_events.

Доставка at-least-once (ADR-011): батч из BATCH_ROWS событий (или данные
старше FLUSH_SECONDS) пишется в Trino INSERT ... VALUES чанками, нарезанными
по бюджету текста стейтмента INSERT_BUDGET (лимит Trino — 1 МБ); offsets
коммитятся строго ПОСЛЕ успешной записи батча.
Физические дубли по (topic, partition_id, kafka_offset) в raw допустимы —
снимаются представлением stg_cdc_events / v_cdc_events (row_number).

Колонки (hws_descriptions/hw2/checker_protocol.md):
  event_key     JSON-текст ключа Kafka после снятия обёртки {schema, payload}
                (на стенде конвертеры Connect с schemas.enable=false — ключ
                уже «голый», обёртка снимается на всякий случай);
  payload       полный envelope Debezium (before/after/source/op/ts_ms)
                JSON-текстом — для переобработки на шаге 3 без Kafka;
  source_system первый сегмент топика: crm | catalog | pos | wms;
  source_table  '<schema>.<table>' из топика.

Старт: auto.offset.reset=earliest — новая группа (и новые топики группы)
читает снапшот с начала; committed offsets у живой группы сохраняются.

Переменные окружения и значения по умолчанию — в CONFIG ниже.
"""
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import OFFSET_INVALID, Consumer, KafkaException, TopicPartition
from trino import dbapi as trino_dbapi

CONFIG = {
    "kafka": os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092"),
    "group": os.environ.get("GROUP_ID", "cdc-ingest-raw"),
    "topics": [t for t in os.environ.get("TOPICS", "").split(",") if t],  # override
    "cdc_sources": os.environ.get("CDC_SOURCES", "/app/cdc_sources.json"),
    "trino_host": os.environ.get("TRINO_HOST", "trino"),
    "trino_port": int(os.environ.get("TRINO_PORT", "8080")),
    "trino_user": os.environ.get("TRINO_USER", "admin"),
    "trino_catalog": os.environ.get("TRINO_CATALOG", "iceberg"),
    "ddl_path": os.environ.get("DDL_PATH", "/app/ddl/00_raw_ctl.sql"),
    "batch_rows": int(os.environ.get("BATCH_ROWS", "5000")),
    "flush_seconds": float(os.environ.get("FLUSH_SECONDS", "5")),
    # бюджет ТЕКСТА одного INSERT (символов): Trino отвергает стейтменты
    # длиннее 1 000 000 (QUERY_TEXT_TOO_LARGE) — режем батч по бюджету,
    # а не по числу строк (payload события ~1 КБ, но бывает шире)
    "insert_budget": int(os.environ.get("INSERT_BUDGET", "850000")),
    "idle_polls_to_marker": int(os.environ.get("IDLE_POLLS_TO_MARKER", "4")),
}

log = logging.getLogger("cdc-ingest")


def unwrap(node):
    """Снять обёртку Kafka Connect {schema, payload}, если она есть.

    Ожидает dict с ОБИМИ ключами: envelope Debezium без обёртки содержит
    before/after/source/op и пары schema+payload сверху не имеет.
    """
    if isinstance(node, dict) and "payload" in node and "schema" in node:
        return node["payload"]
    return node


def dump(node):
    """Компактный JSON-текст (ensure_ascii=False — кириллица как есть)."""
    return json.dumps(node, separators=(",", ":"), ensure_ascii=False)


def load_topics():
    if CONFIG["topics"]:
        return list(CONFIG["topics"])
    sources = json.loads(open(CONFIG["cdc_sources"], encoding="utf-8").read())
    return [f"{service}.public.{table}"
            for service, tables in sources.items() for table in tables]


def ddl_statements(text):
    """Полные строки-комментарии выкинуть, разбить по ';' (файл — плоский DDL)."""
    lines = [ln for ln in text.splitlines() if not ln.lstrip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


class Ingest:
    def __init__(self):
        self.stopping = False
        self.rows = []                                  # батч: 8 полей без ts
        self.batch_offsets = {}                         # (topic, partition) → last offset
        self.first_msg_at = None
        self.totals = {"rows": 0, "batches": 0, "skipped": 0, "parse_errors": 0}
        self.started_at = time.monotonic()
        self.marker_logged = False                      # «CAUGHT UP» один раз на эпизод
        self.idle_polls = 0
        self.conn = None

    # ---------- Trino ----------

    def connect(self):
        self.conn = trino_dbapi.connect(
            host=CONFIG["trino_host"], port=CONFIG["trino_port"],
            user=CONFIG["trino_user"], catalog=CONFIG["trino_catalog"],
            timezone="UTC")
        cur = self.conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchall()

    def apply_ddl(self):
        stmts = ddl_statements(open(CONFIG["ddl_path"], encoding="utf-8").read())
        cur = self.conn.cursor()
        for stmt in stmts:
            cur.execute(stmt)
            cur.fetchall()
        log.info("DDL applied: %d statements (idempotent, IF NOT EXISTS)", len(stmts))

    def build_chunks(self, rows):
        """Нарезка батча на INSERT-чанки по бюджету текста стейтмента
        (детерминированная — ретраи повторяют те же границы чанков)."""
        budget = CONFIG["insert_budget"]
        chunks, current, size = [], [], 0
        for row in rows:
            row_size = len(row[5]) + len(row[7]) + 96   # event_key + payload + остальное
            if current and size + row_size > budget:
                chunks.append(current)
                current, size = [], 0
            current.append(row)
            size += row_size
        if current:
            chunks.append(current)
        return chunks

    def insert_batch(self, rows, ts):
        """INSERT батча чанками по бюджету; при сбое — переподключение и
        повтор УПАВШЕГО чанка (повтор уже применённого чанка даёт
        физический дубль — допустимо, дедуп на чтении)."""
        placeholder = "(?,?,?,?,?,?,?,?,?)"
        for chunk in self.build_chunks(rows):
            attempt = 0
            while True:
                attempt += 1
                try:
                    cur = self.conn.cursor()
                    sql = (f"INSERT INTO {CONFIG['trino_catalog']}.raw.cdc_events VALUES "
                           + ",".join([placeholder] * len(chunk)))
                    cur.execute(sql, [v for row in chunk for v in (*row, ts)])
                    cur.fetchall()
                    break
                except Exception as exc:  # noqa: BLE001 — произвольный сбой клиента/координатора
                    if attempt >= 5:
                        raise
                    wait = min(2 ** attempt, 30)
                    log.warning("insert chunk of %d rows failed (%s), retry in %ss",
                                len(chunk), exc, wait)
                    time.sleep(wait)
                    try:
                        self.connect()
                    except Exception as retry_exc:  # noqa: BLE001
                        log.warning("trino reconnect failed: %s", retry_exc)

    # ---------- Kafka ----------

    def parse_event(self, msg):
        """Kafka-сообщение → строка raw.cdc_events (без ingested_at) или None."""
        if msg.value() is None:          # tombstone (на стенде отключены)
            return None
        try:
            envelope = unwrap(json.loads(msg.value()))
            op = envelope.get("op")
            if not op:                   # не-CDC сообщение (heartbeat/транзакции)
                return None
            key = unwrap(json.loads(msg.key())) if msg.key() is not None else None
        except (json.JSONDecodeError, UnicodeDecodeError, AttributeError) as exc:
            self.totals["parse_errors"] += 1
            log.error("unparsable message %s[%d]@%d: %s",
                      msg.topic(), msg.partition(), msg.offset(), exc)
            return None
        parts = msg.topic().split(".", 2)
        source_system = parts[0]
        source_table = f"{parts[1]}.{parts[2]}" if len(parts) == 3 else msg.topic()
        return (msg.topic(), msg.partition(), msg.offset(),
                source_system, source_table, dump(key), op, dump(envelope))

    def on_revoke(self, _consumer, partitions):
        log.info("rebalance: revoking %d partition(s)", len(partitions))
        if self.rows:
            self.flush("revoke")

    def flush(self, reason):
        if not self.rows:
            return
        ts = datetime.now(timezone.utc)
        self.insert_batch(self.rows, ts)
        offsets = [TopicPartition(topic, partition, offset + 1)
                   for (topic, partition), offset in self.batch_offsets.items()]
        self.consumer.commit(offsets=offsets, asynchronous=False)
        self.totals["rows"] += len(self.rows)
        self.totals["batches"] += 1
        log.info("flush reason=%s rows=%d total=%d elapsed=%.1fs rate=%.0f/s",
                 reason, len(self.rows), self.totals["rows"],
                 time.monotonic() - self.started_at,
                 self.totals["rows"] / max(time.monotonic() - self.started_at, 1e-9))
        self.rows, self.batch_offsets, self.first_msg_at = [], {}, None
        self.idle_polls = 0

    def lag(self):
        total = 0
        try:
            assignment = list(self.consumer.assignment())
            if assignment:
                for tp, pos in zip(assignment, self.consumer.position(assignment)):
                    _, high = self.consumer.get_watermark_offsets(tp, cached=True, timeout=5)
                    if pos.offset != OFFSET_INVALID:
                        total += max(0, high - pos.offset)
        except KafkaException as exc:
            log.warning("lag calculation failed: %s", exc)
        return total

    def maybe_marker(self):
        """CDC RAW CAUGHT UP: lag=0, буфер пуст, новых сообщений нет.
        Повторяется при появлении новой порции данных (эпизод)."""
        if self.rows:
            self.marker_logged = False
            return
        if self.marker_logged:
            return
        self.idle_polls += 1
        if self.idle_polls < CONFIG["idle_polls_to_marker"]:
            return
        current = self.lag()
        if current == 0:
            log.info("CDC RAW CAUGHT UP: total_rows=%d batches=%d skipped=%d "
                     "elapsed=%.1fs lag=0",
                     self.totals["rows"], self.totals["batches"],
                     self.totals["skipped"], time.monotonic() - self.started_at)
            self.marker_logged = True
        else:
            self.idle_polls = 0

    # ---------- основной цикл ----------

    def stop(self, *_args):
        if not self.stopping:
            log.info("stop signal received")
        self.stopping = True

    def run(self):
        logging.basicConfig(
            level=os.environ.get("LOG_LEVEL", "INFO"),
            format="%(asctime)s %(levelname)s %(name)s %(message)s")
        log.info("config: %s", CONFIG)

        deadline = time.monotonic() + 300
        while True:
            try:
                self.connect()
                break
            except Exception as exc:  # noqa: BLE001
                if time.monotonic() > deadline:
                    raise
                log.warning("waiting for trino: %s", exc)
                time.sleep(3)
        self.apply_ddl()

        topics = load_topics()
        log.info("subscribing to %d topics (group=%s, auto.offset.reset=earliest)",
                 len(topics), CONFIG["group"])
        self.consumer = Consumer({
            "bootstrap.servers": CONFIG["kafka"],
            "group.id": CONFIG["group"],
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": False,
        })
        self.consumer.subscribe(topics, on_revoke=self.on_revoke)

        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)

        while not self.stopping:
            msg = self.consumer.poll(0.5)
            if msg is None:
                if self.rows and self.first_msg_at and \
                        time.monotonic() - self.first_msg_at >= CONFIG["flush_seconds"]:
                    self.flush("interval")
                else:
                    self.maybe_marker()
                continue
            if msg.error():
                log.error("kafka error: %s", msg.error())
                if msg.error().fatal():
                    raise KafkaException(msg.error())
                continue
            row = self.parse_event(msg)
            if row is None:
                self.totals["skipped"] += 1
            else:
                self.rows.append(row)
                self.batch_offsets[(msg.topic(), msg.partition())] = msg.offset()
                self.first_msg_at = self.first_msg_at or time.monotonic()
            if len(self.rows) >= CONFIG["batch_rows"]:
                self.flush("size")

        self.flush("shutdown")
        self.consumer.close()
        log.info("stopped: %s", self.totals)
        return 0


if __name__ == "__main__":
    sys.exit(Ingest().run())
