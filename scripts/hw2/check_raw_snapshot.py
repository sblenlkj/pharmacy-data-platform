#!/usr/bin/env python3
"""Сверка raw-снапшота ДЗ-2: Kafka → iceberg.raw.cdc_events.

Повторяет на реальном снапшоте пункты cdc.*.snapshot официального чекера
(hws_descriptions/hw2/checker_protocol.md):

  1. cdc.snapshot — по каждому source_system есть события op='r' (начальный
     снапшот Debezium доставлен);
  2. полнота — по каждому из 23 топиков число событий в v_cdc_events равно
     сумме end-offsets Kafka (нет потерь и лишних);
  3. ключевые таблицы — число snapshot-событий равно числу строк источника:
     customer (crm), sku (catalog), receipt_line (pos), movement (wms);
     счётчики источника снимаются psql через pgbouncer по контрактным
     представлениям (crm: v_customer + soft-deleted, ADR-008 — снапшот
     покрывает ВСЕ строки таблицы, представление скрывает удалённых);
  4. дедуп — координаты (topic, partition_id, kafka_offset) в v_cdc_events
     уникальны; raw ≥ v (физические дубли доставки at-least-once допустимы,
     снимаются stg_cdc_events/v_cdc_events);
  5. форма — event_key валиден как JSON, source_table = 'schema.table',
     source_system ∈ {crm, catalog, pos, wms}.

Запуск из WSL (host-порты недоступны — все обращения через docker.exe exec,
python3 stdlib, jq нет):

  python3 scripts/hw2/check_raw_snapshot.py

Имена контейнеров переопределяются переменными окружения TRINO_CONTAINER,
PG_RO_CONTAINER, PG_RW_CONTAINER, KAFKA_CONTAINER.
"""
import csv
import io
import json
import os
import subprocess
import sys
from pathlib import Path

TRINO = os.environ.get("TRINO_CONTAINER", "pharmacy-hw1-trino-1")
PG_RO = os.environ.get("PG_RO_CONTAINER", "pharmacy-hw1-pgbouncer-ro-1")
PG_RW = os.environ.get("PG_RW_CONTAINER", "pharmacy-hw1-pgbouncer-rw-1")
KAFKA = os.environ.get("KAFKA_CONTAINER", "pharmacy-hw1-kafka-1")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_INNER", "localhost:29092")

V = "iceberg.contract_dwh.v_cdc_events"
RAW = "iceberg.raw.cdc_events"
REPO = Path(__file__).resolve().parents[2]
with open(REPO / "scripts" / "infra" / "cdc_sources.json", encoding="utf-8") as fh:
    _CDC_SOURCES = json.load(fh)
SOURCE_SYSTEMS = tuple(_CDC_SOURCES)
EXPECTED_TOPICS = frozenset(
    f"{service}.public.{table}"
    for service, tables in _CDC_SOURCES.items() for table in tables)

# ключевые таблицы сверки: топик → (бд, ожидаемое число строк psql, нужен ли rw-пул)
KEY_TABLES = {
    # rw=True — запрос лезет в public.* (нужна роль app через RO-пул не катит:
    # гранты checker только на contract)
    "crm.public.customer": (
        "crm_service_db",
        "SELECT (SELECT count(*) FROM contract.v_customer) + "
        "(SELECT count(*) FROM public.customer WHERE deleted_at IS NOT NULL)", True),
    "catalog.public.sku": ("catalog_service_db",
                           "SELECT count(*) FROM contract.v_sku", False),
    "pos.public.receipt_line": ("pos_service_db",
                                "SELECT count(*) FROM contract.v_receipt_line", False),
    "wms.public.movement": ("wms_service_db",
                            "SELECT count(*) FROM contract.v_movement", False)
}


def run(args, **kwargs):
    result = subprocess.run(args, capture_output=True, text=True, timeout=120, **kwargs)
    if result.returncode != 0:
        raise RuntimeError(f"{' '.join(args[:3])}… → rc={result.returncode}: "
                           f"{result.stderr.strip()[:400]}")
    return result.stdout


def trino(sql):
    out = run(["docker.exe", "exec", TRINO, "trino", "--output-format", "CSV",
               "--execute", sql])
    return list(csv.reader(io.StringIO(out)))


def trino_scalar(sql):
    return trino(sql)[0][0]


def psql(container, user, password, db, sql):
    out = run(["docker.exe", "exec", "-e", f"PGPASSWORD={password}", container,
               "psql", "-h", "127.0.0.1", "-p", "6432", "-U", user, "-d", db,
               "-XAt", "-c", sql])
    return out.strip()


def kafka_end_offsets():
    """{topic: сумма offsets по партициям} одного вызова get-offsets."""
    out = run(["docker.exe", "exec", KAFKA, "/opt/kafka/bin/kafka-get-offsets.sh",
               "--bootstrap-server", KAFKA_BOOTSTRAP])
    offsets = {}
    for line in out.splitlines():
        parts = line.rsplit(":", 2)
        if len(parts) == 3 and parts[0].count(".") == 2 and parts[2].isdigit():
            offsets[parts[0]] = offsets.get(parts[0], 0) + int(parts[2])
    return offsets


class Report:
    def __init__(self):
        self.failures = 0

    def run(self, name, check):
        try:
            detail = check()
        except Exception as exc:  # noqa: BLE001 — отказ проверки не должен ронять остальные
            self.failures += 1
            print(f"FAIL {name}: {exc}")
            return
        print(f"pass {name}: {detail}")


def check_snapshot_ops():
    rows = {r[0]: r for r in trino(
        f"SELECT source_system, count(*), "
        f"count(*) FILTER (WHERE op = 'r'), "
        f"count(*) FILTER (WHERE op <> 'r') FROM {V} GROUP BY 1")}
    missing = [s for s in SOURCE_SYSTEMS if s not in rows]
    assert not missing, f"нет source_system: {missing}"
    for system in SOURCE_SYSTEMS:
        _, total, snap, changes = rows[system]
        assert int(snap) > 0, f"{system}: нет snapshot-событий op='r'"
    return "; ".join(f"{s}: total={rows[s][1]}, op_r={rows[s][2]}, другие={rows[s][3]}"
                     for s in SOURCE_SYSTEMS)


def check_topic_totals():
    kafka = kafka_end_offsets()
    assert set(kafka) == EXPECTED_TOPICS, (
        f"ожидались {len(EXPECTED_TOPICS)} топиков из cdc_sources.json, "
        f"в Kafka {len(kafka)}; нет: {sorted(EXPECTED_TOPICS - set(kafka))}, "
        f"лишние: {sorted(set(kafka) - EXPECTED_TOPICS)}")
    rows = {r[0]: int(r[1]) for r in trino(f"SELECT topic, count(*) FROM {V} GROUP BY 1")}
    mismatch = []
    for topic in sorted(kafka):
        if rows.get(topic, 0) != kafka[topic]:
            mismatch.append(f"{topic}: kafka={kafka[topic]} v={rows.get(topic, 0)}")
    assert not mismatch, "расхождения: " + "; ".join(mismatch)
    extra = set(rows) - set(kafka)
    assert not extra, f"в v есть топики вне Kafka: {sorted(extra)}"
    return f"{len(kafka)}/{len(EXPECTED_TOPICS)} топиков, kafka end-offsets == v_cdc_events, всего {sum(kafka.values())}"


def check_key_tables():
    parts = []
    for topic, (db, sql, needs_rw) in KEY_TABLES.items():
        # креды — из env стенда; пул/роль объявлены в KEY_TABLES
        if needs_rw:
            expected = int(psql(PG_RW, "app",
                                os.environ.get("APP_PASSWORD", "app"), db, sql))
        else:
            expected = int(psql(PG_RO, "checker",
                                os.environ.get("CHECKER_PASSWORD", "checker"), db, sql))
        actual = int(trino_scalar(
            f"SELECT count(*) FROM {V} WHERE topic = '{topic}' AND op = 'r'"))
        assert actual == expected, (
            f"{topic}: snapshot op='r' {actual} != строк источника {expected}")
        parts.append(f"{topic.split('.', 2)[2]}={actual}")
    return ("snapshot = строки источника: " + ", ".join(parts)
            + " (crm — v_customer + soft-deleted)")


def check_dedup():
    v_total = int(trino_scalar(f"SELECT count(*) FROM {V}"))
    v_dups = int(trino_scalar(
        f"SELECT count(*) FROM (SELECT topic, partition_id, kafka_offset FROM {V} "
        f"GROUP BY 1, 2, 3 HAVING count(*) > 1)"))
    raw_total = int(trino_scalar(f"SELECT count(*) FROM {RAW}"))
    raw_coords = int(trino_scalar(
        f"SELECT count(*) FROM (SELECT DISTINCT topic, partition_id, kafka_offset "
        f"FROM {RAW})"))
    assert v_dups == 0, f"в v_cdc_events дубли координат: {v_dups}"
    assert raw_coords == v_total, (
        f"уникальных координат raw {raw_coords} != строк v {v_total}")
    assert raw_total >= v_total, f"raw {raw_total} < v {v_total}"
    return f"v={v_total}, raw={raw_total} (дублей доставки {raw_total - v_total})"


def check_shape():
    bad = trino_scalar(
        f"SELECT count(*) FROM {V} WHERE event_key IS NULL "
        f"OR try(json_parse(event_key)) IS NULL")
    bad_table = trino_scalar(
        f"SELECT count(*) FROM {V} WHERE NOT regexp_like(source_table, "
        f"'^[a-z_][a-z0-9_]*[.][a-z_][a-z0-9_]*$')")
    bad_system = trino_scalar(
        f"SELECT count(*) FROM {V} WHERE source_system NOT IN "
        f"('crm', 'catalog', 'pos', 'wms')")
    assert bad == "0", f"event_key не является валидным JSON у {bad} событий"
    assert bad_table == "0", f"source_table не в формате schema.table у {bad_table}"
    assert bad_system == "0", f"source_system вне домена у {bad_system}"
    return ("event_key валиден (JSON), source_table = schema.table, "
            "source_system ∈ {crm, catalog, pos, wms} во всех строках")


def main():
    print(f"стенд: trino={TRINO}, pg={PG_RO}/{PG_RW}, kafka={KAFKA}")
    report = Report()
    report.run("cdc.snapshot (4 сервиса)", check_snapshot_ops)
    report.run("топики: end-offsets == v_cdc_events", check_topic_totals)
    report.run("ключевые таблицы == источник (psql)", check_key_tables)
    report.run("дедуп координат at-least-once", check_dedup)
    report.run("форма event_key/source_table/source_system", check_shape)
    if report.failures:
        print(f"\nПровалено проверок: {report.failures}")
        return 1
    print("\nOK: снапшот доставлен полностью, без потерь, дедуп работает")
    return 0


if __name__ == "__main__":
    sys.exit(main())
