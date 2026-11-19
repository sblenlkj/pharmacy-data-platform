#!/usr/bin/env python3
"""Обслуживание таблиц Iceberg: expire_snapshots + optimize + remove_orphan_files.

Закрывает TODO ADR-011 «no-op MERGE создаёт снапшот»: каждая порция (и каждый
replay, даже с 0 изменений) оставляет снапшоты и мелкие файлы (MERGE — ещё и
positional-delete файлы); накопление метаданных нельзя оставлять бесконтрольным.

Точки запуска (вне тайм-пути команд load чекера — таймаут 90 с):
  * dwh-init — один раз после успешной начальной загрузки (маркер
    INITIAL DDS LOAD DONE → обслуживание → ICEBERG MAINTENANCE DONE);
  * вручную: docker compose run --rm -T --no-deps tools \
      python scripts/hw2/maintain_iceberg.py [--retention 7d]

Что делает по каждой таблице (все физические таблицы dwh_detailed,
автообнаружение через information_schema, + raw.cdc_events):
  1. метрики до: снапшоты (кол-во, возраст), файлы (данные/delete), байты, строки;
  2. ALTER TABLE ... EXECUTE expire_snapshots (retention_threshold => R);
  3. ALTER TABLE ... EXECUTE optimize — компактация мелких файлов
     (идемпотентен: нечего переписывать → 0 файлов, снапшот не создаётся);
  4. ALTER TABLE ... EXECUTE remove_orphan_files — подметает файлы,
     потерянные очисткой/неудачными транзакциями;
  5. метрики после + время каждой операции.

Retention: по умолчанию 7d — консервативный производственный порог и он же
минимум Trino (iceberg.expire-snapshots.min-retention /
iceberg.remove-orphan-files.min-retention). Меньший порог (напр. --retention
0d для учебного стенда без конкурентных читателей и time-travel) автоматически
опускает session-свойства min-retention на этом подключении — конфигурация
сервера не меняется.

remove_orphan_files удаляет только файлы старше порога: при дефолтных 7d
гонка с пишущим cdc-ingest исключена по построению (in-flight файлы молоды);
--skip-orphans оставлен как аварийный выключатель.

Идемпотентен: повторный запуск на обслуженных таблицах — expire ничего не
снимает, optimize переписывает 0 файлов (без нового снапшота), orphan-скан
удаляет 0. Код возврата 0 — все операции успешны.
"""
import argparse
import os
import re
import sys
import time

TRINO = {
    "host": os.environ.get("TRINO_HOST", "trino"),
    "port": int(os.environ.get("TRINO_PORT", "8080")),
    "user": os.environ.get("TRINO_USER", "admin"),
    "catalog": os.environ.get("TRINO_CATALOG", "iceberg"),
}
DWH_SCHEMA = os.environ.get("DWH_SCHEMA", "dwh_detailed")
RAW_TABLE = "raw.cdc_events"
# Системный минимум Trino: expire_snapshots не принимает retention меньше
# (config iceberg.expire-snapshots.min-retention, по умолчанию 7.00d).
SYSTEM_MIN_RETENTION_SECONDS = 7 * 86400
DURATION_RE = re.compile(r"^(\d+(?:\.\d+)?)([smhd])$")


def log(message):
    print(f"maintain_iceberg: {message}", flush=True)


def parse_duration(text):
    """'7d'/'12h'/'30m'/'0s' → секунды (float); иначе SystemExit."""
    match = DURATION_RE.match(text.strip().lower())
    if not match:
        raise SystemExit(f"maintain_iceberg: не разобрать длительность {text!r} "
                         "(поддерживаются s/m/h/d, напр. 7d или 90m)")
    value, unit = match.groups()
    return float(value) * {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]


def human_bytes(size):
    for unit in ("Б", "КБ", "МБ", "ГБ"):
        if size < 1024 or unit == "ГБ":
            return f"{size:.1f} {unit}" if unit != "Б" else f"{int(size)} {unit}"
        size /= 1024


def trino_connect():
    import trino.dbapi

    conn = trino.dbapi.connect(
        host=TRINO["host"], port=TRINO["port"], user=TRINO["user"],
        catalog=TRINO["catalog"], timezone="UTC")
    cur = conn.cursor()
    cur.execute("SELECT 1")
    cur.fetchall()
    return conn


def query(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    names = [c[0].lower() for c in cur.description] if cur.description else []
    return [dict(zip(names, row)) for row in cur.fetchall()]


def run_op(conn, table, key, label, sql, op_seconds):
    """Процедура обслуживания с единым таймингом и форматом лога."""
    t0 = time.monotonic()
    result = execute_procedure(conn, sql)
    elapsed = time.monotonic() - t0
    op_seconds[key] += elapsed
    log(f"{table}: {label}: {elapsed:.1f} с "
        + ("; ".join(f"{k}={v}" for k, v in result.items()) if result else ""))


def execute_procedure(conn, sql):
    """ALTER TABLE ... EXECUTE — вернуть счётчики процедуры, если есть.

    Процедуры Trino (optimize, remove_orphan_files) возвращают результат
    построчно парами (metric_name, metric_value) — сворачиваем в словарь."""
    cur = conn.cursor()
    cur.execute(sql)
    names = [c[0].lower() for c in cur.description] if cur.description else []
    rows = cur.fetchall()
    if not names or not rows:
        return {}
    if "metric_name" in names:
        i_name, i_value = names.index("metric_name"), names.index("metric_value")
        return {row[i_name]: row[i_value] for row in rows}
    return dict(zip(names, rows[0]))


def discover_tables(conn):
    """Физические таблицы dwh_detailed (stg_* — вью, отфильтрованы) + raw.cdc_events."""
    rows = query(conn, f"SELECT table_name FROM {TRINO['catalog']}.information_schema.tables "
                       f"WHERE table_schema = '{DWH_SCHEMA}' AND table_type = 'BASE TABLE' "
                       "ORDER BY table_name")
    tables = [f"{DWH_SCHEMA}.{r['table_name']}" for r in rows]
    if not tables:
        raise SystemExit(f"maintain_iceberg: в {DWH_SCHEMA} нет физических таблиц — "
                         "обслуживать нечего (стенд инициализирован?)")
    ctl = query(conn, f"SELECT table_name FROM {TRINO['catalog']}.information_schema.tables "
                      "WHERE table_schema = 'ctl' AND table_type = 'BASE TABLE' "
                      "ORDER BY table_name")
    # ctl.* копят снапшот на каждый DML порции — обслуживаем вместе с DDS
    return tables + [f"ctl.{r['table_name']}" for r in ctl] + [RAW_TABLE]


def table_metrics(conn, table):
    """{snapshots, oldest, newest, data_files, delete_files, bytes, records}."""
    schema, name = table.split(".")
    quoted = f"{TRINO['catalog']}.{schema}.\"{name}$snapshots\""
    snap = query(conn, f"SELECT count(*) AS n, min(committed_at) AS oldest, "
                       f"max(committed_at) AS newest FROM {quoted}")[0]
    quoted = f"{TRINO['catalog']}.{schema}.\"{name}$files\""
    files = query(conn, f"SELECT content, count(*) AS n, sum(file_size_in_bytes) AS bytes, "
                        f"sum(record_count) AS records FROM {quoted} GROUP BY content")
    by_content = {r["content"]: r for r in files}
    data, delete = by_content.get(0, {}), by_content.get(1, {})
    return {
        "snapshots": snap["n"],
        "oldest": snap["oldest"],
        "newest": snap["newest"],
        "data_files": data.get("n", 0),
        "delete_files": delete.get("n", 0),
        "bytes": (data.get("bytes") or 0) + (delete.get("bytes") or 0),
        "records": data.get("records") or 0,
    }


def fmt_ts(value):
    if value is None:
        return "—"
    return value.strftime("%Y-%m-%dT%H:%M")


def fmt_metrics(m):
    return (f"snapshots={m['snapshots']} ({fmt_ts(m['oldest'])}…{fmt_ts(m['newest'])} UTC), "
            f"files={m['data_files']}+{m['delete_files']}del, {human_bytes(m['bytes'])} / {m['records']} строк")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--retention", default="7d",
                        help="retention_threshold для expire_snapshots (по умолчанию 7d — "
                             "консервативный порог и минимум Trino; меньший порог требует "
                             "отсутствия конкурентных читателей)")
    parser.add_argument("--tables", nargs="*", metavar="SCHEMA.TABLE",
                        help="подмножество таблиц (по умолчанию все dwh_detailed + raw.cdc_events)")
    parser.add_argument("--skip-orphans", action="store_true",
                        help="не запускать remove_orphan_files (живой поток событий в raw)")
    args = parser.parse_args()

    retention = args.retention
    retention_seconds = parse_duration(retention)
    started = time.monotonic()
    conn = trino_connect()

    # Меньше системного минимума (7d) — опускаем min-retention на этой сессии
    # (expire и orphan-очистка): конфиг сервера не трогаем, обоснование —
    # учебный стенд без конкурентных читателей (time-travel не используется).
    if retention_seconds < SYSTEM_MIN_RETENTION_SECONDS:
        for prop in ("iceberg.expire_snapshots_min_retention",
                     "iceberg.remove_orphan_files_min_retention"):
            execute_procedure(conn, f"SET SESSION {prop} = '{int(retention_seconds)}s'")
        log(f"retention {retention} ниже минимума Trino 7d — session min-retention "
            f"опущен до {int(retention_seconds)}s (конкурентных читателей нет)")

    tables = args.tables or discover_tables(conn)
    log(f"таблиц к обслуживанию: {len(tables)} ({', '.join(tables)}); "
        f"retention={retention}, orphans={'выкл' if args.skip_orphans else 'вкл'}")

    summary, failures, op_seconds = [], [], {"expire": 0.0, "optimize": 0.0, "orphans": 0.0}
    for table in tables:
        schema, name = table.split(".")
        ref = f"{TRINO['catalog']}.{schema}.{name}"
        try:
            before = table_metrics(conn, table)
            log(f"[до]    {table}: {fmt_metrics(before)}")

            run_op(conn, table, "expire", f"expire_snapshots({retention})",
                   f"ALTER TABLE {ref} EXECUTE expire_snapshots "
                   f"(retention_threshold => '{retention}')", op_seconds)
            run_op(conn, table, "optimize", "optimize",
                   f"ALTER TABLE {ref} EXECUTE optimize", op_seconds)
            if not args.skip_orphans:
                # Порог очистки — тот же retention (session-минимум опущен
                # выше при retention < 7d; при >= 7d явный порог валиден):
                # без явного порога Trino применил бы дефолт 7d и почистил
                # больше, чем запрошено.
                run_op(conn, table, "orphans", "remove_orphan_files",
                       f"ALTER TABLE {ref} EXECUTE remove_orphan_files"
                       f"(retention_threshold => '{retention}')", op_seconds)

            after = table_metrics(conn, table)
            log(f"[после] {table}: {fmt_metrics(after)}")
            summary.append((table, before, after))
        except Exception as exc:  # noqa: BLE001 — отказ одной таблицы не должен
            failures.append((table, exc))  # останавливать обслуживание остальных
            log(f"{table}: ОШИБКА: {exc}")

    conn.close()

    total = time.monotonic() - started
    log("сводка (до → после):")
    sums = {k: [0, 0] for k in ("snapshots", "data_files", "delete_files", "bytes")}
    for table, before, after in summary:
        log(f"  {table:24s} snapshots {before['snapshots']:>4} → {after['snapshots']:<4}"
            f" files {before['data_files']:>3}+{before['delete_files']}del → "
            f"{after['data_files']}+{after['delete_files']}del"
            f" {human_bytes(before['bytes'])} → {human_bytes(after['bytes'])}")
        for k in sums:
            sums[k][0] += before[k]
            sums[k][1] += after[k]
    log(f"итого: snapshots {sums['snapshots'][0]} → {sums['snapshots'][1]}, "
        f"файлов {sums['data_files'][0]}+{sums['delete_files'][0]}del → "
        f"{sums['data_files'][1]}+{sums['delete_files'][1]}del, "
        f"{human_bytes(sums['bytes'][0])} → {human_bytes(sums['bytes'][1])}")
    log(f"время: expire {op_seconds['expire']:.1f} с, optimize {op_seconds['optimize']:.1f} с, "
        f"orphans {op_seconds['orphans']:.1f} с; всего {total:.1f} с "
        f"({len(summary)} таблиц ок, {len(failures)} ошибок)")
    for table, exc in failures:
        print(f"maintain_iceberg: FAILED {table}: {exc}", file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
