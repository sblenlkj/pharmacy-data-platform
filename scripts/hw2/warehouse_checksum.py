#!/usr/bin/env python3
"""Контрольные суммы DDS для теста идемпотентности повтора порции.

Печатает по каждому представлению контракта sha256 канонизированного
набора строк (ключ → строка, как индексирует чекер) и распределение
_load_batch_id по таблицам dwh_detailed. Сравнение вывода до/после
`load_dds --batch-id ID --replay` — протокол ADR-011 «повтор порции не
меняет данные»: суммы совпадают, «впервые записано порцией» не растёт.

Запуск — контейнером tools:

  docker compose run --rm -T --no-deps tools \
    python scripts/hw2/warehouse_checksum.py [--views v_sale_line ...]
"""
import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

import yaml
import trino.dbapi

REPO = Path(__file__).resolve().parents[2]
CONTRACT = REPO / "hws_descriptions" / "hw2" / "hw02_contract.yaml"
DATA_VIEWS = (
    "v_sale_line", "v_purchase_line", "v_movement", "v_sku", "v_customer",
    "v_pharmacy", "v_batch", "v_supplier", "v_distribution_center",
    "v_sku_history", "v_price_history",
)
# Таблицы dwh_detailed — из каталога (information_schema), не копией списка
# (единый источник — фактическая схема; freshness поддерживается сама).


def canon(value):
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, ensure_ascii=False)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--views", nargs="*", default=list(DATA_VIEWS),
                        help="подмножество представлений (по умолчанию все данные)")
    args = parser.parse_args()

    catalog = os.environ.get("TRINO_CATALOG", "iceberg")
    dwh_schema = os.environ.get("DWH_SCHEMA", "dwh_detailed")
    contract_schema = os.environ.get("CONTRACT_SCHEMA", "contract_dwh")
    spec = yaml.safe_load(CONTRACT.read_text(encoding="utf-8"))["views"]
    conn = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "admin"),
        catalog=catalog, schema=contract_schema, timezone="UTC")

    def query(sql):
        cur = conn.cursor()
        cur.execute(sql)
        names = [c[0].lower() for c in cur.description]
        rows = [dict(zip(names, row)) for row in cur.fetchall()]
        cur.close()
        return rows

    digest = hashlib.sha256()
    for name in args.views:
        columns = list(spec[name]["columns"])
        rows = query(f"SELECT {', '.join(columns)} FROM {catalog}.{contract_schema}.{name}")
        payload = sorted(
            json.dumps({k: canon(r[k]) for k in sorted(r)}, ensure_ascii=False,
                       sort_keys=True) for r in rows)
        view_hash = hashlib.sha256("\n".join(payload).encode()).hexdigest()
        digest.update(view_hash.encode())
        print(f"{name}: rows={len(rows)} sha256={view_hash}")

    # инвентарь таблиц — из каталога (как в maintain_iceberg), не копией списка
    dds_tables = [r["table_name"] for r in query(
        f"SELECT table_name FROM {catalog}.information_schema.tables "
        f"WHERE table_schema = '{dwh_schema}' AND table_type = 'BASE TABLE' "
        "ORDER BY table_name")]
    for table in dds_tables:
        rows = query(f"SELECT _load_batch_id, count(*) AS n FROM "
                     f"{catalog}.{dwh_schema}.{table} GROUP BY _load_batch_id "
                     "ORDER BY _load_batch_id")
        dist = ", ".join(f"{r['_load_batch_id'][:13]}…={r['n']}" for r in rows) or "—"
        print(f"{table}: {dist}")

    print(f"TOTAL sha256={digest.hexdigest()}")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
