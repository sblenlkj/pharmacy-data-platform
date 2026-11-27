"""Execute the actual SQL models offline (Trino SELECTs transpiled to DuckDB).

This tests transformation logic, NOT Iceberg, Docker or Trino compatibility.
Run: python -m unittest discover -s tests/hw2 -v
"""
import contextlib
import io
import json
import os
from pathlib import Path
import runpy
import tempfile
import unittest
from unittest.mock import patch

import duckdb
from jinja2 import Environment, StrictUndefined
import sqlglot

ROOT = Path(__file__).resolve().parents[2]
MODELS = ROOT / "dwh/models"
DDS = sorted(p.stem for p in (MODELS / "dds").glob("*.sql"))


class Warehouse:
    def __init__(self):
        self.db = duckdb.connect()
        self.db.execute("""CREATE TABLE raw_cdc_events (
            source_system varchar, source_table varchar, event_key varchar,
            op varchar, topic varchar, partition_id integer, kafka_offset bigint,
            payload varchar, ingested_at timestamptz)""")
        self.offsets = {}
        self.positions = {}
        self.batches = {}
        self.config = {}
        self.batch_id = None
        self.ranges = []

    def event(self, svc, table, key, after, op="c"):
        topic = f"{svc}.public.{table}"
        offset = self.offsets.get(topic, 0)
        self.offsets[topic] = offset + 1
        self.db.execute("INSERT INTO raw_cdc_events VALUES (?,?,?,?,?,?,?,?,?)", [
            svc, f"public.{table}", json.dumps(key, sort_keys=True), op, topic, 0,
            offset, json.dumps({"after": after, "op": op}), "2026-11-01T00:00:00Z"])

    def render(self, path, incremental=False):
        self.config = {}
        def config(**kwargs):
            self.config.update(kwargs)
            return ""
        env = Environment(undefined=StrictUndefined, extensions=["jinja2.ext.do"])
        env.globals.update(
            config=config, ref=lambda name: name,
            source=lambda schema, name: f"{schema}_{name}",
            var=lambda key, default=None: {
                "batch_ranges": self.ranges, "load_batch_id": self.batch_id
            }.get(key, default),
            is_incremental=lambda: incremental, this="stg_source_state")
        source = (ROOT / "dwh/macros/cdc.sql").read_text() + path.read_text()
        trino_sql = env.from_string(source).render()
        return sqlglot.transpile(trino_sql, read="trino", write="duckdb")[0]

    def exists(self, model):
        return bool(self.db.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name=?", [model]).fetchone()[0])

    def merge(self, path):
        name = path.stem
        exists = self.exists(name)
        select = self.render(path, incremental=exists)
        if not exists:
            self.db.execute(f"CREATE TABLE {name} AS {select}")
            return
        keys = self.config["unique_key"]
        keys = [keys] if isinstance(keys, str) else keys
        columns = [r[0] for r in self.db.execute(f"DESCRIBE {name}").fetchall()]
        updates = [c for c in columns if c != "_load_batch_id"]
        self.db.execute(
            f"MERGE INTO {name} t USING ({select}) s ON " +
            " AND ".join(f"t.{k}=s.{k}" for k in keys) +
            " WHEN MATCHED THEN UPDATE SET " +
            ", ".join(f"{c}=s.{c}" for c in updates) +
            " WHEN NOT MATCHED THEN INSERT (" + ",".join(columns) +
            ") VALUES (" + ",".join("s."+c for c in columns) + ")")

    def load(self, batch_id, replay=False):
        self.batch_id = batch_id
        if replay:
            self.ranges = self.batches[batch_id]
        else:
            self.ranges = [
                {"topic": topic, "partition": 0,
                 "start_offset": self.positions.get(topic, 0), "end_offset": end}
                for topic, end in self.offsets.items()
                if end > self.positions.get(topic, 0)]
            self.batches[batch_id] = self.ranges
        self.merge(MODELS / "staging/stg_source_state.sql")
        for p in sorted((MODELS / "staging").glob("*.sql")):
            if p.stem == "stg_source_state":
                continue
            self.db.execute(f"CREATE OR REPLACE VIEW {p.stem} AS {self.render(p)}")
        for name in DDS:
            self.merge(MODELS / f"dds/{name}.sql")
        for p in sorted((MODELS / "contract").glob("*.sql")):
            if p.stem in ("v_load_batches", "v_cdc_events"):
                continue
            self.db.execute(f"CREATE OR REPLACE VIEW {p.stem} AS {self.render(p)}")
        if not replay:
            self.positions = dict(self.offsets)

    def rows(self, sql):
        return self.db.execute(sql).fetchall()

    def checksum(self):
        return {name: sorted(map(repr, self.rows(f"SELECT * FROM {name}"))) for name in DDS}

    def seed(self):
        self.event("pos", "cash_register", {"register_bk":"reg"}, {"register_bk":"reg","pharmacy_bk":"store"})
        self.event("pos", "shift", {"shift_bk":"shift"}, {"shift_bk":"shift","register_bk":"reg"})
        self.event("pos", "receipt", {"receipt_bk":"sale"}, {
            "receipt_bk":"sale","shift_bk":"shift","customer_bk":"customer",
            "receipt_dt":"2026-09-11T12:00:00Z","doc_type":"sale","currency":"RUB"})
        self.sale_line(1)
        self.event("wms", "purchase_order", {"purchase_bk":"order"}, {
            "purchase_bk":"order","supplier_bk":"supplier","dc_bk":"dc","ordered_at":"2026-09-01T00:00:00Z"})
        self.purchase_line(1)
        self.event("wms", "batch", {"batch_bk":"batch"}, {
            "batch_bk":"batch","sku":"sku","received_at":"2026-09-10T00:00:00Z"})
        self.event("wms", "movement", {"movement_bk":"move"}, {
            "movement_bk":"move","batch_bk":"batch","movement_type":"sale",
            "dst_type":"customer","dst_bk":"customer","moved_at":"2026-09-11T12:00:00Z"})
        self.event("crm", "customer", {"customer_bk":"customer"}, {
            "customer_bk":"customer","birth_year":1980,"sex":"M",
            "registered_at":"2026-01-01T00:00:00Z"})
        self.profile(city="Moscow")
        self.event("crm", "loyalty_level", {"id":1}, {"id":1,"level_name":"base"})
        self.event("crm", "consent_type", {"id":1}, {"id":1,"type_name":"marketing"})
        self.consent()
        self.event("catalog", "manufacturer", {"manufacturer_bk":"m"}, {
            "manufacturer_bk":"m","manufacturer_name":"Factory"})
        self.event("catalog", "sku_category", {"id":1}, {"id":1,"category_name":"category"})
        self.event("catalog", "sku", {"sku_bk":"sku"}, {
            "sku_bk":"sku","manufacturer_bk":"m","pack_qty":"1","is_active":True})
        self.event("catalog", "sku_version", {"id":1}, {
            "id":1,"sku_bk":"sku","sku_name":"Old name","category_id":1,
            "is_rx":False,"valid_from":"2026-01-01T00:00:00Z"})
        self.event("catalog", "price_scope", {"id":1}, {"id":1,"scope_name":"chain"})
        self.price(1, "100.00", "2026-01-01T00:00:00Z")
        self.event("catalog", "active_ingredient", {"ingredient_bk":"ai"}, {
            "ingredient_bk":"ai","inn_name":"Old INN"})
        self.event("catalog", "sku_active_ingredient", {"sku_bk":"sku","ingredient_bk":"ai"}, {
            "sku_bk":"sku","ingredient_bk":"ai"})

    def profile(self, city):
        self.event("crm", "customer_profile_version", {"id":1}, {
            "id":1,"customer_bk":"customer","city":city,"loyalty_level_id":1,"is_current":True})

    def consent(self, revoked=None):
        self.event("crm", "consent", {"consent_bk":"consent"}, {
            "consent_bk":"consent","customer_bk":"customer","consent_type_id":1,"revoked_at":revoked})

    def sale_line(self, n, receipt="sale"):
        self.event("pos", "receipt_line", {"receipt_bk":receipt,"line_no":n}, {
            "receipt_bk":receipt,"line_no":n,"sku":"sku","batch_bk":"batch",
            "quantity":"1","unit_price":"100.00","line_discount":"0","sku_name_snapshot":"Old name"})

    def purchase_line(self, n):
        self.event("wms", "purchase_line", {"purchase_bk":"order","line_no":n}, {
            "purchase_bk":"order","line_no":n,"sku":"sku","batch_bk":"batch","quantity":"10","unit_cost":"50","currency":"RUB"})

    def price(self, n, price, start, end=None):
        self.event("catalog", "price_version", {"id":n}, {
            "id":n,"sku_bk":"sku","price_scope_id":1,"scope_bk":"CHAIN",
            "price":price,"currency":"RUB","valid_from":start,"valid_to":end})


class SQLRegressionTests(unittest.TestCase):
    def setUp(self):
        self.w = Warehouse()
        self.w.seed()
        self.w.load("initial")

    def tearDown(self):
        self.w.db.close()

    def test_late_lines_use_old_headers_and_cdc_location(self):
        self.w.sale_line(2)
        self.w.purchase_line(2)
        self.w.load("lines")
        self.assertEqual(self.w.rows("select line_no, pharmacy_bk from fact_sale_line order by line_no"), [(1,"store"),(2,"store")])
        self.assertEqual(self.w.rows("select count(*) from fact_purchase_line"), [(2,)])
        self.w.event("pos", "cash_register", {"register_bk":"reg"}, {
            "register_bk":"reg","pharmacy_bk":"other-store"})
        self.w.load("register")
        self.assertEqual(self.w.rows("select distinct pharmacy_bk from fact_sale_line"), [("other-store",)])

    def test_late_header_recovers_previously_unjoined_line(self):
        self.w.sale_line(1, receipt="late")
        self.w.load("orphan-line")
        self.assertEqual(self.w.rows("select count(*) from fact_sale_line where receipt_bk='late'"), [(0,)])
        self.w.event("pos", "receipt", {"receipt_bk":"late"}, {
            "receipt_bk":"late","shift_bk":"shift","receipt_dt":"2026-09-11T12:00:00Z","doc_type":"sale","currency":"RUB"})
        self.w.load("late-header")
        self.assertEqual(self.w.rows("select count(*) from fact_sale_line where receipt_bk='late'"), [(1,)])

    def test_satellite_only_changes_propagate(self):
        self.w.profile(city="Kazan")
        self.w.consent("2026-11-01T00:00:00Z")
        self.w.event("catalog", "active_ingredient", {"ingredient_bk":"ai"}, {
            "ingredient_bk":"ai","inn_name":"New INN"})
        self.w.load("satellites")
        self.assertEqual(self.w.rows("select city, marketing_opt_in from dim_customer"), [("Kazan",False)])
        self.assertEqual(self.w.rows("select inn_name from dim_sku"), [("New INN",)])

    def test_header_only_update_recalculates_facts(self):
        self.w.event("pos", "receipt", {"receipt_bk":"sale"}, {
            "receipt_bk":"sale","shift_bk":"shift","customer_bk":"customer",
            "receipt_dt":"2026-09-11T12:00:00Z","doc_type":"sale","currency":"USD"})
        self.w.event("wms", "purchase_order", {"purchase_bk":"order"}, {
            "purchase_bk":"order","supplier_bk":"new-supplier","dc_bk":"dc",
            "ordered_at":"2026-09-01T00:00:00Z"})
        self.w.load("headers")
        self.assertEqual(self.w.rows("select currency from fact_sale_line"), [("USD",)])
        self.assertEqual(self.w.rows("select supplier_bk from fact_purchase_line"), [("new-supplier",)])

    def test_late_replay_cannot_resurrect_customer_or_reopen_price(self):
        self.w.price(1, "100.00", "2026-01-01T00:00:00Z", "2026-11-01T00:00:00Z")
        self.w.price(2, "120.00", "2026-11-01T00:00:00Z")
        self.w.event("catalog", "sku_version", {"id":2}, {
            "id":2,"sku_bk":"sku","sku_name":"New name","category_id":1,
            "is_rx":False,"valid_from":"2026-11-01T00:00:00Z"})
        self.w.load("history")
        self.w.event("crm", "customer", {"customer_bk":"customer"}, {
            "customer_bk":"customer","birth_year":1980,"sex":"M",
            "registered_at":"2026-01-01T00:00:00Z","deleted_at":"2026-11-02T00:00:00Z"})
        self.w.load("privacy")
        before, positions = self.w.checksum(), dict(self.w.positions)
        self.w.load("initial", replay=True)
        self.assertEqual(before, self.w.checksum())
        self.assertEqual(positions, self.w.positions)
        self.assertEqual(self.w.rows("select count(*) from v_customer"), [(0,)])
        self.assertEqual(self.w.rows("select customer_bk from v_sale_line"), [(None,)])
        self.assertEqual(self.w.rows("select dst_bk from v_movement"), [(None,)])
        self.assertEqual(self.w.rows("select count(*) from v_price_history where valid_to is null"), [(1,)])
        self.assertEqual(self.w.rows("select sku_name_snapshot from v_sale_line"), [("Old name",)])

    def test_saved_window_excludes_future_raw_and_replay_is_idempotent(self):
        original = self.w.checksum()
        self.w.profile(city="future")
        # Replaying old ranges must not accidentally consume the unprocessed event.
        self.w.load("initial", replay=True)
        self.assertEqual(original, self.w.checksum())
        self.w.load("new")
        current = self.w.checksum()
        self.w.load("new", replay=True)
        self.assertEqual(current, self.w.checksum())
        self.assertEqual(self.w.rows("select city from dim_customer"), [("future",)])


class BatchProtocolTests(unittest.TestCase):
    def setUp(self):
        self.ns = runpy.run_path(str(ROOT / "scripts/load_dds"))
        self.g = self.ns["prepare_batch"].__globals__
        self.ranges = [{"topic":"crm.public.customer","partition":0,"start_offset":2,"end_offset":3}]

    def test_retry_reuses_saved_bounds_even_when_raw_grows(self):
        with patch.dict(self.g, read_batch=lambda *a: {"offsets_json":json.dumps(self.ranges),"status":"failed"},
                        query=lambda *a: [{"batch_id":"failed"}], has_successful_batch=lambda *a: True,
                        set_batch_status=lambda *a: None,
                        compute_ranges=lambda *a: self.fail("must not recompute saved bounds")):
            actual, _, full = self.ns["prepare_batch"](None,"failed",False)
        self.assertEqual(actual,self.ranges)
        self.assertFalse(full)

    def test_successful_id_cannot_be_reused_for_a_new_window(self):
        with patch.dict(self.g, read_batch=lambda *a: {"offsets_json":"[]","status":"success"},
                        query=lambda *a: []):
            with self.assertRaisesRegex(ValueError,"--replay"):
                self.ns["prepare_batch"](None,"used",False)

    def test_failed_dbt_does_not_advance_watermarks(self):
        statuses = []
        with patch.dict(self.g, prepare_batch=lambda *a: (self.ranges,None,False),
                        validate_events=lambda *a: None,
                        run_dbt=lambda *a: (False,[]),
                        set_batch_status=lambda c,b,s: statuses.append(s),
                        advance_positions=lambda *a: self.fail("watermarks must not advance")):
            with contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(ValueError,"dbt failed"):
                self.ns["load_batch"](None,"failing",False)
        self.assertEqual(statuses,["failed"])

    def test_concurrent_loader_is_rejected(self):
        with tempfile.TemporaryDirectory() as d, patch.dict(os.environ,HW02_LOAD_LOCK=str(Path(d)/"lock")):
            with self.ns["loader_lock"]():
                with self.assertRaisesRegex(ValueError,"другая загрузка"):
                    with self.ns["loader_lock"]():
                        self.fail("acquired twice")


if __name__ == "__main__":
    unittest.main()
