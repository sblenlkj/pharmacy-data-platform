#!/usr/bin/env python3
"""Additional live regression: delta -> history -> privacy -> old delta replay.

Creates test records in all four source DBs. Run in tools after initialization.
Uses the unchanged teacher fixture/Kafka/raw protocol and compares all business
contract rows, not just row counts. Does not run automatically at startup.
"""
import argparse
import importlib.util
from pathlib import Path
import sys
import uuid

import yaml

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--submission", type=Path, default=ROOT / "hw02_submission.container.yaml")
    parser.add_argument("--timeout", type=int, default=90)
    args = parser.parse_args()
    path = ROOT / "hws_descriptions/hw2/hw02_check.py"
    spec = importlib.util.spec_from_file_location("teacher_hw02", path)
    teacher = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = teacher
    spec.loader.exec_module(teacher)
    contract = yaml.safe_load((path.parent / "hw02_contract.yaml").read_text())
    submission = yaml.safe_load(args.submission.read_text())
    teacher.validate_manifest(submission, scenario=True)
    checker = teacher.Checker(contract, submission, args.submission.resolve().parent, args.timeout)
    prefix = "late-" + uuid.uuid4().hex
    try:
        with teacher.KafkaProbe(submission["kafka"], args.timeout) as consumer:
            fixture, _ = checker.run_phase(consumer, "delta", {}, prefix+"-delta")
            keys = fixture["keys"]
            checker.run_phase(consumer, "history", {"keys":keys, "sku_name":"LATE REPLAY TEST"}, prefix+"-history")
            checker.run_phase(consumer, "privacy", {"keys":keys}, prefix+"-privacy")
            views = [name for name in checker.views if name not in ("v_cdc_events", "v_load_batches")]
            before = {name: checker.target_rows(name) for name in views}
            checker.load(prefix+"-delta", replay=True)
            for name in views:
                teacher.compare(before[name], checker.target_rows(name))
            if (keys["customer_bk"],) in checker.target_rows("v_customer"):
                raise ValueError("late replay resurrected the customer")
            print(f"PASS: old delta replay preserved all {len(views)} business views; customer remains hidden")
        return 0
    finally:
        checker.close()


if __name__ == "__main__":
    sys.exit(main())
