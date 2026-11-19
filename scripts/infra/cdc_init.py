"""One-shot CDC bootstrap for HW2 (runs as the cdc-init service).

Uses the same image as Kafka Connect, so python3 (stdlib only) and the
Kafka CLI (/kafka/bin) are available inside the compose network.

Steps:
1. wait for the Connect REST endpoint;
2. register (PUT, idempotent) one Debezium connector per source DB with
   table.include.list from scripts/infra/cdc_sources.json (= dwh_mapping §9);
3. wait until every connector and task reports RUNNING and the end offsets
   of all expected topics are non-empty and stable — i.e. the initial
   snapshots are complete;
4. log the "CDC SNAPSHOT DONE" marker (intermediate readiness flag; the
   INITIAL DDS LOAD DONE flag comes later from the dwh-init loader).
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.error
import urllib.request

CONNECT_URL = os.environ.get("CONNECT_URL", "http://kafka-connect:8083")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_BIN = os.environ.get("KAFKA_BIN", "/kafka/bin")
CDC_SOURCES = Path(os.environ.get("CDC_SOURCES", "/workspace/scripts/infra/cdc_sources.json"))

READY_TIMEOUT = int(os.environ.get("CDC_READY_TIMEOUT", "1800"))   # overall cap, s
STABLE_WINDOW = int(os.environ.get("CDC_STABLE_WINDOW", "15"))     # between offset samples, s


def rest(method, path, payload=None, timeout=10):
    data = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(f"{CONNECT_URL}{path}", data=data, method=method)
    if data is not None:
        request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode()
            return response.status, json.loads(body) if body else {}
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read().decode() or "{}")


def wait_for_connect():
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        try:
            status, _ = rest("GET", "/connectors")
            if status == 200:
                print("connect REST is ready", flush=True)
                return
        except (urllib.error.URLError, OSError):
            pass
        time.sleep(3)
    raise SystemExit("timeout waiting for Kafka Connect REST")


def connector_config(service, tables):
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        # The connector follows the leader through haproxy:5000 (rw route).
        "database.hostname": "haproxy",
        "database.port": "5000",
        "database.user": "debezium",
        "database.password": os.environ["DEBEZIUM_PASSWORD"],
        "database.dbname": f"{service}_service_db",
        "plugin.name": "pgoutput",
        "slot.name": f"cdc_{service}",
        "publication.name": f"pub_cdc_{service}",
        # Publications are created by db-init (scripts/infra/bootstrap.py).
        "publication.autocreate.mode": "disabled",
        "topic.prefix": service,
        "table.include.list": ",".join(f"public.{t}" for t in tables),
        "snapshot.mode": "initial",
        # Слоты объявлены permanent_slots у Patroni и пересоздаются на новом
        # лидере после рестарта/смены лидера кластера. Их новая позиция может
        # быть «впереди» сохранённых оффсетов коннектора; стратегия trust_slot
        # считает слот источником истины (серверная позиция авторитетна) и не
        # требует повторного снапшота: рестарт кластера и пересоздание слотов
        # коннекторы переживают без участия оператора (README
        # «Отказоустойчивость CDC»).
        "offset.mismatch.strategy": "trust_slot",
        # decimal as string keeps the JSON envelope readable for the DDS loader
        "decimal.handling.mode": "string",
        "tombstones.on.delete": "false",
        "errors.log.enable": "true",
        "errors.log.include.messages": "true",
    }


def register_connectors(sources):
    names = {}
    for service, tables in sources.items():
        name = f"cdc-{service}"
        status, body = rest("PUT", f"/connectors/{name}/config",
                            connector_config(service, tables))
        if status not in (200, 201):
            raise SystemExit(f"failed to register {name}: HTTP {status}: {body}")
        created = "created" if status == 201 else "updated"
        print(f"connector {name}: {created} ({len(tables)} tables)", flush=True)
        names[service] = name
    return names


def connector_states(name):
    status, body = rest("GET", f"/connectors/{name}/status")
    if status != 200:
        return [], None, None
    states = [task["state"] for task in body.get("tasks", [])]
    return states, body.get("connector", {}).get("state"), body


def check_connectors(names):
    """True when every connector and all its tasks are RUNNING."""
    ok = True
    for service, name in names.items():
        states, connector_state, body = connector_states(name)
        if connector_state != "RUNNING" or any(s != "RUNNING" for s in states):
            ok = False
            print(f"connector {name}: state={connector_state} tasks={states}", flush=True)
            if connector_state == "FAILED" or "FAILED" in states:
                raise SystemExit(f"connector {name} FAILED: {json.dumps(body)[:4000]}")
    return ok


def all_topic_offsets():
    """End offsets of every topic in one JVM run: {topic: sum(offsets)}."""
    result = subprocess.run(
        [f"{KAFKA_BIN}/kafka-get-offsets.sh", "--bootstrap-server", KAFKA_BOOTSTRAP],
        capture_output=True, text=True, timeout=120)
    offsets = {}
    for line in result.stdout.splitlines():
        parts = line.rsplit(":", 2)
        if len(parts) == 3 and parts[2].lstrip("-").isdigit():
            offsets[parts[0]] = offsets.get(parts[0], 0) + int(parts[2])
    return offsets


def sample_topics(topics):
    offsets = all_topic_offsets()
    return {topic: offsets.get(topic, 0) for topic in topics}


def wait_for_snapshots(names, sources):
    topics = [f"{service}.public.{table}"
              for service, tables in sources.items() for table in tables]
    print(f"waiting for initial snapshots of {len(topics)} topics "
          f"(stable end offsets over {STABLE_WINDOW}s, cap {READY_TIMEOUT}s)", flush=True)
    deadline = time.monotonic() + READY_TIMEOUT
    previous = None
    ready_rounds = 0
    while time.monotonic() < deadline:
        running = check_connectors(names)
        current = sample_topics(topics)
        # Топика нет в Kafka = таблица пуста и снапшот по ней ничего не даст
        # (протокол чекера допускает пустые топики). Готовность по пустым
        # признаём после двух подряд стабильных окон с живыми коннекторами,
        # чтобы не принять за пустоту ещё не начавшийся снапшот таблицы.
        absent = [t for t in topics if current.get(t, 0) == 0]
        stable = previous is not None and current == previous
        ready_rounds = ready_rounds + 1 if (running and stable) else 0
        total = sum(current.values())
        print(f"end-offset sum={total} empty-topics={len(absent)} "
              f"stable={stable} connectors-running={running}", flush=True)
        if running and stable and (not absent or ready_rounds >= 2):
            for topic in topics:
                print(f"  {topic}: {current[topic]}", flush=True)
            print(f"CDC SNAPSHOT DONE: {len(topics)} topics, {total} records total", flush=True)
            return
        previous = current
        time.sleep(STABLE_WINDOW)
    raise SystemExit("timeout waiting for CDC snapshots; last offsets: "
                     + json.dumps(previous))


def main():
    sources = json.loads(CDC_SOURCES.read_text())
    tables_total = sum(len(v) for v in sources.values())
    print(f"cdc-init: {len(sources)} connectors, {tables_total} tables", flush=True)
    wait_for_connect()
    names = register_connectors(sources)
    wait_for_snapshots(names, sources)
    print("cdc-init completed: connectors registered, initial snapshots done", flush=True)


if __name__ == "__main__":
    sys.exit(main())
