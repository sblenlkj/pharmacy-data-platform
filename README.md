# Pharmacy data warehouse homework

## HW1 local source databases

The initial HW1 source schemas run in one PostgreSQL container with four
separate databases:

- `crm_service_db`
- `catalog_service_db`
- `wms_service_db`
- `pos_service_db`

Start the local stack and check its status:

```sh
docker compose up -d
docker compose ps
```

PostgreSQL is exposed on `localhost:5432`. The local development credentials
are `pharmacy` / `pharmacy`. On the first start, the scripts under
`docker/postgres/init/` create the databases and apply the numbered migrations
from `db/migrations/<service>/`.

To exercise initialization again from an empty local data volume:

```sh
docker compose down -v
docker compose up -d
```

Run the isolated database-invariant tests after the stack is healthy:

```sh
./scripts/test_invariants.sh
```

The runner executes every negative test in its own transaction, verifies the
observed PostgreSQL SQLSTATE, and rolls back the two positive tests.

Validate the `contract.*` interface, rollback-only representative fixtures,
and course checks C01-C23:

```sh
./scripts/test_contracts.sh
```

The contract validator reads `hws_descriptions/hw1/hw01_contract.yaml`
directly and requires Ruby's standard YAML library.

## Run the official course checker

The repository already contains a working `hw01_submission.yaml` for the current local setup.

Run the part of the official checker that is already implemented and expected to pass:

```sh
uv run python hws_descriptions/hw1/hw01_check.py \
  --submission ./hw01_submission.yaml \
  --sections contract,keys,dq,inv \
  --skip-exec \
  -v
```

The checker requires the Python dependencies declared in `pyproject.toml` (`PyYAML` and `psycopg2-binary`), so run it through `uv` as shown above.

This command has already been executed successfully on the current implementation. The result was fully green for the implemented sections:

- contract views: 14/14;
- contract nullability and uniqueness: 27/27;
- data-quality checks C01-C23: 23/23;
- invariants I1-I12 and positive tests P1/P2: 14/14.

The remaining checker sections (`volume`, `cross`, `repl`, `cdc`, `ha`, `realism`) are not expected to pass yet because the seed generator, assignment-scale dataset, replication, CDC, and HA layers have not been implemented yet.
