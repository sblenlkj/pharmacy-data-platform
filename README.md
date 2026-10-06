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
