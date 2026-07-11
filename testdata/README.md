# DiMer test fixtures

Seed data for exercising diff functionality across connectors. All files
create a `customers` table (key column `id`) with the same base dataset,
adapted per engine's SQL dialect and type system.

- **`postgres_customers.sql`** — the baseline / "source A". Has rows 1-8, 10, 11.
- All other files (`databricks_customers.sql`, `mysql_customers.sql`,
  `snowflake_customers.sql`, `bigquery_customers.sql`,
  `cockroachdb_customers.sql`, `yugabyte_customers.sql`, `tidb_customers.sql`)
  are "source B" variants. Each has rows 1-7, 9, 10, 12.

Diffing Postgres against any one of them (with `id` as the key) produces the
same result:

| ids | Postgres | Other side | Expected `RowStatus` |
|---|---|---|---|
| 1-7 | ✓ | ✓ | MATCHED |
| 8, 11 | ✓ | — | DELETED |
| 9, 12 | — | ✓ | ADDED |
| 10 | amount=220.00, status='active' | amount=275.50, status='inactive' | MODIFIED |

Expected summary: 7 matched, 2 added, 2 deleted, 1 modified (mismatched
columns on row 10: `amount`, `status`).

Diffing any two non-Postgres sides against each other (e.g. MySQL vs.
Snowflake) should report a full match (0 differences) since they all share
the identical "source B" dataset — useful as a sanity check that the
connector/algorithm pairing itself introduces no false positives.

## Usage

1. Apply `postgres_customers.sql` to a Postgres instance.
2. Apply the matching fixture to whichever other engine you want to test.
3. Run `dimer-diff`, select the two sources, table `customers` on both sides,
   key column `id`.

Cockroach DB and YugabyteDB are PostgreSQL wire-compatible and TiDB is MySQL
wire-compatible, so their fixtures reuse the same DDL/data as Postgres/MySQL
— only the CLI/tooling used to apply them and the connection env vars differ.
