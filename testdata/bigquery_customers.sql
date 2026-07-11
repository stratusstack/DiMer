-- DiMer test fixture: BigQuery side (source B / "target")
-- Apply with: bq query --use_legacy_sql=false < bigquery_customers.sql
-- (or paste into the BigQuery console). Replace `DATASET` below with
-- $BIGQUERY_DATASET (BigQuery has no PRIMARY KEY / foreign key enforcement).
-- Pair with testdata/postgres_customers.sql (source A) to exercise ADDED/DELETED/MODIFIED.

DROP TABLE IF EXISTS `DATASET.customers`;

CREATE TABLE `DATASET.customers` (
    id         INT64,
    name       STRING NOT NULL,
    email      STRING,
    amount     NUMERIC,
    status     STRING,
    created_at TIMESTAMP
);

-- Rows 1-7: identical to Postgres -> MATCHED
INSERT INTO `DATASET.customers` (id, name, email, amount, status, created_at) VALUES
(1, 'Alice Johnson',   'alice@example.com',   100.50, 'active',   TIMESTAMP '2026-01-01 10:00:00'),
(2, 'Bob Smith',       'bob@example.com',     250.00, 'active',   TIMESTAMP '2026-01-02 11:00:00'),
(3, 'Carla Diaz',      'carla@example.com',    75.25, 'inactive', TIMESTAMP '2026-01-03 09:30:00'),
(4, 'David Lee',       'david@example.com',   500.00, 'active',   TIMESTAMP '2026-01-04 14:15:00'),
(5, 'Emma Wilson',     'emma@example.com',    320.75, 'active',   TIMESTAMP '2026-01-05 08:45:00'),
(6, 'Frank Miller',    'frank@example.com',    45.00, 'inactive', TIMESTAMP '2026-01-06 16:20:00'),
(7, 'Grace Chen',      'grace@example.com',   890.10, 'active',   TIMESTAMP '2026-01-07 12:00:00');

-- Row 9: only in BigQuery -> ADDED (missing from Postgres)
INSERT INTO `DATASET.customers` (id, name, email, amount, status, created_at) VALUES
(9, 'Isabel Turner',   'isabel@example.com',  310.00, 'active',   TIMESTAMP '2026-01-09 10:45:00');

-- Row 10: present on both sides but amount/status differ from Postgres -> MODIFIED
INSERT INTO `DATASET.customers` (id, name, email, amount, status, created_at) VALUES
(10, 'Julia Roberts',  'julia@example.com',   275.50, 'inactive', TIMESTAMP '2026-01-10 15:30:00');

-- Row 12: only in BigQuery -> ADDED
INSERT INTO `DATASET.customers` (id, name, email, amount, status, created_at) VALUES
(12, 'Liam Baker',     'liam@example.com',    99.99,  'active',   TIMESTAMP '2026-01-12 11:20:00');

-- Note: id 8 and 11 are intentionally absent here but present in Postgres -> DELETED
