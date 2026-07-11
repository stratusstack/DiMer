-- DiMer test fixture: TiDB side (source B / "target")
-- TiDB is MySQL wire-compatible; apply with:
-- mysql -h $TIDB_HOST -P $TIDB_PORT -u $TIDB_USER -p $TIDB_DATABASE < tidb_customers.sql
-- Pair with testdata/postgres_customers.sql (source A) to exercise ADDED/DELETED/MODIFIED.

DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
    id         INT PRIMARY KEY,
    name       VARCHAR(100) NOT NULL,
    email      VARCHAR(150),
    amount     DECIMAL(10, 2),
    status     VARCHAR(20),
    created_at DATETIME
);

-- Rows 1-7: identical to Postgres -> MATCHED
INSERT INTO customers (id, name, email, amount, status, created_at) VALUES
(1, 'Alice Johnson',   'alice@example.com',   100.50, 'active',   '2026-01-01 10:00:00'),
(2, 'Bob Smith',       'bob@example.com',     250.00, 'active',   '2026-01-02 11:00:00'),
(3, 'Carla Diaz',      'carla@example.com',    75.25, 'inactive', '2026-01-03 09:30:00'),
(4, 'David Lee',       'david@example.com',   500.00, 'active',   '2026-01-04 14:15:00'),
(5, 'Emma Wilson',     'emma@example.com',    320.75, 'active',   '2026-01-05 08:45:00'),
(6, 'Frank Miller',    'frank@example.com',    45.00, 'inactive', '2026-01-06 16:20:00'),
(7, 'Grace Chen',      'grace@example.com',   890.10, 'active',   '2026-01-07 12:00:00');

-- Row 9: only in TiDB -> ADDED (missing from Postgres)
INSERT INTO customers (id, name, email, amount, status, created_at) VALUES
(9, 'Isabel Turner',   'isabel@example.com',  310.00, 'active',   '2026-01-09 10:45:00');

-- Row 10: present on both sides but amount/status differ from Postgres -> MODIFIED
INSERT INTO customers (id, name, email, amount, status, created_at) VALUES
(10, 'Julia Roberts',  'julia@example.com',   275.50, 'inactive', '2026-01-10 15:30:00');

-- Row 12: only in TiDB -> ADDED
INSERT INTO customers (id, name, email, amount, status, created_at) VALUES
(12, 'Liam Baker',     'liam@example.com',    99.99,  'active',   '2026-01-12 11:20:00');

-- Note: id 8 and 11 are intentionally absent here but present in Postgres -> DELETED
