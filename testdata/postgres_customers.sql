-- DiMer test fixture: PostgreSQL side (source A)
-- Apply with: psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DATABASE -f postgres_customers.sql

DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
    id         INTEGER PRIMARY KEY,
    name       VARCHAR(100) NOT NULL,
    email      VARCHAR(150),
    amount     NUMERIC(10, 2),
    status     VARCHAR(20),
    created_at TIMESTAMP
);

-- Rows 1-7: identical on both sides -> MATCHED
INSERT INTO customers (id, name, email, amount, status, created_at) VALUES
(1, 'Alice Johnson',   'alice@example.com',   100.50, 'active',   '2026-01-01 10:00:00'),
(2, 'Bob Smith',       'bob@example.com',     250.00, 'active',   '2026-01-02 11:00:00'),
(3, 'Carla Diaz',      'carla@example.com',    75.25, 'inactive', '2026-01-03 09:30:00'),
(4, 'David Lee',       'david@example.com',   500.00, 'active',   '2026-01-04 14:15:00'),
(5, 'Emma Wilson',     'emma@example.com',    320.75, 'active',   '2026-01-05 08:45:00'),
(6, 'Frank Miller',    'frank@example.com',    45.00, 'inactive', '2026-01-06 16:20:00'),
(7, 'Grace Chen',      'grace@example.com',   890.10, 'active',   '2026-01-07 12:00:00');

-- Row 8: only in Postgres -> DELETED (present in source A, missing from source B)
INSERT INTO customers (id, name, email, amount, status, created_at) VALUES
(8, 'Henry Adams',     'henry@example.com',   150.00, 'active',   '2026-01-08 13:10:00');

-- Row 10: present on both sides but with different amount/status -> MODIFIED
INSERT INTO customers (id, name, email, amount, status, created_at) VALUES
(10, 'Julia Roberts',  'julia@example.com',   220.00, 'active',   '2026-01-10 15:30:00');

-- Row 11: only in Postgres -> DELETED
INSERT INTO customers (id, name, email, amount, status, created_at) VALUES
(11, 'Kevin Wright',   'kevin@example.com',   410.40, 'inactive', '2026-01-11 09:00:00');

-- Note: id 9 and 12 are intentionally absent here but present in Databricks -> ADDED
