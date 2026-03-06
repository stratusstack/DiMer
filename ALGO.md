# DiMer Diff Algorithms

DiMer implements four diff algorithms. Three are selected automatically based on context; one requires explicit opt-in.

| Algorithm | Selected when | Key characteristic |
|---|---|---|
| `JOIN_DIFF` | Both tables on the same DB instance | SQL JOINs only — no data leaves the DB |
| `HASH_DIFF` | Tables on different DB instances (default) | Narrow Phase 1 fetch; targeted Phase 2 |
| `CROSS_DB_DIFF` | Legacy / explicit fallback | Full table fetch into Python |
| `BISECTION` | Explicit opt-in | NTILE segment hashing; best for large tables |

The selection logic in `compare()`:

```
use_bisection flag set?  →  BISECTION
same host + database?    →  JOIN_DIFF
otherwise               →  HASH_DIFF
```

`CROSS_DB_DIFF` is not selected automatically — it is available by calling `compare_cross_database()` directly.

---

## JOIN_DIFF

**File:** `dimer/core/compare.py` → `compare_within_database()`

**Used when:** both connectors share the same `host` and `database`.

### How it works

All queries run on the left connector since both tables are reachable from a single connection. No data is fetched into Python except for the column-level detail of a small number of modified rows.

**Step 1 — Schema metadata**

Fetches column lists for both tables and computes the intersection (`common_columns`). Columns present on only one side are logged as warnings but do not abort the diff.

**Step 2 — Row counts**

```sql
SELECT COUNT(*) AS row_count FROM <table_a>
SELECT COUNT(*) AS row_count FROM <table_b>
```

Used for the summary statistics only; does not affect which rows are compared.

**Step 3 — Deleted rows** (in source A, not in target B)

```sql
SELECT a.key_col
FROM table_a a
LEFT JOIN table_b b ON a.key_col = b.key_col
WHERE b.key_col IS NULL
```

Only key columns are selected — the full row is never fetched.

**Step 4 — Added rows** (in target B, not in source A)

Same pattern with the join reversed.

**Step 5 — Modified rows** (present in both, non-key columns differ)

A per-row hash is built in SQL using the connector's `DIALECTS["hash"]`, `DIALECTS["cast_to_text"]`, and `DIALECTS["concatenation"]`. For PostgreSQL this expands to:

```sql
SELECT a.key_col
FROM table_a a
INNER JOIN table_b b ON a.key_col = b.key_col
WHERE MD5(CAST(a.col1 AS TEXT) || CAST(a.col2 AS TEXT))
   != MD5(CAST(b.col1 AS TEXT) || CAST(b.col2 AS TEXT))
```

Only key columns are returned — the hash comparison happens entirely inside the DB.

**Step 6 — Column-level detail** (for up to `MAX_DETAIL_ROWS = 100` modified rows)

```sql
SELECT col1, col2, ... FROM table_a WHERE (key_col = ?) OR (key_col = ?) ...
SELECT col1, col2, ... FROM table_b WHERE (key_col = ?) OR (key_col = ?) ...
```

Full row values are fetched for both sides, then compared column-by-column in Python to populate `DiffRow.mismatched_columns`, `source_values`, and `target_values`.

### Data transferred

| Step | Columns fetched |
|---|---|
| Deleted / Added | Key columns only |
| Modified (detection) | None (hash computed in SQL) |
| Modified (detail) | All common columns, ≤ 100 rows per side |

### When it excels

- Tables of any size — even billions of rows — since the DB does the heavy lifting
- Tables with many identical rows and few differences (modified detection is O(1) in SQL)
- No network transfer for the bulk of comparison work

---

## HASH_DIFF

**File:** `dimer/core/compare.py` → `compare_hash_diff()`

**Used when:** tables are on different DB instances (the default for cross-database diffs).

### How it works

Two phases. Phase 1 is always a narrow fetch (two logical columns per row regardless of table width). Phase 2 is a targeted fetch of only the rows that require closer inspection.

### Phase 1 — Narrow fetch

```sql
-- on connector A
SELECT key_col, MD5(CAST(col1 AS TEXT) || CAST(col2 AS TEXT)) AS _dimer_row_hash
FROM schema.table_a

-- on connector B
SELECT key_col, HASH(TO_VARCHAR(col1) || TO_VARCHAR(col2)) AS _dimer_row_hash
FROM schema.table_b
```

The hash expression is built with `_build_hash_expr()` using each connector's `DIALECTS`. Regardless of how many non-key columns the table has, each row produces exactly one hash value.

Python builds two dictionaries: `{key_tuple → hash}` for each side.

### Set operations (no further fetch needed)

```
keys_only_in_a  →  DELETED rows  (done — no Phase 2 needed)
keys_only_in_b  →  ADDED rows   (done — no Phase 2 needed)
keys_in_both    →  modification candidates
```

### Phase 2 — Modification candidates

**Same DB type** (e.g. PostgreSQL ↔ PostgreSQL on different hosts):

Both sides use the same `DIALECTS["hash"]` function, so the hash values are directly comparable. Rows whose hashes match are provably identical — they are counted as matched and skipped entirely.

```
candidates = [key for key in keys_in_both if hash_a[key] != hash_b[key]]
```

Only `candidates` rows are fetched in Phase 2 — potentially zero rows if the tables are identical.

**Different DB types** (e.g. PostgreSQL ↔ Snowflake):

`MD5(...)` and `HASH(...)` produce different values for the same data so the hashes are not cross-comparable. All `keys_in_both` become candidates, but only their rows are fetched — ADDED and DELETED rows are never re-fetched.

### Phase 2 — Targeted fetch

Non-key column values are fetched only for candidate rows, chunked into batches of `_WHERE_CHUNK_SIZE = 500` keys to avoid generating overly long SQL:

```sql
SELECT col1, col2, ...
FROM schema.table_a
WHERE (key_col = v1) OR (key_col = v2) OR ...   -- up to 500 keys per chunk
```

B rows are remapped to A-side canonical column names, then:

- **Same DB type:** all candidates are confirmed modified (hash already differed); column-level detail is computed for up to `MAX_DETAIL_ROWS = 100` rows
- **Different DB type:** Python `_python_row_hash()` is used to determine which candidates actually differ, then `_classify_rows()` populates `DiffRow` entries

### Data transferred

| Step | Columns fetched | Rows fetched |
|---|---|---|
| Phase 1 | 2 (key + hash) | All rows |
| ADDED / DELETED | None | 0 |
| Phase 2 (same DB type) | All common columns | Only hash-differing rows |
| Phase 2 (different DB type) | All common columns | All common-key rows |

### Compared to CROSS_DB_DIFF

For a 1 M-row table with 30 columns and 500 modifications:

| | CROSS_DB_DIFF | HASH_DIFF (same type) | HASH_DIFF (diff type) |
|---|---|---|---|
| Phase 1 rows × cols | 1 M × 30 | 1 M × 2 | 1 M × 2 |
| Phase 2 rows × cols | — | 500 × 30 | common × 30 |
| ADDED/DELETED fetch | Full rows | None | None |

### When it excels

- Wide tables (many columns) where Phase 1 is much cheaper than a full row fetch
- Same-DB-type cross-instance diffs (e.g. prod ↔ staging on separate PostgreSQL hosts) — identical rows cost nothing beyond Phase 1
- Tables where ADDED/DELETED rows are the majority of differences

---

## CROSS_DB_DIFF

**File:** `dimer/core/compare.py` → `compare_cross_database()`

**Used when:** called directly. Not selected automatically (superseded by `HASH_DIFF`).

### How it works

Fetches every row from both tables into Python memory, computes a per-row MD5 hash in Python, then compares key-by-key.

**Step 1 — Schema metadata** — same as other algorithms.

**Step 2 — Full fetch**

```sql
SELECT col1, col2, ... FROM schema.table_a ORDER BY key_col
SELECT col1, col2, ... FROM schema.table_b ORDER BY key_col
```

All columns, all rows, from both sides. A warning is logged if either side exceeds `CROSS_DB_ROW_LIMIT = 100_000` rows.

B rows are remapped to A-side canonical column names to normalise casing differences.

**Step 3 — Python classification**

Two key → row dictionaries are built. Set operations identify ADDED and DELETED rows. For common keys, `_python_row_hash()` computes `MD5(str(val1) + "|" + str(val2) + ...)` on the non-key columns. Rows with differing hashes are MODIFIED.

**Step 4 — Column-level detail**

For up to `MAX_DETAIL_ROWS = 100` modified rows: both `row_a` and `row_b` are already in memory, so a per-column string comparison is done with no additional queries.

### Limitation

Fetches the entire table from both sides before any comparison can begin. Memory usage and network transfer grow linearly with table size × column count. Use `HASH_DIFF` or `BISECTION` for large tables.

---

## BISECTION

**File:** `dimer/core/compare.py` → `compare_bisection()`

**Used when:** `use_bisection=True` is set in the config, or the user opts in via the CLI prompt (auto-suggested when the source table exceeds 1 million rows).

### Core idea

Divide each table into N equal-sized buckets ordered by a sortable `bisection_key` column, compute an aggregate hash per bucket, and only fetch rows for buckets where the hashes differ. For tables with localised differences (e.g. only recent rows changed), only a small fraction of the data is ever transferred.

### Constants

| Constant | Default | Meaning |
|---|---|---|
| `BISECTION_DEFAULT_SEGMENTS` | 16 | Initial number of NTILE buckets |
| `BISECTION_DEFAULT_THRESHOLD` | 1000 | Bucket row count above which a warning is issued |

### Step 1 — Schema metadata and row counts

Same as other algorithms. The `bisection_key` defaults to `keys[0]` if not specified. A warning is logged if `bisection_key` is not a join key column (NTILE ties on non-unique columns produce non-deterministic bucket assignments).

### Step 2 — Segment hash queries

For each side independently:

```sql
SELECT
    bucket,
    COUNT(*) AS row_count,
    BIT_XOR(CONV(SUBSTRING(MD5("col1" || "col2"), 1, 16), 16, 10)) AS seg_hash
FROM (
    SELECT *, NTILE(16) OVER (ORDER BY key_col) AS bucket
    FROM schema.table_a
) _bisect_inner
GROUP BY bucket
ORDER BY bucket
```

The aggregate hash expression is built by `_build_aggregate_hash_expr()` using each connector's `DIALECTS["aggregate_hash"]`:

| Connector | `aggregate_hash` function |
|---|---|
| PostgreSQL | `BIT_XOR(CONV(SUBSTRING(MD5({COL}), 1, 16), 16, 10))` |
| MySQL | `BIT_XOR(CONV(SUBSTRING(MD5(CONCAT({COL})), 1, 16), 16, 10))` |
| Snowflake | `BIT_XOR(HASH({COL}))` |
| BigQuery | `BIT_XOR(FARM_FINGERPRINT({COL}))` |
| Databricks | `BIT_XOR(HASH({COL}))` |

This returns one row per bucket: `{bucket_num → {cnt, seg_hash}}`.

### Step 3 — Identify differing buckets

Buckets where `seg_hash_a != seg_hash_b` (or where the bucket exists on only one side) are collected as differing. If no buckets differ, the tables are identical — the algorithm returns immediately with `match=True`.

### Step 4 — Row-level comparison for differing buckets

For each differing bucket, rows are fetched from both sides using a second NTILE query filtered to the bucket number:

```sql
SELECT col1, col2, ...
FROM (
    SELECT *, NTILE(16) OVER (ORDER BY key_col) AS _bisect_bucket
    FROM schema.table_a
) _bisect_inner
WHERE _bisect_bucket = 3
```

B rows are remapped to A-side canonical names. The static `_classify_rows()` helper then performs Python-side ADDED / DELETED / MODIFIED classification using `_python_row_hash()`.

A warning is logged when a bucket's row count exceeds `bisection_threshold` — all rows are still fetched and compared in-memory; no further subdivision is performed in the current implementation.

### Result metadata

`DiffRun.metadata` is populated with algorithm-specific stats:

```python
{
    "segment_count": 16,          # initial number of buckets
    "depth_reached": 1,           # always 1 in current implementation
    "segments_compared": 16,      # buckets present on at least one side
    "segments_differing": 2,      # buckets with hash mismatches
}
```

The CLI displays these stats after the diff result.

### Data transferred

| Step | Data |
|---|---|
| Segment hash query | One aggregate value per bucket (16 rows) |
| Identical buckets | Nothing |
| Differing buckets | All rows in those buckets from both sides |

For a 10 M-row table with 16 buckets and differences in 1 bucket, only ~625k rows are fetched (vs. 10 M for a full scan).

### When it excels

- Very large tables (tens of millions of rows) where only a small number of buckets differ
- Append-heavy tables where recent inserts are concentrated in the last few buckets
- Any case where the fraction of changed data is small relative to table size

### Limitation

The NTILE partitioning is based on row ordering, not key ranges. If the two tables have significantly different row counts, the same bucket number will cover different key ranges on each side, producing false-positive hash mismatches and causing more buckets to be fetched than necessary. The correctness of results is not affected — only efficiency.

---

## Algorithm selection guide

```
Same database instance?
  └── Yes  →  JOIN_DIFF  (SQL JOINs; no data leaves the DB)
  └── No
        └── use_bisection=True?
              └── Yes  →  BISECTION  (NTILE hashing; best for very large tables)
              └── No   →  HASH_DIFF  (two-phase; default for cross-DB)
                              └── (for debugging / comparison: CROSS_DB_DIFF)
```

| Scenario | Recommended algorithm |
|---|---|
| Both tables on the same host | `JOIN_DIFF` (automatic) |
| Cross-DB, tables < 100k rows | `HASH_DIFF` (automatic) |
| Cross-DB, same DB type (e.g. prod ↔ staging PostgreSQL) | `HASH_DIFF` (automatic) — identical rows cost only a hash |
| Cross-DB, mixed DB types, < 1M rows | `HASH_DIFF` (automatic) |
| Any table > 1M rows with localised changes | `BISECTION` (CLI auto-suggests; set `use_bisection=True`) |
| Debugging / verifying HASH_DIFF results | `CROSS_DB_DIFF` (call `compare_cross_database()` directly) |
