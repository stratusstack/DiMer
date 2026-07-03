# DiMer Diff Algorithms

DiMer implements ten diff algorithms. Three are selected automatically based on context; seven require explicit opt-in.

| Algorithm       | Selected when                              | Key characteristic                                      |
| --------------- | ------------------------------------------ | ------------------------------------------------------- |
| `JOIN_DIFF`     | Both tables on the same DB instance        | SQL JOINs only — no data leaves the DB                  |
| `HASH_DIFF`     | Tables on different DB instances (default) | Narrow Phase 1 fetch; targeted Phase 2                  |
| `FULL_FETCH_DIFF` | Explicit fallback                          | Full table fetch into Python                            |
| `BISECTION`     | Explicit opt-in                            | NTILE segment hashing; best for large tables            |
| `SAMPLED`       | Explicit opt-in; cross-DB only             | Statistical sample — estimates diff rate with Wilson CI |
| `BLOOM`         | Explicit opt-in                            | Prefilter — cheap "definitely differs" signal; no Phase 2 fetch |
| `EMBEDDING_SIMILARITY` | Explicit opt-in                     | Vector diff — same id, distance beyond tolerance = MODIFIED |
| `SCHEMA_DIFF`   | Explicit opt-in                            | Structure compare from catalog metadata — no data read |
| `PROFILE_DIFF`  | Explicit opt-in                            | Per-column aggregate stats compare — pushdown, no row data read |
| `SKETCH_DIFF`   | Explicit opt-in                            | Approximate per-column cardinality (HLL-family) + approximate median — cheaper than PROFILE_DIFF at huge scale |

## Algorithm selection guide

```
use_schema_diff=True?  (structure only; UC2)
  └── Yes  →  SCHEMA_DIFF  (catalog metadata compare; no data read, no keys needed)
  └── No
        ↓
use_profile_diff=True?  (per-column exact aggregates; UC3)
  └── Yes  →  PROFILE_DIFF  (pushdown stats compare; no row data read, no keys needed)
  └── No
        ↓
use_sketch_diff=True?  (per-column approximate cardinality/median; UC3)
  └── Yes  →  SKETCH_DIFF  (native sketch functions where available, exact fallback otherwise; no row data read, no keys needed)
  └── No
        ↓
use_embedding=True?  (vector columns, e.g. pgvector)
  └── Yes  →  EMBEDDING_SIMILARITY  (per-id vector distance vs tolerance)
  └── No
        └── use_bloom=True?
              └── Yes  →  BLOOM  (prefilter; certain differences only, no detail fetch)
              └── No
                    └── Same database instance?  (never true for non-SQL sources)
                          └── Yes  →  JOIN_DIFF  (SQL JOINs; no data leaves the DB)
                          └── No
                                └── use_bisection=True?
                                      └── Yes  →  BISECTION  (NTILE hashing; best for very large tables)
                                      └── No
                                            └── use_sampling=True?
                                                  └── Yes  →  SAMPLED  (statistical sample; probabilistic answer)
                                                  └── No   →  HASH_DIFF  (two-phase; default for cross-DB)
                                                                  └── (for debugging / comparison: FULL_FETCH_DIFF)
```

| Scenario | Recommended algorithm |
|---|---|
| Both tables on the same host | `JOIN_DIFF` (automatic) |
| Cross-DB, tables < 100k rows | `HASH_DIFF` (automatic) |
| Cross-DB, same DB type (e.g. prod ↔ staging PostgreSQL) | `HASH_DIFF` (automatic) — identical rows cost only a hash |
| Cross-DB, mixed DB types, < 1M rows | `HASH_DIFF` (automatic) |
| Cross-DB, > 1M rows with localised changes | `BISECTION` (CLI auto-suggests; set `use_bisection=True`) |
| Cross-DB, extremely large tables, probabilistic answer OK | `SAMPLED` (set `use_sampling=True`; does not detect ADDED rows) |
| Quick "is a full diff even worth running?" check | `BLOOM` (set `use_bloom=True`; reports only certain differences) |
| Vector/embedding columns (pgvector, vector stores) | `EMBEDDING_SIMILARITY` (set `use_embedding=True` + `vector_column`) |
| CI/CD gate before a data diff; detecting silent type drift | `SCHEMA_DIFF` (set `use_schema_diff=True`; near-zero cost, no keys needed) |
| Cheap first-pass triage before committing to a row-level diff | `PROFILE_DIFF` (set `use_profile_diff=True`; one aggregation scan/side, no keys needed) |
| Same triage, but tables are huge and exact `COUNT(DISTINCT)` is too slow | `SKETCH_DIFF` (set `use_sketch_diff=True`; native HLL-family sketch where the engine has one) |
| Debugging / verifying HASH_DIFF results | `FULL_FETCH_DIFF` (call `compare_cross_database()` directly) |


`FULL_FETCH_DIFF` is not selected automatically — it is available by calling `compare_cross_database()` directly.

## Non-SQL execution path

Connectors that cannot execute SQL declare `SUPPORTS_SQL = False` and expose
data-access primitives that the algorithms call in place of generated SQL:

| Primitive | Replaces | Used by |
|---|---|---|
| `count_rows(table)` | `SELECT COUNT(*)` | BISECTION*, SAMPLED |
| `fetch_all_rows(table, columns)` | full `SELECT` | FULL_FETCH_DIFF, EMBEDDING_SIMILARITY |
| `fetch_rows_by_keys(table, columns, key_dicts, key_cols)` | `WHERE (k=v) OR …` | HASH_DIFF Phase 2, SAMPLED target fetch |
| `sample_rows(table, columns, n)` | `ORDER BY RANDOM() LIMIT n` | SAMPLED source |
| `fetch_key_hashes(table, keys, non_key_cols)` | `SELECT keys, <hash>` | HASH_DIFF Phase 1, BLOOM |

`fetch_key_hashes` computes the row hash client-side with the same Python MD5
recipe used elsewhere for cross-database hashing, so two sides of the same
connector class are hash-comparable to each other (but never to a SQL
engine's pushdown hash, or to a different non-SQL connector class).

`JOIN_DIFF` and `BISECTION` are unavailable for non-SQL sources: there are no
SQL joins, and none of these engines has a server-side aggregate hash — a
client-side bisection would fetch every row and add nothing over `HASH_DIFF`.

**UC1 (`FULL_FETCH_DIFF`, via `compare_cross_database()`) and UC2
(`SCHEMA_DIFF`, via `compare_schema_only()`) require no algorithm-layer
changes at all** — both already dispatch on `SUPPORTS_SQL` /
`get_table_metadata()`, so any connector implementing the primitives above
plus `get_table_metadata()` gets both use cases for free. This is how the
following six non-relational store families are supported, in addition to
MongoDB (DOC):

| Family | Connector | `table_name` maps to | Row identity | Schema (UC2) source |
|---|---|---|---|---|
| KV (key-value) | `dimer/connectors/redis` | key namespace pattern (`user` → `user:*`) | Redis key (`_key`) | sampled Hash-field inference |
| WIDE (wide-column) | `dimer/connectors/cassandra` | `keyspace.table` | CQL primary key columns | real catalog (`system_schema.columns`) |
| SRCH (search engine) | `dimer/connectors/elasticsearch` | index name | document `_id` | real catalog (index `_mapping`) |
| GRPH (graph) | `dimer/connectors/neo4j` | node label | `elementId(n)` (`_id`) | real catalog (`db.schema.nodeTypeProperties()`), sampled fallback |
| VEC (vector store) | `dimer/connectors/qdrant` | collection name | point `id` (`_id`) | real vector config + sampled payload inference |
| TS (time-series) | `dimer/connectors/influxdb` | measurement name | point `time` | real catalog (`SHOW FIELD KEYS` / `SHOW TAG KEYS`) |

Each follows the MongoDB template exactly: `SUPPORTS_SQL = False`,
`DIALECTS = {}`, the five primitives above implemented against the engine's
native client, and `get_table_metadata()` built from whichever schema source
the engine actually has — a real catalog where one exists (Cassandra,
Elasticsearch, Neo4j's node-type-properties procedure, InfluxDB), sampled
inference where the store is genuinely schemaless (Redis, Qdrant payloads).

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

A per-row hash is built in SQL using the connector's `DIALECTS["hash"]`, `DIALECTS["cast_to_text"]`, and `DIALECTS["concatenation"]`. 
For example, for PostgreSQL this expands to:

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

| Step                 | Columns fetched                         |
| -------------------- | --------------------------------------- |
| Deleted / Added      | Key columns only                        |
| Modified (detection) | None                                    |
| Modified (detail)    | All common columns, ≤ 100 rows per side |

### When it excels

- Tables of any size — even billions of rows — since the DB does the heavy lifting
- Tables with many identical rows and few differences (modified detection is O(1) in SQL)
- No network transfer for the bulk of comparison work

---

## HASH_DIFF

**File:** `dimer/core/compare.py` → `compare_hash_diff()`

**Used when:** tables are on different DB instances (the default for cross-database diffs).

### How it works

Two phases. Phase 1 is always a narrow fetch. Phase 2 is a targeted fetch of only the rows that require closer inspection.

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

Analyzing this output from both source and target will help find DELETED rows, ADDED rows and MODIFIED rows.

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

| Step                        | Columns fetched    | Rows fetched             |
| --------------------------- | ------------------ | ------------------------ |
| Phase 1                     | 2 (key + hash)     | All rows                 |
| ADDED / DELETED             | None               | 0                        |
| Phase 2 (same DB type)      | All common columns | Only hash-differing rows |
| Phase 2 (different DB type) | All common columns | All common-key rows      |

### Compared to FULL_FETCH_DIFF

For a 1 M-row table with 30 columns and 500 modifications:

|                      | FULL_FETCH_DIFF | HASH_DIFF (same type) | HASH_DIFF (diff type) |
| -------------------- | --------------- | --------------------- | --------------------- |
| Phase 1: rows × cols | 1 M × 30        | 1 M × 2               | 1 M × 2               |
| Phase 2: rows × cols | —               | 500 × 30              | common × 30           |
| ADDED/DELETED fetch  | Full rows       | None                  | None                  |

### When it excels

- Wide tables (many columns) where Phase 1 is much cheaper than a full row fetch
- Same-DB-type cross-instance diffs (e.g. prod ↔ staging on separate PostgreSQL hosts) — identical rows cost nothing beyond Phase 1
- Tables where ADDED/DELETED rows are the majority of differences

---

## FULL_FETCH_DIFF

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

| Constant                      | Default | Meaning                                          |
| ----------------------------- | ------- | ------------------------------------------------ |
| `BISECTION_DEFAULT_SEGMENTS`  | 16      | Initial number of NTILE buckets                  |
| `BISECTION_DEFAULT_THRESHOLD` | 1000    | Bucket row count above which a warning is issued |

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

## SAMPLED

**File:** `dimer/core/compare.py` → `compare_sampled()`

**Used when:** `use_sampling=True` is set in the config (cross-database only). The CLI offers this option automatically when BISECTION is declined and the tables are on different DB instances.

### Core idea

Instead of comparing every row, sample `sample_size` rows from the **source** table, fetch those same rows by primary key from the **target**, and classify the differences. The observed diff rate is then used to estimate the true diff rate for the entire table, with a Wilson score confidence interval bounding the estimate.

This gives a probabilistic answer in a fraction of the time a full diff would take — regardless of whether the table has 10 million or 10 billion rows.

### Source-perspective limitation

Because rows are sampled **from the source**, any rows that exist only in the target (ADDED rows) are never seen by the algorithm. The diff rate and confidence interval reflect **source-perspective differences only** (DELETED + MODIFIED). This is an inherent property of Option B1 sampling.

If detecting added rows is a requirement, use `HASH_DIFF` or `BISECTION` instead.

### Constants

| Constant | Default | Meaning |
|---|---|---|
| `SAMPLED_DEFAULT_SIZE` | 10,000 | Default number of rows to sample |
| `SAMPLED_DEFAULT_CONFIDENCE` | 0.95 | Default Wilson CI confidence level |

### Step 1 — Schema metadata and full row count

Schema metadata is resolved exactly as in other algorithms. Then a `COUNT(*)` is run on the full source table — this count is used later for extrapolation and is stored in `metadata["source_row_count_full"]`.

```sql
SELECT COUNT(*) AS row_count FROM schema.table_a
```

### Step 2 — Sample rows from source

Rows are drawn randomly from the source using `ORDER BY {random_func} LIMIT n`, where `random_func` comes from `DIALECTS["random_func"]`:

| Connector | `random_func` |
|---|---|
| PostgreSQL | `RANDOM()` |
| Snowflake | `RANDOM()` |
| MySQL | `RAND()` |
| BigQuery | `RAND()` |
| Databricks | `RAND()` |

For PostgreSQL this expands to:

```sql
SELECT col1, col2, ...
FROM schema.table_a
ORDER BY RANDOM()
LIMIT 10000
```

All common columns are selected so that the sample rows are immediately usable for the column-level diff.

### Step 3 — Fetch matching rows from target

The key values from the sampled rows are used to build a `WHERE key IN (...)` query on the target, chunked into batches of `_WHERE_CHUNK_SIZE = 500` keys to keep individual SQL statements within safe length limits:

```sql
SELECT col1, col2, ...
FROM schema.table_b
WHERE (key_col = v1) OR (key_col = v2) OR ...   -- up to 500 keys per chunk
```

Rows missing from the target result set are DELETED. Rows present in both sides are candidates for MODIFIED.

### Step 4 — Classify rows

The static `_classify_rows()` helper is reused here exactly as in `BISECTION` leaf-node processing:

- Keys in source sample but not in target → **DELETED**
- Keys in both, but non-key column values differ → **MODIFIED**
- Keys in both, values identical → **matched** (not reported)

### Step 5 — Wilson score confidence interval

The observed diff rate is `p̂ = k / n` where `k` = differing rows and `n` = actual sample size.

The Wilson score CI (`_wilson_ci(k, n, confidence)`) is used rather than the naive normal approximation because it remains accurate when `p̂` is near 0 (tables that are mostly identical — the common case):

```
center = (p̂ + z²/2n) / (1 + z²/n)
spread = z × sqrt(p̂(1−p̂)/n + z²/4n²) / (1 + z²/n)

lower = center − spread
upper = center + spread
```

`z` is the standard normal quantile for the chosen confidence level (1.96 for 95%). Pre-computed for 0.90, 0.95, and 0.99; a rational approximation is used for other values.

### Step 6 — Extrapolation

```python
estimated_total_diffs = int(p_hat * source_row_count_full)
```

### Result metadata

`DiffRun.metadata` is populated with:

```python
{
    "sample_size": 9604,               # actual rows sampled (≤ requested)
    "source_row_count_full": 50000000, # COUNT(*) of the full source table
    "sampled_diff_count": 50,          # DELETED + MODIFIED in the sample
    "observed_diff_rate": 0.0052,      # k / n
    "estimated_diff_pct": 0.52,        # observed_diff_rate × 100
    "ci_lower": 0.39,                  # Wilson CI lower bound (%)
    "ci_upper": 0.65,                  # Wilson CI upper bound (%)
    "margin_of_error": 0.13,           # (ci_upper − ci_lower) / 2 (%)
    "confidence_level": 0.95,
    "estimated_total_diffs": 260000,   # int(observed_diff_rate × full_count)
}
```

`DiffRun.summary` counts are over the **sample** only (not extrapolated): `source_row_count = sample_size`, `target_row_count = matching rows found in target`.

### Data transferred

| Step | Data |
|---|---|
| Full count | One integer (COUNT result) |
| Source sample | `sample_size` rows × all common columns from source |
| Target fetch | Up to `sample_size` rows × all common columns from target |

Total data transfer is `2 × sample_size × row_width` regardless of total table size. For a 10 M-row, 20-column table sampled at 10,000 rows, this is ~1/1000th of what `HASH_DIFF` Phase 1 alone would transfer.

### Choosing a sample size

The margin of error depends only on `sample_size`, not on total table size (binomial proportion CI property):

| Target margin of error | Sample size needed (95% CI) |
|---|---|
| ±5% | 385 rows |
| ±2% | 2,401 rows |
| ±1% | 9,604 rows |
| ±0.5% | 38,416 rows |

### When it excels

- Extremely large tables (hundreds of millions to billions of rows) where even BISECTION would be slow
- Monitoring / alerting use cases where a probabilistic "roughly X% of rows differ" answer is sufficient
- Initial investigation before committing to a full diff

### When not to use it

- When you need an exact diff (use `HASH_DIFF` or `BISECTION`)
- When detecting ADDED rows in the target is a requirement
- When the table is small enough for `HASH_DIFF` to complete quickly

---

## BLOOM

**File:** `dimer/core/algorithms/bloom.py` → `BloomPrefilterAlgorithm`

**Used when:** `use_bloom=True` is set in the config. The CLI offers it for
cross-instance comparisons before the BISECTION/SAMPLED prompts.

### Core idea

A **prefilter**, not an exact diff. Fetch only `(key columns, row hash)` from
each side — exactly HASH_DIFF Phase 1 — but instead of building key→hash
dictionaries and running a Phase-2 row fetch, insert each side into Bloom
filters and stream the opposite side through them:

- a key that *misses* the opposite key-filter is **definitely** ADDED/DELETED
  (Bloom filters have no false negatives);
- a `key#hash` that misses the opposite key+hash filter (when hashes are
  comparable) is **definitely** MODIFIED;
- a *hit* may be a false positive, so up to `bloom_fpr` (default 1%) of truly
  differing rows can be missed.

The asymmetry is the point: every difference BLOOM reports is certain, but
`match=True` only means "no differences detected" — run `HASH_DIFF` or
`BISECTION` afterwards to prove parity.

### Configuration

| Key | Default | Meaning |
|---|---|---|
| `use_bloom` | — | explicit opt-in |
| `bloom_fpr` | 0.01 | target false-positive rate; sizes the filters (~9.6 bits/row at 1%) |

### Hash comparability

Same rules as HASH_DIFF: same connector type → row hashes comparable →
MODIFIED detectable. Different connector types → key membership only
(ADDED/DELETED signal; `metadata["hash_comparable"] = False`). Non-SQL
connectors compute the Python MD5 row hash client-side via
`fetch_key_hashes()`, so two MongoDB sides are comparable.

### Result semantics

- `row_diffs` holds key-only `DiffRow`s capped at `MAX_DETAIL_ROWS` (no
  column detail is ever fetched — that is what keeps it cheap).
- `match=True` requires zero definite differences **and** equal row counts.
- `DiffRun.metadata`: `prefilter`, `bloom_fpr`, `bloom_bits_per_side`,
  `bloom_hash_count`, `hash_comparable`, `definite_added`,
  `definite_deleted`, `definite_modified`.

### Data transferred

Identical to HASH_DIFF Phase 1 (keys + one hash per row) with **no Phase 2**.
The filters themselves are ~1.2 KB per 1,000 rows at 1% FPR.

### When it excels

- Deciding cheaply whether a full diff is worth scheduling (UC5 heartbeats)
- Very wide tables where even the Phase-2 candidate fetch would be expensive
- Memory-constrained comparisons (no key→hash dictionaries are kept)

### When not to use it

- When you need exact counts or row detail (use `HASH_DIFF`)
- When a guaranteed-complete difference list is required (FPs hide diffs)

---

## EMBEDDING_SIMILARITY

**File:** `dimer/core/algorithms/embedding.py` → `EmbeddingSimilarityAlgorithm`

**Used when:** `use_embedding=True` is set in the config (requires
`vector_column`).

### Core idea

Vector stores need their own notion of "modified": two index builds can store
the same logical embedding with float noise, so row-hash equality is
meaningless. A row is MODIFIED when
`distance(vec_source, vec_target) > distance_threshold` for the same id.

Steps:

1. Fetch `(keys, vector_column)` from both sides. On SQL sources the vector
   column is cast to text via the dialect (pgvector's `'[…]'` literal parses
   directly); non-SQL/vector connectors use `fetch_all_rows()`.
2. ADDED / DELETED via key-set operations, as in other algorithms.
3. For common ids, parse both vectors and compute the distance. Ids beyond
   the threshold — or with unparseable / dimension-mismatched vectors — are
   MODIFIED.

### Configuration

| Key | Default | Meaning |
|---|---|---|
| `use_embedding` | — | explicit opt-in |
| `vector_column` | — (required) | column holding the embedding |
| `distance_metric` | `cosine` | `cosine` (1 − cosine similarity) or `l2` (Euclidean) |
| `distance_threshold` | `1e-3` | max tolerated distance before MODIFIED |

### Result metadata

`DiffRun.metadata`: `vector_column`, `distance_metric`, `distance_threshold`,
`compared_pairs`, `max_distance`, `mean_distance`, `over_threshold`,
`dimension_mismatches`, `parse_failures`. MODIFIED `DiffRow`s carry the
distance in `source_values["_distance"]`.

### Supported sources

Works today against **pgvector through the PostgreSQL connector** and any SQL
connector that can return the vector column as text. Dedicated vector-DB
connectors (Pinecone, Milvus, Qdrant) plug in through the same non-SQL
primitives as MongoDB and are still planned.

### When it excels

- Verifying a vector-store migration or re-index (same ids, same embeddings?)
- Detecting embedding drift after a model or pipeline change
- Any diff where float noise below a tolerance must count as "equal"

### When not to use it

- Non-vector data (use the standard algorithms)
- When exact float equality matters (set the threshold to 0 — but HASH_DIFF
  is then usually cheaper, since it pushes hashing down to the database)

---

## SCHEMA_DIFF

**File:** `dimer/core/algorithms/schema_diff.py` → `SchemaDiffAlgorithm`

**Used when:** `use_schema_diff=True` is set in the config, or by calling
`Diffcheck.compare_schema_only()` directly. The CLI offers it right after the
table names are entered — join keys are never prompted for, because a
structure compare does not need them (`keys` may be `[]`).

### Core idea

UC2: compare table *structure* from catalog metadata — column sets, data
types, nullability, and primary keys — **without reading a single data row**.
This is the CI/CD gate: run it before a data diff to catch silent type or
precision drift, dropped columns, or primary-key changes at near-zero cost.

Both sides are read through the connector's existing `get_table_metadata()`:

| Source family | Catalog source |
|---|---|
| REL (PostgreSQL, MySQL) | `information_schema.columns` + key constraints |
| DWH (Snowflake, BigQuery, Databricks, DuckDB) | native catalog / information schema |
| NSQL (CockroachDB, TiDB, Yugabyte) | inherited from the PostgreSQL / MySQL connectors |
| DOC (MongoDB) | works incidentally via sampled schema inference (see caveat) |
| KV (Redis) | sampled Hash-field inference (see [Non-SQL execution path](#non-sql-execution-path)) |
| WIDE (Cassandra) | real catalog — `system_schema.columns` |
| SRCH (Elasticsearch) | real catalog — index `_mapping` |
| GRPH (Neo4j) | real catalog — `db.schema.nodeTypeProperties()`, sampled fallback |
| VEC (Qdrant) | real vector config + sampled payload-field inference |
| TS (InfluxDB) | real catalog — `SHOW FIELD KEYS` / `SHOW TAG KEYS` |

### What is compared

Column names are matched **case-insensitively** (identifier-case differences
across engines are not structural drift). Data types are compared after
`DataTypeMapper` normalisation, so PostgreSQL `character varying` equals
Snowflake `VARCHAR`.

| Attribute | Compared | Notes |
|---|---|---|
| column presence | always | ADDED / DELETED per column |
| `data_type` | always | common-type normalised |
| `nullable` | always | |
| `is_primary_key` | always | plus whole-PK set comparison in metadata |
| `max_length`, `precision`, `scale` | only with `schema_strict=True` | noisy across engines (e.g. Snowflake NUMBER defaults to precision 38) |

Row-count drift is surfaced in `metadata` (from catalog statistics) but never
affects `match` — it is not structural.

### Configuration

| Key | Default | Meaning |
|---|---|---|
| `use_schema_diff` | — | explicit opt-in |
| `schema_strict` | `False` | also compare max_length / precision / scale |

### Result semantics

Each differing **column** becomes one `DiffRow` (`key_values = {"column":
name}`), so persistence and the CLI detail display work unchanged:

- `DELETED` — column exists only in the source
- `ADDED` — column exists only in the target
- `MODIFIED` — attribute drift; `mismatched_columns` lists the differing
  *attribute names* and `source_values` / `target_values` hold each side's
  attribute dict

`DiffRun.summary` counts are over **columns**, not rows.
`DiffRun.metadata`: `strict`, `columns_source`, `columns_target`,
`columns_common`, `primary_key_source`, `primary_key_target`,
`primary_key_match`, `table_row_count_source`, `table_row_count_target`.

The legacy `Diffcheck.check_schema(table_a, table_b) -> bool` is now a thin
wrapper over this algorithm (presence-only verdict, unchanged behavior).

### Data transferred

Catalog metadata only — a handful of information-schema rows per side. No
data pages are touched.

### When it excels

- CI/CD gating: fail fast before spending compute on a row-level diff
- Detecting silent type/precision drift after migrations or dbt runs
- Cross-engine migration validation (common-type mapping absorbs dialect noise)

### When not to use it

- Equal schemas say nothing about equal *data* — follow up with HASH_DIFF /
  JOIN_DIFF / BISECTION
- Document stores: MongoDB metadata is inferred from a 100-document sample,
  so absent/rare fields may be missed — treat DOC schema diffs as indicative,
  not exact

---

## PROFILE_DIFF

**File:** `dimer/core/algorithms/profile_diff.py` → `ProfileDiffAlgorithm`

**Used when:** `use_profile_diff=True` is set in the config, or by calling
`Diffcheck.compare_profile_only()` directly. The CLI offers it right after
the schema-diff prompt (both skip key detection — `keys` may be `[]`).

### Core idea

UC3: compare per-column **aggregate statistics** — count, nulls, distinct
count, min/max, avg/sum — instead of row data. One aggregation query per side
computes every profiled column's stats in a single scan; no row values leave
the database. This is a **triage signal, not a row-level diff**: two tables
can have identical counts/min/max/avg with completely different row
contents, but differing profiles *prove* the tables differ. Run it before
committing to a full row-level diff (HASH_DIFF/JOIN_DIFF/BISECTION).

### Which stats are computed per column

Decided independently per side from that side's own catalog metadata (the
same `DataTypeMapper`-normalised common type used by `SCHEMA_DIFF`), so a
type mismatch between sides never fails the query — it just means fewer
stats are comparable for that column.

| Stat | Requires | Skipped for |
|---|---|---|
| `count`, `null_count` | always computed | — |
| `distinct_count` | `COUNT(DISTINCT col)` — needs an equality operator | `json`, `array`, `object`, `binary` (equality semantics vary or are unsupported, e.g. Postgres `json` vs `jsonb`) |
| `min`, `max` | orderable type | numeric, date/time, and string/text only |
| `avg`, `sum` | numeric type | non-numeric columns |

Only stats present on **both** sides for a column are compared; the rest are
silently skipped (visible per-column in `source_values`/`target_values` on a
`DiffRow`, since only the keys that exist are stored).

### SQL shape

One query per side, all columns profiled in a single `SELECT`, using
positional aliases (`c0__count`, `c0__distinct`, `c1__min`, …) rather than
column-name-derived aliases — this sidesteps identifier-quoting and
case-folding differences across engines entirely; results are matched back
to columns by position, using case-insensitive lookups for the raw alias
each engine happens to return.

```sql
SELECT COUNT(*) AS _dimer_row_count,
       COUNT("amount") AS c0__count,
       COUNT(DISTINCT "amount") AS c0__distinct,
       MIN("amount") AS c0__min,
       MAX("amount") AS c0__max,
       AVG("amount") AS c0__avg,
       SUM("amount") AS c0__sum,
       COUNT("name") AS c1__count,
       COUNT(DISTINCT "name") AS c1__distinct,
       MIN("name") AS c1__min,
       MAX("name") AS c1__max
FROM schema.table_a
```

`COUNT`, `COUNT(DISTINCT …)`, `MIN`, `MAX`, `AVG`, `SUM` are standard SQL
supported identically across PostgreSQL, MySQL, Snowflake, BigQuery,
Databricks, DuckDB, and the NSQL connectors that inherit from them — no new
`DIALECTS` entries were needed.

### Configuration

| Key | Default | Meaning |
|---|---|---|
| `use_profile_diff` | — | explicit opt-in |
| `profile_columns` | all common columns | restrict profiling to a subset |
| `profile_numeric_tolerance` | `1e-6` | relative tolerance for `min`/`max`/`avg`/`sum` comparison |

### Comparison semantics

`count`, `null_count`, and `distinct_count` are integers, compared exactly.
`min`, `max`, `avg`, `sum` are compared with a **relative tolerance**
(`abs(a - b) <= tolerance * max(|a|, |b|, 1.0)`) to absorb cross-engine
floating-point and aggregation-order noise; exact integer/date values
naturally pass with zero delta, so the tolerance never hides a real integer
mismatch.

### Result semantics

Each column with any differing stat becomes one `DiffRow`
(`key_values = {"column": name}`, `status = MODIFIED`); `mismatched_columns`
lists the differing *stat names* and `source_values` / `target_values` hold
each side's full stat dict. `DiffRun.summary` counts are over **profiled
columns**, not rows — there are no ADDED/DELETED rows in this algorithm
(column presence drift is `SCHEMA_DIFF`'s job).

`DiffRun.metadata`: `numeric_tolerance`, `columns_profiled`,
`columns_common`, `table_row_count_source`, `table_row_count_target`.

### Data transferred

One aggregate row per side, regardless of table size — a handful of numbers
per profiled column. The database still performs a full scan internally
(`COUNT(DISTINCT …)` in particular is not free), but no row data crosses the
network.

### When it excels

- Cheap first-pass triage before a row-level diff (UC5 heartbeat candidate)
- Wide tables where even HASH_DIFF's narrow Phase 1 is heavier than needed
- Spotting a bad load early (row count / null rate / distinct count off) without waiting on a full diff

### When not to use it

- When you need to know *which* rows differ (use HASH_DIFF/JOIN_DIFF/BISECTION)
- When a clean profile must be trusted as proof of parity — it is not; two
  very different tables can share identical count/min/max/avg by coincidence

---

## SKETCH_DIFF

**File:** `dimer/core/algorithms/sketch_diff.py` → `SketchDiffAlgorithm`

**Used when:** `use_sketch_diff=True` is set in the config, or by calling
`Diffcheck.compare_sketch_only()` directly. The CLI's aggregate-diff prompt
(right after the schema-diff prompt) offers PROFILE_DIFF and SKETCH_DIFF as
two options with a short description each — see below.

### Core idea

Where PROFILE_DIFF's `distinct_count` is exact `COUNT(DISTINCT col)` —
correct but requires scanning/hashing every value — SKETCH_DIFF asks each
connector for its **native probabilistic sketch function** instead: a
HyperLogLog-family structure that estimates cardinality within a few percent
using a fixed, tiny amount of memory regardless of table size. It does the
same for the median, using a quantile-summary sketch (t-Digest,
Greenwald-Khanna, or engine-specific) where one exists. This is the natural
next step over PROFILE_DIFF at very large cardinalities, exactly as
flagged in `USE_CASE_MATRIX.md`.

**Research note:** sketch availability is genuinely per-engine, not
per-dialect. Two connectors that are wire-compatible for SQL execution can
still differ completely in sketch support — TiDB has a native
`APPROX_COUNT_DISTINCT` that vanilla MySQL lacks entirely; CockroachDB
accepts `CREATE EXTENSION` syntactically but treats it as a documented
no-op, so the `postgresql-hll` extension can never actually be installed
there even though it's wire-compatible with PostgreSQL. Because of this,
sketch capability is declared per **connector class** (a `SKETCH_FUNCS`
dict, following the same pattern as `DIALECTS`), not inherited by default —
NSQL connectors that do inherit it (CockroachDB, Yugabyte from PostgreSQL)
do so because their actual capability matches, verified individually below.

### Per-engine algorithm (verified against vendor documentation)

| Engine | `APPROX_COUNT_DISTINCT` equivalent | Algorithm | `median` equivalent | Algorithm |
|---|---|---|---|---|
| Snowflake | `APPROX_COUNT_DISTINCT(col)` | HyperLogLog (bias-corrected, Flajolet et al.) | `APPROX_PERCENTILE(col, 0.5)` | improved t-Digest |
| BigQuery | `APPROX_COUNT_DISTINCT(col)` | HyperLogLog++ | `APPROX_QUANTILES(col, 2)[OFFSET(1)]` | quantile-summary sketch (algorithm undocumented by Google) |
| Databricks | `approx_count_distinct(col)` | HyperLogLog++ (dense variant) | `approx_percentile(col, 0.5)` | Greenwald-Khanna quantile summary |
| DuckDB | `approx_count_distinct(col)` | HyperLogLog | `approx_quantile(col, 0.5)` | t-Digest |
| TiDB | `APPROX_COUNT_DISTINCT(col)` | BJKST algorithm | `APPROX_PERCENTILE(col, 50)` — **percentage 0–100, not a 0–1 fraction** | undocumented by PingCAP |
| PostgreSQL | *(none — falls back to exact `COUNT(DISTINCT col)`)* | exact | `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY col)` | exact (not a sketch, but still single-scan pushdown) |
| CockroachDB | *(none — `CREATE EXTENSION` is a documented no-op, so `postgresql-hll` can't be installed)* | exact | inherited from PostgreSQL (`PERCENTILE_CONT` is natively supported) | exact |
| YugabyteDB | *(none in the current implementation — see limitation below)* | exact | inherited from PostgreSQL (YSQL supports `PERCENTILE_CONT` natively) | exact |
| MySQL | *(none — falls back to exact `COUNT(DISTINCT col)`)* | exact | *(omitted entirely — vanilla MySQL has no `PERCENTILE_CONT`/`PERCENTILE_DISC` at all)* | unsupported |

**YugabyteDB limitation:** the `hll` extension *can* be installed
(`CREATE EXTENSION hll`, unlike CockroachDB where the statement is a no-op),
but nothing guarantees a given cluster has actually done so — DiMer cannot
assume a server-side admin action it didn't perform, so Yugabyte gets the
same exact fallback as PostgreSQL for now. Detecting the extension at
connect time and opportunistically using it is tracked in
`TODO_FOR_LATER.md`.

**MySQL limitation:** the only engine where a stat is *entirely* unavailable
rather than falling back to an exact equivalent — vanilla MySQL has neither
a cardinality sketch nor any percentile function (MySQL HeatWave's `HLL()`
is a separate managed engine, not applicable here). `distinct_estimate`
still works (exact `COUNT(DISTINCT)` fallback); `median_estimate` is simply
never computed for MySQL-family columns.

### Which stats are computed per column

Same eligibility rules as PROFILE_DIFF, decided independently per side:

| Stat | Eligible types | Notes |
|---|---|---|
| `distinct_estimate` | same as PROFILE_DIFF's `distinct_count` (numeric, date/time, string/text, boolean, uuid) | uses the connector's `SKETCH_FUNCS["distinct"]` template, or exact `COUNT(DISTINCT)` if absent |
| `median_estimate` | numeric + date/time only (not string/text — sketches model numeric distributions) | uses `SKETCH_FUNCS["median"]`; entirely omitted if the connector declares none |

`distinct_method` / `median_method` accompany each estimate in
`source_values`/`target_values` (e.g. `"HyperLogLog"`, `"exact"`) for
transparency, but are never compared — a genuinely different algorithm on
each side (e.g. Snowflake vs. BigQuery) is expected, not a mismatch.

### Configuration

| Key | Default | Meaning |
|---|---|---|
| `use_sketch_diff` | — | explicit opt-in |
| `sketch_columns` | all common columns | restrict to a subset |
| `sketch_relative_tolerance` | `0.05` (5%) | relative tolerance for estimate comparison |

The default tolerance is deliberately looser than PROFILE_DIFF's `1e-6`:
HyperLogLog-family estimators carry a few percent error by design (Snowflake
documents ~1.6% average relative error at default precision), and cross-side
comparisons can even mix two *different* sketch algorithms or an exact value
against an estimate.

### Result semantics

Same shape as PROFILE_DIFF (shared via `BaseAlgorithm._diff_stat_dicts`):
one `DiffRow` per column with any differing stat, `mismatched_columns`
listing the differing stat names, `source_values`/`target_values` holding
the full per-column stat dict (including the method labels).
`DiffRun.summary` counts are over **profiled columns**.

`DiffRun.metadata`: `relative_tolerance`, `columns_profiled`,
`columns_common`, `table_row_count_source`, `table_row_count_target`,
`distinct_algorithm_source`, `distinct_algorithm_target`,
`median_algorithm_source`, `median_algorithm_target` (`"unsupported"` when a
side has no median capability at all — currently only MySQL).

### When it excels

- Cardinality/median triage on tables too large for PROFILE_DIFF's exact
  `COUNT(DISTINCT)` to be cheap (billions of rows, high-cardinality columns)
- Warehouse-to-warehouse comparisons where both sides already have HLL-family
  functions built in (Snowflake/BigQuery/Databricks/DuckDB) — genuinely free
  accuracy, no extension installation required
- Continuous/scheduled monitoring (UC5) where the tolerance for a false
  "differs" signal is higher than for a one-off audit

### When not to use it

- Small-to-medium tables where PROFILE_DIFF's exact stats are cheap anyway —
  SKETCH_DIFF trades exactness for scale, which isn't worth it below the
  threshold where `COUNT(DISTINCT)` is already fast
- MySQL-only comparisons where median matters — it's never computed there
- When the 5% default tolerance would hide a difference you care about (tune
  `sketch_relative_tolerance` down, but remember the estimates themselves
  carry inherent error below a few percent regardless of tolerance setting)

### Sources for the per-engine algorithm matrix

- [Snowflake — APPROX_COUNT_DISTINCT](https://docs.snowflake.com/en/sql-reference/functions/approx_count_distinct), [APPROX_PERCENTILE](https://docs.snowflake.com/en/sql-reference/functions/approx_percentile)
- [BigQuery — Approximate aggregate functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/approximate_aggregate_functions), [HyperLogLog++ functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/hll_functions)
- [Databricks — approx_count_distinct](https://docs.databricks.com/aws/en/sql/language-manual/functions/approx_count_distinct), [percentile_approx](https://docs.databricks.com/aws/en/sql/language-manual/functions/percentile_approx); [Spark PR #14298 implementing percentile_approx via Greenwald-Khanna](https://github.com/apache/spark/pull/14298)
- [DuckDB — approx_count_distinct / approx_quantile (t-Digest)](https://database.guide/understanding-duckdbs-approx_count_distinct-function/)
- [TiDB — Aggregate (GROUP BY) Functions (APPROX_COUNT_DISTINCT / APPROX_PERCENTILE)](https://docs.pingcap.com/tidb/stable/aggregate-group-by-functions/)
- [PostgreSQL — postgresql-hll extension (citusdata)](https://github.com/citusdata/postgresql-hll), [tdigest extension (tvondra)](https://github.com/tvondra/tdigest) — both confirm neither ships with core PostgreSQL
- [CockroachDB — aggregate function reference](https://github.com/cockroachdb/cockroach/blob/master/docs/generated/sql/aggregates.md) (no APPROX_*/HLL functions; PERCENTILE_CONT/DISC present), [CREATE EXTENSION is a documented no-op](https://github.com/cockroachdb/cockroach/issues/74777)
- [YugabyteDB — postgresql-hll extension support](https://docs.yugabyte.com/stable/additional-features/pg-extensions/extension-postgresql-hll/)
- [MySQL — no PERCENTILE_CONT/DISC support](https://bugs.mysql.com/bug.php?id=93234); [MySQL HeatWave HLL() (separate managed engine)](https://dev.mysql.com/doc/heatwave/en/mys-hw-aggregate-functions.html)

---

