I'll help you analyze the project and explain the data diff algorithm. Let me start by exploring the folder structure to understand what we're working with.Now, let me examine the key files to understand the data diff algorithm. Let me start with the README to get an overview, then look at the main diff implementation files.Now let me look at the technical explanation document to understand the algorithm better:Now let me examine the main implementation files for the different algorithms:Now let me examine the main entry point and how the algorithms are selected:Now let me look at the table_segment module to understand how data is segmented:Now let me check if there are any test files that might give more insight into how the algorithm works:Now let me also look at the lexicographic space module which seems important for the algorithm:Now I have a comprehensive understanding of the data-diff project. Let me generate an extensive summary of the algorithm and its implementation.

## Extensive Summary: Data-Diff Algorithm and Implementation

### **Project Overview**

Data-diff is a sophisticated tool designed to efficiently compare datasets within or across SQL databases. It employs two distinct algorithms optimized for different scenarios:

1. **JoinDiff**: For comparing tables within the same database
2. **HashDiff**: For cross-database comparisons or large tables with few differences

### **Core Algorithm Architecture**

#### **1. HashDiff Algorithm (Cross-Database Diffing)**

The HashDiff algorithm is the centerpiece of data-diff's cross-database comparison capability. It uses a **divide-and-conquer approach** based on hashing and binary search.

**Key Principles:**
- **Hierarchical Segmentation**: Recursively divides tables into smaller segments
- **Checksum Comparison**: Uses checksums to quickly identify segments with differences
- **Progressive Refinement**: Only explores segments that contain differences
- **Database-Native Computation**: Pushes computation to the database engines

**Algorithm Steps:**

1. **Initial Bounds Discovery**:
   - Queries both databases for `min(key)` and `max(key)` values
   - Establishes the key space boundaries for comparison
   - Handles compound keys using lexicographic ordering

2. **Segment Division**:
   - Divides the key space into `bisection_factor` segments (default: 32)
   - Creates evenly-spaced checkpoints using the `split_key_space` function
   - For compound keys, creates an N-dimensional grid of split points

3. **Checksum Calculation**:
   - For each segment, calculates a checksum using database-native functions
   - Query pattern:
   ```sql
   SELECT count(*), 
          sum(cast(conv(substring(md5(concat(columns)), 18), 16, 10) as unsigned))
   FROM table
   WHERE (key >= min) AND (key < max)
   ```
   - Runs multiple queries in parallel (controlled by `threads` parameter)

4. **Recursive Refinement**:
   - If checksums match → segments are identical, stop processing
   - If checksums differ → further divide the segment
   - Continue until segment size < `bisection_threshold` (default: 16,384 rows)

5. **Local Comparison**:
   - When segments are small enough, download rows to memory
   - Perform direct row-by-row comparison
   - Handle JSON columns with semantic equality checking

**Implementation Details:**

```python
class HashDiffer(TableDiffer):
    def _diff_segments(self, ti, table1, table2, info_tree, max_rows, level=0):
        # Calculate checksums for both segments
        (count1, checksum1), (count2, checksum2) = self._threaded_call(
            "count_and_checksum", [table1, table2]
        )
        
        # If checksums match, segments are identical
        if checksum1 == checksum2:
            info_tree.info.is_diff = False
            return
        
        # If below threshold, download and compare locally
        if max_rows < self.bisection_threshold:
            rows1, rows2 = self._threaded_call("get_values", [table1, table2])
            diff = list(diff_sets(rows1, rows2, json_cols))
            return diff
        
        # Otherwise, bisect and recurse
        return self._bisect_and_diff_segments(ti, table1, table2, info_tree, level, max_rows)
```

#### **2. JoinDiff Algorithm (Same-Database Diffing)**

JoinDiff leverages SQL JOIN operations for efficient comparison within the same database.

**Key Features:**
- Uses OUTER JOIN to identify differences
- Can materialize results to a table
- Supports sampling of exclusive rows
- Validates key uniqueness

**Algorithm Implementation:**

```python
def _create_outer_join(self, table1, table2):
    # Create comparison expressions for each column
    is_diff_cols = {
        f"is_diff_{c1}": bool_to_int(a[c1].is_distinct_from(b[c2])) 
        for c1, c2 in safezip(cols1, cols2)
    }
    
    # Normalize columns for comparison
    a_cols = {f"{c}_a": NormalizeAsString(a[c]) for c in cols1}
    b_cols = {f"{c}_b": NormalizeAsString(b[c]) for c in cols2}
    
    # Perform outer join
    all_rows = outerjoin(a, b).on(keys).select(
        is_exclusive_a=is_exclusive_a,
        is_exclusive_b=is_exclusive_b,
        **is_diff_cols,
        **cols
    )
    
    # Filter to only different rows
    diff_rows = all_rows.where(or_(this[c] == 1 for c in is_diff_cols))
```

### **Advanced Features**

#### **3. Compound Key Support**

The system handles multi-column primary keys using **lexicographic space** abstraction:

```python
class BoundedLexicographicSpace:
    """Handles N-dimensional key spaces with arbitrary bounds"""
    
    def range(self, min_value: Vector, max_value: Vector, count: int):
        """Generate evenly-spaced points in N-dimensional space"""
        # Converts to unit space, divides evenly, converts back
        return [
            self.from_uspace(v) 
            for v in self.uspace.range(
                self.to_uspace(min_value), 
                self.to_uspace(max_value), 
                count
            )
        ]
```

This enables:
- Uniform distribution of checkpoints across multiple key dimensions
- Proper handling of composite key boundaries
- Efficient segmentation regardless of key complexity

#### **4. Type Normalization System**

Data-diff implements sophisticated type handling to ensure correct comparisons across different database systems:

```python
class NormalizeAsString(Expr):
    """Normalizes values to strings for consistent comparison"""
    
    def compile(self, compiler):
        # Database-specific normalization logic
        if isinstance(db, MySQL):
            return f"CAST({expr} AS CHAR)"
        elif isinstance(db, PostgreSQL):
            return f"{expr}::TEXT"
        # ... database-specific implementations
```

**Supported Type Conversions:**
- UUID normalization (handles different UUID representations)
- Numeric precision alignment
- Datetime format standardization
- JSON semantic comparison
- NULL value handling

#### **5. Parallel Execution Framework**

The system uses a sophisticated threading model:

```python
class ThreadedYielder:
    """Manages parallel execution with priority queuing"""
    
    def submit(self, func, *args, priority=0):
        """Submit work with priority-based scheduling"""
        self.futures.put((priority, future))
```

**Features:**
- Thread pool management per database connection
- Priority-based task scheduling (deeper segments have higher priority)
- Adaptive parallelism based on database capabilities

### **Performance Optimizations**

#### **6. Smart Segmentation Strategy**

The algorithm adapts segmentation based on data distribution:

1. **Gap Detection**: Identifies large gaps in key columns
2. **Dynamic Adjustment**: Adjusts bisection factor for sparse data
3. **Early Termination**: Stops processing empty segments

```python
def choose_checkpoints(self, count: int):
    """Intelligently choose split points based on data distribution"""
    # Uses statistical sampling for better checkpoint selection
    # Adapts to key distribution patterns
```

#### **7. Database-Specific Optimizations**

Different strategies for different database engines:

```python
if isinstance(db, (Snowflake, BigQuery, DuckDB)):
    # Cloud databases: Let database handle parallelization
    yield from self._diff_segments(None, table1, table2, info_tree, None)
else:
    # Traditional databases: Manual segmentation
    yield from self._bisect_and_diff_tables(table1, table2, info_tree)
```

### **Key Implementation Components**

#### **8. TableSegment Class**

Core abstraction for table portions:

```python
@attrs.define(frozen=True)
class TableSegment:
    database: Database
    table_path: DbPath
    key_columns: Tuple[str, ...]
    update_column: Optional[str]
    extra_columns: Tuple[str, ...]
    min_key: Optional[Vector]
    max_key: Optional[Vector]
    
    def count_and_checksum(self) -> Tuple[int, int]:
        """Single-pass count and checksum calculation"""
        q = self.make_select().select(Count(), Checksum(columns))
        return self.database.query(q, tuple)
```

#### **9. InfoTree for Statistics Collection**

Hierarchical information tracking:

```python
class InfoTree:
    """Tracks statistics for each segment recursively"""
    
    def aggregate_info(self):
        """Aggregates statistics from all segments"""
        # Collects row counts, diff counts, timing information
```

### **Algorithm Complexity Analysis**

**Time Complexity:**
- Best case (identical tables): O(log N) queries where N is table size
- Worst case (completely different): O(N) for downloading all rows
- Typical case (few differences): O(D × log(N/D)) where D is number of differences

**Space Complexity:**
- Memory usage: O(bisection_threshold) for local comparison
- Database resources: O(threads × query_complexity)

### **Configuration Parameters**

Key tuning parameters:

1. **bisection_factor** (default: 32):
   - Number of segments per iteration
   - Higher values = more parallel queries, faster convergence
   - Trade-off: Database load vs. speed

2. **bisection_threshold** (default: 16,384):
   - When to switch from checksumming to downloading
   - Higher values = less network transfer
   - Trade-off: Memory usage vs. network overhead

3. **threads** (default: 1):
   - Concurrent queries per database
   - Optimal value depends on database capacity

### **Error Handling and Edge Cases**

The implementation handles numerous edge cases:

1. **Empty Tables**: Special handling with early detection
2. **NULL Values**: Proper NULL concatenation handling
3. **Duplicate Keys**: Validation and error reporting
4. **Type Mismatches**: Automatic precision reduction
5. **Large Gaps**: Detection and warning system

### **Conclusion**

Data-diff's algorithm represents a sophisticated approach to database comparison that:

1. **Minimizes Data Transfer**: Only downloads actual differences
2. **Leverages Database Power**: Pushes computation to database engines
3. **Scales Efficiently**: Handles tables with billions of rows
4. **Adapts Dynamically**: Adjusts strategy based on data characteristics
5. **Supports Heterogeneity**: Works across different database systems

The implementation demonstrates excellent software engineering practices with:
- Clear separation of concerns
- Extensive type safety
- Comprehensive testing
- Database-agnostic abstractions
- Performance-conscious design

This makes data-diff a robust, production-ready tool for data validation in modern data pipelines, particularly useful for data migration, replication validation, and development testing scenarios.

-----------

## Deep Dive: Compound Key Support in Data-Diff

The compound key support in data-diff is a sophisticated system that enables the algorithm to handle tables with multi-column primary keys. This is crucial because many real-world databases use composite keys. Let me explain how this works in detail.

### **The Challenge of Compound Keys**

When dealing with compound (composite) keys, the main challenges are:

1. **Ordering**: How to maintain a consistent order across multiple columns
2. **Segmentation**: How to divide a multi-dimensional key space evenly
3. **Range Queries**: How to express "between X and Y" for multi-column keys
4. **Distribution**: How to handle uneven data distribution across dimensions

### **Core Concept: Lexicographic Ordering**

SQL databases order compound keys lexicographically (dictionary order). For example, with a compound key `(col1, col2)`:
- `(1, 1)` comes before `(1, 2)`
- `(1, 999)` comes before `(2, 0)`
- `(a, z)` comes before `(b, a)`

This is the natural ORDER BY behavior in SQL:
```sql
ORDER BY col1, col2, col3  -- Lexicographic ordering
```

### **The Vector Abstraction**

Data-diff represents compound keys as **Vectors** - tuples of values:

```python
from data_diff.utils import Vector

# A compound key with 3 columns
key = Vector((100, 'abc', datetime(2024, 1, 1)))

# Min and max bounds for a segment
min_key = Vector((0, 'aaa', datetime(2024, 1, 1)))
max_key = Vector((1000, 'zzz', datetime(2024, 12, 31)))
```

### **Lexicographic Space Implementation**

The `LexicographicSpace` class treats the key space as an N-dimensional number system:

```python
@attrs.define(frozen=True)
class LexicographicSpace:
    """
    Treats N-dimensional space as a mixed-radix number system.
    Each dimension has its own "base" (maximum value).
    """
    
    def __init__(self, dims: Vector):
        # dims = (1000, 26, 365) means:
        # - First column: values 0-999
        # - Second column: values 0-25 (like 'a'-'z')
        # - Third column: values 0-364 (days in year)
        self.dims = dims
```

#### **Key Operations**

1. **Addition with Carry**:
```python
def add(self, v1: Vector, v2: Vector) -> Vector:
    """Add two vectors with carry propagation"""
    carry = 0
    res = []
    
    # Process from right to left (least significant first)
    for i1, i2, d in reversed(list(zip(v1, v2, self.dims))):
        n = i1 + i2 + carry
        carry = n // d  # Carry to next dimension
        n %= d          # Remainder stays in current dimension
        res.append(n)
    
    return tuple(reversed(res))

# Example: In space (10, 10), adding (0, 8) + (0, 5) = (1, 3)
# Because 8 + 5 = 13, which is 1*10 + 3
```

2. **Division for Even Spacing**:
```python
def divide(self, v: Vector, count: int) -> Vector:
    """Divide a vector by a scalar to get evenly spaced points"""
    # This enables splitting a range into equal segments
    # Critical for the bisection algorithm
```

### **BoundedLexicographicSpace: Handling Arbitrary Ranges**

Real table segments don't start at (0,0,0). The `BoundedLexicographicSpace` handles arbitrary bounds:

```python
class BoundedLexicographicSpace:
    def __init__(self, min_bound: Vector, max_bound: Vector):
        # Example: Keys range from (100, 'b') to (500, 'y')
        self.min_bound = min_bound  # (100, 'b')
        self.max_bound = max_bound  # (500, 'y')
        
        # Create a normalized space starting at (0, 0)
        dims = tuple(mx - mn for mn, mx in zip(min_bound, max_bound))
        self.uspace = LexicographicSpace(dims)  # (400, 23)
    
    def to_uspace(self, v: Vector) -> Vector:
        """Convert from actual space to unit space"""
        # (150, 'd') -> (50, 2)
        return sub_v(v, self.min_bound)
    
    def from_uspace(self, v: Vector) -> Vector:
        """Convert from unit space back to actual space"""
        # (50, 2) -> (150, 'd')
        return add_v(v, self.min_bound)
```

### **Creating Checkpoints for Segmentation**

The key insight is creating evenly-distributed checkpoints in multi-dimensional space:

```python
def choose_checkpoints(self, count: int) -> List[List[DbKey]]:
    """
    Create checkpoints for segmenting the table.
    For compound keys, creates a grid of checkpoints.
    """
    # For 2D keys with count=9, we want 3x3 grid
    # Take Nth root to get points per dimension
    points_per_dim = int(count ** (1 / len(self.key_columns))) or 1
    
    return split_compound_key_space(self.min_key, self.max_key, points_per_dim)
```

Example with 2D compound key and bisection_factor=9:
```python
# Input: min_key=(0, 'a'), max_key=(900, 'z'), count=9
# Output checkpoints:
# Dimension 1: [0, 300, 600, 900]
# Dimension 2: ['a', 'i', 'r', 'z']
```

### **Creating Segment Mesh**

The `create_mesh_from_points` function creates a grid of segments:

```python
def create_mesh_from_points(*values_per_dim: list) -> List[Tuple[Vector, Vector]]:
    """
    Given checkpoints for each dimension, create all box combinations.
    
    Example:
        dim1 = [0, 50, 100]
        dim2 = ['a', 'm', 'z']
        
    Creates 4 boxes:
        [(0, 'a'), (50, 'm')]  # Box 1
        [(0, 'm'), (50, 'z')]  # Box 2
        [(50, 'a'), (100, 'm')] # Box 3
        [(50, 'm'), (100, 'z')] # Box 4
    """
    ranges = [list(zip(values[:-1], values[1:])) for values in values_per_dim]
    return [tuple(Vector(a) for a in zip(*r)) for r in product(*ranges)]
```

### **SQL Query Generation for Compound Keys**

When querying segments with compound keys:

```python
def _make_key_range(self):
    """Generate WHERE clause for compound key range"""
    conditions = []
    
    # min_key = (100, 'b', 5)
    # max_key = (200, 'd', 10)
    
    if self.min_key is not None:
        for mn, k in zip(self.min_key, self.key_columns):
            conditions.append(f"{k} >= {mn}")
    
    if self.max_key is not None:
        for k, mx in zip(self.key_columns, self.max_key):
            conditions.append(f"{k} < {mx}")
    
    # Results in: WHERE col1 >= 100 AND col1 < 200 
    #              AND col2 >= 'b' AND col2 < 'd'
    #              AND col3 >= 5 AND col3 < 10
```

However, this naive approach is **incorrect** for lexicographic ordering! The actual implementation uses proper lexicographic comparison:

```python
# Correct lexicographic WHERE clause for compound keys (simplified):
WHERE (col1, col2, col3) >= (100, 'b', 5) 
  AND (col1, col2, col3) < (200, 'd', 10)

# Or expanded (what actually gets generated):
WHERE (col1 > 100) OR 
      (col1 = 100 AND col2 > 'b') OR 
      (col1 = 100 AND col2 = 'b' AND col3 >= 5)
  AND similar_for_max_bound
```

### **Handling Mixed Data Types**

Compound keys can mix different data types:

```python
class TableSegment:
    def query_key_range(self) -> Tuple[tuple, tuple]:
        """Query min/max for compound keys with mixed types"""
        # Use database functions that work across types
        select = self.make_select().select(
            ApplyFuncAndNormalizeAsString(this[k], f) 
            for k in self.key_columns 
            for f in (min_, max_)
        )
        
        # Normalizes each column appropriately:
        # - Integers: stays as int
        # - Strings: proper string comparison
        # - UUIDs: normalized to comparable format
        # - Dates: converted to comparable format
```

### **Example: 3-Column Compound Key Diff**

Let's trace through a real example:

```python
# Table with compound key: (customer_id, order_date, order_id)
table1 = TableSegment(
    database=db,
    table_path=('sales', 'orders'),
    key_columns=('customer_id', 'order_date', 'order_id')
)

# Step 1: Query key ranges
min_key, max_key = table1.query_key_range()
# Returns: min_key=(1, '2024-01-01', 1000)
#          max_key=(9999, '2024-12-31', 99999)

# Step 2: Create checkpoints (bisection_factor=8)
checkpoints = table1.choose_checkpoints(8)
# Cube root of 8 ≈ 2, so 2 points per dimension
# Dimension 1: [1, 5000, 9999]
# Dimension 2: ['2024-01-01', '2024-07-01', '2024-12-31']  
# Dimension 3: [1000, 50000, 99999]

# Step 3: Create segment mesh (2x2x2 = 8 segments)
segments = table1.segment_by_checkpoints(checkpoints)
# Segment 1: (1, '2024-01-01', 1000) to (5000, '2024-07-01', 50000)
# Segment 2: (1, '2024-01-01', 50000) to (5000, '2024-07-01', 99999)
# ... 6 more segments

# Step 4: Checksum each segment
for segment in segments:
    count, checksum = segment.count_and_checksum()
    # SQL generated:
    # SELECT COUNT(*), SUM(HASH(customer_id || order_date || order_id || ...))
    # FROM orders
    # WHERE (customer_id, order_date, order_id) >= (1, '2024-01-01', 1000)
    #   AND (customer_id, order_date, order_id) < (5000, '2024-07-01', 50000)
```

### **Edge Cases and Optimizations**

#### **1. Uneven Distribution Handling**

When data is unevenly distributed across dimensions:

```python
# Problematic case: 
# customer_id: 1-10 (only 10 customers)
# order_date: 365 different dates
# order_id: 1-1000000 (million orders)

# The algorithm adapts by:
# 1. Detecting dimension cardinality
# 2. Adjusting checkpoint distribution
# 3. Warning about gaps in key space
```

#### **2. Gap Detection**

The algorithm detects large gaps in compound key space:

```python
if count1 == 0 and count2 == 0:
    logger.debug(
        "Uneven distribution of keys detected in segment %s..%s "
        "(big gaps in the key column). "
        "For better performance, increase bisection-threshold.",
        table1.min_key, table1.max_key
    )
```

#### **3. Type-Specific Handling**

Different key types require special handling:

```python
# UUID keys
if isinstance(col_type, ColType_UUID):
    # Normalize UUID representation for comparison
    
# String keys with collation
if isinstance(col_type, StringType):
    # Handle case-sensitivity and collation rules
    
# Numeric keys with different precision
if isinstance(col_type, NumericType):
    # Align precision between databases
```

### **Performance Implications**

Compound keys affect performance in several ways:

1. **Index Usage**: Compound key queries must use index efficiently
2. **Checkpoint Calculation**: More complex for N-dimensional space
3. **Memory Usage**: Storing N values per key instead of 1
4. **Query Complexity**: More complex WHERE clauses

The algorithm optimizes by:
- Using database-native tuple comparison where available
- Minimizing the number of checkpoints needed
- Leveraging compound indexes effectively

### **Summary**

The compound key support in data-diff is a sophisticated system that:

1. **Abstracts** multi-dimensional key spaces using lexicographic ordering
2. **Segments** the space evenly using mathematical division
3. **Generates** efficient SQL queries respecting lexicographic order
4. **Handles** mixed data types within compound keys
5. **Optimizes** for common patterns and edge cases

This enables data-diff to work with real-world database schemas where compound keys are common, maintaining the same efficiency and accuracy as with single-column keys. The mathematical foundation ensures that the segmentation is uniform and the comparisons are correct, regardless of the number or types of columns in the compound key.

----

## Compound Key Support: Walkthrough with Examples

Let me walk through the entire compound key support system using two concrete example tables that we'll compare.

### **Example Tables Setup**

Let's create two tables tracking customer orders with a compound primary key:

**Table A (Source)**: `sales.orders_prod`
```sql
CREATE TABLE orders_prod (
    customer_id INT,
    order_date DATE,
    order_seq INT,  -- sequence number for orders on same date
    amount DECIMAL(10,2),
    status VARCHAR(20),
    PRIMARY KEY (customer_id, order_date, order_seq)
);
```

**Table B (Target)**: `sales.orders_staging`
```sql
CREATE TABLE orders_staging (
    customer_id INT,
    order_date DATE,
    order_seq INT,
    amount DECIMAL(10,2),
    status VARCHAR(20),
    PRIMARY KEY (customer_id, order_date, order_seq)
);
```

**Sample Data**:
```sql
-- Table A has 10,000 rows
-- Table B has 9,998 rows (missing 2 rows)
-- Compound key ranges:
--   customer_id: 1-100
--   order_date: '2024-01-01' to '2024-12-31'
--   order_seq: 1-50

-- Example rows in Table A:
(1, '2024-01-01', 1, 100.00, 'completed')
(1, '2024-01-01', 2, 150.00, 'completed')
(1, '2024-01-02', 1, 200.00, 'completed')
...
(50, '2024-06-15', 25, 500.00, 'pending')  -- This row missing in Table B
...
(100, '2024-12-31', 50, 999.00, 'completed')

-- Table B is identical except missing row (50, '2024-06-15', 25)
-- and has different amount for row (75, '2024-09-20', 10):
(75, '2024-09-20', 10, 600.00, 'completed')  -- Table A has 650.00
```

Now let's trace through each step of the algorithm:

---

## **Step 1: Initialize TableSegments**

```python
from data_diff import TableSegment, HashDiffer

# Create table segments with compound keys
table_a = TableSegment(
    database=db_connection,
    table_path=('sales', 'orders_prod'),
    key_columns=('customer_id', 'order_date', 'order_seq'),
    extra_columns=('amount', 'status')
)

table_b = TableSegment(
    database=db_connection,
    table_path=('sales', 'orders_staging'),
    key_columns=('customer_id', 'order_date', 'order_seq'),
    extra_columns=('amount', 'status')
)

differ = HashDiffer(
    bisection_factor=8,  # Will create 8 segments
    bisection_threshold=100  # Switch to local comparison below 100 rows
)
```

---

## **Step 2: Query Key Ranges**

The algorithm first needs to understand the bounds of the data:

```python
# table_a.query_key_range() executes:
```

**SQL Generated**:
```sql
-- For Table A
SELECT 
    MIN(customer_id), MAX(customer_id),
    MIN(order_date), MAX(order_date),
    MIN(order_seq), MAX(order_seq)
FROM sales.orders_prod;

-- Returns: (1, 100, '2024-01-01', '2024-12-31', 1, 50)
```

**Processing**:
```python
# Inside query_key_range()
min_key_a = Vector((1, '2024-01-01', 1))
max_key_a = Vector((100, '2024-12-31', 50))

# Same for Table B
min_key_b = Vector((1, '2024-01-01', 1))
max_key_b = Vector((100, '2024-12-31', 50))

# Since both tables have same bounds, we proceed with:
min_key = Vector((1, '2024-01-01', 1))
max_key = Vector((101, '2025-01-01', 1))  # max_key + 1 for exclusive upper bound
```

---

## **Step 3: Create Lexicographic Space**

The algorithm creates a bounded lexicographic space to handle the compound keys:

```python
# Create the space boundaries
space = BoundedLexicographicSpace(
    min_bound=(1, date_to_int('2024-01-01'), 1),  # (1, 0, 1)
    max_bound=(101, date_to_int('2025-01-01'), 1)  # (101, 366, 1)
)

# Internal representation:
# Dimension sizes: (100, 366, 50)
# This means:
#   - customer_id: 100 possible values (1-100)
#   - order_date: 366 possible values (366 days)
#   - order_seq: 50 possible values (1-50, but handled specially)
```

**Key Insight**: Dates are converted to integers for arithmetic operations:
```python
def date_to_int(date_str):
    # Convert '2024-01-01' to days since epoch or year start
    return (date - date(2024, 1, 1)).days
```

---

## **Step 4: Calculate Checkpoints**

With `bisection_factor=8`, we need to create 8 segments:

```python
def choose_checkpoints(self, count=8):
    # For 3D space, take cube root: ∛8 = 2
    points_per_dimension = 2
    
    # Calculate evenly-spaced points in each dimension
    checkpoints = split_compound_key_space(min_key, max_key, points_per_dimension)
```

**Checkpoint Calculation**:
```python
# Dimension 1 (customer_id): 
# Range: 1-100, split into 2 points
checkpoints_dim1 = [1, 50, 101]

# Dimension 2 (order_date):
# Range: '2024-01-01' to '2024-12-31', split into 2 points
checkpoints_dim2 = ['2024-01-01', '2024-07-01', '2025-01-01']

# Dimension 3 (order_seq):
# Range: 1-50, split into 2 points
checkpoints_dim3 = [1, 25, 51]
```

**Visual Representation**:
```
Customer ID axis:    1 -------- 50 -------- 101
                     |           |           |
Date axis:      Jan 1 ---- July 1 ---- Jan 1 '25
                     |           |           |
Order Seq axis:      1 ------ 25 ------ 51
```

---

## **Step 5: Create Segment Mesh**

The algorithm creates a 3D grid of segments (2×2×2 = 8 segments):

```python
segments = create_mesh_from_points(
    [1, 50, 101],                    # customer_id checkpoints
    ['2024-01-01', '2024-07-01', '2025-01-01'],  # date checkpoints
    [1, 25, 51]                      # order_seq checkpoints
)
```

**Generated Segments**:
```python
# Segment 1: Lower-lower-lower octant
Segment(
    min_key=(1, '2024-01-01', 1),
    max_key=(50, '2024-07-01', 25)
)

# Segment 2: Lower-lower-upper octant
Segment(
    min_key=(1, '2024-01-01', 25),
    max_key=(50, '2024-07-01', 51)
)

# Segment 3: Lower-upper-lower octant
Segment(
    min_key=(1, '2024-07-01', 1),
    max_key=(50, '2025-01-01', 25)
)

# Segment 4: Lower-upper-upper octant
Segment(
    min_key=(1, '2024-07-01', 25),
    max_key=(50, '2025-01-01', 51)
)

# Segment 5: Upper-lower-lower octant
Segment(
    min_key=(50, '2024-01-01', 1),
    max_key=(101, '2024-07-01', 25)
)

# ... Segments 6, 7, 8 following the same pattern
```

---

## **Step 6: Checksum Each Segment**

For each segment, calculate checksums in both tables:

```python
# For Segment 5 (which contains our missing row)
segment_5a = table_a.new_key_bounds(
    min_key=(50, '2024-01-01', 1),
    max_key=(101, '2024-07-01', 25)
)
segment_5b = table_b.new_key_bounds(
    min_key=(50, '2024-01-01', 1),
    max_key=(101, '2024-07-01', 25)
)
```

**SQL Generated for Segment 5**:
```sql
-- For Table A
SELECT 
    COUNT(*) as cnt,
    SUM(CAST(
        CONV(SUBSTRING(
            MD5(CONCAT(
                CAST(customer_id AS CHAR),
                CAST(order_date AS CHAR),
                CAST(order_seq AS CHAR),
                CAST(amount AS CHAR),
                CAST(status AS CHAR)
            )), 
        18), 16, 10) 
    AS UNSIGNED)) as checksum
FROM sales.orders_prod
WHERE (customer_id = 50 AND order_date = '2024-01-01' AND order_seq >= 1)
   OR (customer_id = 50 AND order_date > '2024-01-01')
   OR (customer_id > 50)
AND (customer_id < 101
   OR (customer_id = 101 AND order_date < '2024-07-01')
   OR (customer_id = 101 AND order_date = '2024-07-01' AND order_seq < 25));

-- Returns: (1250, 8374629384)  -- 1250 rows, checksum value

-- For Table B (same query on orders_staging)
-- Returns: (1249, 8371523421)  -- 1249 rows, different checksum!
```

**Checksum Results**:
```python
# Segments 1-4: Checksums match ✓
# Segment 5: Checksums differ ✗ (contains missing row)
# Segments 6-7: Checksums match ✓
# Segment 8: Checksums differ ✗ (contains modified row)
```

---

## **Step 7: Recursive Bisection of Differing Segments**

For segments with different checksums, recursively bisect:

```python
# Segment 5 has differences, so bisect it further
# New sub-segments of Segment 5 (using bisection_factor=8 again):

sub_checkpoints = calculate_checkpoints_for_segment(
    min_key=(50, '2024-01-01', 1),
    max_key=(101, '2024-07-01', 25),
    count=8
)

# Creates 8 sub-segments within Segment 5
# Each sub-segment has ~156 rows (1250/8)
```

**Sub-segment Creation**:
```python
# Sub-segment 5.3 (contains the missing row):
SubSegment(
    min_key=(50, '2024-04-15', 1),
    max_key=(75, '2024-07-01', 13)
)
# This has ~156 rows, still above bisection_threshold (100)
# So it will be bisected again...

# After another iteration:
# Sub-sub-segment 5.3.2:
SubSegment(
    min_key=(50, '2024-06-01', 13),
    max_key=(63, '2024-07-01', 1)
)
# This has ~20 rows, below bisection_threshold!
# Switch to local comparison
```

---

## **Step 8: Local Comparison**

When a segment is small enough (< `bisection_threshold`), download and compare:

```python
# For sub-sub-segment with ~20 rows
rows_a = segment_5_3_2_a.get_values()
rows_b = segment_5_3_2_b.get_values()
```

**SQL Generated**:
```sql
-- Download rows from Table A
SELECT 
    CAST(customer_id AS CHAR),
    CAST(order_date AS CHAR),
    CAST(order_seq AS CHAR),
    CAST(amount AS CHAR),
    CAST(status AS CHAR)
FROM sales.orders_prod
WHERE (customer_id = 50 AND order_date = '2024-06-01' AND order_seq >= 13)
   OR (customer_id = 50 AND order_date > '2024-06-01')
   OR (customer_id > 50 AND customer_id < 63)
   OR (customer_id = 63 AND order_date < '2024-07-01')
   OR (customer_id = 63 AND order_date = '2024-07-01' AND order_seq < 1);

-- Returns 20 rows including:
-- ('50', '2024-06-15', '25', '500.00', 'pending')
```

**Local Comparison**:
```python
def diff_sets(rows_a, rows_b):
    set_a = set(rows_a)
    set_b = set(rows_b)
    
    # Find exclusive rows
    only_in_a = set_a - set_b
    only_in_b = set_b - set_a
    
    # Group by key to find updates
    by_key = defaultdict(list)
    for row in only_in_a:
        key = row[:3]  # (customer_id, order_date, order_seq)
        by_key[key].append(('-', row))
    for row in only_in_b:
        key = row[:3]
        by_key[key].append(('+', row))
    
    return by_key

# Results:
# ('50', '2024-06-15', '25'): [('-', ('50', '2024-06-15', '25', '500.00', 'pending'))]
# Missing in Table B!
```

---

## **Step 9: Process All Segments**

The algorithm continues processing all segments in parallel:

```python
# Segment 8 also had differences (the modified amount)
# After bisection and local comparison:
# ('75', '2024-09-20', '10'): [
#     ('-', ('75', '2024-09-20', '10', '650.00', 'completed')),
#     ('+', ('75', '2024-09-20', '10', '600.00', 'completed'))
# ]
```

---

## **Step 10: Aggregate Results**

Finally, collect all differences:

```python
final_diff = [
    ('-', ('50', '2024-06-15', '25', '500.00', 'pending')),     # Missing row
    ('-', ('75', '2024-09-20', '10', '650.00', 'completed')),   # Old value
    ('+', ('75', '2024-09-20', '10', '600.00', 'completed'))    # New value
]

# Statistics:
stats = {
    'table1_count': 10000,
    'table2_count': 9998,
    'exclusive_to_table1': 1,
    'exclusive_to_table2': 0,
    'updated_rows': 1,
    'unchanged_rows': 9997
}
```

---

## **Key Optimizations for Compound Keys**

### **1. Lexicographic WHERE Clause Generation**

The trickiest part is generating correct WHERE clauses for lexicographic ranges:

```python
def generate_where_clause(min_key, max_key):
    """
    For min_key=(50, '2024-06-01', 13) and max_key=(63, '2024-07-01', 1)
    
    Generates:
    WHERE (
        (customer_id > 50) OR
        (customer_id = 50 AND order_date > '2024-06-01') OR
        (customer_id = 50 AND order_date = '2024-06-01' AND order_seq >= 13)
    ) AND (
        (customer_id < 63) OR
        (customer_id = 63 AND order_date < '2024-07-01') OR
        (customer_id = 63 AND order_date = '2024-07-01' AND order_seq < 1)
    )
    """
```

### **2. Handling Data Type Differences**

Different databases handle compound keys differently:

```python
# PostgreSQL supports tuple comparison directly:
WHERE (customer_id, order_date, order_seq) >= (50, '2024-06-01', 13)
  AND (customer_id, order_date, order_seq) < (63, '2024-07-01', 1)

# MySQL requires expanded form:
WHERE (customer_id > 50 OR 
       (customer_id = 50 AND order_date > '2024-06-01') OR
       (customer_id = 50 AND order_date = '2024-06-01' AND order_seq >= 13))
  AND ...

# The algorithm adapts based on database capabilities
```

### **3. Efficient Checksum Calculation**

For compound keys, the checksum concatenates all key columns:

```python
# Checksum includes all columns in order
CONCAT(
    customer_id,    # Key column 1
    order_date,     # Key column 2  
    order_seq,      # Key column 3
    amount,         # Extra column 1
    status          # Extra column 2
)
```

---

## **Performance Analysis for Our Example**

```
Initial query: 2 queries (min/max for each table)
Level 1: 8 segments × 2 tables = 16 checksum queries
Level 2: 2 differing segments × 8 sub-segments × 2 tables = 32 queries
Level 3: 1 segment × 8 sub-segments × 2 tables = 16 queries
Local download: 2 queries (one per table for small segment)

Total: 66 queries to find 2 differences among 10,000 rows

Without compound key optimization (downloading all):
2 queries × 10,000 rows = significant network transfer
```

---

## **Summary**

The compound key support enables data-diff to:

1. **Correctly handle multi-column primary keys** using lexicographic ordering
2. **Efficiently segment N-dimensional key spaces** into evenly-distributed regions
3. **Generate optimal SQL queries** that leverage compound indexes
4. **Minimize data transfer** by precisely targeting segments with differences
5. **Scale to any number of key columns** while maintaining efficiency

The example shows how the algorithm found 2 differences (1 missing row, 1 changed value) among 10,000 rows using only 66 targeted queries instead of downloading all 20,000 rows for comparison. This efficiency becomes even more dramatic with larger tables (millions or billions of rows).

----

## Why Lexicographic WHERE Clauses Are More Complex

Let me demonstrate why with concrete examples from our data.

### **The Problem with simple WHERE Clause**

Your suggested clause:
```sql
WHERE (customer_id >= 50 AND order_date >= '2024-01-01' AND order_seq >= 1)
AND   (customer_id <= 101 AND order_date <= '2024-07-01' AND order_seq < 25);
```

This creates a **rectangular box** in 3D space, but lexicographic ordering doesn't work that way. Let me show you with actual data rows why this fails.

### **Counter-Example 1: False Positives**

Consider this row:
```sql
Row X: (customer_id=51, order_date='2024-12-31', order_seq=45)
```

**Your WHERE clause evaluation:**
- `customer_id >= 50` ✓ (51 >= 50)
- `order_date >= '2024-01-01'` ✓ ('2024-12-31' >= '2024-01-01')
- `order_seq >= 1` ✓ (45 >= 1)
- `customer_id <= 101` ✓ (51 <= 101)
- `order_date <= '2024-07-01'` ✗ ('2024-12-31' > '2024-07-01')
- Result: **Row excluded** ❌

**But wait!** In lexicographic order:
- `(51, '2024-12-31', 45)` is BETWEEN `(50, '2024-01-01', 1)` and `(101, '2024-07-01', 25)`
- Because `51 > 50`, the date and sequence don't matter for the lower bound
- Because `51 < 101`, the date and sequence don't matter for the upper bound
- Result: **Should be included!** ✓

### **Counter-Example 2: More False Positives**

Consider this row:
```sql
Row Y: (customer_id=50, order_date='2024-03-15', order_seq=30)
```

**Your WHERE clause evaluation:**
- `customer_id >= 50` ✓ (50 >= 50)
- `order_date >= '2024-01-01'` ✓ ('2024-03-15' >= '2024-01-01')
- `order_seq >= 1` ✓ (30 >= 1)
- `customer_id <= 101` ✓ (50 <= 101)
- `order_date <= '2024-07-01'` ✓ ('2024-03-15' <= '2024-07-01')
- `order_seq < 25` ✗ (30 >= 25)
- Result: **Row excluded** ❌

**Lexicographic evaluation:**
- `(50, '2024-03-15', 30)` compared to lower bound `(50, '2024-01-01', 1)`:
  - customer_id equal (50 = 50), check next column
  - order_date: '2024-03-15' > '2024-01-01' ✓
- Result: **Should be included!** ✓

### **Visual Explanation: Box vs Lexicographic Range**

Let me visualize the difference with a simplified 2D example:

```
Your WHERE clause creates a RECTANGLE:
        
Date    │  ❌ Wrong Region
Jul 1   │  ┌─────────────┐
        │  │             │
        │  │  Rectangle  │
        │  │             │
Jan 1   │  └─────────────┘
        └──┴─────────────┴──
           50           101
           Customer ID

Lexicographic ordering creates a DIFFERENT SHAPE:

Date    │  ✓ Correct Region
Dec 31  │  ░░░░░░█████████  <- All dates for customer 51-100
        │  ░░░░░░█████████
Jul 1   │  ░░░░░░███┐      <- Only up to Jul 1 for customer 101
        │  ░░░░░░███│
        │  ░░░░░░███│
Jan 1   │  ┌────────┘      <- Starting Jan 1 for customer 50
        └──┴─────────────┴──
           50           101
           Customer ID

░░░ = Included in lexicographic range
███ = Included in both
```

### **The Correct Lexicographic WHERE Clause**

Here's why we need the complex form:

```sql
-- For range: (50, '2024-01-01', 1) to (101, '2024-07-01', 25)

WHERE (
    -- Lower bound: (customer_id, order_date, order_seq) >= (50, '2024-01-01', 1)
    (customer_id > 50) OR                                          -- Any customer > 50
    (customer_id = 50 AND order_date > '2024-01-01') OR           -- Customer 50, later dates
    (customer_id = 50 AND order_date = '2024-01-01' AND order_seq >= 1)  -- Customer 50, Jan 1, seq >= 1
) AND (
    -- Upper bound: (customer_id, order_date, order_seq) < (101, '2024-07-01', 25)
    (customer_id < 101) OR                                         -- Any customer < 101
    (customer_id = 101 AND order_date < '2024-07-01') OR          -- Customer 101, earlier dates
    (customer_id = 101 AND order_date = '2024-07-01' AND order_seq < 25)  -- Customer 101, Jul 1, seq < 25
)
```

### **Let's Test Our Examples Again**

**Row X: `(51, '2024-12-31', 45)`**
- Lower bound: `customer_id > 50` → 51 > 50 ✓
- Upper bound: `customer_id < 101` → 51 < 101 ✓
- **Result: INCLUDED** ✓

**Row Y: `(50, '2024-03-15', 30)`**
- Lower bound: `customer_id = 50 AND order_date > '2024-01-01'` → '2024-03-15' > '2024-01-01' ✓
- Upper bound: `customer_id < 101` → 50 < 101 ✓
- **Result: INCLUDED** ✓

### **Complete Example with All Edge Cases**

Let me show you which rows are included/excluded correctly:

```sql
-- Test rows and their inclusion status:

-- ✓ INCLUDED (Correctly within range):
(50, '2024-01-01', 1)    -- Exact lower bound
(50, '2024-01-01', 50)   -- Same date as lower bound, higher seq
(50, '2024-12-31', 50)   -- Same customer as lower bound, any later date
(75, '2024-12-31', 50)   -- Middle customer, any date
(100, '2024-12-31', 50)  -- High customer, any date
(101, '2024-06-30', 50)  -- Highest customer, before cutoff date
(101, '2024-07-01', 24)  -- Exact upper bound minus 1

-- ❌ EXCLUDED (Correctly outside range):
(49, '2024-12-31', 50)   -- Customer too low
(50, '2023-12-31', 50)   -- Correct customer, date too early
(101, '2024-07-02', 1)   -- Correct customer, date too late
(101, '2024-07-01', 25)  -- Exact upper bound (exclusive)
(101, '2024-07-01', 26)  -- Beyond upper bound
(102, '2024-01-01', 1)   -- Customer too high
```

### **Performance Impact**

The correct lexicographic WHERE clause is more complex but necessary:

1. **Index Usage**: Database can still use compound index efficiently
2. **Correctness**: Gets exactly the right rows
3. **No Post-Filtering**: Doesn't need additional filtering in application

### **Database-Specific Optimizations**

Some databases handle this better natively:

```sql
-- PostgreSQL/Modern SQL (much cleaner!):
WHERE (customer_id, order_date, order_seq) >= (50, '2024-01-01', 1)
  AND (customer_id, order_date, order_seq) < (101, '2024-07-01', 25)

-- MySQL/Older databases need the expanded form we showed

-- The data-diff library generates the appropriate form for each database
```

### **Why This Matters for Data-Diff**

If data-diff used your simpler WHERE clause:

1. **Missing Rows**: Would miss many valid rows (like our examples)
2. **Incorrect Checksums**: Checksums would be computed on wrong row sets
3. **False Differences**: Would report differences that don't exist
4. **Algorithm Failure**: The bisection wouldn't converge correctly

The complex WHERE clause ensures:
- Segments contain exactly the rows they should
- Checksums are accurate
- Bisection converges to actual differences
- The algorithm works correctly across all databases

### **Summary**

Your intuition about a simpler WHERE clause is natural, but compound keys follow **lexicographic ordering**, not component-wise comparison. The complex WHERE clause correctly implements lexicographic range queries, ensuring the algorithm works accurately. This is one of the subtle but critical details that makes data-diff robust and reliable for real-world use cases with compound keys.