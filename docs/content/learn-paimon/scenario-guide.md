---
title: "Scenario Guide"
weight: 2
type: docs
aliases:
- /learn-paimon/scenario-guide.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Scenario Guide

This guide helps you choose the right Paimon table type and configuration for your specific use case. Paimon provides
**Primary Key Table**, **Append Table**, and **Multimodal Data Lake** capabilities — each with different modes and
configurations that are suited for different scenarios.

## Quick Decision

| Scenario | Table Type | Key Configuration |
|---|---|---|
| CDC real-time sync from database | Primary Key Table | `deletion-vectors.enabled = true` |
| Streaming aggregation / metrics | Primary Key Table | `merge-engine = aggregation` |
| Multi-stream partial column updates | Primary Key Table | `merge-engine = partial-update` |
| Log deduplication (keep first) | Primary Key Table | `merge-engine = first-row` |
| Batch ETL / data warehouse layers | Append Table | Default (unaware-bucket) |
| High-frequency point queries on key | Append Table | `bucket = N, bucket-key = col` |
| Queue-like ordered streaming | Append Table | `bucket = N, bucket-key = col` |
| Large-scale OLAP with ad-hoc queries | Append Table | Incremental Clustering |
| Store images / videos / documents | Append Table (Blob) | `blob-field`, Data Evolution enabled |
| AI vector search / RAG | Append Table (Vector) | `VECTOR` type, Global Index (DiskANN) |
| AI feature engineering & column evolution | Append Table | `data-evolution.enabled = true` |
| Python AI pipeline (Ray / PyTorch) | Append Table | PyPaimon SDK |

---

## Primary Key Table

Use a Primary Key Table when your data has a natural unique key and you need **real-time updates** (insert, update, delete). 
See [Primary Key Table Overview]({{< ref "primary-key-table/overview" >}}).

### Scenario 1: CDC Real-Time Sync

**When:** You want to synchronize a MySQL / PostgreSQL / MongoDB table to the data lake in real-time with upsert
semantics. This is the most common use case for Primary Key Tables.

**Recommended Configuration:**

```sql
CREATE TABLE orders (
    order_id BIGINT,
    user_id BIGINT,
    amount DECIMAL(10,2),
    status STRING,
    update_time TIMESTAMP,
    dt STRING,
    PRIMARY KEY (order_id, dt) NOT ENFORCED
) PARTITIONED BY (dt) WITH (
    'deletion-vectors.enabled' = 'true',
    'changelog-producer' = 'lookup',
    'sequence.field' = 'update_time'
);
```

**Why this configuration:**

- **`deletion-vectors.enabled = true`** (MOW mode): Enables [Merge On Write]({{< ref "primary-key-table/table-mode#merge-on-write" >}}) with Deletion Vectors.
  This mode gives you the best balance of write and read performance. Compared to the default MOR mode, MOW
  avoids merging at read time, which greatly improves OLAP query performance.
- **`changelog-producer = lookup`**: Generates a complete [changelog]({{< ref "primary-key-table/changelog-producer#lookup" >}})
  for downstream streaming consumers. If your CDC source is directly connected to a database (e.g., MySQL CDC, Postgres CDC),
  you can use `changelog-producer = input` instead, since the database CDC stream already provides a complete changelog.
  However, if your CDC source comes from Kafka (or other message queues), `input` may not be reliable — use `lookup` to
  ensure changelog correctness. If no downstream streaming read is needed, you can omit this to save compaction resources.
- **`sequence.field = update_time`**: Guarantees correct update ordering even when data arrives out of order.
- **Bucketing**: Use the default Dynamic Bucket (`bucket = -1`). The system automatically adjusts bucket count based
  on data volume. If you are sensitive to data visibility latency, set a fixed bucket number (e.g. `'bucket' = '5'`)
  — roughly 1 bucket per 1GB of data in a partition.

**CDC Ingestion Tip:** Use [Paimon CDC Ingestion]({{< ref "cdc-ingestion/overview" >}}) for whole-database sync with
automatic table creation and schema evolution support.

### Scenario 2: Multi-Stream Partial Column Updates

**When:** Multiple data sources each contribute different columns to the same record, and you want to progressively
merge them into a complete wide table (e.g., orders from one stream + logistics info from another).

**Recommended Configuration:**

```sql
CREATE TABLE order_wide (
    order_id BIGINT PRIMARY KEY NOT ENFORCED,
    -- from order stream
    user_name STRING,
    amount DECIMAL(10,2),
    order_time TIMESTAMP,
    -- from logistics stream
    tracking_no STRING,
    delivery_status STRING,
    delivery_time TIMESTAMP
) WITH (
    'merge-engine' = 'partial-update',
    'fields.order_time.sequence-group' = 'user_name,amount',
    'fields.delivery_time.sequence-group' = 'tracking_no,delivery_status',
    'deletion-vectors.enabled' = 'true',
    'changelog-producer' = 'lookup'
);
```

**Why:** The [partial-update]({{< ref "primary-key-table/merge-engine/partial-update" >}}) merge engine allows each
stream to update only its own columns without overwriting the others. `sequence-group` ensures ordering within each
stream independently.

### Scenario 3: Streaming Aggregation / Metrics

**When:** You need to pre-aggregate metrics in real-time (e.g., page views, total sales, UV count). Each incoming record
should be aggregated with the existing value, not replace it.

**Recommended Configuration:**

```sql
CREATE TABLE product_metrics (
    product_id BIGINT,
    dt STRING,
    total_sales BIGINT,
    max_price DOUBLE,
    uv VARBINARY,
    PRIMARY KEY (product_id, dt) NOT ENFORCED
) PARTITIONED BY (dt) WITH (
    'merge-engine' = 'aggregation',
    'fields.total_sales.aggregate-function' = 'sum',
    'fields.max_price.aggregate-function' = 'max',
    'fields.uv.aggregate-function' = 'hll_sketch',
    'deletion-vectors.enabled' = 'true',
    'changelog-producer' = 'lookup'
);
```

**Why:** The [aggregation]({{< ref "primary-key-table/merge-engine/aggregation" >}}) merge engine supports 20+
aggregate functions (`sum`, `max`, `min`, `count`, `hll_sketch`, `theta_sketch`, `collect`, `merge_map`, etc.), ideal
for real-time metric accumulation.

### Scenario 4: Log Deduplication (Keep First Record)

**When:** You receive a high-volume log stream with possible duplicates and only want to keep the first occurrence of
each key (e.g., first login event per user per day).

**Recommended Configuration:**

```sql
CREATE TABLE first_login (
    user_id BIGINT,
    dt STRING,
    login_time TIMESTAMP,
    device STRING,
    ip STRING,
    PRIMARY KEY (user_id, dt) NOT ENFORCED
) PARTITIONED BY (dt) WITH (
    'merge-engine' = 'first-row',
    'changelog-producer' = 'lookup'
);
```

**Why:** The [first-row]({{< ref "primary-key-table/merge-engine/first-row" >}}) merge engine keeps only the earliest
record for each primary key and produces insert-only changelog, making it perfect for streaming log deduplication.

### Primary Key Table: Bucket Mode Comparison

| Mode | Config | Best For | Trade-off |
|---|---|---|---|
| Dynamic Bucket (default) | `bucket = -1` | Most scenarios, auto-scaling | Requires single write job |
| Fixed Bucket | `bucket = N` | Stable workloads, bucketed join | Manual rescaling needed |
| Postpone Bucket | `bucket = -2` | Adaptive Partition Level Bucket | New data not visible until compaction |

**General guideline:** ~1 bucket per 1GB of data in a partition, with each bucket containing 200MB–1GB of data.

### Primary Key Table: Table Mode Comparison

| Mode | Config | Write Perf | Read Perf | Best For |
|---|---|---|---|---|
| MOR (default) | — | Very good | Not so good | Write-heavy, less query |
| COW | `full-compaction.delta-commits = 1` | Very bad | Very good | Read-heavy, batch jobs |
| MOW | `deletion-vectors.enabled = true` | Good | Good | Balanced (recommended for most) |

---

## Append Table

Use an Append Table when your data **has no natural primary key**, or you are working with **batch ETL** pipelines
where data is only inserted and does not need upsert semantics.
See [Append Table Overview]({{< ref "append-table/overview" >}}).

Compared to Primary Key Tables, Append Tables have much better batch read/write performance, simpler design, and lower
resource consumption. **We recommend using Append Tables for most batch processing scenarios.**

### Scenario 5: Batch ETL / Data Warehouse Layers (Unaware-Bucket)

**When:** Standard data warehouse layering (ODS → DWD → DWS → ADS), bulk INSERT OVERWRITE, or Spark / Hive style
batch processing.

**Recommended Configuration:**

```sql
CREATE TABLE dwd_events (
    event_id BIGINT,
    user_id BIGINT,
    event_type STRING,
    event_time TIMESTAMP,
    dt STRING
) PARTITIONED BY (dt);
```

No bucket configuration needed. This is an **unaware-bucket** append table — the simplest and most commonly used form.
Paimon automatically handles small file merging and supports:

- **Time travel** and version rollback.
- **Schema evolution** (add/drop/rename columns).
- **Data skipping** via min-max stats in manifest files.
- **File Index** (BloomFilter, Bitmap, Range Bitmap) for further query acceleration.
- **Row-level operations** (DELETE / UPDATE / MERGE INTO in Spark SQL).
- **Incremental Clustering** for advanced data layout optimization.

**Query Optimization Tip:** If your queries frequently filter on specific columns, consider using
[Incremental Clustering]({{< ref "append-table/incremental-clustering" >}}) to sort data by those columns:

```sql
ALTER TABLE dwd_events SET (
    'clustering.incremental' = 'true',
    'clustering.columns' = 'user_id'
);
```

Or define a File Index for point lookups:

```sql
ALTER TABLE dwd_events SET (
    'file-index.bloom-filter.columns' = 'user_id'
);
```

### Scenario 6: High-Frequency Point Queries (Bucketed Append)

**When:** Your append table is frequently queried with equality or IN filters on a specific column
(e.g., `WHERE product_id = xxx`). This is the **most impactful** advantage of a bucketed append table.

**Recommended Configuration:**

```sql
CREATE TABLE product_logs (
    product_id BIGINT,
    log_time TIMESTAMP,
    message STRING,
    dt STRING
) PARTITIONED BY (dt) WITH (
    'bucket' = '16',
    'bucket-key' = 'product_id'
);
```

**Why this is powerful:** The `bucket-key` enables **data skipping** — when a query contains `=` or `IN` conditions on
the bucket-key, Paimon pushes these predicates down and prunes all irrelevant bucket files entirely. With 16 buckets,
a point query on `product_id` only reads ~1/16 of the data.

```sql
-- Only reads the bucket containing product_id=12345, skips all other 15 buckets
SELECT * FROM product_logs WHERE product_id = 12345;

-- Only reads buckets for these 3 values
SELECT * FROM product_logs WHERE product_id IN (1, 2, 3);
```

See [Bucketed Append — Data Skipping]({{< ref "append-table/bucketed#data-skipping" >}}).

**Bucketed Join Bonus:** If two bucketed tables share the same `bucket-key` and bucket count, Spark can join them
**without shuffle**, significantly accelerating batch join queries:

```sql
SET spark.sql.sources.v2.bucketing.enabled = true;

-- Both tables have bucket=16, bucket-key=product_id
SELECT * FROM product_logs JOIN product_dim
ON product_logs.product_id = product_dim.product_id;
```

See [Bucketed Join]({{< ref "append-table/bucketed#bucketed-join" >}}).

### Scenario 7: Queue-Like Ordered Streaming (Bucketed Append)

**When:** You want to use Paimon as a message queue replacement with strict ordering guarantees per key (similar to
Kafka partitioning), with the benefits of filter push-down and lower cost.

**Recommended Configuration:**

```sql
CREATE TABLE event_stream (
    user_id BIGINT,
    event_type STRING,
    event_time TIMESTAMP(3),
    payload STRING,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'bucket' = '8',
    'bucket-key' = 'user_id'
);
```

**Why:** Within the same bucket, records are strictly ordered by write time. Streaming reads deliver records in exact
write order per bucket. This gives you Kafka-like partitioned ordering at data lake cost.

See [Bucketed Streaming]({{< ref "append-table/bucketed#bucketed-streaming" >}}).

### Append Table: Bucket Mode Comparison

| Mode | Config | Data Skipping | Bucketed Join | Ordered Streaming | Incremental Clustering |
|---|---|---|---|---|---|
| Unaware-Bucket (default) | No bucket config | Via min-max / file index | No | No | Yes |
| Bucketed | `bucket = N, bucket-key = col` | **Bucket-key filter pushdown** | Yes | Yes | No |

---

## Multimodal Data Lake

Paimon is a multimodal lakehouse for AI. You can keep multimodal data, metadata, and embeddings in the same table and
query them via vector search, full-text search, or SQL. All multimodal features are built on top of Append Tables with
[Data Evolution]({{< ref "append-table/data-evolution" >}}) mode enabled.

### Scenario 8: Storing Multimodal Data (Blob Table)

**When:** You need to store images, videos, audio files, documents, or model weights alongside structured metadata in
the data lake, and want efficient column projection without loading large binary data.

**Recommended Configuration:**

{{< tabs "blob-scenario" >}}
{{< tab "Flink SQL" >}}
```sql
CREATE TABLE image_table (
    id INT,
    name STRING,
    label STRING,
    image BYTES
) WITH (
    'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true',
    'blob-field' = 'image'
);
```
{{< /tab >}}
{{< tab "Spark SQL" >}}
```sql
CREATE TABLE image_table (
    id INT,
    name STRING,
    label STRING,
    image BINARY
) TBLPROPERTIES (
    'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true',
    'blob-field' = 'image'
);
```
{{< /tab >}}
{{< /tabs >}}

**Why:** The [Blob Storage]({{< ref "append-table/blob" >}}) separates large binary data into dedicated `.blob` files
while metadata stays in standard columnar files (Parquet/ORC). This means:

- `SELECT id, name, label FROM image_table` does **not** load any blob data — very fast.
- Blob data supports streaming reads for large objects (videos, model weights) without loading entire files into memory.
- Supports multiple input methods: local files, HTTP URLs, InputStreams, and byte arrays.

**For external storage (e.g., blobs already in S3):**

```sql
CREATE TABLE video_table (
    id INT,
    title STRING,
    video BYTES
) WITH (
    'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true',
    'blob-descriptor-field' = 'video',
    'blob-external-storage-field' = 'video',
    'blob-external-storage-path' = 's3://my-bucket/paimon-blobs/'
);
```

This stores only descriptor references inline, while the actual blob data resides in external storage.

### Scenario 9: Vector Search / RAG Applications

**When:** You are building a recommendation system, image retrieval, or RAG (Retrieval Augmented Generation)
application that needs approximate nearest neighbor (ANN) search on embeddings.

**Recommended Configuration:**

{{< tabs "vector-scenario" >}}
{{< tab "Spark SQL" >}}
```sql
CREATE TABLE doc_embeddings (
    doc_id INT,
    title STRING,
    content STRING,
    embedding ARRAY<FLOAT>
) TBLPROPERTIES (
    'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true',
    'global-index.enabled' = 'true',
    'vector-field' = 'embedding',
    'field.embedding.vector-dim' = '768',
    'vector.file.format' = 'lance'
);
```
{{< /tab >}}
{{< tab "Java API" >}}
```java
Schema schema = Schema.newBuilder()
    .column("doc_id", DataTypes.INT())
    .column("title", DataTypes.STRING())
    .column("content", DataTypes.STRING())
    .column("embedding", DataTypes.VECTOR(768, DataTypes.FLOAT()))
    .option("bucket", "-1")
    .option("row-tracking.enabled", "true")
    .option("data-evolution.enabled", "true")
    .option("global-index.enabled", "true")
    .option("vector.file.format", "lance")
    .build();
```
{{< /tab >}}
{{< /tabs >}}

**Build the vector index and search:**

```sql
-- Build DiskANN vector index
CALL sys.create_global_index(
    table => 'db.doc_embeddings',
    index_column => 'embedding',
    index_type => 'lumina',
    options => 'lumina.index.dimension=768'
);

-- Search for top-5 nearest neighbors
SELECT * FROM vector_search('doc_embeddings', 'embedding', array(0.1f, 0.2f, ...), 5);
```

The legacy index type `lumina-vector-ann` is still accepted for existing tables and SQL compatibility.

**Why:** The [Global Index]({{< ref "append-table/global-index" >}}) with DiskANN provides high-performance ANN search.
Vector data is stored in dedicated `.vector.lance` files optimized for dense vectors, while scalar columns stay in
Parquet. You can also build a **BTree Index** on scalar columns for efficient filtering:

```sql
-- Build BTree index for scalar filtering
CALL sys.create_global_index(
    table => 'db.doc_embeddings',
    index_column => 'title',
    index_type => 'btree'
);

-- Scalar lookup is accelerated by BTree index
SELECT * FROM doc_embeddings WHERE title IN ('doc_a', 'doc_b');
```

### Scenario 10: AI Feature Engineering with Data Evolution

**When:** You have a feature store or data pipeline where new feature columns are added frequently, and you want to
backfill or update specific columns without rewriting entire data files.

**Recommended Configuration:**

```sql
CREATE TABLE feature_store (
    user_id INT,
    age INT,
    gender STRING,
    purchase_count INT
) TBLPROPERTIES (
    'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true'
);
```

**Update only specific columns via MERGE INTO:**

```sql
-- Later, add a new feature column
ALTER TABLE feature_store ADD COLUMNS (embedding ARRAY<FLOAT>);

-- Backfill only the new column — no full file rewrite!
MERGE INTO feature_store AS t
USING embedding_source AS s
ON t.user_id = s.user_id
WHEN MATCHED THEN UPDATE SET t.embedding = s.embedding;
```

**Why:** [Data Evolution]({{< ref "append-table/data-evolution" >}}) mode writes only the updated columns to new files
and merges them at read time. This is ideal for:

- Adding new feature columns and backfilling data without rewriting the entire table.
- Iterative ML feature engineering — add, update, or refine features as your model evolves.
- Reducing I/O cost and storage overhead for frequent partial column updates.

### Scenario 11: Python AI Pipeline (PyPaimon)

**When:** You are building ML training or inference pipelines in Python and need to read/write Paimon tables natively
without JDK dependency.

**Example: Read data for model training:**

```python
from pypaimon import CatalogFactory
from torch.utils.data import DataLoader

# Connect to Paimon
catalog = CatalogFactory.create({'warehouse': 's3://my-bucket/warehouse'})
table = catalog.get_table('db.feature_store')

# Read with filter and projection
read_builder = table.new_read_builder()
read_builder = read_builder.with_projection(['user_id', 'embedding'])
read_builder = read_builder.with_filter(
    read_builder.new_predicate_builder().equal('gender', 'M')
)
splits = read_builder.new_scan().plan().splits()
table_read = read_builder.new_read()

# Option 1: Load into PyTorch DataLoader
dataset = table_read.to_torch(splits, streaming=True, prefetch_concurrency=2)
dataloader = DataLoader(dataset, batch_size=32, num_workers=4)
for batch in dataloader:
    # Training loop
    pass

# Option 2: Load into Ray for distributed processing
ray_dataset = table_read.to_ray(splits, override_num_blocks=8)
mapped = ray_dataset.map(lambda row: {'feat': row['embedding']})

# Option 3: Load into Pandas / PyArrow
df = table_read.to_pandas(splits)
arrow_table = table_read.to_arrow(splits)
```

**Why:** [PyPaimon]({{< ref "pypaimon/overview" >}}) is a pure Python SDK (no JDK required) that integrates seamlessly
with the Python AI ecosystem:

- **PyTorch**: Direct `DataLoader` integration with streaming and prefetch support.
- **Ray**: Distributed data processing with configurable parallelism.
- **Pandas / PyArrow**: Native DataFrame and Arrow Table support for data science workflows.
- **Data Evolution**: Python API supports `update_by_arrow_with_row_id` and `upsert_by_arrow_with_key` for
  row-level updates from Python. See [PyPaimon Data Evolution]({{< ref "pypaimon/data-evolution" >}}).

---

## Summary: Table Type Decision Tree

```
Do you need upsert / update / delete?
├── YES → Primary Key Table
│   ├── Simple upsert (keep latest)? → merge-engine = deduplicate (default)
│   ├── Progressive multi-column updates? → merge-engine = partial-update
│   ├── Pre-aggregate metrics? → merge-engine = aggregation
│   └── Dedup keep first? → merge-engine = first-row
│
│   Table mode:
│   ├── Most scenarios → deletion-vectors.enabled = true (MOW, recommended)
│   ├── Write-heavy, query-light → default MOR
│   └── Read-heavy, batch → full-compaction.delta-commits = 1 (COW)
│
│   Bucket mode:
│   ├── Most scenarios → bucket = -1 (Dynamic, default)
│   ├── Stable workload, need bucketed join → bucket = N (Fixed)
│   └── Unknown distribution → bucket = -2 (Postpone)
│
└── NO → Append Table
    ├── Standard batch ETL? → No bucket config (unaware-bucket)
    │   └── Need query acceleration? → Incremental Clustering or File Index
    │
    ├── Need bucket-key filter pushdown / join / ordered streaming?
    │   → bucket = N, bucket-key = col (Bucketed Append)
    │
    └── AI / Multimodal scenarios? → Enable Data Evolution
        ├── Store images / videos / docs? → Blob Table (blob-field)
        ├── Vector search / RAG? → VECTOR type + Global Index (DiskANN)
        ├── Feature engineering? → Data Evolution (MERGE INTO partial columns)
        └── Python pipeline? → PyPaimon (Ray / PyTorch / Pandas)
```
