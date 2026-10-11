---
title: "SQL Queries"
sidebar_position: 4
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

# SQL Queries

Query Paimon tables with `SELECT` after selecting a [Paimon catalog](./catalogs).
Choose the read scope before adding filters:

| Read scope | Use it for | Entry point |
| --- | --- | --- |
| Latest snapshot | Current table state | [`SELECT`](#batch-query) |
| Historical snapshot or tag | Reproduce a past table state | [Time travel](#batch-time-travel) |
| Bounded changes | Process an interval of updates | [Batch incremental](#batch-incremental) |
| Continuous changes | Keep consuming new commits | [Structured Streaming](./structured-streaming) |

![A snapshot read returns one table state; an incremental read selects changes in a bounded interval; a streaming read continues to later snapshots.](/img/spark-read-scopes.svg)

## Batch Query

Paimon's batch read returns all the data in a snapshot of the table. By default, batch reads return the latest snapshot.

```sql
-- read all columns
SELECT * FROM t;
```

Paimon also supports reading some hidden metadata columns, currently supporting the following columns:

- `__paimon_partition`: The partition of the record.
- `__paimon_bucket`: The bucket of the record.
- `__paimon_row_index`: The row index of the record. (Available only for non-PK or deletion vector or full compacted PK table).
- `__paimon_file_path`: The file path of the record. (Available only for non-PK or deletion vector or full compacted PK table).
- `_ROW_ID`: The unique row id of the record (Available only for row-tracking tables).
- `_SEQUENCE_NUMBER`: The sequence number of the record (Available only for row-tracking tables).

For example:

```sql
-- read all columns and the corresponding file path, partition, bucket, rowIndex of the record
SELECT *, __paimon_file_path, __paimon_partition, __paimon_bucket, __paimon_row_index FROM t;
```

### Scan Layout and Storage Partition Joins

By default, Paimon plans batch read tasks using its regular split packing. It does not report
bucket distribution or scan ordering to Spark, so scan parallelism is not limited by the number
of selected buckets. Spark still adds the exchanges and sorts required by the query.

To let Spark use a fixed-bucket table's layout for storage partition joins or grouped aggregates,
enable both options before planning the query:

```sql
SET spark.paimon.scan.preserve-data-grouping=true;
SET spark.sql.sources.v2.bucketing.enabled=true;

SELECT * FROM t1 JOIN t2 ON t1.bucket_key = t2.bucket_key;
```

`scan.preserve-data-grouping` defaults to `false`. It can also be set as a table property or as a
DataFrame read option. Session and read options follow the precedence described in
[Configuration](./configuration). The effective choice is fixed when a scan is created;
changing a session option later affects newly created scans.

In grouped mode, Paimon preserves complete splits and can provide multiple read units for the
same bucket. Spark may group those units into one task per bucket. When a supported join uses
`spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled=true`, Spark can use
multiple tasks for a bucket. This is a join optimization; enabling grouped mode can still reduce
the parallelism of a plain scan or TopN query.

Grouped mode requires Spark 3.3 or later and a supported fixed-bucket layout. If Spark V2
bucketing is disabled, or the scan cannot report a supported bucket layout, Paimon uses regular
split packing. Grouping does not guarantee that every join can avoid a shuffle.

**Migration:** Enabling Spark V2 bucketing alone, including its default of `true` in Spark 4.1,
no longer opts Paimon into grouped scans. Workloads that depend on bucket distribution to avoid
shuffles must also enable `scan.preserve-data-grouping`. The former Paimon adaptive rule that
disabled bucket scans after physical planning has been removed;
`spark.sql.sources.bucketing.autoBucketedScan.enabled` no longer changes a Paimon scan's layout.
AQE and non-AQE queries use the same scan policy.

### Batch Time Travel

Paimon batch reads with time travel can specify a snapshot or a tag and read the corresponding data.

Requires Spark 3.3+.

you can use `VERSION AS OF` and `TIMESTAMP AS OF` in query to do time travel:

```sql
-- read the snapshot with id 1L (use snapshot id as version)
SELECT * FROM t VERSION AS OF 1;

-- read the snapshot from specified timestamp
SELECT * FROM t TIMESTAMP AS OF '2023-06-01 00:00:00.123';

-- read the snapshot from specified timestamp in unix seconds
SELECT * FROM t TIMESTAMP AS OF 1678883047;

-- read tag 'my-tag'
SELECT * FROM t VERSION AS OF 'my-tag';

-- read the snapshot from specified watermark. will match the first snapshot after the watermark
SELECT * FROM t VERSION AS OF 'watermark-1678883047356';

```

:::warning

If tag's name is a number and equals to a snapshot id, the VERSION AS OF syntax will consider tag first. For example, if
you have a tag named '1' based on snapshot 2, the statement `SELECT * FROM t VERSION AS OF '1'` actually queries snapshot 2
instead of snapshot 1.

:::

### Batch Incremental

Read incremental changes between a start snapshot (exclusive) and an end snapshot (inclusive).

For example:
- '5,10' means changes between snapshot 5 and snapshot 10.
- 'TAG1,TAG3' means changes between TAG1 and TAG3.

For snapshot-ID and timestamp ranges, the default scan uses changelog files when the table
has a changelog producer; otherwise it scans newly changed files. Tag-to-tag ranges default to
a snapshot diff. Use `incremental-between-scan-mode` to select a supported scan mode explicitly.

Paimon provides table-valued functions for incremental SQL queries. These require the
[Paimon SQL extensions](./quick-start#setup).

```sql
-- read the incremental data between snapshot id 12 and snapshot id 20.
SELECT * FROM paimon_incremental_query('tableName', 12, 20);

-- read the incremental data between timestamps 1692169000000 and 1692169900000.
SELECT * FROM paimon_incremental_between_timestamp('tableName', '1692169000000', '1692169900000');
SELECT * FROM paimon_incremental_between_timestamp('tableName', '2025-03-12 00:00:00', '2025-03-12 00:08:00');

-- read the incremental data to tag '2024-12-04'.
-- Paimon will find an earlier tag and return changes between them.
-- If the tag doesn't exist or the earlier tag doesn't exist, return empty.
SELECT * FROM paimon_incremental_to_auto_tag('tableName', '2024-12-04');
```

In Batch SQL, the `DELETE` records are not allowed to be returned, so records of `-D` will be dropped.
To inspect row kinds, including deletes, query the [`audit_log` system table](../concepts/system-tables#audit-log-table).

## Query Optimization

It is highly recommended to specify partition and primary key filters
along with the query, which will speed up the data skipping of the query.

The filter functions that can accelerate data skipping are:
- `=`
- `<`
- `<=`
- `>`
- `>=`
- `IN (...)`
- `LIKE 'abc%'`
- `IS NULL`

Paimon will sort the data by primary key, which speeds up the point queries
and range queries. When using a composite primary key, it is best for the query
filters to form a [leftmost prefix](https://dev.mysql.com/doc/refman/5.7/en/multiple-column-indexes.html)
of the primary key for good acceleration.

Suppose that a table has the following specification:

```sql
CREATE TABLE orders (
    catalog_id BIGINT,
    order_id BIGINT,
    amount DECIMAL(10, 2)
) TBLPROPERTIES (
    'primary-key' = 'catalog_id,order_id'
);
```

The query obtains a good acceleration by specifying a range filter for
the leftmost prefix of the primary key.

```sql
SELECT * FROM orders WHERE catalog_id=1025;

SELECT * FROM orders WHERE catalog_id=1025 AND order_id=29495;

SELECT * FROM orders
  WHERE catalog_id=1025
  AND order_id>2035 AND order_id<6000;
```

However, the following filter cannot accelerate the query well.

```sql
SELECT * FROM orders WHERE order_id=29495;

SELECT * FROM orders WHERE catalog_id=1025 OR order_id=29495;
```
