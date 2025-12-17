---
title: "SQL Query"
weight: 4
type: docs
aliases:
- /spark/sql-query.html
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

# SQL Query

Just like all other tables, Paimon tables can be queried with `SELECT` statement.

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

{{< hint warning >}}
If tag's name is a number and equals to a snapshot id, the VERSION AS OF syntax will consider tag first. For example, if
you have a tag named '1' based on snapshot 2, the statement `SELECT * FROM t VERSION AS OF '1'` actually queries snapshot 2
instead of snapshot 1.
{{< /hint >}}

### Batch Incremental

Read incremental changes between start snapshot (exclusive) and end snapshot.

For example:
- '5,10' means changes between snapshot 5 and snapshot 10.
- 'TAG1,TAG3' means changes between TAG1 and TAG3.

By default, will scan changelog files for the table which produces changelog files. Otherwise, scan newly changed files.
You can also force specifying `'incremental-between-scan-mode'`.

Paimon supports that use Spark SQL to do the incremental query that implemented by Spark Table Valued Function.

```sql
-- read the incremental data between snapshot id 12 and snapshot id 20.
SELECT * FROM paimon_incremental_query('tableName', 12, 20);

-- read the incremental data between ts 1692169900000 and ts 1692169900000.
SELECT * FROM paimon_incremental_between_timestamp('tableName', '1692169000000', '1692169900000');
SELECT * FROM paimon_incremental_between_timestamp('tableName', '2025-03-12 00:00:00', '2025-03-12 00:08:00');

-- read the incremental data to tag '2024-12-04'.
-- Paimon will find an earlier tag and return changes between them.
-- If the tag doesn't exist or the earlier tag doesn't exist, return empty.
SELECT * FROM paimon_incremental_to_auto_tag('tableName', '2024-12-04');
```

In Batch SQL, the `DELETE` records are not allowed to be returned, so records of `-D` will be dropped.
If you want see `DELETE` records, you can query audit_log table.

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
    .....,
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
