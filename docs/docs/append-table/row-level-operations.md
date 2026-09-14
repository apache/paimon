---
title: "Row-Level Operations"
sidebar_position: 5
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

# Row-Level Operations

An append table stores each inserted row without key-based deduplication, but you can still modify stored data with
explicit Spark SQL `DELETE`, `UPDATE`, and `MERGE INTO` statements. These operations locate matching rows; they do not
turn ordinary inserts into upserts.

The examples below use a Paimon catalog and the `my_table` schema from the [overview](./). Configure Spark with
`org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions` as shown in the [Spark quick start](../spark/quick-start).
See [Spark SQL write](../spark/sql-write) for statement syntax.

## Delete, Update, and Merge

```sql
DELETE FROM my_table WHERE price < 0;

UPDATE my_table SET price = 12.0
WHERE product_id = 1 AND dt = '2026-09-10';
```

For a merge, create a source with the same columns and match on the columns that identify rows in your application:

```sql
CREATE TEMPORARY VIEW updates AS
SELECT CAST(1 AS BIGINT) AS product_id, CAST(12.0 AS DOUBLE) AS price,
       CAST(3 AS BIGINT) AS sales, '2026-09-10' AS dt;

MERGE INTO my_table AS target
USING updates AS source
ON target.product_id = source.product_id AND target.dt = source.dt
WHEN MATCHED THEN UPDATE SET
    target.price = source.price, target.sales = source.sales
WHEN NOT MATCHED THEN INSERT *;
```

There is no primary key constraint on this table. In the example, matching uses `(product_id, dt)`:

- If several target rows have the same matching values, one source row can update all of them.
- With a `WHEN MATCHED` clause, a target row must not match more than one source row. Paimon rejects that ambiguous
  match, so resolve duplicate source keys before the merge.
- Source rows that do not match the target are inserted independently. The merge does not deduplicate those rows.

Choose the merge condition and prepare source rows to express the intended matching behavior. A bucket key does not
enforce uniqueness either.

## Choose How Changes Are Stored

For regular append tables, two approaches are available:

| Approach | Configuration | What happens |
| --- | --- | --- |
| Copy on write (COW) | Deletion vectors disabled (default). | Affected files are replaced with files containing the surviving or updated rows. |
| Deletion vectors | `deletion-vectors.enabled = true` | Deleted positions are marked in deletion-vector files. Updates mark old row versions as deleted and write new row versions. |

Deletion vectors avoid rewriting an entire data file just to remove some rows. Readers apply the deletion information
when scanning the data. They do not remove the need to find matching rows or write updated values.

![Deleting B produces the same visible rows A, C, and D. Copy on write replaces the affected file; deletion vectors retain it and mark B's position as deleted.](/img/append-row-level-storage.svg)

The diagram shows a delete affecting part of one file. A delete that can remove a whole partition may instead use a
metadata-only operation. Replaced files can remain available to older snapshots until snapshot expiration makes them
eligible for cleanup.

For example, create a Spark table with deletion vectors:

```sql
CREATE TABLE mutable_events (
    event_id BIGINT,
    payload STRING
) USING paimon
TBLPROPERTIES (
    'deletion-vectors.enabled' = 'true'
);
```

Bucketed append tables with incremental clustering cannot enable deletion vectors. Check
[clustering requirements](./incremental-clustering#requirements) before combining these features.

## Track Row Identity

Enable [row tracking](./row-tracking) at table creation when you need a hidden row ID and a row version across updates
and ordinary compaction. Row tracking is supported only for unaware-bucket append tables (`bucket = -1`). It is separate
from deletion vectors and does not need to be enabled for the basic statements above.

Neither feature turns streaming reads into a complete change feed for these mutations. See
[streaming read behavior](./streaming#overwrite-commits) when a table also has downstream streaming consumers.

These examples cover regular append tables through Spark SQL. For the separate Data Evolution storage model and its
supported operations, see [Data Evolution](../multimodal-table/data-evolution).
