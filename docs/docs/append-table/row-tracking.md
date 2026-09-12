---
title: "Row Tracking"
sidebar_position: 6
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

# Row Tracking

Row tracking adds two hidden metadata columns to an append table. They distinguish a row's identity from the snapshot
in which its current version was written.

| Column | Type | Meaning |
| --- | --- | --- |
| `_ROW_ID` | `BIGINT` | Paimon's identifier for a row, preserved across updates and ordinary compaction. |
| `_SEQUENCE_NUMBER` | `BIGINT` | The snapshot ID assigned when this version of the row was inserted or updated. Unchanged rows retain their version during ordinary compaction. |

These fields are managed by Paimon and are non-null when read. A row version is not the ID of every snapshot that
contains the row: many later snapshots can still contain an unchanged row with an older sequence number.

:::note Experimental

Row tracking is experimental. Enable it when creating an unaware-bucket append table (`bucket = -1`, with no primary
key or bucket key). `row-tracking.enabled` is immutable and cannot be enabled later with `ALTER TABLE`.

:::

## Enable Row Tracking

For example, create a partitioned table in Flink SQL:

```sql
CREATE TABLE part_t (
    id INT,
    data STRING,
    dt STRING
) PARTITIONED BY (dt) WITH (
    'bucket' = '-1',
    'row-tracking.enabled' = 'true'
);
```

Insert data as usual; do not add the hidden columns to the user-defined schema. The following walkthrough uses
Spark SQL for both querying the metadata columns and performing row-level changes on a regular append table.

## Follow a Row Through Changes

![An update assigns a new row version while retaining the row ID. Ordinary compaction preserves both values.](/img/append-row-tracking.svg)

### Insert and Read

Create a separate, unpartitioned table in a Paimon Spark catalog:

```sql
CREATE TABLE t (id INT, data STRING) USING paimon
TBLPROPERTIES ('row-tracking.enabled' = 'true');

INSERT INTO t VALUES (11, 'a'), (22, 'b');
SELECT id, data, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id;
```

The results below illustrate an initially empty table with one commit per write statement and no intervening commits.
Row-ID assignment can depend on file and write parallelism; do not rely on a business key receiving a particular ID.

```text
+---+----+-------+----------------+
| id|data|_ROW_ID|_SEQUENCE_NUMBER|
+---+----+-------+----------------+
| 11|   a|      0|               1|
| 22|   b|      1|               1|
+---+----+-------+----------------+
```

### Update

```sql
UPDATE t SET data = 'a2' WHERE id = 11;
SELECT id, data, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id;
```

The changed row retains its ID and receives a new sequence number. The untouched row keeps both values:

```text
+---+----+-------+----------------+
| id|data|_ROW_ID|_SEQUENCE_NUMBER|
+---+----+-------+----------------+
| 11|  a2|      0|               2|
| 22|   b|      1|               1|
+---+----+-------+----------------+
```

You can alternatively match an update with `WHERE _ROW_ID = 0`, using the ID returned by a previous query. Run either
form once if you are following the illustrated sequence numbers.

The sequence number records a write version, not a comparison of the old and new field values. A matching `UPDATE`
can assign a new sequence number even when the assigned value is the same, such as `UPDATE t SET data = data WHERE id = 11`.

### Merge

```sql
CREATE TEMPORARY VIEW s AS
SELECT * FROM VALUES (22, 'b2'), (33, 'c') AS source(id, data);

MERGE INTO t USING s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET t.data = s.data
WHEN NOT MATCHED THEN INSERT *;

SELECT id, data, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id;
```

The updated row retains its ID; the inserted row receives a new ID. Both versions come from the merge commit:

```text
+---+----+-------+----------------+
| id|data|_ROW_ID|_SEQUENCE_NUMBER|
+---+----+-------+----------------+
| 11|  a2|      0|               2|
| 22|  b2|      1|               3|
| 33|   c|      2|               3|
+---+----+-------+----------------+
```

### Delete

```sql
DELETE FROM t WHERE id = 11;
SELECT id, data, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id;
```

The deleted row is no longer visible. The remaining rows retain their identity and version:

```text
+---+----+-------+----------------+
| id|data|_ROW_ID|_SEQUENCE_NUMBER|
+---+----+-------+----------------+
| 22|  b2|      1|               3|
| 33|   c|      2|               3|
+---+----+-------+----------------+
```

You can also delete by a previously queried `_ROW_ID`. These metadata columns do not turn the table into a primary key
table and do not deduplicate inserted business keys.

## How Metadata Is Stored

For newly appended rows, Paimon can assign IDs and sequence numbers lazily during commit using file metadata rather
than writing the hidden values into every row. Readers use stored row metadata when available and fall back to the
file metadata when a hidden value is absent.

When an ordinary rewrite moves a row to another data file, its row ID is carried forward. Rows copied without being
updated keep their sequence numbers; updated rows receive a new sequence number at commit. Ordinary compaction
therefore does not by itself change a row's version.

For the separate Data Evolution storage model, including maintenance that can reassign physical row IDs, see
[Data Evolution](../multimodal-table/data-evolution).
