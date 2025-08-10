---
title: "Row Tracking"
weight: 5
type: docs
aliases:
- /append-table/row-tracking.html
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

# Use row tracking for Paimon Tables

## What is row tracking

Row tracking allows Paimon to track row-level lineage in a Paimon append table. Once enabled on a Paimon table, two more hidden columns will be added to the table schema:
- `_ROW_ID`: BIGINT, this is a unique identifier for each row in the table. It is used to track the lineage of the row and can be used to identify the row in case of updates or merge into.
- `_SEQUENCE_NUMBER`: BIGINT, this is field indicates which `version` of this record is. It actually is the snapshot-id of the snapshot that this row belongs to. It is used to track the lineage of the row version.

## Enable row tracking

To enable row-tracking, you must config `row-tracking.enabled` to `true` in the table options when creating an append table.
Consider an example via Flink SQL:
```sql
CREATE TABLE part_t (
    f0 INT,
    f1 STRING,
    dt STRING
) PARTITIONED BY (dt)
WITH ('row-tracking.enabled' = 'true');
```
Notice that:
- Row tracking is only supported for unaware append tables, not for primary key tables. Which means you can't define `bucket` and `bucket-key` for the table.
- Only spark support update and merge into operations on row-tracking tables, Flink SQL does not support these operations yet.
- This function is experimental, this line will be removed after being stable.

## How to use row tracking

After creating a row-tracking table, you can insert data into it as usual. The `_ROW_ID` and `_SEQUENCE_NUMBER` columns will be automatically managed by Paimon.
```sql
CREATE TABLE t (id INT, data STRING) TBLPROPERTIES ('row-tracking.enabled' = 'true');
INSERT INTO t VALUES (11, 'a'), (22, 'b')
```

You can select the row lineage meta column with the following sql in spark:
```sql
SELECT id, data, _ROW_ID, _SEQUENCE_NUMBER FROM t;
```
You will get the following result:
```text
+---+----+-------+----------------+
| id|data|_ROW_ID|_SEQUENCE_NUMBER|
+---+----+-------+----------------+
| 11|   a|      0|               1|
| 22|   b|      1|               1|
+---+----+-------+----------------+
```

Then you can update and query the table again:
```sql
UPDATE t SET data = 'new-data-update' WHERE id = 11;
SELECT id, data, _ROW_ID, _SEQUENCE_NUMBER FROM t;
```

You will get:
```text
+---+---------------+-------+----------------+
| id|           data|_ROW_ID|_SEQUENCE_NUMBER|
+---+---------------+-------+----------------+
| 22|              b|      1|               1|
| 11|new-data-update|      0|               2|
+---+---------------+-------+----------------+
```

You can also merge into the table, suppose you have a source table `s` that contains (22, 'new-data-merge') and (33, 'c'):
```sql
MERGE INTO t USING s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET t.data = s.data
WHEN NOT MATCHED THEN INSERT *
```

You will get:
```text
+---+---------------+-------+----------------+
| id|           data|_ROW_ID|_SEQUENCE_NUMBER|
+---+---------------+-------+----------------+
| 11|new-data-update|      0|               2|
| 22| new-data-merge|      1|               3|
| 33|              c|      2|               3|
+---+---------------+-------+----------------+
```

## Spec

`_ROW_ID` and `_SEQUENCE_NUMBER` fields follows the following rules:
- Whenever we read from one table with row tracking enabled, the `_ROW_ID` and `_SEQUENCE_NUMBER` will be `NOT NULL`.
- If we append records to row-tracking table in the first time, we don't actually write them to the data file, they are lazy assigned by committer.
- If one row moved from one file to another file for **any reason**, the `_ROW_ID` column should be copied to the target file. The `_SEQUENCE_NUMBER` field should be set to `NULL` if the record is changed, otherwise, copy it too.
- Whenever we read from a row-tracking table, we firstly read `_ROW_ID` and `_SEQUENCE_NUMBER` from the data file, then we read the value columns from the data file. If they found `NULL`, we read from `DataFileMeta` to fall back to the lazy assigned values. Anyway, it has no way to be `NULL`.
