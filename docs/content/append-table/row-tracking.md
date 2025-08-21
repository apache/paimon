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
- `_ROW_ID`: BIGINT, this is a unique identifier for each row in the table. It is used to track the lineage of the row and can be used to identify the row in case of update, merge into or delete.
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
- Only spark support update, merge into and delete operations on row-tracking tables, Flink SQL does not support these operations yet.
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
WHEN NOT MATCHED THEN INSERT *;
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

You can also delete from the table:

```sql
DELETE FROM t WHERE id = 11;
```

You will get:
```text
+---+---------------+-------+----------------+
| id|           data|_ROW_ID|_SEQUENCE_NUMBER|
+---+---------------+-------+----------------+
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

# Data Evolution Mode

## What is data evolution mode
Data Evolution Mode is a new feature for Paimon's append tables that revolutionizes how you handle schema evolution, particularly when adding new columns. 
This mode allows you to update partial columns without rewriting entire data files. 
Instead, it writes new column data to separate files and intelligently merges them with the original data during read operations.


## Key Features and Benefits
The data evolution mode offers significant advantages for your data lake architecture:

* Efficient Partial Column Updates: With this mode, you can use Spark's MERGE INTO statement to update a subset of columns. This avoids the high I/O cost of rewriting the whole file, as only the updated columns are written.

* Reduced File Rewrites: In scenarios with frequent schema changes, such as adding new columns, the traditional method requires constant file rewriting. Data evolution mode eliminates this overhead by appending new column data to dedicated files. This approach is much more efficient and reduces the burden on your storage system.

* Optimized Read Performance: The new mode is designed for seamless data retrieval. During query execution, Paimon's engine efficiently combines the original data with the new column data, ensuring that read performance remains uncompromised. The merge process is highly optimized, so your queries run just as fast as they would on a single, consolidated file.

## Enabling Data Evolution Mode
To enable data evolution, you must enable row-tracking and set the `data-evolution.enabled` property to `true` when creating an append table. This ensures that the table is ready for efficient schema evolution operations.
Use Spark Sql as an example:
```sql
CREATE TABLE target (a INT, b INT, c STRING) TBLPROPERTIES ('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true')
```

## Partially update columns
Now we could only support spark 'MERGE INTO' statement to update partial columns.
```sql
MERGE INTO t
USING s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET t.b = s.b
WHEN NOT MATCHED THEN INSERT (id, b, c) VALUES (id, b, 11)
```
This statement updates only the `b` column in the target table `t` based on the matching records from the source table `s`. The `id` column and `c` column remain unchanged, and new records are inserted with the specified values.

Note that: 
* Data Evolution Table does not support 'Delete' statement yet
* Merge Into for Data Evolution Table does not support 'WHEN NOT MATCHED BY SOURCE' clause
* Only Spark version greater than 3.5.0 is supported for Data Evolution Table