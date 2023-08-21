---
title: "Altering Tables"
weight: 3
type: docs
aliases:
- /how-to/altering-tables.html
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

# Altering Tables

## Changing/Adding Table Properties

The following SQL sets `write-buffer-size` table property to `256 MB`.

{{< tabs "set-properties-example" >}}

{{< tab "Flink" >}}

```sql
ALTER TABLE my_table SET (
    'write-buffer-size' = '256 MB'
);
```

{{< /tab >}}

{{< tab "Spark3" >}}

```sql
ALTER TABLE my_table SET TBLPROPERTIES (
    'write-buffer-size' = '256 MB'
);
```

{{< /tab >}}

{{< tab "Trino" >}}

```sql
ALTER TABLE my_table SET PROPERTIES write_buffer_size = '256 MB';
```

> NOTE: Versions below Trino 368 do not support changing/adding table properties.

{{< /tab >}}

{{< /tabs >}}

## Rename Table Name

The following SQL rename the table name to new name.

{{< tabs "rename-table-name" >}}

{{< tab "Flink" >}}

```sql
ALTER TABLE my_table RENAME TO my_table_new;
```

{{< /tab >}}

{{< tab "Spark3" >}}

The simplest sql to call is:
```sql
ALTER TABLE my_table RENAME TO my_table_new;
```

Note that: we can rename paimon table in spark this way:
```sql
ALTER TABLE [catalog.[database.]]test1 RENAME to [database.]test2;
```
But we can't put catalog name before the renamed-to table, it will throw an error if we write sql like this:
```sql
ALTER TABLE catalog.database.test1 RENAME to catalog.database.test2;
```

{{< /tab >}}

{{< tab "Trino" >}}

```sql
ALTER TABLE my_table RENAME TO my_table_new;
```

{{< /tab >}}

{{< tab "Presto" >}}

```sql
ALTER TABLE my_table RENAME TO my_table_new;
```

{{< /tab >}}

{{< /tabs >}}

{{< hint info >}}

If you use object storage, such as S3 or OSS, please use this syntax carefully, because the renaming of object storage is not atomic, and only partial files may be moved in case of failure.

{{< /hint >}}

## Removing Table Properties

The following SQL removes `write-buffer-size` table property.

{{< tabs "unset-properties-example" >}}

{{< tab "Flink" >}}

```sql
ALTER TABLE my_table RESET ('write-buffer-size');
```

{{< /tab >}}

{{< tab "Spark3" >}}

```sql
ALTER TABLE my_table UNSET TBLPROPERTIES ('write-buffer-size');
```

{{< /tab >}}

{{< /tabs >}}

## Adding New Columns

The following SQL adds two columns `c1` and `c2` to table `my_table`.

{{< tabs "add-columns-example" >}}

{{< tab "Flink" >}}

```sql
ALTER TABLE my_table ADD (c1 INT, c2 STRING);
```

{{< /tab >}}

{{< tab "Spark3" >}}

```sql
ALTER TABLE my_table ADD COLUMNS (
    c1 INT,
    c2 STRING
);
```

{{< /tab >}}

{{< tab "Trino" >}}

```sql
ALTER TABLE my_table ADD COLUMN c1 VARCHAR;
```

{{< /tab >}}

{{< tab "Presto" >}}

```sql
ALTER TABLE my_table ADD COLUMN c1 VARCHAR;
```

{{< /tab >}}

{{< /tabs >}}

## Renaming Column Name
The following SQL renames column `c0` in table `my_table` to `c1`.

{{< tabs "rename-column-name-example" >}}

{{< tab "Flink" >}}

```sql
ALTER TABLE my_table RENAME c0 TO c1;
```

{{< /tab >}}


{{< tab "Spark3" >}}

```sql
ALTER TABLE my_table RENAME COLUMN c0 TO c1;
```

{{< /tab >}}

{{< tab "Trino" >}}

```sql
ALTER TABLE my_table RENAME COLUMN c0 TO c1;
```

{{< /tab >}}

{{< tab "Presto" >}}

```sql
ALTER TABLE my_table RENAME COLUMN c0 TO c1;
```

{{< /tab >}}

{{< /tabs >}}

## Dropping Columns

The following SQL drops two columns `c1` and `c2` from table `my_table`. In hive catalog, you need to ensure disable `hive.metastore.disallow.incompatible.col.type.changes` in your hive server,
otherwise this operation may fail, throws an exception like `The following columns have types incompatible with the existing columns in their respective positions`.

{{< tabs "drop-columns-example" >}}

{{< tab "Flink" >}}

```sql
ALTER TABLE my_table DROP (c1, c2);
```

{{< /tab >}}

{{< tab "Spark3" >}}

```sql
ALTER TABLE my_table DROP COLUMNS (c1, c2);
```

{{< /tab >}}

{{< tab "Trino" >}}

```sql
ALTER TABLE my_table DROP COLUMN c1;
```

{{< /tab >}}

{{< tab "Presto" >}}

```sql
ALTER TABLE my_table DROP COLUMN c1;
```

{{< /tab >}}

{{< /tabs >}}

## Changing Column Nullability

The following SQL changes nullability of column `coupon_info`.

{{< tabs "change-nullability-example" >}}

{{< tab "Flink" >}}

```sql
CREATE TABLE my_table (id INT PRIMARY KEY NOT ENFORCED, coupon_info FLOAT NOT NULL);

-- Change column `coupon_info` from NOT NULL to nullable
ALTER TABLE my_table MODIFY coupon_info FLOAT;

-- Change column `coupon_info` from nullable to NOT NULL
-- If there are NULL values already, set table option as below to drop those records silently before altering table.
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
ALTER TABLE my_table MODIFY coupon_info FLOAT NOT NULL;
```

{{< /tab >}}


{{< tab "Spark3" >}}

```sql
ALTER TABLE my_table ALTER COLUMN coupon_info DROP NOT NULL;
```

{{< /tab >}}

{{< /tabs >}}

{{< hint info >}}

Changing nullable column to NOT NULL is only supported by Flink currently.

{{< /hint >}}

## Changing Column Comment

The following SQL changes comment of column `buy_count` to `buy count`.

{{< tabs "change-comment-example" >}}

{{< tab "Flink" >}}

```sql
ALTER TABLE my_table MODIFY buy_count BIGINT COMMENT 'buy count';
```

{{< /tab >}}

{{< tab "Spark3" >}}

```sql
ALTER TABLE my_table ALTER COLUMN buy_count COMMENT 'buy count';
```

{{< /tab >}}

{{< /tabs >}}

## Adding Column Position

To add a new column with specified position, use FIRST or AFTER col_name.

{{< tabs "add-column-position" >}}


{{< tab "Flink" >}}

```sql
ALTER TABLE my_table ADD c INT FIRST;

ALTER TABLE my_table ADD c INT AFTER b;
```

{{< /tab >}}

{{< tab "Spark3" >}}

```sql
ALTER TABLE my_table ADD COLUMN c INT FIRST;

ALTER TABLE my_table ADD COLUMN c INT AFTER b;
```

{{< /tab >}}

{{< /tabs >}}

## Changing Column Position

To modify an existent column to a new position, use FIRST or AFTER col_name.

{{< tabs "change-column-position" >}}

{{< tab "Flink" >}}

```sql
ALTER TABLE my_table MODIFY col_a DOUBLE FIRST;

ALTER TABLE my_table MODIFY col_a DOUBLE AFTER col_b;
```

{{< /tab >}}

{{< tab "Spark3" >}}

```sql
ALTER TABLE my_table ALTER COLUMN col_a FIRST;

ALTER TABLE my_table ALTER COLUMN col_a AFTER col_b;
```

{{< /tab >}}

{{< /tabs >}}

## Changing Column Type

The following SQL changes type of column `col_a` to `DOUBLE`.

{{< tabs "change-column-type" >}}

{{< tab "Flink" >}}

```sql
ALTER TABLE my_table MODIFY col_a DOUBLE;
```

{{< /tab >}}

{{< tab "Spark3" >}}

```sql
ALTER TABLE my_table ALTER COLUMN col_a TYPE DOUBLE;
```

{{< /tab >}}

{{< tab "Trino" >}}

```sql
ALTER TABLE my_table ALTER COLUMN col_a SET DATA TYPE DOUBLE;
```

{{< /tab >}}

{{< /tabs >}}

Supported Type Conversions.

{{< generated/column_type_conversion >}}

## Adding watermark

The following SQL adds a computed column `ts` from existing column `log_ts`, and a watermark with strategy `ts - INTERVAL '1' HOUR` on column `ts` which is marked as event time attribute of table `my_table`.

{{< tabs "add-watermark" >}}

{{< tab "Flink" >}}

```sql
ALTER TABLE my_table ADD (
    ts AS TO_TIMESTAMP(log_ts) AFTER log_ts,
    WATERMARK FOR ts AS ts - INTERVAL '1' HOUR
);
```

{{< /tab >}}

{{< /tabs >}}

## Dropping watermark

The following SQL drops the watermark of table `my_table`.

{{< tabs "drop-watermark" >}}

{{< tab "Flink" >}}

```sql
ALTER TABLE my_table DROP WATERMARK;
```

{{< /tab >}}

{{< /tabs >}}

## Changing watermark

The following SQL modifies the watermark strategy to `ts - INTERVAL '2' HOUR`.

{{< tabs "change-watermark" >}}

{{< tab "Flink" >}}

```sql
ALTER TABLE my_table MODIFY WATERMARK FOR ts AS ts - INTERVAL '2' HOUR
```

{{< /tab >}}

{{< /tabs >}}