---
title: "SQL Alter"
weight: 7
type: docs
aliases:
- /flink/sql-alter.html
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

```sql
ALTER TABLE my_table SET (
    'write-buffer-size' = '256 MB'
);
```

## Removing Table Properties

The following SQL removes `write-buffer-size` table property.

```sql
ALTER TABLE my_table RESET ('write-buffer-size');
```

##  Changing/Adding Table Comment

The following SQL changes comment of table `my_table` to `table comment`.

```sql
ALTER TABLE my_table SET (
    'comment' = 'table comment'
    );
```

## Removing Table Comment

The following SQL removes table comment.

```sql
ALTER TABLE my_table RESET ('comment');
```

## Rename Table Name

The following SQL rename the table name to new name.

```sql
ALTER TABLE my_table RENAME TO my_table_new;
```

{{< hint info >}}
If you use object storage, such as S3 or OSS, please use this syntax carefully, because the renaming of object storage is not atomic, and only partial files may be moved in case of failure.
{{< /hint >}}

## Adding New Columns

The following SQL adds two columns `c1` and `c2` to table `my_table`.

{{< hint info >}}
Flink SQL does not support adding nested columns. To add a column in a row type, see [Changing Column Type](#changing-column-type).
{{< /hint >}}

```sql
ALTER TABLE my_table ADD (c1 INT, c2 STRING);
```

## Renaming Column Name

The following SQL renames column `c0` in table `my_table` to `c1`.

```sql
ALTER TABLE my_table RENAME c0 TO c1;
```

## Dropping Columns

The following SQL drops two columns `c1` and `c2` from table `my_table`. In hive catalog, you need to ensure disable `hive.metastore.disallow.incompatible.col.type.changes` in your hive server,
otherwise this operation may fail, throws an exception like `The following columns have types incompatible with the existing columns in their respective positions`.

```sql
ALTER TABLE my_table DROP (c1, c2);
```

{{< hint info >}}
Flink SQL does not support dropping nested columns. To drop a column in a row type, see [Changing Column Type](#changing-column-type).
{{< /hint >}}

## Dropping Partitions

The following SQL drops the partitions of the paimon table.

For flink sql, you can specify the partial columns of partition columns, and you can also specify multiple partition values at the same time.

```sql
ALTER TABLE my_table DROP PARTITION (`id` = 1);

ALTER TABLE my_table DROP PARTITION (`id` = 1, `name` = 'paimon');

ALTER TABLE my_table DROP PARTITION (`id` = 1), PARTITION (`id` = 2);

```

## Adding Partitions

The following SQL adds the partitions of the paimon table.

For flink sql, you can specify the partial columns of partition columns, and you can also specify multiple partition values at the same time, only with metastore configured metastore.partitioned-table=true.

```sql
ALTER TABLE my_table ADD PARTITION (`id` = 1);

ALTER TABLE my_table ADD PARTITION (`id` = 1, `name` = 'paimon');

ALTER TABLE my_table ADD PARTITION (`id` = 1), PARTITION (`id` = 2);

```

## Changing Column Nullability

The following SQL changes nullability of column `coupon_info`.

```sql
CREATE TABLE my_table (id INT PRIMARY KEY NOT ENFORCED, coupon_info FLOAT NOT NULL);

-- Change column `coupon_info` from NOT NULL to nullable
ALTER TABLE my_table MODIFY coupon_info FLOAT;

-- Change column `coupon_info` from nullable to NOT NULL
-- If there are NULL values already, set table option as below to drop those records silently before altering table.
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
ALTER TABLE my_table MODIFY coupon_info FLOAT NOT NULL;
```

{{< hint info >}}
Changing nullable column to NOT NULL is only supported by Flink currently.
{{< /hint >}}

## Changing Column Comment

The following SQL changes comment of column `buy_count` to `buy count`.

```sql
ALTER TABLE my_table MODIFY buy_count BIGINT COMMENT 'buy count';
```

## Adding Column Position

To add a new column with specified position, use FIRST or AFTER col_name.

```sql
ALTER TABLE my_table ADD c INT FIRST;

ALTER TABLE my_table ADD c INT AFTER b;
```

## Changing Column Position

To modify an existent column to a new position, use FIRST or AFTER col_name.

```sql
ALTER TABLE my_table MODIFY col_a DOUBLE FIRST;

ALTER TABLE my_table MODIFY col_a DOUBLE AFTER col_b;
```

## Changing Column Type

The following SQL changes type of column `col_a` to `DOUBLE`.

```sql
ALTER TABLE my_table MODIFY col_a DOUBLE;
```

Paimon also supports changing columns of row type, array type, and map type.

```sql
-- col_a previously has type ARRAY<MAP<INT, ROW(f1 INT, f2 STRING)>>
-- the following SQL changes f1 to BIGINT, drops f2, and adds f3
ALTER TABLE my_table MODIFY col_a ARRAY<MAP<INT, ROW(f1 BIGINT, f3 DOUBLE)>>;
```

## Adding watermark

The following SQL adds a computed column `ts` from existing column `log_ts`, and a watermark with strategy `ts - INTERVAL '1' HOUR` on column `ts` which is marked as event time attribute of table `my_table`.

```sql
ALTER TABLE my_table ADD (
    ts AS TO_TIMESTAMP(log_ts) AFTER log_ts,
    WATERMARK FOR ts AS ts - INTERVAL '1' HOUR
);
```

## Dropping watermark

The following SQL drops the watermark of table `my_table`.

```sql
ALTER TABLE my_table DROP WATERMARK;
```

## Changing watermark

The following SQL modifies the watermark strategy to `ts - INTERVAL '2' HOUR`.

```sql
ALTER TABLE my_table MODIFY WATERMARK FOR ts AS ts - INTERVAL '2' HOUR
```
