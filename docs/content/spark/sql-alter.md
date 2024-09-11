---
title: "SQL Alter"
weight: 6
type: docs
aliases:
- /spark/sql-alter.html
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
ALTER TABLE my_table SET TBLPROPERTIES (
    'write-buffer-size' = '256 MB'
);
```

## Removing Table Properties

The following SQL removes `write-buffer-size` table property.

```sql
ALTER TABLE my_table UNSET TBLPROPERTIES ('write-buffer-size');
```

##  Changing/Adding Table Comment

The following SQL changes comment of table `my_table` to `table comment`.

```sql
ALTER TABLE my_table SET TBLPROPERTIES (
    'comment' = 'table comment'
    );
```

## Removing Table Comment

The following SQL removes table comment.

```sql
ALTER TABLE my_table UNSET TBLPROPERTIES ('comment');
```

## Rename Table Name

The following SQL rename the table name to new name.

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

{{< hint info >}}
If you use object storage, such as S3 or OSS, please use this syntax carefully, because the renaming of object storage is not atomic, and only partial files may be moved in case of failure.
{{< /hint >}}

## Adding New Columns

The following SQL adds two columns `c1` and `c2` to table `my_table`.

```sql
ALTER TABLE my_table ADD COLUMNS (
    c1 INT,
    c2 STRING
);
```

## Renaming Column Name

The following SQL renames column `c0` in table `my_table` to `c1`.

```sql
ALTER TABLE my_table RENAME COLUMN c0 TO c1;
```

## Dropping Columns

The following SQL drops two columns `c1` and `c2` from table `my_table`.

```sql
ALTER TABLE my_table DROP COLUMNS (c1, c2);
```

## Dropping Partitions

The following SQL drops the partitions of the paimon table. For spark sql, you need to specify all the partition columns.

```sql
ALTER TABLE my_table DROP PARTITION (`id` = 1, `name` = 'paimon');
```

## Changing Column Comment

The following SQL changes comment of column `buy_count` to `buy count`.

```sql
ALTER TABLE my_table ALTER COLUMN buy_count COMMENT 'buy count';
```

## Adding Column Position

```sql
ALTER TABLE my_table ADD COLUMN c INT FIRST;

ALTER TABLE my_table ADD COLUMN c INT AFTER b;
```

## Changing Column Position

```sql
ALTER TABLE my_table ALTER COLUMN col_a FIRST;

ALTER TABLE my_table ALTER COLUMN col_a AFTER col_b;
```

## Changing Column Type

```sql
ALTER TABLE my_table ALTER COLUMN col_a TYPE DOUBLE;
```
