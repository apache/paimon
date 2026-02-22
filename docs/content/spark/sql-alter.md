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
If you use object storage without REST Catalog, such as S3 or OSS, please use this syntax carefully, because the renaming of object storage is not atomic, and only partial files may be moved in case of failure.
{{< /hint >}}

## Adding New Columns

The following SQL adds two columns `c1` and `c2` to table `my_table`.

```sql
ALTER TABLE my_table ADD COLUMNS (
    c1 INT,
    c2 STRING
);
```

The following SQL adds a nested column `f3` to a struct type.

```sql
-- column v previously has type STRUCT<f1: STRING, f2: INT>
ALTER TABLE my_table ADD COLUMN v.f3 STRING;
```

The following SQL adds a nested column `f3` to a struct type, which is the element type of an array type.

```sql
-- column v previously has type ARRAY<STRUCT<f1: STRING, f2: INT>>
ALTER TABLE my_table ADD COLUMN v.element.f3 STRING;
```

The following SQL adds a nested column `f3` to a struct type, which is the value type of a map type.

```sql
-- column v previously has type MAP<INT, STRUCT<f1: STRING, f2: INT>>
ALTER TABLE my_table ADD COLUMN v.value.f3 STRING;
```

## Renaming Column Name

The following SQL renames column `c0` in table `my_table` to `c1`.

```sql
ALTER TABLE my_table RENAME COLUMN c0 TO c1;
```

The following SQL renames a nested column `f1` to `f100` in a struct type.

```sql
-- column v previously has type STRUCT<f1: STRING, f2: INT>
ALTER TABLE my_table RENAME COLUMN v.f1 to f100;
```

The following SQL renames a nested column `f1` to `f100` in a struct type, which is the element type of an array type.

```sql
-- column v previously has type ARRAY<STRUCT<f1: STRING, f2: INT>>
ALTER TABLE my_table RENAME COLUMN v.element.f1 to f100;
```

The following SQL renames a nested column `f1` to `f100` in a struct type, which is the value type of a map type.

```sql
-- column v previously has type MAP<INT, STRUCT<f1: STRING, f2: INT>>
ALTER TABLE my_table RENAME COLUMN v.value.f1 to f100;
```

## Dropping Columns

The following SQL drops two columns `c1` and `c2` from table `my_table`.

```sql
ALTER TABLE my_table DROP COLUMNS (c1, c2);
```

The following SQL drops a nested column `f2` from a struct type.

```sql
-- column v previously has type STRUCT<f1: STRING, f2: INT>
ALTER TABLE my_table DROP COLUMN v.f2;
```

The following SQL drops a nested column `f2` from a struct type, which is the element type of an array type.

```sql
-- column v previously has type ARRAY<STRUCT<f1: STRING, f2: INT>>
ALTER TABLE my_table DROP COLUMN v.element.f2;
```

The following SQL drops a nested column `f2` from a struct type, which is the value type of a map type.

```sql
-- column v previously has type MAP<INT, STRUCT<f1: STRING, f2: INT>>
ALTER TABLE my_table DROP COLUMN v.value.f2;
```

In hive catalog, you need to ensure:

1. disable `hive.metastore.disallow.incompatible.col.type.changes` in your hive server
2. or `spark-sql --conf spark.hadoop.hive.metastore.disallow.incompatible.col.type.changes=false` in your spark.

Otherwise this operation may fail, throws an exception like `The following columns have types incompatible with the
existing columns in their respective positions`.

## Dropping Partitions

The following SQL drops the partitions of the paimon table. For spark sql, you need to specify all the partition columns.

```sql
ALTER TABLE my_table DROP PARTITION (`id` = 1, `name` = 'paimon');
```

## Archiving Partitions

Paimon supports archiving partition files to different storage tiers (Archive, ColdArchive) in object stores like S3 and OSS. This feature helps optimize storage costs by moving infrequently accessed data to lower-cost storage tiers.

{{< hint info >}}
Archive operations are only supported for object stores (S3, OSS, etc.). The feature is not available for local file systems.
{{< /hint >}}

### Archive Partition

The following SQL archives a partition to Archive storage tier:

```sql
ALTER TABLE my_table PARTITION (dt='2024-01-01') ARCHIVE;
```

### Cold Archive Partition

The following SQL archives a partition to ColdArchive storage tier (lowest cost, longest access time):

```sql
ALTER TABLE my_table PARTITION (dt='2024-01-01') COLD ARCHIVE;
```

### Restore Archived Partition

The following SQL restores an archived partition to make it accessible for reading:

```sql
ALTER TABLE my_table PARTITION (dt='2024-01-01') RESTORE ARCHIVE;
```

You can optionally specify a duration to keep the partition restored:

```sql
ALTER TABLE my_table PARTITION (dt='2024-01-01') RESTORE ARCHIVE WITH DURATION 7 DAYS;
```

### Unarchive Partition

The following SQL moves an archived partition back to standard storage:

```sql
ALTER TABLE my_table PARTITION (dt='2024-01-01') UNARCHIVE;
```

### Examples

Archive old partitions for cost optimization:

```sql
-- Create a partitioned table
CREATE TABLE sales (id INT, amount DOUBLE, dt STRING) 
PARTITIONED BY (dt) USING paimon;

-- Insert data
INSERT INTO sales VALUES (1, 100.0, '2024-01-01');
INSERT INTO sales VALUES (2, 200.0, '2024-01-02');

-- Archive old partition
ALTER TABLE sales PARTITION (dt='2024-01-01') ARCHIVE;

-- Data is still accessible (may require restore first depending on storage tier)
SELECT * FROM sales WHERE dt='2024-01-01';

-- Restore archived partition when needed
ALTER TABLE sales PARTITION (dt='2024-01-01') RESTORE ARCHIVE;

-- Move back to standard storage
ALTER TABLE sales PARTITION (dt='2024-01-01') UNARCHIVE;
```

### Notes

- Archive operations preserve all files in the partition including data files, manifest files, and extra files
- Metadata paths remain unchanged - FileIO implementations handle storage tier changes transparently
- Reading archived data may require restoring the partition first, depending on the storage tier
- Archive operations are distributed and can process large partitions efficiently using Spark
- Supported object stores: S3, OSS (other object stores may be added in future versions)

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

The following SQL changes the type of a nested column `f2` to `BIGINT` in a struct type.

```sql
-- column v previously has type STRUCT<f1: STRING, f2: INT>
ALTER TABLE my_table ALTER COLUMN v.f2 TYPE BIGINT;
```

The following SQL changes the type of a nested column `f2` to `BIGINT` in a struct type, which is the element type of an array type.

```sql
-- column v previously has type ARRAY<STRUCT<f1: STRING, f2: INT>>
ALTER TABLE my_table ALTER COLUMN v.element.f2 TYPE BIGINT;
```

The following SQL changes the type of a nested column `f2` to `BIGINT` in a struct type, which is the value type of a map type.

```sql
-- column v previously has type MAP<INT, STRUCT<f1: STRING, f2: INT>>
ALTER TABLE my_table ALTER COLUMN v.value.f2 TYPE BIGINT;
```


# ALTER DATABASE

The following SQL sets one or more properties in the specified database. If a particular property is already set in the database, override the old value with the new one.

```sql
ALTER { DATABASE | SCHEMA | NAMESPACE } my_database
    SET { DBPROPERTIES | PROPERTIES } ( property_name = property_value [ , ... ] )
```

## Altering Database Location

The following SQL sets the location of the specified database to `file:/temp/my_database.db`.

```sql
ALTER DATABASE my_database SET LOCATION 'file:/temp/my_database.db'
```