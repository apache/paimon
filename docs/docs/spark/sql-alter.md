---
title: "Alter Tables"
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

# Alter Tables

Change table properties and schemas with explicit DDL. For automatic schema changes during
a write, see [Schema Evolution on Write](./schema-evolution). For default expressions, see
[Default Values](./default-value).

| Change | Guide |
| --- | --- |
| Properties and comments | [Set properties](#changingadding-table-properties), [unset properties](#removing-table-properties), [comments](#changingadding-table-comment) |
| Table identity | [Rename a table](#rename-table-name) |
| Schema | [Add](#adding-new-columns), [rename](#renaming-column-name), [drop](#dropping-columns), [reorder](#changing-column-position), or [change types](#changing-column-type) |
| Data partitions | [Drop partitions](#dropping-partitions), or [manage Format Table partitions](./format-table) |
| Database metadata | [Alter a database](#alter-database) |

Examples are independent: run the form that matches your existing table schema. For nested
fields, `v.f1` addresses a struct field, `v.element.f1` an array element's struct field, and
`v.value.f1` a map value's struct field.

## Set Table Properties {#changingadding-table-properties}

The following SQL sets `write-buffer-size` table property to `256 MB`.

```sql
ALTER TABLE my_table SET TBLPROPERTIES (
    'write-buffer-size' = '256 MB'
);
```

## Unset Table Properties {#removing-table-properties}

The following SQL removes `write-buffer-size` table property.

```sql
ALTER TABLE my_table UNSET TBLPROPERTIES ('write-buffer-size');
```

## Set a Table Comment {#changingadding-table-comment}

The following SQL changes comment of table `my_table` to `table comment`.

```sql
ALTER TABLE my_table SET TBLPROPERTIES (
    'comment' = 'table comment'
);
```

## Remove a Table Comment {#removing-table-comment}

The following SQL removes table comment.

```sql
ALTER TABLE my_table UNSET TBLPROPERTIES ('comment');
```

## Rename a Table {#rename-table-name}

Rename a table within the current catalog:
```sql
ALTER TABLE my_table RENAME TO my_table_new;
```

The source may be catalog-qualified, but the destination must not include a catalog name:

```sql
ALTER TABLE paimon.default.my_table RENAME TO default.my_table_new;
```

A destination such as `paimon.default.my_table_new` is rejected. This operation does not move a
table between catalogs.

:::info

If you use object storage without REST Catalog, such as S3 or OSS, please use this syntax carefully, because the renaming of object storage is not atomic, and only partial files may be moved in case of failure.

:::

## Add Columns {#adding-new-columns}

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

## Rename Columns {#renaming-column-name}

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

## Drop Columns {#dropping-columns}

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

:::warning

When using a `hive` catalog, this operation requires `hive.metastore.disallow.incompatible.col.type.changes=false`
to be set on the **Hive Metastore server** (in its `hive-site.xml`, then restart HMS). Setting this key via
`--conf spark.hadoop.hive.metastore.disallow.incompatible.col.type.changes=false` only configures the *client-side*
`HiveConf`; the value is **not** propagated to the remote Hive Metastore service over Thrift, so setting it on
the client has no effect.

See [HIVE-17832](https://issues.apache.org/jira/browse/HIVE-17832) for the historical discussion.

:::

Otherwise, the operation can fail with `The following columns have types incompatible with the
existing columns in their respective positions`.

## Drop Partitions {#dropping-partitions}

For a Paimon snapshot table, supply every partition column. For example, on a table partitioned
by `(id, name)`:

```sql
ALTER TABLE my_table DROP PARTITION (`id` = 1, `name` = 'paimon');
```

## Set a Column Comment {#changing-column-comment}

The following SQL changes comment of column `buy_count` to `buy count`.

```sql
ALTER TABLE my_table ALTER COLUMN buy_count COMMENT 'buy count';
```

## Choose a New Column Position {#adding-column-position}

```sql
ALTER TABLE my_table ADD COLUMN c INT FIRST;

ALTER TABLE my_table ADD COLUMN c INT AFTER b;
```

## Reorder Columns {#changing-column-position}

```sql
ALTER TABLE my_table ALTER COLUMN col_a FIRST;

ALTER TABLE my_table ALTER COLUMN col_a AFTER col_b;
```

## Change Column Types {#changing-column-type}

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

## Alter a Database {#alter-database}

Set database properties; an existing value for the same key is replaced. `SCHEMA` and
`NAMESPACE` are aliases for `DATABASE` in this syntax.

```sql
ALTER DATABASE my_database SET DBPROPERTIES ('owner' = 'analytics');
```

### Altering Database Location

The following SQL sets the location of the specified database to `file:/temp/my_database.db`.

```sql
ALTER DATABASE my_database SET LOCATION 'file:/temp/my_database.db';
```
