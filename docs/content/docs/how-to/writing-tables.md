---
title: "Writing Tables"
weight: 4
type: docs
aliases:
- /how-to/writing-tables.html
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

# Writing Tables

You can use the `INSERT` statement to inserts new rows into a table 
or overwrites the existing data in the table. The inserted rows can 
be specified by value expressions or result from a query.

## Syntax

```sql
INSERT { INTO | OVERWRITE } table_identifier [ part_spec ] [ column_list ] { value_expr | query }
```
- part_spec

    An optional parameter that specifies a comma-separated list of key and value pairs for partitions. 
    Note that one can use a typed literal (e.g., date’2019-01-02’) in the partition spec.

    Syntax: PARTITION ( partition_col_name = partition_col_val [ , ... ] )

- column_list

    An optional parameter that specifies a comma-separated list of columns belonging to the 
    table_identifier table.
    
    Syntax: (col_name1 [, column_name2, ...])
    
    {{< hint info >}}

    All specified columns should exist in the table and not be duplicated from each other.
    It includes all columns except the static partition columns.
      
    The size of the column list should be exactly the size of the data from VALUES clause or query.
    
    {{< /hint >}}

- value_expr

    Specifies the values to be inserted. Either an explicitly specified value or a NULL can be 
    inserted. A comma must be used to separate each value in the clause. More than one set of 
    values can be specified to insert multiple rows.

    Syntax: VALUES ( { value | NULL } [ , … ] ) [ , ( … ) ]

    {{< hint info >}}

    Currently, Flink doesn't support use NULL directly, so the NULL should be cast to actual 
    data type by `CAST (NULL AS data_type)`.

    {{< /hint >}}

For more information, please check the syntax document:

[Flink INSERT Statement](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/table/sql/insert/)

[Spark INSERT Statement](https://spark.apache.org/docs/latest/sql-ref-syntax-dml-insert-table.html)

## Applying Records/Changes to Tables

{{< tabs "insert-into-example" >}}

{{< tab "Flink" >}}

Use `INSERT INTO` to apply records and changes to tables.

```sql
INSERT INTO MyTable SELECT ...
```

{{< /tab >}}

{{< tab "Spark3" >}}

Use `INSERT INTO` to apply records and changes to tables.

```sql
INSERT INTO MyTable SELECT ...
```

{{< /tab >}}

{{< /tabs >}}

## Overwriting the Whole Table

For unpartitioned tables, Table Store supports overwriting the whole table.

{{< tabs "insert-overwrite-unpartitioned-example" >}}

{{< tab "Flink" >}}

Use `INSERT OVERWRITE` to overwrite the whole unpartitioned table.

```sql
INSERT OVERWRITE MyTable SELECT ...
```

{{< /tab >}}

{{< /tabs >}}

## Overwriting a Partition

For partitioned tables, Table Store supports overwriting a partition.

{{< tabs "insert-overwrite-partitioned-example" >}}

{{< tab "Flink" >}}

Use `INSERT OVERWRITE` to overwrite a partition.

```sql
INSERT OVERWRITE MyTable PARTITION (key1 = value1, key2 = value2, ...) SELECT ...
```

{{< /tab >}}

{{< /tabs >}}

## Purging tables

You can use `INSERT OVERWRITE` to purge tables by inserting empty value.

{{< tabs "purge-tables-syntax" >}}

{{< tab "Flink" >}}

```sql
INSERT OVERWRITE MyTable SELECT * FROM MyTable WHERE false
```

{{< /tab >}}

{{< /tabs >}}

## Purging Partitions

Currently, Table Store supports two ways to purge partitions.

1. Like purging tables, you can use `INSERT OVERWRITE` to purge data of partitions by inserting empty value to them.

2. Method #1 dose not support to drop multiple partitions. In case that you need to drop multiple partitions, you can submit the drop-partition job through `flink run`.

{{< tabs "purge-partitions-syntax" >}}

{{< tab "Flink" >}}

```sql
-- Syntax
INSERT OVERWRITE MyTable PARTITION (key1 = value1, key2 = value2, ...) SELECT selectSpec FROM MyTable WHERE false

-- The following SQL is an example:
-- table definition
CREATE TABLE MyTable (
    k0 INT,
    k1 INT,
    v STRING
) PARTITIONED BY (k0, k1);

-- you can use
INSERT OVERWRITE MyTable PARTITION (k0 = 0) SELECT k1, v FROM MyTable WHERE false

-- or
INSERT OVERWRITE MyTable PARTITION (k0 = 0, k1 = 0) SELECT v FROM MyTable WHERE false
```

{{< /tab >}}

{{< tab "Flink Job" >}}

Run the following command to submit a drop-partition job for the table.

```bash
<FLINK_HOME>/bin/flink run \
    -c org.apache.flink.table.store.connector.action.FlinkActions \
    /path/to/flink-table-store-dist-{{< version >}}.jar \
    drop-partition \
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table <table-name>
    --partition <partition_spec>
    [--partition <partition_spec> ...]

partition_spec:
key1=value1,key2=value2...
```

For more information of drop-partition, see

```bash
<FLINK_HOME>/bin/flink run \
    -c org.apache.flink.table.store.connector.action.FlinkActions \
    /path/to/flink-table-store-dist-{{< version >}}.jar \
    drop-partition --help
```

{{< /tab >}}

{{< /tabs >}}
