---
title: "Table Types"
weight: 5
type: docs
aliases:
- /concepts/table-types.html
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

# Table Types

Paimon supports table types:

1. table with pk: Paimon Data Table with Primary key
2. table w/o pk: Paimon Data Table without Primary key
3. view: metastore required, views in SQL are a kind of virtual table
4. format-table: file format table refers to a directory that contains multiple files of the same format, where
   operations on this table allow for reading or writing to these files, compatible with Hive tables
5. materialized-table: aimed at simplifying both batch and stream data pipelines, providing a consistent development
   experience, see [Flink Materialized Table](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/materialized-table/overview/)

## Table with PK

See [Paimon with Primary key]({{< ref "primary-key-table/overview" >}}).

Primary keys consist of a set of columns that contain unique values for each record. Paimon enforces data ordering by
sorting the primary key within each bucket, allowing streaming update and streaming changelog read.

The definition of primary key is similar to that of standard SQL, as it ensures that there is only one data entry for
the same primary key during batch queries.

{{< tabs "primary-table" >}}
{{< tab "Flink SQL" >}}

```sql
CREATE TABLE my_table (
    a INT PRIMARY KEY NOT ENFORCED,
    b STRING
) WITH (
    'bucket'='8'
)
```
{{< /tab >}}

{{< tab "Spark SQL" >}}

```sql
CREATE TABLE my_table (
    a INT,
    b STRING
) TBLPROPERTIES (
    'primary-key' = 'a',
    'bucket' = '8'
)
```

{{< /tab >}}
{{< /tabs >}}

## Table w/o PK

See [Paimon w/o Primary key]({{< ref "append-table/overview" >}}).

If a table does not have a primary key defined, it is an append table. Compared to the primary key table, it does not
have the ability to directly receive changelogs. It cannot be directly updated with data through streaming upsert. It 
can only receive incoming data from append data.

However, it also supports batch sql: DELETE, UPDATE, and MERGE-INTO.

```sql
CREATE TABLE my_table (
    a INT,
    b STRING
)
```

## View

View is supported when the metastore can support view, for example, hive metastore.

View will currently save the original SQL. If you need to use View across engines, you can write a cross engine
SQL statement. For example:

```sql
CREATE VIEW my_view AS SELECT a + 1, b FROM my_db.my_source;
```

## Format Table

Format table is supported when the metastore can support format table, for example, hive metastore. The Hive tables
inside the metastore will be mapped to Paimon's Format Table for computing engines (Spark, Hive, Flink) to read and write.

Format table refers to a directory that contains multiple files of the same format, where operations on this table
allow for reading or writing to these files, facilitating the retrieval of existing data and the addition of new files.

Partitioned file format table just like the standard hive format. Partitions are discovered and inferred based on
directory structure.

Format Table is enabled by default, you can disable it by configuring Catalog option: `'format-table.enabled'`.

Currently only support `CSV`, `Parquet`, `ORC` formats.

{{< tabs "format-table" >}}
{{< tab "Flink-CSV" >}}

```sql
CREATE TABLE my_csv_table (
    a INT,
    b STRING
) WITH (
    'type'='format-table',
    'file.format'='csv',
    'field-delimiter'=','
)
```
{{< /tab >}}

{{< tab "Spark-CSV" >}}

```sql
CREATE TABLE my_csv_table (
    a INT,
    b STRING
) USING csv OPTIONS ('field-delimiter' ',')
```

{{< /tab >}}

{{< tab "Flink-Parquet" >}}

```sql
CREATE TABLE my_parquet_table (
    a INT,
    b STRING
) WITH (
    'type'='format-table',
    'file.format'='parquet'
)
```
{{< /tab >}}

{{< tab "Spark-Parquet" >}}

```sql
CREATE TABLE my_parquet_table (
    a INT,
    b STRING
) USING parquet
```

{{< /tab >}}

{{< /tabs >}}

## Object Table

Object Table provides metadata indexes for unstructured data objects in the specified Object Storage storage directory.
Object tables allow users to analyze unstructured data in Object Storage:

1. Use Python API to manipulate these unstructured data, such as converting images to PDF format.
2. Model functions can also be used to perform inference, and then the results of these operations can be concatenated
   with other structured data in the Catalog.

The object table is managed by Catalog and can also have access permissions and the ability to manage blood relations.

{{< tabs "object-table" >}}

{{< tab "Flink-SQL" >}}

```sql
-- Create Object Table

CREATE TABLE `my_object_table` WITH (
  'type' = 'object-table',
  'object-location' = 'oss://my_bucket/my_location' 
);

-- Refresh Object Table

CALL sys.refresh_object_table('mydb.my_object_table');

-- Query Object Table

SELECT * FROM `my_object_table`;

-- Query Object Table with Time Travel

SELECT * FROM `my_object_table` /*+ OPTIONS('scan.snapshot-id' = '1') */;
```

{{< /tab >}}

{{< tab "Spark-SQL" >}}

```sql
-- Create Object Table

CREATE TABLE `my_object_table` TBLPROPERTIES (
  'type' = 'object-table',
  'object-location' = 'oss://my_bucket/my_location' 
);

-- Refresh Object Table

CALL sys.refresh_object_table('mydb.my_object_table');

-- Query Object Table

SELECT * FROM `my_object_table`;

-- Query Object Table with Time Travel

SELECT * FROM `my_object_table` VERSION AS OF 1;
```

{{< /tab >}}

{{< /tabs >}}

## Materialized Table

Materialized Table aimed at simplifying both batch and stream data pipelines, providing a consistent development
experience, see [Flink Materialized Table](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/materialized-table/overview/).

Now only Flink SQL integrate to Materialized Table, we plan to support it in Spark SQL too.

```sql
CREATE MATERIALIZED TABLE continuous_users_shops
PARTITIONED BY (ds)
FRESHNESS = INTERVAL '30' SECOND
AS SELECT
  user_id,
  ds,
  SUM (payment_amount_cents) AS payed_buy_fee_sum,
  SUM (1) AS PV
FROM (
  SELECT user_id, order_created_at AS ds, payment_amount_cents
    FROM json_source
  ) AS tmp
GROUP BY user_id, ds;
```
