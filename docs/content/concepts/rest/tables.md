---
title: "Tables"
weight: 5
type: docs
aliases:
- /concepts/rest/tables.html
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

# Tables

Paimon supports tables:

1. paimon table: Paimon Data Table with or without Primary key
2. format-table: file format table refers to a directory that contains multiple files of the same format, where
   operations on this table allow for reading or writing to these files, compatible with Hive tables.
3. object table: provides metadata indexes for unstructured data objects in the specified Object Storage directory.

## Paimon Table

### Primary Key Table

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

### Append Table

See [Append Table]({{< ref "append-table/overview" >}}).

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

## Format Table

The Hive tables inside the metastore will be mapped to Paimon's Format Table for computing engines (Spark, Hive, Flink)
to read and write.

Format table refers to a directory that contains multiple files of the same format, where operations on this table
allow for reading or writing to these files, facilitating the retrieval of existing data and the addition of new files.

Partitioned file format table just like the standard hive format. Partitions are discovered and inferred based on
directory structure.

Currently only support `CSV`, `Parquet`, `ORC`, `JSON` formats.

{{< tabs "format-table" >}}
{{< tab "Flink-CSV" >}}

```sql
CREATE TABLE my_csv_table (
    a INT,
    b STRING
) WITH (
    'type'='format-table',
    'file.format'='csv',
    'csv.field-delimiter'=','
)
```
{{< /tab >}}

{{< tab "Spark-CSV" >}}

```sql
CREATE TABLE my_csv_table (
    a INT,
    b STRING
) USING csv OPTIONS ('csv.field-delimiter' ',')
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

{{< tab "Flink-JSON" >}}

```sql
CREATE TABLE my_json_table (
    a INT,
    b STRING
) WITH (
    'type'='format-table',
    'file.format'='json'
)
```
{{< /tab >}}

{{< tab "Spark-JSON" >}}

```sql
CREATE TABLE my_json_table (
    a INT,
    b STRING
) USING json
```

{{< /tab >}}

{{< /tabs >}}

## Object Table

Object Table is a virtual table for unstructured data objects in the specified object storage directory. Users can:

1. Use the virtual file system (Under development) to read and write files.
2. Or use the SQL computing engine to read it as a structured file list.

The object table is managed by Catalog and can also have access permissions. Now, only REST Catalog supports Object
Table.

To Create an object table:

{{< tabs "create-object-table" >}}
{{< tab "Flink-SQL" >}}
```sql
CREATE TABLE `my_object_table` WITH (
  'type' = 'object-table'
);
```
{{< /tab >}}
{{< tab "Spark-SQL" >}}
```sql
CREATE TABLE `my_object_table` TBLPROPERTIES (
  'type' = 'object-table'
);
```
{{< /tab >}}
{{< /tabs >}}

We recommend using [pvfs]({{< ref "concepts/rest/pvfs" >}}). to access files in the object table, access to the files
through the permission system of Paimon REST Catalog.
