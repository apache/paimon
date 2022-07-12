---
title: "Spark"
weight: 3
type: docs
aliases:
- /engines/spark.html
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

# Spark

Table Store supports reading table store tables through Spark.

## Version

Table Store supports Spark 3+. It is highly recommended to use Spark 3+ version with many improvements.

## Install

{{< stable >}}
Download [flink-table-store-spark-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-table-store-spark/{{< version >}}/flink-table-store-spark-{{< version >}}.jar).
{{< /stable >}}
{{< unstable >}}
You are using an unreleased version of Table Store, you need to manually [Build Spark Bundled Jar]({{< ref "docs/engines/build" >}}) from the source code.
{{< /unstable >}}

Copy Table Store Spark bundle jar to `spark/jars`.

## Table Store Catalog

The following command registers the Table Store's Spark catalog with the name `table_store`:

```bash
spark-sql --conf spark.sql.catalog.table_store=org.apache.flink.table.store.spark.SparkCatalog \
    --conf spark.sql.catalog.table_store.warehouse=file:/tmp/warehouse
```

## Create Temporary View

Use the `CREATE TEMPORARY VIEW` command to create a Spark mapping table on top of
an existing Table Store table if you don't want to use Table Store Catalog.

```sql
CREATE TEMPORARY VIEW myTable
USING tablestore
OPTIONS (
  path "file:/tmp/warehouse/default.db/myTable"
)
```

## Query Table

```sql
SELECT * FROM table_store.default.myTable;
```

## DDL

`ALTER TABLE ... SET TBLPROPERTIES`
```sql
ALTER TABLE table_store.default.myTable SET TBLPROPERTIES (
    'write-buffer-size'='256 MB'
)
```

`ALTER TABLE ... UNSET TBLPROPERTIES`
```sql
ALTER TABLE table_store.default.myTable UNSET TBLPROPERTIES ('write-buffer-size')
```

`ALTER TABLE ... ADD COLUMN`
```sql
ALTER TABLE table_store.default.myTable
ADD COLUMNS (new_column STRING)
```

`ALTER TABLE ... ALTER COLUMN ... TYPE`
```sql
ALTER TABLE table_store.default.myTable
ALTER COLUMN column_name TYPE BIGINT
```
