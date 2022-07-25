---
title: "Spark2"
weight: 4
type: docs
aliases:
- /engines/spark2.html
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

# Spark2

Table Store supports reading table store tables through Spark.

## Version

Table Store supports Spark 2.4+. It is highly recommended to use Spark 2.4+ version with many improvements.

## Install

{{< stable >}}
Download [flink-table-store-spark2-{{< version >}}.jar](https://www.apache.org/dyn/closer.lua/flink/flink-table-store-{{< version >}}/flink-table-store-spark2-{{< version >}}.jar).
{{< /stable >}}
{{< unstable >}}
You are using an unreleased version of Table Store, you need to manually [Build Spark Bundled Jar]({{< ref "docs/engines/build" >}}) from the source code.
{{< /unstable >}}

Use `--jars` in spark-sql:
```bash
spark-sql ... --jars flink-table-store-spark2-{{< version >}}.jar
```

Alternatively, you can copy `flink-table-store-spark2-{{< version >}}.jar` under `spark/jars` in your Spark installation.

## Create Temporary View

Use the `CREATE TEMPORARY VIEW` command to create a Spark mapping table on top of
an existing Table Store table.

```sql
CREATE TEMPORARY VIEW myTable
USING tablestore
OPTIONS (
  path "file:/tmp/warehouse/default.db/myTable"
)
```

## Query Table

```sql
SELECT * FROM myTable;
```
