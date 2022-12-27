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

This documentation is a guide for using Table Store in Spark2. 

## Version

Table Store supports Spark 2.4+. It is highly recommended to use Spark 2.4+ version with many improvements.

## Preparing Table Store Jar File

{{< stable >}}

Download [flink-table-store-spark2-{{< version >}}.jar](https://www.apache.org/dyn/closer.lua/flink/flink-table-store-{{< version >}}/flink-table-store-spark2-{{< version >}}.jar).

You can also manually build bundled jar from the source code.

{{< /stable >}}

{{< unstable >}}

You are using an unreleased version of Table Store so you need to manually build bundled jar from the source code.

{{< /unstable >}}

To build from source code, either [download the source of a release](https://flink.apache.org/downloads.html) or [clone the git repository]({{< github_repo >}}).

Build bundled jar with the following command.

```bash
mvn clean install -DskipTests
```

You can find the bundled jar in `./flink-table-store-spark2/target/flink-table-store-spark2-{{< version >}}.jar`.

## Quick Start

{{< hint info >}}

If you are using HDFS, make sure that the environment variable `HADOOP_HOME` or `HADOOP_CONF_DIR` is set.

{{< /hint >}}

**Step 1: Prepare Test Data**

Table Store currently only supports reading tables through Spark2. To create a Table Store table with records, please follow our [Flink quick start guide]({{< ref "docs/engines/flink#quick-start" >}}).

After the guide, all table files should be stored under the path `/tmp/table_store`, or the warehouse path you've specified.

**Step 2: Specify Table Store Jar File**

You can append path to table store jar file to the `--jars` argument when starting `spark-shell`.

```bash
spark-shell ... --jars /path/to/flink-table-store-spark2-{{< version >}}.jar
```

Alternatively, you can copy `flink-table-store-spark2-{{< version >}}.jar` under `spark/jars` in your Spark installation directory.

**Step 3: Query Table**

Table store with Spark 2.4 does not support DDL. You can use the `Dataset` reader and register the `Dataset` as a temporary table. In spark shell:

```scala
val dataset = spark.read.format("tablestore").load("file:/tmp/table_store/default.db/word_count")
dataset.createOrReplaceTempView("word_count")
spark.sql("SELECT * FROM word_count").show()
```
