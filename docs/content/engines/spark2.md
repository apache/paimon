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

This documentation is a guide for using Paimon in Spark2. 

## Version

Paimon supports Spark 2.4+. It is highly recommended to use Spark 2.4+ version with much improvement.

## Preparing Paimon Jar File

{{< stable >}}

Download [paimon-spark-2-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-spark-2/{{< version >}}/paimon-spark-2-{{< version >}}.jar).

{{< /stable >}}

{{< unstable >}}

Download [paimon-spark-2-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-spark-2/{{< version >}}/).

{{< /unstable >}}

You can also manually build bundled jar from the source code.

To build from source code, [clone the git repository]({{< github_repo >}}).

Build bundled jar with the following command.

```bash
mvn clean install -DskipTests
```

You can find the bundled jar in `./paimon-spark/paimon-spark-2/target/paimon-spark-2-{{< version >}}.jar`.

## Quick Start

{{< hint info >}}

If you are using HDFS, make sure that the environment variable `HADOOP_HOME` or `HADOOP_CONF_DIR` is set.

{{< /hint >}}

**Step 1: Prepare Test Data**

Paimon currently only supports reading tables through Spark2. To create a Paimon table with records, please follow our [Flink quick start guide]({{< ref "engines/flink#quick-start" >}}).

After the guide, all table files should be stored under the path `/tmp/paimon`, or the warehouse path you've specified.

**Step 2: Specify Paimon Jar File**

You can append path to paimon jar file to the `--jars` argument when starting `spark-shell`.

```bash
spark-shell ... --jars /path/to/paimon-spark-2-{{< version >}}.jar
```

Alternatively, you can copy `paimon-spark-2-{{< version >}}.jar` under `spark/jars` in your Spark installation directory.

**Step 3: Query Table**

Paimon with Spark 2.4 does not support DDL. You can use the `Dataset` reader and register the `Dataset` as a temporary table. In spark shell:

```scala
val dataset = spark.read.format("paimon").load("file:/tmp/paimon/default.db/word_count")
dataset.createOrReplaceTempView("word_count")
spark.sql("SELECT * FROM word_count").show()
```
