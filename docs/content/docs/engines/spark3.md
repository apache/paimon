---
title: "Spark3"
weight: 3
type: docs
aliases:
- /engines/spark3.html
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

# Spark3

This documentation is a guide for using Table Store in Spark3.

## Preparing Table Store Jar File

{{< stable >}}

Table Store currently supports Spark 3.3, 3.2 and 3.1. We recommend the latest Spark version for a better experience.

Download [flink-table-store-spark-{{< version >}}.jar](https://www.apache.org/dyn/closer.lua/flink/flink-table-store-{{< version >}}/flink-table-store-spark-{{< version >}}.jar).

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

For Spark 3.3, you can find the bundled jar in `./flink-table-store-spark/flink-table-store-spark-3.3/target/flink-table-store-spark-3.3-{{< version >}}.jar`.

## Quick Start

{{< hint info >}}

If you are using HDFS, make sure that the environment variable `HADOOP_HOME` or `HADOOP_CONF_DIR` is set.

{{< /hint >}}

**Step 1: Specify Table Store Jar File**

Append path to table store jar file to the `--jars` argument when starting `spark-sql`.

```bash
spark-sql ... --jars /path/to/flink-table-store-spark-{{< version >}}.jar
```

Alternatively, you can copy `flink-table-store-spark-{{< version >}}.jar` under `spark/jars` in your Spark installation directory.

**Step 2: Specify Table Store Catalog**

When starting `spark-sql`, use the following command to register Table Store’s Spark catalog with the name `tablestore`. Table files of the warehouse is stored under `/tmp/table_store`.

```bash
spark-sql ... \
    --conf spark.sql.catalog.tablestore=org.apache.flink.table.store.spark.SparkCatalog \
    --conf spark.sql.catalog.tablestore.warehouse=file:/tmp/table_store
```

After `spark-sql` command line has started, run the following SQL to create and switch to database `tablestore.default`.

```sql
CREATE DATABASE tablestore.default;
USE tablestore.default;
```

**Step 3: Create a table and Write Some Records**

```sql
create table my_table (
    k int,
    v string
) tblproperties (
    'primary-key' = 'k'
);

INSERT INTO my_table VALUES (1, 'Hi'), (2, 'Hello');
```

**Step 4: Query Table with SQL**

```sql
SELECT * FROM my_table;
/*
1	Hi
2	Hello
*/
```

**Step 5: Update the Records**

```sql
INSERT INTO my_table VALUES (1, 'Hi Again'), (3, 'Test');

SELECT * FROM my_table;
/*
1	Hi Again
2	Hello
3	Test
*/
```

**Step 6: Query Table with Scala API**

If you don't want to use Table Store catalog, you can also run `spark-shell` and query the table with Scala API.

```bash
spark-shell ... --jars /path/to/flink-table-store-spark-{{< version >}}.jar
```

```scala
val dataset = spark.read.format("tablestore").load("file:/tmp/table_store/default.db/my_table")
dataset.createOrReplaceTempView("my_table")
spark.sql("SELECT * FROM my_table").show()
```

## Spark Type Conversion

This section lists all supported type conversion between Spark and Flink.
All Spark's data types are available in package `org.apache.spark.sql.types`.

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 10%">Spark Data Type</th>
      <th class="text-left" style="width: 10%">Flink Data Type</th>
      <th class="text-left" style="width: 5%">Atomic Type</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>StructType</code></td>
      <td><code>RowType</code></td>
      <td>false</td>
    </tr>
    <tr>
      <td><code>MapType</code></td>
      <td><code>MapType</code></td>
      <td>false</td>
    </tr>
    <tr>
      <td><code>ArrayType</code></td>
      <td><code>ArrayType</code></td>
      <td>false</td>
    </tr>
    <tr>
      <td><code>BooleanType</code></td>
      <td><code>BooleanType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>ByteType</code></td>
      <td><code>TinyIntType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>ShortType</code></td>
      <td><code>SmallIntType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>IntegerType</code></td>
      <td><code>IntType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>LongType</code></td>
      <td><code>BigIntType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>FloatType</code></td>
      <td><code>FloatType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>DoubleType</code></td>
      <td><code>DoubleType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>StringType</code></td>
      <td><code>VarCharType</code>, <code>CharType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>DateType</code></td>
      <td><code>DateType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>TimestampType</code></td>
      <td><code>TimestampType</code>, <code>LocalZonedTimestamp</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>DecimalType(precision, scale)</code></td>
      <td><code>DecimalType(precision, scale)</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>BinaryType</code></td>
      <td><code>VarBinaryType</code>, <code>BinaryType</code></td>
      <td>true</td>
    </tr>
    </tbody>
</table>

{{< hint info >}}

- Currently, Spark's field comment cannot be described under Flink CLI.
- Conversion between Spark's `UserDefinedType` and Flink's `UserDefinedType` is not supported.

{{< /hint >}}
