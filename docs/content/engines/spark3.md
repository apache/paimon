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

This documentation is a guide for using Paimon in Spark3.

## Preparation

Paimon currently supports Spark 3.4, 3.3, 3.2 and 3.1. We recommend the latest Spark version for a better experience.

Download the jar file with corresponding version.

{{< stable >}}

| Version   | Jar                                                                                                                                                                  |
|-----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Spark 3.4 | [paimon-spark-3.4-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-spark-3.4/{{< version >}}/paimon-spark-3.4-{{< version >}}.jar) |
| Spark 3.3 | [paimon-spark-3.3-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-spark-3.3/{{< version >}}/paimon-spark-3.3-{{< version >}}.jar) |
| Spark 3.2 | [paimon-spark-3.2-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-spark-3.2/{{< version >}}/paimon-spark-3.2-{{< version >}}.jar) |
| Spark 3.1 | [paimon-spark-3.1-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-spark-3.1/{{< version >}}/paimon-spark-3.1-{{< version >}}.jar) |

{{< /stable >}}

{{< unstable >}}

| Version   | Jar                                                                                                                                 |
|-----------|-------------------------------------------------------------------------------------------------------------------------------------|
| Spark 3.4 | [paimon-spark-3.4-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-spark-3.4/{{< version >}}/) |
| Spark 3.3 | [paimon-spark-3.3-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-spark-3.3/{{< version >}}/) |
| Spark 3.2 | [paimon-spark-3.2-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-spark-3.2/{{< version >}}/) |
| Spark 3.1 | [paimon-spark-3.1-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-spark-3.1/{{< version >}}/) |

{{< /unstable >}}

You can also manually build bundled jar from the source code.

To build from source code, [clone the git repository]({{< github_repo >}}).

Build bundled jar with the following command.

```bash
mvn clean install -DskipTests
```

For Spark 3.3, you can find the bundled jar in `./paimon-spark/paimon-spark-3.3/target/paimon-spark-3.3-{{< version >}}.jar`.

## Setup

{{< hint info >}}

If you are using HDFS, make sure that the environment variable `HADOOP_HOME` or `HADOOP_CONF_DIR` is set.

{{< /hint >}}

**Step 1: Specify Paimon Jar File**

Append path to paimon jar file to the `--jars` argument when starting `spark-sql`.

```bash
spark-sql ... --jars /path/to/paimon-spark-3.3-{{< version >}}.jar
```

Alternatively, you can copy `paimon-spark-3.3-{{< version >}}.jar` under `spark/jars` in your Spark installation directory.

**Step 2: Specify Paimon Catalog**

{{< tabs "Specify Paimon Catalog" >}}

{{< tab "Catalog" >}}

When starting `spark-sql`, use the following command to register Paimon’s Spark catalog with the name `paimon`. Table files of the warehouse is stored under `/tmp/paimon`.

```bash
spark-sql ... \
    --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
    --conf spark.sql.catalog.paimon.warehouse=file:/tmp/paimon
```

Catalogs are configured using properties under spark.sql.catalog.(catalog_name). In above case, 'paimon' is the
catalog name, you can change it to your own favorite catalog name.

After `spark-sql` command line has started, run the following SQL to create and switch to database `default`.

```sql
USE paimon;
USE default;
```

After switching to the catalog (`'USE paimon'`), Spark's existing tables will not be directly accessible, you
can use the `spark_catalog.${database_name}.${table_name}` to access Spark tables.

{{< /tab >}}

{{< tab "Generic Catalog" >}}

When starting `spark-sql`, use the following command to register Paimon’s Spark Generic catalog to replace Spark
default catalog `spark_catalog`. (default warehouse is Spark `spark.sql.warehouse.dir`)

Currently, it is only recommended to use `SparkGenericCatalog` in the case of Hive metastore, Paimon will infer
Hive conf from Spark session, you just need to configure Spark's Hive conf.

```bash
spark-sql ... \
    --conf spark.sql.catalog.spark_catalog=org.apache.paimon.spark.SparkGenericCatalog
```

Using `SparkGenericCatalog`, you can use Paimon tables in this Catalog or non-Paimon tables such as Spark's csv,
parquet, Hive tables, etc.

{{< /tab >}}

{{< /tabs >}}

## Create Table

{{< tabs "Create Paimon Table" >}}

{{< tab "Catalog" >}}

```sql
create table my_table (
    k int,
    v string
) tblproperties (
    'primary-key' = 'k'
);
```

{{< /tab >}}

{{< tab "Generic Catalog" >}}

```sql
create table my_table (
    k int,
    v string
) USING paimon
tblproperties (
    'primary-key' = 'k'
) ;

```

{{< /tab >}}

{{< /tabs >}}

## Insert Table

```sql
INSERT INTO my_table VALUES (1, 'Hi'), (2, 'Hello');
```

## Query Table

{{< tabs "Query Paimon Table" >}}

{{< tab "SQL" >}}

```sql
SELECT * FROM my_table;

/*
1	Hi
2	Hello
*/
```

{{< /tab >}}

{{< tab "DataFrame" >}}

```scala
val dataset = spark.read.format("paimon").load("file:/tmp/paimon/default.db/my_table")
dataset.show()

/*
+---+------+
| k |     v|
+---+------+
|  1|    Hi|
|  2| Hello|
+---+------+
*/
```

{{< /tab >}}

{{< /tabs >}}


## Update Table

For now, Paimon does not support `UPDATE` syntax. But we can use `INSERT INTO` syntax instead for changelog tables.

```sql
INSERT INTO my_table VALUES (1, 'Hi Again'), (3, 'Test');

SELECT * FROM my_table;

/*
1	Hi Again
2	Hello
3	Test
*/
```

## Streaming Write

{{< hint info >}}

Paimon Structured Streaming only supports the two `append` and `complete` modes.

{{< /hint >}}

```scala
// Create a paimon table if not exists.
spark.sql(s"""
           |CREATE TABLE T (k INT, v STRING)
           |TBLPROPERTIES ('primary-key'='a', 'write-mode'='change-log', 'bucket'='3')
           |""".stripMargin)

// Here we use MemoryStream to fake a streaming source.
val inputData = MemoryStream[(Int, String)]
val df = inputData.toDS().toDF("k", "v")

// Streaming Write to paimon table.
val stream = df
  .writeStream
  .outputMode("append")
  .option("checkpointLocation", "/path/to/checkpoint")
  .format("paimon")
  .start("/path/to/paimon/sink/table")
```

## Streaming Read

Paimon supports rich scan mode for streaming read. There is a list:
<table class="configuration table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 20%">Scan Mode</th>
            <th class="text-left" style="width: 60%">Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>latest</h5></td>
            <td>For streaming sources, continuously reads latest changes without producing a snapshot at the beginning. </td>
        </tr>
        <tr>
            <td><h5>latest-full</h5></td>
            <td>For streaming sources, produces the latest snapshot on the table upon first startup, and continue to read the latest changes.</td>
        </tr>
        <tr>
            <td><h5>from-timestamp</h5></td>
            <td>For streaming sources, continuously reads changes starting from timestamp specified by "scan.timestamp-millis", without producing a snapshot at the beginning. </td>
        </tr>
        <tr>
            <td><h5>from-snapshot</h5></td>
            <td>For streaming sources, continuously reads changes starting from snapshot specified by "scan.snapshot-id", without producing a snapshot at the beginning. </td>
        </tr>
        <tr>
            <td><h5>from-snapshot-full</h5></td>
            <td>For streaming sources, produces from snapshot specified by "scan.snapshot-id" on the table upon first startup, and continuously reads changes.</td>
        </tr>
        <tr>
            <td><h5>default</h5></td>
            <td>It is equivalent to from-snapshot if "scan.snapshot-id" is specified. It is equivalent to from-timestamp if "timestamp-millis" is specified. Or, It is equivalent to latest-full.</td>
        </tr>
    </tbody>
</table>

A simple example with default scan mode:

```scala
// no any scan-related configs are provided, that will use latest-full scan mode.
val query = spark.readStream
  .format("paimon")
  .load("/path/to/paimon/source/table")
  .writeStream
  .format("console")
  .start()
```

Paimon Structured Streaming also supports a variety of streaming read modes, it can support many triggers and many read limits.

These read limits are supported:

<table class="configuration table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 20%">Key</th>
            <th class="text-left" style="width: 15%">Default</th>
            <th class="text-left" style="width: 10%">Type</th>
            <th class="text-left" style="width: 55%">Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>read.stream.maxFilesPerTrigger</h5></td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>Integer</td>
            <td>The maximum number of files returned in a single batch.</td>
        </tr>
        <tr>
            <td><h5>read.stream.maxBytesPerTrigger</h5></td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>Long</td>
            <td>The maximum number of bytes returned in a single batch.</td>
        </tr>
        <tr>
            <td><h5>read.stream.maxRowsPerTrigger</h5></td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>Long</td>
            <td>The maximum number of rows returned in a single batch.</td>
        </tr>
        <tr>
            <td><h5>read.stream.minRowsPerTrigger</h5></td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>Long</td>
            <td>The minimum number of rows returned in a single batch, which used to create MinRowsReadLimit with read.stream.maxTriggerDelayMs together.</td>
        </tr>
        <tr>
            <td><h5>read.stream.maxTriggerDelayMs</h5></td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>Long</td>
            <td>The maximum delay between two adjacent batches, which used to create MinRowsReadLimit with read.stream.minRowsPerTrigger together.</td>
        </tr>
    </tbody>
</table>

**Example: One**

Use `org.apache.spark.sql.streaming.Trigger.AvailableNow()` and `maxBytesPerTrigger` defined by paimon.

```scala
// Trigger.AvailableNow()) processes all available data at the start
// of the query in one or multiple batches, then terminates the query.
// That set read.stream.maxBytesPerTrigger to 128M means that each
// batch processes a maximum of 128 MB of data.
val query = spark.readStream
  .format("paimon")
  .option("read.stream.maxBytesPerTrigger", "134217728")
  .load("/path/to/paimon/source/table")
  .writeStream
  .format("console")
  .trigger(Trigger.AvailableNow())
  .start()
```

**Example: Two**

Use `org.apache.spark.sql.connector.read.streaming.ReadMinRows`.

```scala
// It will not trigger a batch until there are more than 5,000 pieces of data,
// unless the interval between the two batches is more than 300 seconds.
val query = spark.readStream
  .format("paimon")
  .option("read.stream.minRowsPerTrigger", "5000")
  .option("read.stream.maxTriggerDelayMs", "300000")
  .load("/path/to/paimon/source/table")
  .writeStream
  .format("console")
  .start()
```

## Spark Type Conversion

This section lists all supported type conversion between Spark and Paimon.
All Spark's data types are available in package `org.apache.spark.sql.types`.

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 10%">Spark Data Type</th>
      <th class="text-left" style="width: 10%">Paimon Data Type</th>
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
- Conversion between Spark's `UserDefinedType` and Paimon's `UserDefinedType` is not supported.

{{< /hint >}}
