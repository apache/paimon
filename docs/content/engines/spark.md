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

# Spark3

This documentation is a guide for using Paimon in Spark3.

## Preparation

Paimon currently supports Spark 3.5, 3.4, 3.3, 3.2 and 3.1. We recommend the latest Spark version for a better experience.

Download the jar file with corresponding version.

{{< stable >}}

| Version   | Jar                                                                                                                                                                  |
|-----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Spark 3.5 | [paimon-spark-3.5-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-spark-3.5/{{< version >}}/paimon-spark-3.5-{{< version >}}.jar) |
| Spark 3.4 | [paimon-spark-3.4-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-spark-3.4/{{< version >}}/paimon-spark-3.4-{{< version >}}.jar) |
| Spark 3.3 | [paimon-spark-3.3-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-spark-3.3/{{< version >}}/paimon-spark-3.3-{{< version >}}.jar) |
| Spark 3.2 | [paimon-spark-3.2-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-spark-3.2/{{< version >}}/paimon-spark-3.2-{{< version >}}.jar) |
| Spark 3.1 | [paimon-spark-3.1-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-spark-3.1/{{< version >}}/paimon-spark-3.1-{{< version >}}.jar) |

{{< /stable >}}

{{< unstable >}}

| Version   | Jar                                                                                                                                 |
|-----------|-------------------------------------------------------------------------------------------------------------------------------------|
| Spark 3.5 | [paimon-spark-3.5-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-spark-3.5/{{< version >}}/) |
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
    --conf spark.sql.catalog.paimon.warehouse=file:/tmp/paimon \
    --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions
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
    --conf spark.sql.catalog.spark_catalog=org.apache.paimon.spark.SparkGenericCatalog \
    --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions
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

{{< hint info >}}
Paimon currently supports Spark 3.2+ for SQL write.
{{< /hint >}}

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

{{< hint info >}}
Important table properties setting:
1. Only [primary key table]({{< ref "concepts/primary-key-table" >}}) supports this feature.
2. [MergeEngine]({{< ref "concepts/primary-key-table/merge-engine" >}}) needs to be [deduplicate]({{< ref "concepts/primary-key-table#deduplicate" >}}) or [partial-update]({{< ref "concepts/primary-key-table/merge-engine#partial-update" >}}) to support this feature.
   {{< /hint >}}

{{< hint warning >}}
Warning: we do not support updating primary keys.
{{< /hint >}}

```sql
UPDATE my_table SET v = 'new_value' WHERE id = 1;
```

## Merge Into Table

Paimon currently supports Merge Into syntax in Spark 3+, which allow a set of updates, insertions and deletions based on a source table in a single commit.

{{< hint into >}}
1. This only work with primary-key table.
2. In update clause, to update primary key columns is not supported.
3. `WHEN NOT MATCHED BY SOURCE` syntax is not supported.
{{< /hint >}}

**Example: One**

This is a simple demo that, if a row exists in the target table update it, else insert it.

```sql

-- Here both source and target tables have the same schema: (a INT, b INT, c STRING), and a is a primary key.

MERGE INTO target
USING source
ON target.a = source.a
WHEN MATCHED THEN
UPDATE SET *
WHEN NOT MATCHED
THEN INSERT *

```

**Example: Two**

This is a demo with multiple, conditional clauses.

```sql

-- Here both source and target tables have the same schema: (a INT, b INT, c STRING), and a is a primary key.

MERGE INTO target
USING source
ON target.a = source.a
WHEN MATCHED AND target.a = 5 THEN
   UPDATE SET b = source.b + target.b      -- when matched and meet the condition 1, then update b;
WHEN MATCHED AND source.c > 'c2' THEN
   UPDATE SET *    -- when matched and meet the condition 2, then update all the columns;
WHEN MATCHED THEN
   DELETE      -- when matched, delete this row in target table;
WHEN NOT MATCHED AND c > 'c9' THEN
   INSERT (a, b, c) VALUES (a, b * 1.1, c)      -- when not matched but meet the condition 3, then transform and insert this row;
WHEN NOT MATCHED THEN
INSERT *      -- when not matched, insert this row without any transformation;

```

## Streaming Write

{{< hint info >}}

Paimon currently supports Spark 3+ for streaming write.

Paimon Structured Streaming only supports the two `append` and `complete` modes.

{{< /hint >}}

```scala
// Create a paimon table if not exists.
spark.sql(s"""
           |CREATE TABLE T (k INT, v STRING)
           |TBLPROPERTIES ('primary-key'='a', 'bucket'='3')
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

{{< hint info >}}

Paimon currently supports Spark 3.3+ for streaming read.

{{< /hint >}}

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

Paimon Structured Streaming supports read row in the form of changelog (add rowkind column in row to represent its 
change type) in two ways:

- Direct streaming read with the system audit_log table
- Set `read.changelog` to true (default is false), then streaming read with table location

**Example:**

```scala
// Option 1
val query1 = spark.readStream
  .format("paimon")
  .table("`table_name$audit_log`")
  .writeStream
  .format("console")
  .start()

// Option 2
val query2 = spark.readStream
  .format("paimon")
  .option("read.changelog", "true")
  .load("/path/to/paimon/source/table")
  .writeStream
  .format("console")
  .start()

/*
+I   1  Hi
+I   2  Hello
*/
```

## Schema Evolution

Schema evolution is a feature that allows users to easily modify the current schema of a table to adapt to existing data, or new data that changes over time, while maintaining data integrity and consistency.

Paimon supports automatic schema merging of source data and current table data while data is being written, and uses the merged schema as the latest schema of the table, and it only requires configuring `write.merge-schema`.

```scala
data.write
  .format("paimon")
  .mode("append")
  .option("write.merge-schema", "true")
  .save(location)
```

When enable `write.merge-schema`, Paimon can allow users to perform the following actions on table schema by default:
- Adding columns
- Up-casting the type of column(e.g. Int -> Long)

Paimon also supports explicit type conversions between certain types (e.g. String -> Date, Long -> Int), it requires an explicit configuration `write.merge-schema.explicit-cast`.

Schema evolution can be used in streaming mode at the same time.

```scala
val inputData = MemoryStream[(Int, String)]
inputData
  .toDS()
  .toDF("col1", "col2")
  .writeStream
  .format("paimon")
  .option("checkpointLocation", "/path/to/checkpoint")
  .option("write.merge-schema", "true")
  .option("write.merge-schema.explicit-cast", "true")
  .start(location)
```

Here list the configurations.

<table class="configuration table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 20%">Scan Mode</th>
            <th class="text-left" style="width: 60%">Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>write.merge-schema</h5></td>
            <td>If true, merge the data schema and the table schema automatically before write data.</td>
        </tr>
        <tr>
            <td><h5>write.merge-schema.explicit-cast</h5></td>
            <td>If true, allow to merge data types if the two types meet the rules for explicit casting.</td>
        </tr>
    </tbody>
</table>

## Spark Procedure

This section introduce all available spark procedures about paimon.
s
<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 4%">Procedure Name</th>
      <th class="text-left" style="width: 20%">Explaination</th>
      <th class="text-left" style="width: 4%">Example</th>
    </tr>
    </thead>
    <tbody style="font-size: 12px; ">
    <tr>
      <td>compact</td>
      <td>identifier: the target table identifier. Cannot be empty.<br><br><nobr>partitions: partition filter. Left empty for all partitions.<br> "," means "AND"<br>";" means "OR"</nobr><br><br>order_strategy: 'order' or 'zorder' or 'hilbert' or 'none'. Left empty for 'none'. <br><br><nobr>order_columns: the columns need to be sort. Left empty if 'order_strategy' is 'none'. </nobr><br><br>If you want sort compact two partitions date=01 and date=02, you need to write 'date=01;date=02'<br><br>If you want sort one partition with date=01 and day=01, you need to write 'date=01,day=01'</td>
      <td><nobr>SET spark.sql.shuffle.partitions=10; --set the compact parallelism</nobr><br><nobr>CALL sys.compact(table => 'T', partitions => 'p=0',  order_strategy => 'zorder', order_by => 'a,b')</nobr></td>
    </tr>
    <tr>
      <td>expire_snapshots</td>
      <td>
         To expire snapshots. Argument:
            <li>table: the target table identifier. Cannot be empty.</li>
            <li>retain_max: the maximum number of completed snapshots to retain.</li>
            <li>retain_min: the minimum number of completed snapshots to retain.</li>
            <li>older_than: timestamp before which snapshots will be removed.</li>
            <li>max_deletes: the maximum number of snapshots that can be deleted at once.</li>
      </td>
      <td>CALL sys.expire_snapshots(table => 'default.T', retain_max => 10)</td>
    </tr>
    </tbody>
</table>

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

## Spark 2

Paimon supports Spark 2.4+. We highly recommend using versions above Spark3, as Spark2 only provides reading capabilities.

{{< stable >}}

Download [paimon-spark-2-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-spark-2/{{< version >}}/paimon-spark-2-{{< version >}}.jar).

{{< /stable >}}

{{< unstable >}}

Download [paimon-spark-2-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-spark-2/{{< version >}}/).

{{< /unstable >}}

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
