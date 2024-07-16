---
title: "Quick Start"
weight: 1
type: docs
aliases:
- /spark/quick-start.html
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

# Quick Start

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

OR use the `--packages` option.

```bash
spark-sql ... --packages org.apache.paimon:paimon-spark-3.3:{{< version >}}
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
      <td><code>VarCharType(Integer.MAX_VALUE)</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>VarCharType(length)</code></td>
      <td><code>VarCharType(length)</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>CharType(length)</code></td>
      <td><code>CharType(length)</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>DateType</code></td>
      <td><code>DateType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>TimestampType</code></td>
      <td><code>LocalZonedTimestamp</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>TimestampNTZType(Spark3.4+)</code></td>
      <td><code>TimestampType</code></td>
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

{{< hint warning >}}
Due to the previous design, in Spark3.3 and below, Paimon will map both Paimon's TimestampType and LocalZonedTimestamp to Spark's TimestampType, and only correctly handle with TimestampType.

Therefore, when using Spark3.3 and below, reads Paimon table with LocalZonedTimestamp type written by other engines, such as Flink, the query result of LocalZonedTimestamp type will have time zone offset, which needs to be adjusted manually.

When using Spark3.4 and above, all timestamp types can be parsed correctly.
{{< /hint >}}