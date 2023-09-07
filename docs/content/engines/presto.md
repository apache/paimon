---
title: "Presto"
weight: 5
type: docs
aliases:
- /engines/presto.html
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

# Presto

This documentation is a guide for using Paimon in Presto.

## Version

Paimon currently supports Presto 0.236 and above.

## Preparing Paimon Jar File

Download the jar file with corresponding version.

| Version        | Jar                                                                                                                                                                                                            |
|----------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| [0.236, 0.268) | [paimon-presto-0.236-0.5-SNAPSHOT.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-presto-0.236/0.5-SNAPSHOT/) |
| [0.268, 0.273) | [paimon-presto-0.268-0.5-SNAPSHOT.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-presto-0.268/0.5-SNAPSHOT/) |
| [0.273, latest] | [paimon-presto-0.273-0.5-SNAPSHOT.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-presto-0.273/0.5-SNAPSHOT/) |
| Presto SQL 332  | [paimon-prestosql-332-0.5-SNAPSHOT.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-prestosql-332/0.5-SNAPSHOT/)                           |

You can also manually build a bundled jar from the source code.

To build from the source code, [clone the git repository]({{< presto_github_repo >}}).

Build bundled jar with the following command.

```
mvn clean install -DskipTests
```

You can find Presto connector jar in `./paimon-presto-<presto-version>/target/paimon-presto-*.jar`.

Then, copy `paimon-presto-*.jar and flink-shaded-hadoop-*-uber-*.jar` to plugin/paimon.

## Tmp Dir

Paimon will unzip some jars to the tmp directory for codegen. By default, Presto will use `'/tmp'` as the temporary
directory, but `'/tmp'` may be periodically deleted.

You can configure this environment variable when Presto starts:
```shell
-Djava.io.tmpdir=/path/to/other/tmpdir
```

Let Paimon use a secure temporary directory.

## Configure Paimon Catalog

Catalogs are registered by creating a catalog properties file in the etc/catalog directory. For example, create etc/catalog/paimon.properties with the following contents to mount the paimon connector as the paimon catalog:

```
connector.name=paimon
warehouse=file:/tmp/warehouse
```

If you are using HDFS, choose one of the following ways to configure your HDFS:

- set environment variable HADOOP_HOME.
- set environment variable HADOOP_CONF_DIR.
- configure fs.hdfs.hadoopconf in the properties.

## Kerberos

You can configure kerberos keytab file when using KERBEROS authentication in the properties.

```
security.kerberos.login.principal=hadoop-user
security.kerberos.login.keytab=/etc/presto/hdfs.keytab
```

Keytab files must be distributed to every node in the cluster that runs Presto.

## Create Schema

```
CREATE SCHEMA paimon.test_db;
```

## Create Table

```
CREATE TABLE paimon.test_db.orders (
    order_key bigint,
    orders_tatus varchar,
    total_price decimal(18,4),
    order_date date
)
WITH (
    file_format = 'ORC',
    primary_key = ARRAY['order_key','order_date'],
    partitioned_by = ARRAY['order_date'],
    bucket = '2',
    bucket_key = 'order_key',
    changelog_producer = 'input'
)
```

## Add Column

```
CREATE TABLE paimon.test_db.orders (
    order_key bigint,
    orders_tatus varchar,
    total_price decimal(18,4),
    order_date date
)
WITH (
    file_format = 'ORC',
    primary_key = ARRAY['order_key','order_date'],
    partitioned_by = ARRAY['order_date'],
    bucket = '2',
    bucket_key = 'order_key',
    changelog_producer = 'input'
)

ALTER TABLE paimon.test_db.orders ADD COLUMN "shipping_address varchar;
```

## Query

```
SELECT * FROM paimon.default.MyTable
```

## Presto to Paimon type mapping

This section lists all supported type conversion between Presto and Paimon.
All Presto's data types are available in package ` com.facebook.presto.common.type`.

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 10%">Presto Data Type</th>
      <th class="text-left" style="width: 10%">Paimon Data Type</th>
      <th class="text-left" style="width: 5%">Atomic Type</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>RowType</code></td>
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
      <td><code>TinyintType</code></td>
      <td><code>TinyIntType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>SmallintType</code></td>
      <td><code>SmallIntType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>IntegerType</code></td>
      <td><code>IntType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>BigintType</code></td>
      <td><code>BigIntType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>RealType</code></td>
      <td><code>FloatType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>DoubleType</code></td>
      <td><code>DoubleType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>CharType(length)</code></td>
      <td><code>CharType(length)</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>VarCharType(VarCharType.MAX_LENGTH)</code></td>
      <td><code>VarCharType(VarCharType.MAX_LENGTH)</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>VarCharType(length)</code></td>
      <td><code>VarCharType(length), length is less than VarCharType.MAX_LENGTH</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>DateType</code></td>
      <td><code>DateType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>TimestampType</code></td>
      <td><code>TimestampType</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>DecimalType(precision, scale)</code></td>
      <td><code>DecimalType(precision, scale)</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>VarBinaryType(length)</code></td>
      <td><code>VarBinaryType(length)</code></td>
      <td>true</td>
    </tr>
    <tr>
      <td><code>TimestampWithTimeZoneType</code></td>
      <td><code>LocalZonedTimestampType</code></td>
      <td>true</td>
    </tr>
    </tbody>
</table>