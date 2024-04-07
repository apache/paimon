---
title: "Presto"
weight: 6
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

{{< stable >}}

Download from master:
https://paimon.apache.org/docs/master/project/download/

{{< /stable >}}

{{< unstable >}}

| Version         | Jar                                                                                                                                                 |
|-----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| [0.236, 0.268)  | [paimon-presto-0.236-{{< version >}}-plugin.tar.gz](https://repository.apache.org/snapshots/org/apache/paimon/paimon-presto-0.236/{{< version >}}/) |
| [0.268, 0.273)  | [paimon-presto-0.268-{{< version >}}-plugin.tar.gz](https://repository.apache.org/snapshots/org/apache/paimon/paimon-presto-0.268/{{< version >}}/) |
| [0.273, latest] | [paimon-presto-0.273-{{< version >}}-plugin.tar.gz](https://repository.apache.org/snapshots/org/apache/paimon/paimon-presto-0.273/{{< version >}}/) |

{{< /unstable >}}

You can also manually build a bundled jar from the source code.

To build from the source code, [clone the git repository]({{< presto_github_repo >}}).

Build presto connector plugin with the following command.

```
mvn clean install -DskipTests
```

After the packaging is complete, you can choose the corresponding connector based on your own Presto version:

| Version         | Package                                                                          |
|-----------------|----------------------------------------------------------------------------------|
| [0.236, 0.268)  | `./paimon-presto-0.236/target/paimon-presto-0.236-{{< version >}}-plugin.tar.gz` |
| [0.268, 0.273)  | `./paimon-presto-0.268/target/paimon-presto-0.268-{{< version >}}-plugin.tar.gz` |
| [0.273, latest] | `./paimon-presto-0.273/target/paimon-presto-0.273-{{< version >}}-plugin.tar.gz` |

Of course, we also support different versions of Hive and Hadoop. But note that we utilize
Presto-shaded versions of Hive and Hadoop packages to address dependency conflicts.
You can check the following two links to select the appropriate versions of Hive and Hadoop:

[hadoop-apache2](https://mvnrepository.com/artifact/com.facebook.presto.hadoop/hadoop-apache2)

[hive-apache](https://mvnrepository.com/artifact/com.facebook.presto.hive/hive-apache)

Both Hive 2 and 3, as well as Hadoop 2 and 3, are supported.

For example, if your presto version is 0.274, hive and hadoop version is 2.x, you could run:

```bash
mvn clean install -DskipTests -am -pl paimon-presto-0.273 -Dpresto.version=0.274 -Dhadoop.apache2.version=2.7.4-9 -Dhive.apache.version=1.2.2-2
```

## Tmp Dir

Paimon will unzip some jars to the tmp directory for codegen. By default, Presto will use `'/tmp'` as the temporary
directory, but `'/tmp'` may be periodically deleted.

You can configure this environment variable when Presto starts:
```shell
-Djava.io.tmpdir=/path/to/other/tmpdir
```

Let Paimon use a secure temporary directory.

## Configure Paimon Catalog

### Install Paimon Connector

```bash
tar -zxf paimon-presto-${PRESTO_VERSION}/target/paimon-presto-${PRESTO_VERSION}-${PAIMON_VERSION}-plugin.tar.gz -C ${PRESTO_HOME}/plugin
```

Note that, the variable `PRESTO_VERSION` is module name, must be one of 0.236, 0.268, 0.273.

### Configuration

```bash
cd ${PRESTO_HOME}
mkdir -p etc/catalog
```

```properties
connector.name=paimon
# set your filesystem path, such as hdfs://namenode01:8020/path and s3://${YOUR_S3_BUCKET}/path
warehouse=${YOUR_FS_PATH}
```

If you are using HDFS FileSystem, you will also need to do one more thing: choose one of the following ways to configure your HDFS:

- set environment variable HADOOP_HOME.
- set environment variable HADOOP_CONF_DIR.
- configure `hadoop-conf-dir` in the properties.

If you are using S3 FileSystem, you need to add `paimon-s3-${PAIMON_VERSION}.jar` in `${PRESTO_HOME}/plugin/paimon` and additionally configure the following properties in `paimon.properties`:

```properties
s3.endpoint=${YOUR_ENDPOINTS}
s3.access-key=${YOUR_AK}
s3.secret-key=${YOUR_SK}
```

**Query HiveCatalog table:**

```bash
vim etc/catalog/paimon.properties
```

and set the following config:

```properties
connector.name=paimon
# set your filesystem path, such as hdfs://namenode01:8020/path and s3://${YOUR_S3_BUCKET}/path
warehouse=${YOUR_FS_PATH}
metastore=hive
uri=thrift://${YOUR_HIVE_METASTORE}:9083
```

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
    order_status varchar,
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
