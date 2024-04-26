---
title: "Trino"
weight: 5
type: docs
aliases:
- /engines/trino.html
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

# Trino

This documentation is a guide for using Paimon in Trino.

## Version

Paimon currently supports Trino 420 and above.

## Filesystem

From version 0.8, paimon share trino filesystem for all actions, which means, you should 
config trino filesystem before using trino-paimon. You can find information about how to config
filesystems for trino on trino official website.

## Preparing Paimon Jar File

{{< stable >}}

Download from master:
https://paimon.apache.org/docs/master/project/download/

{{< /stable >}}

{{< unstable >}}

| Version       | Package                                                                                                                                       |
|---------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| [420, 426]    | [paimon-trino-420-{{< version >}}-plugin.tar.gz](https://repository.apache.org/snapshots/org/apache/paimon/paimon-trino-420/{{< version >}}/) |
| [427, latest] | [paimon-trino-427-{{< version >}}-plugin.tar.gz](https://repository.apache.org/snapshots/org/apache/paimon/paimon-trino-427/{{< version >}}/) |

{{< /unstable >}}

You can also manually build a bundled jar from the source code. However, there are a few preliminary steps that need to be taken before compiling:

- To build from the source code, [clone the git repository]({{< trino_github_repo >}}).
- Install JDK17 locally, and configure JDK17 as a global environment variable;

Then,you can build bundled jar with the following command:

```
mvn clean install -DskipTests
```

You can find Trino connector jar in `./paimon-trino-<trino-version>/target/paimon-trino-<trino-version>-{{< version >}}-plugin.tar.gz`.

We use [hadoop-apache](https://mvnrepository.com/artifact/io.trino.hadoop/hadoop-apache) as a dependency for Hadoop,
and the default Hadoop dependency typically supports both Hadoop 2 and Hadoop 3. 
If you encounter an unsupported scenario, you can specify the corresponding Apache Hadoop version.

For example, if you want to use Hadoop 3.3.5-1, you can use the following command to build the jar:
```
mvn clean install -DskipTests -Dhadoop.apache.version=3.3.5-1
```

## Tmp Dir

Paimon will unzip some jars to the tmp directory for codegen. By default, Trino will use `'/tmp'` as the temporary
directory, but `'/tmp'` may be periodically deleted.

You can configure this environment variable when Trino starts: 
```shell
-Djava.io.tmpdir=/path/to/other/tmpdir
```

Let Paimon use a secure temporary directory.

## Configure Paimon Catalog

### Install Paimon Connector
```bash
tar -zxf paimon-trino-<trino-version>-{{< version >}}-plugin.tar.gz -C ${TRINO_HOME}/plugin
```
the variable `trino-version` is module name, must be one of 420, 427.
> NOTE: For JDK 17, when Deploying Trino, should add jvm options: `--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED`

### Configure
Catalogs are registered by creating a catalog properties file in the etc/catalog directory. For example, create etc/catalog/paimon.properties with the following contents to mount the paimon connector as the paimon catalog:

```
connector.name=paimon
warehouse=file:/tmp/warehouse
```

If you are using HDFS, choose one of the following ways to configure your HDFS:

- set environment variable HADOOP_HOME.
- set environment variable HADOOP_CONF_DIR.
- configure `hadoop-conf-dir` in the properties.

If you are using a hadoop filesystem, you can still use trino-hdfs and trino-hive to config it.
For example, if you use oss as a storage, you can write in `paimon.properties` according to [Trino Reference](https://trino.io/docs/current/connector/hive.html#hdfs-configuration):

```
hive.config.resources=/path/to/core-site.xml
```

Then, config core-site.xml according to [Jindo Reference](https://github.com/aliyun/alibabacloud-jindodata/blob/master/docs/user/4.x/4.6.x/4.6.12/oss/presto/jindosdk_on_presto.md)

## Kerberos

You can configure kerberos keytab file when using KERBEROS authentication in the properties.

```
security.kerberos.login.principal=hadoop-user
security.kerberos.login.keytab=/etc/trino/hdfs.keytab
```

Keytab files must be distributed to every node in the cluster that runs Trino.

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

ALTER TABLE paimon.test_db.orders ADD COLUMN shipping_address varchar;
```

## Query

```
SELECT * FROM paimon.test_db.orders
```

## Query with Time Traveling
{{< tabs "time-travel-example" >}}

{{< tab "version >=420" >}}

```sql
-- read the snapshot from specified timestamp
SELECT * FROM t FOR TIMESTAMP AS OF TIMESTAMP '2023-01-01 00:00:00 Asia/Shanghai';

-- read the snapshot with id 1L (use snapshot id as version)
SELECT * FROM t FOR VERSION AS OF 1;
```

{{< /tab >}}


{{< /tabs >}}

## Trino to Paimon type mapping

This section lists all supported type conversion between Trino and Paimon.
All Trino's data types are available in package `io.trino.spi.type`.

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 10%">Trino Data Type</th>
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
