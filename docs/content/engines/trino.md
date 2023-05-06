---
title: "Trino"
weight: 6
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

Paimon currently supports Trino 358 and above.

## Preparing Paimon Jar File

{{< stable >}}

Download the jar file with corresponding version.

| Version       | Jar                                                                                                                                                                     |
|---------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [358, 368)    | [paimon-trino-358-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-trino-358/{{< version >}}/paimon-trino-358-{{< version >}}.jar)    |
| [368, 369)    | [paimon-trino-368-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-trino-368/{{< version >}}/paimon-trino-368-{{< version >}}.jar)    |
| [369, 370)    | [paimon-trino-369-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-trino-369/{{< version >}}/paimon-trino-369-{{< version >}}.jar)    |
| [370, 388)    | [paimon-trino-370-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-trino-370/{{< version >}}/paimon-trino-370-{{< version >}}.jar)    |
| [388, 393)    | [paimon-trino-388-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-trino-388/{{< version >}}/paimon-trino-388-{{< version >}}.jar)    |
| [393, latest] | [paimon-trino-393-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-trino-393/{{< version >}}/paimon-trino-393-{{< version >}}.jar)    |

{{< /stable >}}

{{< unstable >}}

| Version       | Jar                                                                                                                                 |
|---------------|-------------------------------------------------------------------------------------------------------------------------------------|
| [358, 368)    | [paimon-trino-358-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-trino-358/{{< version >}}/) |
| [368, 369)    | [paimon-trino-368-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-trino-368/{{< version >}}/) |
| [369, 370)    | [paimon-trino-369-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-trino-369/{{< version >}}/) |
| [370, 388)    | [paimon-trino-370-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-trino-370/{{< version >}}/) |
| [388, 393)    | [paimon-trino-388-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-trino-388/{{< version >}}/) |
| [393, latest] | [paimon-trino-393-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-trino-393/{{< version >}}/) |

{{< /unstable >}}

You can also manually build bundled jar from the source code.But before compiling, you need to do a few things first:

- To build from source code, [clone the git repository]({{< trino_github_repo >}}).
- Install JDK11 and JDK17 locally, and configure JDK11 as a global environment variable;
- Configure the toolchains.xml file in ${{ MAVEN_HOME }}, the content is as follows.

```
 <toolchains>
    <toolchain>
        <type>jdk</type>
        <provides>
            <version>17</version>
            <vendor>adopt</vendor>
        </provides>
        <configuration>
            <jdkHome>${{ JAVA_HOME }}</jdkHome>
        </configuration>
    </toolchain>
 </toolchains>          
```

Then,you can build bundled jar with the following command:

```
mvn clean install -DskipTests
```

You can find Trino connector jar in `./paimon-trino-<trino-version>/target/paimon-trino-*.jar`.

Then, copy `paimon-trino-*.jar and flink-shaded-hadoop-*-uber-*.jar` to plugin/paimon.

> NOTE: For JDK 17, when Deploying Trino, should add jvm options: --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED

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

You can configure kerberos keytag file when using KERBEROS authentication in the properties.

```
security.kerberos.login.principal=hadoop-user
security.kerberos.login.keytab=/etc/trino/hdfs.keytab
```

Keytab files must be distributed to every node in the cluster that runs Trino.

## Create Schema

```
CREATE SCHEMA paimon.test;
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
    partitioned_by = ARRAY['orderdate'],
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
    partitioned_by = ARRAY['orderdate'],
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

## Query with Time Traveling

```
SET SESSION paimon.scan_timestamp_millis=1679486589444;
SELECT * FROM paimon.default.MyTable;
```
