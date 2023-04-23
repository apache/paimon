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

This documentation is a guide for using Paimon in Presto.

## Version

Paimon currently supports Presto 0.236 and above.

## Preparing Paimon Jar File

{{< stable >}}

Download the jar file with corresponding version.

|     Version      | Jar                                                                                                                                                                                                            |
|------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [0.236,0.268)    | [paimon-presto-0.236-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-presto-0.236/{{< version >}}/paimon-presto-0.236-{{< version >}}.jar) |
| [0.268,0.273)    | [paimon-presto-0.268-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-presto-0.268/{{< version >}}/paimon-presto-0.268-{{< version >}}.jar) |
| [0.273,0.279]    | [paimon-presto-0.273-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-presto-0.273/{{< version >}}/paimon-presto-0.273-{{< version >}}.jar) |

{{< /stable >}}

{{< unstable >}}

|     Version      | Jar                                                                                                                                                                                                            |
|------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| [0.236,0.268)    | [paimon-presto-0.236-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-presto-0.236/{{< version >}}/) |
| [0.268,0.273)    | [paimon-presto-0.268-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-presto-0.268/{{< version >}}/) |
| [0.273,0.279]    | [paimon-presto-0.273-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-presto-0.273/{{< version >}}/) |

{{< /unstable >}}

You can also manually build bundled jar from the source code.

To build from source code, [clone the git repository]({{< github_repo >}}).

Build bundled jar with the following command.

```
mvn clean install -DskipTests
```

You can find Presto connector jar in `./paimon-presto/paimon-presto-<presto-version>/target/paimon-presto-*.jar`.

Then, copy `paimon-presto-*.jar and flink-shaded-hadoop-*-uber-*.jar` to plugin/paimon.

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

You can configure kerberos keytag file when using KERBEROS authentication in the properties.

```
security.kerberos.login.principal=hadoop-user
security.kerberos.login.keytab=/etc/presto/hdfs.keytab
```

## Query

```
SELECT * FROM paimon.default.MyTable
```