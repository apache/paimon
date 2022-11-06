---
title: "Build"
weight: 2
type: docs
aliases:
- /engines/build.html
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

# Build from Source

In order to build the Flink Table Store you need the source code. Either [download the source of a release](https://flink.apache.org/downloads.html) or [clone the git repository]({{< github_repo >}}).

In addition, you need **Maven 3** and a **JDK** (Java Development Kit). Flink Table Store requires **Java 8** to build.

To clone from git, enter:

```bash
git clone {{< github_repo >}}
```

The simplest way of building Table Store is by running:

```bash
mvn clean install -DskipTests
```

## Flink 1.16

You can find Flink 1.16 bundled jar in `./flink-table-store-dist/target/flink-table-store-dist-{{< version >}}.jar`.

## Hive

| Version      | Command                                                         |
|:-------------|:----------------------------------------------------------------|
| 2.3          | mvn clean install -Dmaven.test.skip=true                        |
| 3.1          | mvn clean install -Dmaven.test.skip=true -Phive-3.1             |
| 2.2          | mvn clean install -Dmaven.test.skip=true -Phive-2.2             |
| 2.1          | mvn clean install -Dmaven.test.skip=true -Phive-2.1             |
| 2.1 CDH 6.3  | mvn clean install -Dmaven.test.skip=true -Phive-2.1-cdh-6.3     |

You can find Hive catalog jar in `./flink-table-store-hive/flink-table-store-hive-catalog/target/flink-table-store-hive-catalog-{{< version >}}.jar`. 

You can find Hive connector jar in `./flink-table-store-hive/flink-table-store-hive-connector/target/flink-table-store-hive-connector-{{< version >}}.jar`.

## Spark

You can find Spark bundled jar in `./flink-table-store-spark/target/flink-table-store-spark-{{< version >}}.jar`.

## Spark2

You can find Spark2 bundled jar in `./flink-table-store-spark2/target/flink-table-store-spark2-{{< version >}}.jar`.

## Flink 1.15

Running:

```bash
mvn clean install -Dmaven.test.skip=true -Pflink-1.15
```

You can find Flink 1.15 bundled jar in `./flink-table-store-dist/target/flink-table-store-dist-{{< version >}}.jar`.

## Flink 1.14

Running:

```bash
mvn clean install -Dmaven.test.skip=true -Pflink-1.14
```

You can find Flink 1.14 bundled jar in `./flink-table-store-dist/target/flink-table-store-dist-{{< version >}}.jar`.

{{< hint info >}}
__Note:__ Please do not use other connector jars (Hive, Spark), they are not available under `-Pflink-1.14`.
  {{< /hint >}}
