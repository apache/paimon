---
title: "Download"
weight: 2
type: docs
aliases:
- /project/download.html
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

# Download

This documentation is a guide for downloading Paimon Jars.

##  Engine Jars

{{< unstable >}}

| Version          | Jar                                                                                                                                                                   |
|------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Flink 1.17       | [paimon-flink-1.17-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-flink-1.17/{{< version >}}/)                                 |
| Flink 1.16       | [paimon-flink-1.16-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-flink-1.16/{{< version >}}/)                                 |
| Flink 1.15       | [paimon-flink-1.15-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-flink-1.15/{{< version >}}/)                                 |
| Flink 1.14       | [paimon-flink-1.14-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-flink-1.14/{{< version >}}/)                                 |
| Flink Action     | [paimon-flink-action-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-flink-action/{{< version >}}/)                             |
| Spark 3.4        | [paimon-spark-3.4-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-spark-3.4/{{< version >}}/)                                   |
| Spark 3.3        | [paimon-spark-3.3-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-spark-3.3/{{< version >}}/)                                   |
| Spark 3.2        | [paimon-spark-3.2-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-spark-3.2/{{< version >}}/)                                   |
| Spark 3.1        | [paimon-spark-3.1-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-spark-3.1/{{< version >}}/)                                   |
| Spark 2          | [paimon-spark-2-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-spark-2/{{< version >}}/)                                       |
| Hive 3.1         | [paimon-hive-connector-3.1-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-hive-connector-3.1/{{< version >}}/)                 |
| Hive 2.3         | [paimon-hive-connector-2.3-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-hive-connector-2.3/{{< version >}}/)                 |
| Hive 2.2         | [paimon-hive-connector-2.2-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-hive-connector-2.2/{{< version >}}/)                 |
| Hive 2.1         | [paimon-hive-connector-2.1-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-hive-connector-2.1/{{< version >}}/)                 |
| Hive 2.1-cdh-6.3 | [paimon-hive-connector-2.1-cdh-6.3-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-hive-connector-2.1-cdh-6.3/{{< version >}}/) |
| Presto 0.236     | [paimon-presto-0.236-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-presto-0.236/{{< version >}}/)                             |
| Presto 0.268     | [paimon-presto-0.268-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-presto-0.268/{{< version >}}/)                             |
| Presto 0.273     | [paimon-presto-0.273-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-presto-0.273/{{< version >}}/)                             |
| Trino 358        | [paimon-trino-358-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-trino-358/{{< version >}}/)                                   |
| Trino 368        | [paimon-trino-368-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-trino-368/{{< version >}}/)                                   |
| Trino 369        | [paimon-trino-369-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-trino-369/{{< version >}}/)                                   |
| Trino 370        | [paimon-trino-370-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-trino-370/{{< version >}}/)                                   |
| Trino 388        | [paimon-trino-388-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-trino-388/{{< version >}}/)                                   |
| Trino 393        | [paimon-trino-393-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-trino-393/{{< version >}}/)                                   |

{{< /unstable >}}

{{< stable >}}

| Version          | Jar                                                                                                                                                                                                                     |
|------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Flink 1.17       | [paimon-flink-1.17-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-1.17/{{< version >}}/paimon-flink-1.17-{{< version >}}.jar)                                                 |
| Flink 1.16       | [paimon-flink-1.16-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-1.16/{{< version >}}/paimon-flink-1.16-{{< version >}}.jar)                                                 |
| Flink 1.15       | [paimon-flink-1.15-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-1.15/{{< version >}}/paimon-flink-1.15-{{< version >}}.jar)                                                 |
| Flink 1.14       | [paimon-flink-1.14-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-1.14/{{< version >}}/paimon-flink-1.14-{{< version >}}.jar)                                                 |
| Flink Action     | [paimon-flink-action-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-action/{{< version >}}/paimon-flink-action-{{< version >}}.jar)                                           |
| Spark 3.4        | [paimon-spark-3.4-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-spark-3.4/{{< version >}}/paimon-spark-3.4-{{< version >}}.jar)                                                    |
| Spark 3.3        | [paimon-spark-3.3-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-spark-3.3/{{< version >}}/paimon-spark-3.3-{{< version >}}.jar)                                                    |
| Spark 3.2        | [paimon-spark-3.2-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-spark-3.2/{{< version >}}/paimon-spark-3.2-{{< version >}}.jar)                                                    |
| Spark 3.1        | [paimon-spark-3.1-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-spark-3.1/{{< version >}}/paimon-spark-3.1-{{< version >}}.jar)                                                    |
| Spark 2          | [paimon-spark-2-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-spark-2/{{< version >}}/paimon-spark-2-{{< version >}}.jar)                                                          |
| Hive 3.1         | [paimon-hive-connector-3.1-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-hive-connector-3.1/{{< version >}}/paimon-hive-connector-3.1-{{< version >}}.jar)                         |
| Hive 2.3         | [paimon-hive-connector-2.3-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-hive-connector-2.3/{{< version >}}/paimon-hive-connector-2.3-{{< version >}}.jar)                         |
| Hive 2.2         | [paimon-hive-connector-2.2-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-hive-connector-2.2/{{< version >}}/paimon-hive-connector-2.2-{{< version >}}.jar)                         |
| Hive 2.1         | [paimon-hive-connector-2.1-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-hive-connector-2.1/{{< version >}}/paimon-hive-connector-2.1-{{< version >}}.jar)                         |
| Hive 2.1-cdh-6.3 | [paimon-hive-connector-2.1-cdh-6.3-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-hive-connector-2.1-cdh-6.3/{{< version >}}/paimon-hive-connector-2.1-cdh-6.3-{{< version >}}.jar) |
| Presto 0.236     | [paimon-presto-0.236-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-presto-0.236/{{< version >}}/paimon-presto-0.236-{{< version >}}.jar)                                           |
| Presto 0.268     | [paimon-presto-0.268-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-presto-0.268/{{< version >}}/paimon-presto-0.268-{{< version >}}.jar)                                           |
| Presto 0.273     | [paimon-presto-0.273-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-presto-0.273/{{< version >}}/paimon-presto-0.273-{{< version >}}.jar)                                           |
| Trino 358        | [paimon-trino-358-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-trino-358/{{< version >}}/paimon-trino-358-{{< version >}}.jar)                                                    |
| Trino 368        | [paimon-trino-368-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-trino-368/{{< version >}}/paimon-trino-368-{{< version >}}.jar)                                                    |
| Trino 369        | [paimon-trino-369-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-trino-369/{{< version >}}/paimon-trino-369-{{< version >}}.jar)                                                    |
| Trino 370        | [paimon-trino-370-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-trino-370/{{< version >}}/paimon-trino-370-{{< version >}}.jar)                                                    |
| Trino 388        | [paimon-trino-388-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-trino-388/{{< version >}}/paimon-trino-388-{{< version >}}.jar)                                                    |
| Trino 393        | [paimon-trino-393-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-trino-393/{{< version >}}/paimon-trino-393-{{< version >}}.jar)                                                    |

{{< /stable >}}

## Filesystem Jars

{{< unstable >}}

| Version    | Jar                                                                                                                     |
|------------|-------------------------------------------------------------------------------------------------------------------------|
| paimon-oss | [paimon-oss-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-oss/{{< version >}}/) |
| paimon-s3  | [paimon-s3-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-s3/{{< version >}}/)   |

{{< /unstable >}}

{{< stable >}}

| Version    | Jar                                                                                                                                                |
|------------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| paimon-oss | [paimon-oss-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-oss/{{< version >}}/paimon-oss-{{< version >}}.jar) |
| paimon-s3  | [paimon-s3-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-s3/{{< version >}}/paimon-s3-{{< version >}}.jar)    |

{{< /stable >}}

## API Jars

{{< unstable >}}

| Version       | Jar                                                                                                                           |
|---------------|-------------------------------------------------------------------------------------------------------------------------------|
| paimon-bundle | [paimon-bundle-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-bundle/{{< version >}}/) |

{{< /unstable >}}

{{< stable >}}

| Version       | Jar                                                                                                                                                         |
|---------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| paimon-bundle | [paimon-bundle-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-bundle/{{< version >}}/paimon-bundle-{{< version >}}.jar) |

{{< /stable >}}
