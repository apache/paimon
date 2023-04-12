---
title: "Overview"
weight: 1
type: docs
aliases:
- /filesystems/overview.html
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

# Overview

Apache Paimon utilizes the same pluggable file systems as Apache Flink. Users can follow the
[standard plugin mechanism](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/filesystems/plugins/)
to configure the plugin structure if using Flink as compute engine. However, for other engines like Spark
or Hive, the provided opt jars (by Flink) may get conflicts and cannot be used directly. It is not convenient
for users to fix class conflicts, thus Paimon provides the self-contained and engine-unified
FileSystem pluggable jars for user to query tables from Spark/Hive side.

## Supported FileSystems

| FileSystem        | URI Scheme | Pluggable | Description                                                            |
|:------------------|:-----------|-----------|:-----------------------------------------------------------------------|
| Local File System | file://    | N         | Built-in Support                                                       |
| HDFS              | hdfs://    | N         | Built-in Support, ensure that the cluster is in the hadoop environment |
| Aliyun OSS        | oss://     | Y         |                                                                        |
| S3                | s3://      | Y         |                                                                        |

## Dependency

We recommend you to download the jar directly: [Download Link]({{< ref "project/download#filesystem-jars" >}}).

You can also manually build bundled jar from the source code.

To build from source code, [clone the git repository]({{< github_repo >}}).

Build shaded jar with the following command.

```bash
mvn clean install -DskipTests
```

You can find the shaded jars under
`./paimon-filesystems/paimon-${fs}/target/paimon-${fs}-{{< version >}}.jar`.
