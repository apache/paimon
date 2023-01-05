---
title: "OSS"
weight: 2
type: docs
aliases:
- /filesystems/oss.html
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

# OSS

## Build

To build from source code, either [download the source of a release](https://flink.apache.org/downloads.html) or [clone the git repository]({{< github_repo >}}).

Build shaded jar with the following command.

```bash
mvn clean install -DskipTests
```

You can find the shaded jars under
`./flink-table-store-filesystems/flink-table-store-oss/target/flink-table-store-oss-{{< version >}}.jar`.

## Usage

{{< tabs "oss" >}}

{{< tab "Flink" >}}

[Prepare OSS jar](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/filesystems/oss/#shaded-hadoop-oss-file-system), then configure `flink-conf.yaml` like

```yaml
fs.oss.endpoint: oss-cn-hangzhou.aliyuncs.com
fs.oss.accessKeyId: xxx
fs.oss.accessKeySecret: yyy
```

{{< /tab >}}

{{< tab "Spark" >}}

Place `flink-table-store-oss-{{< version >}}.jar` together with `flink-table-store-spark-{{< version >}}.jar` under Spark's jars directory, and start like

```shell
spark-sql \ 
  --conf spark.sql.catalog.tablestore=org.apache.flink.table.store.spark.SparkCatalog \
  --conf spark.sql.catalog.tablestore.warehouse=oss://<bucket-name>/ \
  --conf spark.sql.catalog.tablestore.fs.oss.endpoint=oss-cn-hangzhou.aliyuncs.com \
  --conf spark.sql.catalog.tablestore.fs.oss.accessKeyId=xxx \
  --conf spark.sql.catalog.tablestore.fs.oss.accessKeySecret=yyy
```

{{< /tab >}}

{{< tab "Hive" >}}

NOTE: You need to ensure that Hive metastore can access `oss`.

Place `flink-table-store-oss-{{< version >}}.jar` together with `flink-table-store-hive-connector-{{< version >}}.jar` under Hive's auxlib directory, and start like

```sql
SET tablestore.fs.oss.endpoint=oss-cn-hangzhou.aliyuncs.com;
SET tablestore.fs.oss.accessKeyId=xxx;
SET tablestore.fs.oss.accessKeySecret=yyy;
```

And read table from hive metastore, table can be created by Flink or Spark, see [Catalog with Hive Metastore]({{< ref "docs/how-to/creating-catalogs" >}})
```sql
SELECT * FROM test_table;
SELECT COUNT(1) FROM test_table;
```

{{< /tab >}}

{{< /tabs >}}
