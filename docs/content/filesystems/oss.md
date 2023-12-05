---
title: "OSS"
weight: 3
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

{{< stable >}}

Download [paimon-oss-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-oss/{{< version >}}/paimon-oss-{{< version >}}.jar).

{{< /stable >}}

{{< unstable >}}

Download [paimon-oss-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-oss/{{< version >}}/).

{{< /unstable >}}

{{< tabs "oss" >}}

{{< tab "Flink" >}}

{{< hint info >}}
If you have already configured [oss access through Flink](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/filesystems/oss/) (Via Flink FileSystem),
here you can skip the following configuration.
{{< /hint >}}

Put `paimon-oss-{{< version >}}.jar` into `lib` directory of your Flink home, and create catalog:

```sql
CREATE CATALOG my_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 'oss://<bucket>/<path>',
    'fs.oss.endpoint' = 'oss-cn-hangzhou.aliyuncs.com',
    'fs.oss.accessKeyId' = 'xxx',
    'fs.oss.accessKeySecret' = 'yyy'
);
```

{{< /tab >}}

{{< tab "Spark" >}}

{{< hint info >}}
If you have already configured oss access through Spark (Via Hadoop FileSystem), here you can skip the following configuration.
{{< /hint >}}

Place `paimon-oss-{{< version >}}.jar` together with `paimon-spark-{{< version >}}.jar` under Spark's jars directory, and start like

```shell
spark-sql \ 
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.PaimonCatalog \
  --conf spark.sql.catalog.paimon.warehouse=oss://<bucket>/<path> \
  --conf spark.sql.catalog.paimon.fs.oss.endpoint=oss-cn-hangzhou.aliyuncs.com \
  --conf spark.sql.catalog.paimon.fs.oss.accessKeyId=xxx \
  --conf spark.sql.catalog.paimon.fs.oss.accessKeySecret=yyy
```

{{< /tab >}}

{{< tab "Hive" >}}

{{< hint info >}}
If you have already configured oss access through Hive (Via Hadoop FileSystem), here you can skip the following configuration.
{{< /hint >}}

NOTE: You need to ensure that Hive metastore can access `oss`.

Place `paimon-oss-{{< version >}}.jar` together with `paimon-hive-connector-{{< version >}}.jar` under Hive's auxlib directory, and start like

```sql
SET paimon.fs.oss.endpoint=oss-cn-hangzhou.aliyuncs.com;
SET paimon.fs.oss.accessKeyId=xxx;
SET paimon.fs.oss.accessKeySecret=yyy;
```

And read table from hive metastore, table can be created by Flink or Spark, see [Catalog with Hive Metastore]({{< ref "how-to/creating-catalogs" >}})
```sql
SELECT * FROM test_table;
SELECT COUNT(1) FROM test_table;
```

{{< /tab >}}

{{< tab "Trino" >}}

Place `paimon-oss-{{< version >}}.jar` together with `paimon-trino-{{< version >}}.jar` under `plugin/paimon` directory.

Add options in `etc/catalog/paimon.properties`.
```shell
fs.oss.endpoint=oss-cn-hangzhou.aliyuncs.com
fs.oss.accessKeyId=xxx
fs.oss.accessKeySecret=yyy
```

{{< /tab >}}

{{< /tabs >}}
