---
title: "KS3"
weight: 4
type: docs
aliases:
- /filesystems/ks3.html
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

# KS3

{{< stable >}}

Download [paimon-ks3-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-ks3/{{< version >}}/paimon-ks3-{{< version >}}.jar).

{{< /stable >}}

{{< unstable >}}

Download [paimon-ks3-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-ks3/{{< version >}}/).

{{< /unstable >}}

{{< tabs "oss" >}}

{{< tab "Flink" >}}

{{< hint info >}}
If you have already configured [ks3 access through Flink](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/filesystems/ks3/) (Via Flink FileSystem),
here you can skip the following configuration.
{{< /hint >}}

Put `paimon-ks3-{{< version >}}.jar` into `lib` directory of your Flink home, and create catalog:

```sql
CREATE CATALOG my_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 'ks3://<bucket>/<path>',
    'ks3.endpoint' = 'your-endpoint-hostname',
    'ks3.access-key' = 'xxx',
    'ks3.secret-key' = 'yyy'
);
```

{{< /tab >}}

{{< tab "Spark" >}}

{{< hint info >}}
If you have already configured ks3 access through Spark (Via Hadoop FileSystem), here you can skip the following configuration.
{{< /hint >}}

Place `paimon-ks3-{{< version >}}.jar` together with `paimon-spark-{{< version >}}.jar` under Spark's jars directory, and start like

```shell
spark-sql \ 
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.catalog.paimon.warehouse=ks3://<bucket>/<path> \
  --conf spark.sql.catalog.paimon.ks3.endpoint=your-endpoint-hostname \
  --conf spark.sql.catalog.paimon.ks3.access-key=xxx \
  --conf spark.sql.catalog.paimon.ks3.secret-key=yyy
```

{{< /tab >}}

{{< tab "Hive" >}}

{{< hint info >}}
If you have already configured ks3 access through Hive ((Via Hadoop FileSystem)), here you can skip the following configuration.
{{< /hint >}}

NOTE: You need to ensure that Hive metastore can access `ks3`.

Place `paimon-ks3-{{< version >}}.jar` together with `paimon-hive-connector-{{< version >}}.jar` under Hive's auxlib directory, and start like

```sql
SET paimon.ks3.endpoint=your-endpoint-hostname;
SET paimon.ks3.access-key=xxx;
SET paimon.ks3.secret-key=yyy;
```

And read table from hive metastore, table can be created by Flink or Spark, see [Catalog with Hive Metastore]({{< ref "flink/sql-ddl" >}})
```sql
SELECT * FROM test_table;
SELECT COUNT(1) FROM test_table;
```

{{< /tab >}}

{{< /tabs >}}

## KS3 Complaint Object Stores

The KS3 Filesystem also support using KS3 compliant object stores such as MinIO, Tencent's COS and IBM’s Cloud Object
Storage. Just configure your endpoint to the provider of the object store service.

```yaml
ks3.endpoint: your-endpoint-hostname
```
