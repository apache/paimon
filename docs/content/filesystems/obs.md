---
title: "OBS"
weight: 3
type: docs
aliases:
- /filesystems/obs.html
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

# OBS

{{< stable >}}

Download [paimon-obs-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-obs/{{< version >}}/paimon-obs-{{< version >}}.jar).

{{< /stable >}}

{{< unstable >}}

Download [paimon-obs-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-obs/{{< version >}}/).

{{< /unstable >}}

{{< tabs "obs" >}}

{{< tab "Flink" >}}

{{< hint info >}}
If you have already configured obs access through Flink (Via Flink FileSystem), here you can skip the following configuration.
{{< /hint >}}

Put `paimon-obs-{{< version >}}.jar` into `lib` directory of your Flink home, and create catalog:

```sql
CREATE CATALOG my_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 'obs://path/to/warehouse',
    'fs.obs.endpoint' = 'obs.cn-east-3.myhuaweicloud.com',
    'fs.obs.access.key' = 'xxx',
    'fs.obs.secret.key' = 'yyy'
);

CREATE CATALOG my_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 'obs://dc-for-test/paimon',
    'fs.obs.endpoint' = 'obs.cn-east-3.myhuaweicloud.com',
    'fs.obs.access.key' = 'CYJSSRJCD6DIQ8EXOBVL',
    'fs.obs.secret.key' = 'ry5SGppSeqgHWBbJMGoHH4a4P0uB9KQ0XL0YvNk9'
);

```

{{< /tab >}}

{{< tab "Spark" >}}

{{< hint info >}}
If you have already configured obs access through Spark (Via Hadoop FileSystem), here you can skip the following configuration.
{{< /hint >}}

Place `paimon-obs-{{< version >}}.jar` together with `paimon-spark-{{< version >}}.jar` under Spark's jars directory, and start like

```shell
spark-sql \ 
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.catalog.paimon.warehouse=obs://<bucket-name>/ \
  --conf spark.sql.catalog.paimon.fs.obs.endpoint=obs-cn-hangzhou.aliyuncs.com \
  --conf spark.sql.catalog.paimon.fs.obs.access.key=xxx \
  --conf spark.sql.catalog.paimon.fs.obs.secret.key=yyy
```

{{< /tab >}}

{{< tab "Hive" >}}

{{< hint info >}}
If you have already configured obs access through Hive (Via Hadoop FileSystem), here you can skip the following configuration.
{{< /hint >}}

NOTE: You need to ensure that Hive metastore can access `obs`.

Place `paimon-obs-{{< version >}}.jar` together with `paimon-hive-connector-{{< version >}}.jar` under Hive's auxlib directory, and start like

```sql
SET paimon.fs.obs.endpoint=obs.cn-east-3.myhuaweicloud.com;
SET paimon.fs.obs.access.key=xxx;
SET paimon.fs.obs.secret.key=yyy;
```

And read table from hive metastore, table can be created by Flink or Spark, see [Catalog with Hive Metastore]({{< ref "how-to/creating-catalogs" >}})
```sql
SELECT * FROM test_table;
SELECT COUNT(1) FROM test_table;
```

{{< /tab >}}

{{< tab "Trino" >}}

Place `paimon-obs-{{< version >}}.jar` together with `paimon-trino-{{< version >}}.jar` under `plugin/paimon` directory.

Add options in `etc/catalog/paimon.properties`.
```shell
fs.obs.endpoint=obs.cn-east-3.myhuaweicloud.com
fs.obs.access.key=xxx
fs.obs.secret.key=yyy
```

{{< /tab >}}

{{< /tabs >}}
