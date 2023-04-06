---
title: "COSN"
weight: 3
type: docs
aliases:
- /filesystems/cosn.html
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

# COSN

{{< stable >}}

Download [paimon-cosn-{{< version >}}.jar](https://www.apache.org/dyn/closer.lua/flink/paimon-{{< version >}}/paimon-cosn-{{< version >}}.jar).

{{< /stable >}}

{{< unstable >}}

Download [paimon-cosn-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-cosn/{{< version >}}/).

{{< /unstable >}}

{{< tabs "cosn" >}}

{{< tab "Flink" >}}

{{< hint info >}}
If you have already configured cosn access through Flink (Via Flink FileSystem), here you can skip the following configuration.
{{< /hint >}}

Put `paimon-cosn-{{< version >}}.jar` into `lib` directory of your Flink home, and create catalog:

```sql
CREATE CATALOG my_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 'cosn://path/to/warehouse',
    'fs.cosn.bucket.region' = 'ap-city',
    'fs.cosn.userinfo.secretId' = 'xxx',
    'fs.cosn.userinfo.secretKey' = 'yyy'
);
```

{{< /tab >}}

{{< tab "Spark" >}}

{{< hint info >}}
If you have already configured cosn access through Spark (Via Hadoop FileSystem), here you can skip the following configuration.
{{< /hint >}}

Place `paimon-cosn-{{< version >}}.jar` together with `paimon-spark-{{< version >}}.jar` under Spark's jars directory, and start like

```shell
spark-sql \ 
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.catalog.paimon.warehouse=cosn://<bucket-name>/ \
  --conf spark.sql.catalog.paimon.fs.cosn.bucket.region=ap-city \
  --conf spark.sql.catalog.paimon.fs.cosn.userinfo.secretId=xxx \
  --conf spark.sql.catalog.paimon.fs.cosn.userinfo.secretKey=yyy
```

{{< /tab >}}

{{< tab "Hive" >}}

{{< hint info >}}
If you have already configured cosn access through Hive (Via Hadoop FileSystem), here you can skip the following configuration.
{{< /hint >}}

NOTE: You need to ensure that Hive metastore can access `cosn`.

Place `paimon-cosn-{{< version >}}.jar` together with `paimon-hive-connector-{{< version >}}.jar` under Hive's auxlib directory, and start like

```sql
SET paimon.fs.cosn.bucket.region=ap-city;
SET paimon.fs.cosn.userinfo.secretId=xxx;
SET paimon.fs.cosn.userinfo.secretKey=yyy;
```

And read table from hive metastore, table can be created by Flink or Spark, see [Catalog with Hive Metastore]({{< ref "how-to/creating-catalogs" >}})
```sql
SELECT * FROM test_table;
SELECT COUNT(1) FROM test_table;
```

{{< /tab >}}

{{< tab "Trino" >}}

Place `paimon-cosn-{{< version >}}.jar` together with `paimon-trino-{{< version >}}.jar` under `plugin/paimon` directory.

Add options in `etc/catalog/paimon.properties`.
```shell
fs.cosn.bucket.region=ap-city
fs.cosn.userinfo.secretId=xxx
fs.cosn.userinfo.secretKey=yyy
```

{{< /tab >}}

{{< /tabs >}}
