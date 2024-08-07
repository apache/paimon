---
title: "cosn"
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

# cosn

{{< stable >}}

Download [paimon-cosn-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-cosn/{{< version >}}/paimon-oss-{{< version >}}.jar).

{{< /stable >}}

{{< unstable >}}

Download [paimon-cosn-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-cosn/{{< version >}}/).

{{< /unstable >}}

{{< tabs "cosn" >}}

{{< tab "Flink" >}}

Put `paimon-cosn-{{< version >}}.jar` into `lib` directory of your Flink home, and create catalog:

```sql
CREATE CATALOG my_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 'cosn://<bucket>/<path>',
    'fs.cosn.bucket.endpoint_suffix' = 'cos.ap-beijing.myqcloud.com',
    'fs.cosn.userinfo.secretId' = 'AKIDxxxx',
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
  --conf spark.sql.catalog.paimon.warehouse=cosn://<bucket>/<path> \
  --conf spark.sql.catalog.paimon.fs.cosn.bucket.endpoint_suffix=cos.ap-beijing.myqcloud.com \
  --conf spark.sql.catalog.paimon.fs.cosn.userinfo.secretId=AKIDxxxx \
  --conf spark.sql.catalog.paimon.fs.cosn.userinfo.secretKey=yyy
```

{{< /tab >}}

{{< tab "Hive" >}}

{{< hint info >}}
If you have already configured cosn access through Hive (Via Hadoop FileSystem), here you can skip the following configuration.
{{< /hint >}}

NOTE: You need to ensure that Hive metastore can access `cosn`

Place `paimon-cosn-{{< version >}}.jar` together with `paimon-hive-connector-{{< version >}}.jar` under Hive's auxlib directory, and start like

```sql
SET paimon.fs.cosn.bucket.endpoint_suffix=cos.ap-beijing.myqcloud.com;
SET paimon.fs.cosn.userinfo.secretId=AKIDxxxx;
SET paimon.fs.cosn.userinfo.secretKey=yyy;
```

And read table from hive metastore, table can be created by Flink or Spark, see [Catalog with Hive Metastore]({{< ref "how-to/creating-catalogs" >}})
```sql
SELECT * FROM test_table;
SELECT COUNT(1) FROM test_table;
```

{{< /tab >}}

{{< /tabs >}}
