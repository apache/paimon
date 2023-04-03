---
title: "HDFS"
weight: 4
type: docs
aliases:
- /filesystems/hdfs.html
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

# HDFS

You can also manually build bundled jar from the source code.

To build from source code, [clone the git repository]({{< github_repo >}}).

Build shaded jar with the following command.

```bash
mvn clean install -DskipTests
```

{{< tabs "hdfs" >}}

{{< tab "Flink" >}}

{{< hint info >}}

```sql
CREATE CATALOG my_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 'hdfs://path/to/warehouse',
    'fs.defaultFS' = 'hdfs://master:9000',
    'kerberos.principal' = 'HTTP/localhost@LOCALHOST'
    'kerberos.keytab' = '/tmp/auth.keytab'
);
```

{{< /tab >}}

{{< tab "Spark" >}}

```shell
spark-sql \ 
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.catalog.paimon.warehouse=hdfs://<bucket-name>/ \
  --conf spark.sql.catalog.paimon.fs.defaultFS=hdfs://master:9000 \
  --conf spark.sql.catalog.paimon.kerberos.principal=HTTP/localhost@LOCALHOST \
  --conf spark.sql.catalog.paimon.kerberos.keytab=/tmp/auth.keytab
```

{{< /tab >}}

{{< tab "Hive" >}}

```sql
SET paimon.fs.defaultFS=hdfs://master:9000;
SET paimon.kerberos.principal=HTTP/localhost@LOCALHOST;
SET paimon.kerberos.keytab=/tmp/auth.keytab;
```

And read table from hive metastore, table can be created by Flink or Spark, see [Catalog with Hive Metastore]({{< ref "how-to/creating-catalogs" >}})
```sql
SELECT * FROM test_table;
SELECT COUNT(1) FROM test_table;
```

{{< /tab >}}

{{< tab "Trino" >}}

Add options in `etc/catalog/paimon.properties`.
```shell
fs.defaultFS=hdfs://master:9000
kerberos.principal=HTTP/localhost@LOCALHOST
kerberos.keytab=/tmp/auth.keytab
```

{{< /tab >}}

{{< /tabs >}}
