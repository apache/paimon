---
title: "Ecosystem"
weight: 6
type: docs
aliases:
- /iceberg/ecosystem.html
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

# Iceberg Ecosystems

## AWS Athena

AWS Athena may use old manifest reader to read Iceberg manifest by names, we should let Paimon producing legacy Iceberg
manifest list file, you can enable: `'metadata.iceberg.manifest-legacy-version'`.

## DuckDB

Duckdb may rely on files placed in the `root/data` directory, while Paimon is usually placed directly in the `root`
directory, so you can configure this parameter for the table to achieve compatibility:
`'data-file.path-directory' = 'data'`.

## Trino Iceberg

In this example, we use Trino Iceberg connector to access Paimon table through Iceberg Hive catalog.
Before trying out this example, make sure that you have configured Trino Iceberg connector.
See [Trino's document](https://trino.io/docs/current/connector/iceberg.html#general-configuration) for more information.

Let's first create a Paimon table with Iceberg compatibility enabled.

{{< tabs "paimon-append-only-table-trino-1" >}}

{{< tab "Flink SQL" >}}
```sql
CREATE CATALOG paimon_catalog WITH (
    'type' = 'paimon',
    'warehouse' = '<path-to-warehouse>'
);

CREATE TABLE paimon_catalog.`default`.animals (
    kind STRING,
    name STRING
) WITH (
    'metadata.iceberg.storage' = 'hive-catalog',
    'metadata.iceberg.uri' = 'thrift://<host>:<port>'
);

INSERT INTO paimon_catalog.`default`.animals VALUES ('mammal', 'cat'), ('mammal', 'dog'), ('reptile', 'snake'), ('reptile', 'lizard');
```
{{< /tab >}}

{{< tab "Spark SQL" >}}
Start `spark-sql` with the following command line.

```bash
spark-sql --jars <path-to-paimon-jar> \
    --conf spark.sql.catalog.paimon_catalog=org.apache.paimon.spark.SparkCatalog \
    --conf spark.sql.catalog.paimon_catalog.warehouse=<path-to-warehouse> \
    --packages org.apache.iceberg:iceberg-spark-runtime-<iceberg-version> \
    --conf spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.iceberg_catalog.type=hadoop \
    --conf spark.sql.catalog.iceberg_catalog.warehouse=<path-to-warehouse>/iceberg \
    --conf spark.sql.catalog.iceberg_catalog.cache-enabled=false \ # disable iceberg catalog caching to quickly see the result
    --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
```

Run the following Spark SQL to create Paimon table, insert/update data, and query with Iceberg catalog.

```sql
CREATE TABLE paimon_catalog.`default`.animals (
    kind STRING,
    name STRING
) TBLPROPERTIES (
    'metadata.iceberg.storage' = 'hive-catalog',
    'metadata.iceberg.uri' = 'thrift://<host>:<port>'
);

INSERT INTO paimon_catalog.`default`.animals VALUES ('mammal', 'cat'), ('mammal', 'dog'), ('reptile', 'snake'), ('reptile', 'lizard');
```
{{< /tab >}}

{{< /tabs >}}

Start Trino using Iceberg catalog and query from Paimon table.

```sql
SELECT * FROM animals WHERE class = 'mammal';
/*
   kind | name 
--------+------
 mammal | cat  
 mammal | dog  
*/
```
