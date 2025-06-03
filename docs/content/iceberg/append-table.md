---
title: "Append Table"
weight: 2
type: docs
aliases:
- /iceberg/append-table.html
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

# Append Tables

Let's walk through a simple example, where we query Paimon tables with Iceberg connectors in Flink and Spark.
Before trying out this example, make sure that your compute engine already supports Iceberg.
Please refer to Iceberg's document if you haven't set up Iceberg.
* Flink: [Preparation when using Flink SQL Client](https://iceberg.apache.org/docs/latest/flink/#preparation-when-using-flink-sql-client)
* Spark: [Using Iceberg in Spark 3](https://iceberg.apache.org/docs/latest/spark-getting-started/#using-iceberg-in-spark-3)

Let's now create a Paimon append only table with Iceberg compatibility enabled and insert some data.

{{< tabs "create-paimon-append-only-table" >}}

{{< tab "Flink SQL" >}}
```sql
CREATE CATALOG paimon_catalog WITH (
    'type' = 'paimon',
    'warehouse' = '<path-to-warehouse>'
);

CREATE TABLE paimon_catalog.`default`.cities (
    country STRING,
    name STRING
) WITH (
    'metadata.iceberg.storage' = 'hadoop-catalog'
);

INSERT INTO paimon_catalog.`default`.cities VALUES ('usa', 'new york'), ('germany', 'berlin'), ('usa', 'chicago'), ('germany', 'hamburg');
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

Run the following Spark SQL to create Paimon table and insert data.

```sql
CREATE TABLE paimon_catalog.`default`.cities (
    country STRING,
    name STRING
) TBLPROPERTIES (
    'metadata.iceberg.storage' = 'hadoop-catalog'
);

INSERT INTO paimon_catalog.`default`.cities VALUES ('usa', 'new york'), ('germany', 'berlin'), ('usa', 'chicago'), ('germany', 'hamburg');
```
{{< /tab >}}

{{< /tabs >}}

Now let's query this Paimon table with Iceberg connector.

{{< tabs "query-paimon-append-only-table" >}}

{{< tab "Flink SQL" >}}
```sql
CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'catalog-type' = 'hadoop',
    'warehouse' = '<path-to-warehouse>/iceberg',
    'cache-enabled' = 'false' -- disable iceberg catalog caching to quickly see the result
);

SELECT * FROM iceberg_catalog.`default`.cities WHERE country = 'germany';
/*
+----+--------------------------------+--------------------------------+
| op |                        country |                           name |
+----+--------------------------------+--------------------------------+
| +I |                        germany |                         berlin |
| +I |                        germany |                        hamburg |
+----+--------------------------------+--------------------------------+
*/
```
{{< /tab >}}

{{< tab "Spark SQL" >}}
```sql
SELECT * FROM iceberg_catalog.`default`.cities WHERE country = 'germany';
/*
germany berlin
germany hamburg
*/
```
{{< /tab >}}

{{< /tabs >}}

Let's insert more data and query again.

{{< tabs "query-paimon-append-only-table-again" >}}

{{< tab "Flink SQL" >}}
```sql
INSERT INTO paimon_catalog.`default`.cities VALUES ('usa', 'houston'), ('germany', 'munich');

SELECT * FROM iceberg_catalog.`default`.cities WHERE country = 'germany';
/*
+----+--------------------------------+--------------------------------+
| op |                        country |                           name |
+----+--------------------------------+--------------------------------+
| +I |                        germany |                         munich |
| +I |                        germany |                         berlin |
| +I |                        germany |                        hamburg |
+----+--------------------------------+--------------------------------+
*/
```
{{< /tab >}}

{{< tab "Spark SQL" >}}
```sql
INSERT INTO paimon_catalog.`default`.cities VALUES ('usa', 'houston'), ('germany', 'munich');

SELECT * FROM iceberg_catalog.`default`.cities WHERE country = 'germany';
/*
germany munich
germany berlin
germany hamburg
*/
```
{{< /tab >}}

{{< /tabs >}}

