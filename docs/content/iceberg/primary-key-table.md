---
title: "Primary Key Table"
weight: 3
type: docs
aliases:
- /iceberg/primary-key-table.html
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

# Primary Key Tables

Let's walk through a simple example, where we query Paimon tables with Iceberg connectors in Flink and Spark.
Before trying out this example, make sure that your compute engine already supports Iceberg.
Please refer to Iceberg's document if you haven't set up Iceberg.
* Flink: [Preparation when using Flink SQL Client](https://iceberg.apache.org/docs/latest/flink/#preparation-when-using-flink-sql-client)
* Spark: [Using Iceberg in Spark 3](https://iceberg.apache.org/docs/latest/spark-getting-started/#using-iceberg-in-spark-3)

{{< tabs "paimon-primary-key-table" >}}

{{< tab "Flink SQL" >}}
```sql
CREATE CATALOG paimon_catalog WITH (
    'type' = 'paimon',
    'warehouse' = '<path-to-warehouse>'
);

CREATE TABLE paimon_catalog.`default`.orders (
    order_id BIGINT,
    status STRING,
    payment DOUBLE,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'metadata.iceberg.storage' = 'hadoop-catalog',
    'compaction.optimization-interval' = '1ms' -- ATTENTION: this option is only for testing, see "timeliness" section below for more information
);

INSERT INTO paimon_catalog.`default`.orders VALUES (1, 'SUBMITTED', CAST(NULL AS DOUBLE)), (2, 'COMPLETED', 200.0), (3, 'SUBMITTED', CAST(NULL AS DOUBLE));

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'catalog-type' = 'hadoop',
    'warehouse' = '<path-to-warehouse>/iceberg',
    'cache-enabled' = 'false' -- disable iceberg catalog caching to quickly see the result
);

SELECT * FROM iceberg_catalog.`default`.orders WHERE status = 'COMPLETED';
/*
+----+----------------------+--------------------------------+--------------------------------+
| op |             order_id |                         status |                        payment |
+----+----------------------+--------------------------------+--------------------------------+
| +I |                    2 |                      COMPLETED |                          200.0 |
+----+----------------------+--------------------------------+--------------------------------+
*/

INSERT INTO paimon_catalog.`default`.orders VALUES (1, 'COMPLETED', 100.0);

SELECT * FROM iceberg_catalog.`default`.orders WHERE status = 'COMPLETED';
/*
+----+----------------------+--------------------------------+--------------------------------+
| op |             order_id |                         status |                        payment |
+----+----------------------+--------------------------------+--------------------------------+
| +I |                    1 |                      COMPLETED |                          100.0 |
| +I |                    2 |                      COMPLETED |                          200.0 |
+----+----------------------+--------------------------------+--------------------------------+
*/
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
CREATE TABLE paimon_catalog.`default`.orders (
    order_id BIGINT,
    status STRING,
    payment DOUBLE
) TBLPROPERTIES (
    'primary-key' = 'order_id',
    'metadata.iceberg.storage' = 'hadoop-catalog',
    'compaction.optimization-interval' = '1ms' -- ATTENTION: this option is only for testing, see "timeliness" section below for more information
);

INSERT INTO paimon_catalog.`default`.orders VALUES (1, 'SUBMITTED', CAST(NULL AS DOUBLE)), (2, 'COMPLETED', 200.0), (3, 'SUBMITTED', CAST(NULL AS DOUBLE));

SELECT * FROM iceberg_catalog.`default`.orders WHERE status = 'COMPLETED';
/*
2       COMPLETED       200.0
*/

INSERT INTO paimon_catalog.`default`.orders VALUES (1, 'COMPLETED', 100.0);

SELECT * FROM iceberg_catalog.`default`.orders WHERE status = 'COMPLETED';
/*
2       COMPLETED       200.0
1       COMPLETED       100.0
*/
```
{{< /tab >}}

{{< /tabs >}}

Paimon primary key tables organize data files as LSM trees, so data files must be merged in memory before querying.
However, Iceberg readers are not able to merge data files, so they can only query data files on the highest level of LSM trees.
Data files on the highest level are produced by the full compaction process.
So **to conclude, for primary key tables, Iceberg readers can only query data after full compaction**.

By default, there is no guarantee on how frequently Paimon will perform full compaction.
You can configure the following table option, so that Paimon is forced to perform full compaction after several commits.

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 20%">Option</th>
      <th class="text-left" style="width: 5%">Default</th>
      <th class="text-left" style="width: 10%">Type</th>
      <th class="text-left" style="width: 60%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>compaction.optimization-interval</h5></td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Duration</td>
      <td>Full compaction will be constantly triggered per time interval. First compaction after the job starts will always be full compaction.</td>
    </tr>
    <tr>
      <td><h5>full-compaction.delta-commits</h5></td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>Full compaction will be constantly triggered after delta commits. Only implemented in Flink.</td>
    </tr>
    </tbody>
</table>

Note that full compaction is a resource-consuming process, so the value of this table option should not be too small.
We recommend full compaction to be performed once or twice per hour.

## Deletion Vector Support

[Deletion vectors]({{< ref "concepts/spec/tableindex#deletion-vectors" >}}) in Paimon are used to store deleted records for each file.
Under deletion-vector mode, paimon readers can directly filter out unnecessary records during reading phase without merging data.
Fortunately, Iceberg has supported [deletion vectors](https://iceberg.apache.org/spec/?h=deletion#deletion-vectors) in [Version 3](https://iceberg.apache.org/spec/?h=deletion#version-3).
This means that if the Iceberg reader can recognize Paimon's deletion vectors, it will be able to read all of Paimon's data, even without the ability to merge data files.
With Paimon's deletion vectors synchronized to Iceberg, Iceberg reader and Paimon reader can achieve true real-time synchronization.


If the following conditions are met, it will construct metadata about Paimon's deletion vectors for Iceberg.
* '`deletion-vectors.enabled`' and '`deletion-vectors.bitmap64`' should be set to true. Because only 64-bit bitmap implementation of deletion vector in Paimon is compatible with Iceberg.
* '`metadata.iceberg.format-version`'(default value is 2) should be set to 3. Because Iceberg only supports deletion vector in V3.
* Version of Iceberg should be 1.8.0+.
* JDK version should be 11+. Iceberg has stopped supporting JDK 8 since version 1.7.0.

Here is an example:
{{< tabs "deletion-vector-table" >}}

{{< tab "Flink SQL" >}}
```sql
-- flink version: 1.20.1

CREATE CATALOG paimon_catalog WITH (
    'type' = 'paimon',
    'warehouse' = '<path-to-warehouse>'
);

-- Create a paimon table with primary key and enable deletion vector
CREATE TABLE paimon_catalog.`default`.T
(
    pt  INT
    ,k  INT
    ,v  INT
    ,PRIMARY KEY (pt, k) NOT ENFORCED
)PARTITIONED BY (pt)
WITH (
    'metadata.iceberg.storage' = 'hadoop-catalog'
    ,'metadata.iceberg.format-version' = '3'
    ,'deletion-vectors.enabled' = 'true'
    ,'deletion-vectors.bitmap64' = 'true'
);

INSERT INTO paimon_catalog.`default`.T
VALUES (1, 9, 90), (1, 10, 100), (1, 11, 110), (2, 20, 200)
;

-- iceberg version: 1.8.1
CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'catalog-type' = 'hadoop',
    'warehouse' = '<path-to-warehouse>/iceberg',
    'cache-enabled' = 'false' -- disable iceberg catalog caching to quickly see the result
);

SELECT * FROM iceberg_catalog.`default`.T;
/*
+------------+------------+------------+
|         pt |          k |          v |
+------------+------------+------------+
|          2 |         20 |        200 |
|          1 |          9 |         90 |
|          1 |         10 |        100 |
|          1 |         11 |        110 |
+------------+------------+------------+
*/

-- insert some data again, this will generate deletion vectors
INSERT INTO paimon_catalog.`default`.T
VALUES (1, 10, 101), (2, 20, 201), (1, 12, 121)
;

-- select deletion-vector index in paimon
SELECT * FROM paimon_catalog.`default`.`T$table_indexes` WHERE index_type='DELETION_VECTORS';
/*
+------------+-----------+-------------------+------------------------   -----+------------+------------+--------------------------------+
|  partition |    bucket |        index_type |                      file_name |  file_size |  row_count |                      dv_ranges |
+------------+-----------+-------------------+------------------------   -----+------------+------------+--------------------------------+
|        {1} |         0 |  DELETION_VECTORS | index-4ae44c5d-2fc6-40b0-9ff0~ |         43 |          1 | [(data-968fdf3a-2f44-41df-89b~ |
+------------+-----------+-------------------+------------------------   -----+------------+------------+--------------------------------+
*/

-- select in iceberg, the updates was successfully read by iceberg
SELECT * FROM iceberg_catalog.`default`.T;
/*
+------------+------------+------------+
|         pt |          k |          v |
+------------+------------+------------+
|          1 |          9 |         90 |
|          1 |         11 |        110 |
|          2 |         20 |        201 |
|          1 |         10 |        101 |
|          1 |         12 |        121 |
+------------+------------+------------+
*/

```
{{< /tab >}}

{{< /tabs >}}

{{< hint info >}}

note1: Upgrade the implementation of deletion vector to 64-bit bitmap if necessary.

{{< /hint >}}

If your paimon table has already been in deletion-vector mode, but 32-bit bitmap was used for deletion vector.
You need to upgrade the implementation of deletion vector to 64-bit bitmap if you want to synchronize deletion-vector metadata to iceberg.
You can follow the following steps to upgrade to 64-bit deletion-vector:
1. stop all the writing jobs of your paimon table.
2. perform a [full compaction]({{< ref "maintenance/dedicated-compaction#dedicated-compaction-job" >}}) to your paimon table.
3. run `ALTER TABLE tableName SET ('deletion-vectors.bitmap64' = 'true')` to upgrade to 64-bit deletion vector.
4. restart your writing job. If meeting the all the conditions mentioned above, deletion vector metadata will be synchronized to iceberg.

{{< hint info >}}

note2: Upgrade the format version of iceberg to 3 if necessary.

{{< /hint >}}
You can upgrade the format version of iceberg from 2 to 3 by setting `'metadata.iceberg.format-version' = '3'`.
This will recreate the iceberg metadata without using the base metadata.
