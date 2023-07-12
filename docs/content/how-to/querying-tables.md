---
title: "Querying Tables"
weight: 5
type: docs
aliases:
- /how-to/querying-tables.html
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

# Querying Tables

Just like all other tables, Paimon tables can be queried with `SELECT` statement.

## Batch Query

Paimon's batch read returns all the data in a snapshot of the table. By default, batch reads return the latest snapshot.

### Batch Time Travel

Paimon batch reads with time travel can specify a snapshot or a tag and read the corresponding data.

{{< tabs "time-travel-example" >}}

{{< tab "Flink" >}}
```sql
-- read the snapshot with id 1L
SELECT * FROM t /*+ OPTIONS('scan.snapshot-id' = '1') */;

-- read the snapshot from specified timestamp in unix milliseconds
SELECT * FROM t /*+ OPTIONS('scan.timestamp-millis' = '1678883047356') */;

-- read tag 'my-tag'
SELECT * FROM t /*+ OPTIONS('scan.tag-name' = 'my-tag') */;
```
{{< /tab >}}

{{< tab "Spark3" >}}

Requires Spark 3.3+.

you can use `VERSION AS OF` and `TIMESTAMP AS OF` in query to do time travel:

```sql
-- read the snapshot with id 1L (use snapshot id as version)
SELECT * FROM t VERSION AS OF 1;

-- read the snapshot from specified timestamp 
SELECT * FROM t TIMESTAMP AS OF '2023-06-01 00:00:00.123';

-- read the snapshot from specified timestamp in unix seconds
SELECT * FROM t TIMESTAMP AS OF 1678883047;

-- read tag 'my-tag'
SELECT * FROM t VERSION AS OF 'my-tag';
```

{{< /tab >}}

{{< tab "Spark3-DF" >}}

```scala
// read the snapshot from specified timestamp in unix seconds
spark.read
    .option("scan.timestamp-millis", "1678883047000")
    .format("paimon")
    .load("path/to/table")
```

```scala
// read the snapshot with id 1L (use snapshot id as version)
spark.read
    .option("scan.snapshot-id", 1)
    .format("paimon")
    .load("path/to/table")
```

```scala
// read tag 'my-tag'
spark.read
    .option("scan.tag-name", "my-tag")
    .format("paimon")
    .load("path/to/table")
```

{{< /tab >}}

{{< tab "Trino" >}}

```sql
-- read the snapshot from specified timestamp with a long value in unix milliseconds
SET SESSION paimon.scan_timestamp_millis=1679486589444;
SELECT * FROM t;
```

{{< /tab >}}

{{< /tabs >}}

### Batch Incremental

Read incremental changes between start snapshot (exclusive) and end snapshot.

For example:
- '5,10' means changes between snapshot 5 and snapshot 10.
- 'TAG1,TAG3' means changes between TAG1 and TAG3.

{{< tabs "incremental-example" >}}

{{< tab "Flink" >}}
```sql
SELECT * FROM t /*+ OPTIONS('incremental-between' = '12,20') */;
```
{{< /tab >}}

{{< tab "Spark3" >}}

Requires Spark 3.2+.

Paimon supports that use Spark SQL to do the incremental query that implemented by Spark Table Valued Function.
To enable this needs these configs below:

```text
--conf spark.sql.catalog.spark_catalog=org.apache.paimon.spark.SparkGenericCatalog
--conf spark.sql.extensions=org.apache.paimon.spark.PaimonSparkSessionExtension
```

you can use `paimon_incremental_query` in query to extract the incremental data:

```sql
-- read the incremental data between snapshot id 12 and snapshot id 20.
SELECT * FROM paimon_incremental_query('tableName', 12, 20);
```

{{< /tab >}}

{{< tab "Spark-DF" >}}

```java
spark.read()
  .format("paimon")
  .option("incremental-between", "12,20")
  .load("path/to/table")
```

{{< /tab >}}

{{< /tabs >}}

In Batch SQL, the `DELETE` records are not allowed to be returned, so records of `-D` will be dropped.
If you want see `DELETE` records, you can use audit_log table:

{{< tabs "incremental-audit_log" >}}
{{< tab "Flink" >}}
```sql
SELECT * FROM t$audit_log /*+ OPTIONS('incremental-between' = '12,20') */;
```
{{< /tab >}}
{{< /tabs >}}

## Streaming Query

By default, Streaming read produces the latest snapshot on the table upon first startup,
and continue to read the latest changes.

Paimon by default ensures that your startup is properly processed with the full amount
included.

You can also do streaming read without the snapshot data, you can use `latest` scan mode:

{{< tabs "latest streaming read" >}}
{{< tab "Flink" >}}
```sql
-- Continuously reads latest changes without producing a snapshot at the beginning.
SELECT * FROM t /*+ OPTIONS('scan.mode' = 'latest') */
```
{{< /tab >}}
{{< /tabs >}}

### Streaming Time Travel

If you only want to process data for today and beyond, you can do so with partitioned filters:

```sql
SELECT * FROM t WHERE dt > '2023-06-26'
```

If it's not a partitioned table, or you can't filter by partition, you can use Time travel's stream read.

{{< tabs "streaming-time-travel" >}}
{{< tab "Flink" >}}
```sql
-- read changes from snapshot id 1L 
SELECT * FROM t /*+ OPTIONS('scan.snapshot-id' = '1') */;

-- read changes from snapshot specified timestamp
SELECT * FROM t /*+ OPTIONS('scan.timestamp-millis' = '1678883047356') */;

-- read snapshot id 1L upon first startup, and continue to read the changes
SELECT * FROM t /*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id' = '1') */;
```
{{< /tab >}}
{{< /tabs >}}

### Consumer ID

{{< hint info >}}
This is an experimental feature.
{{< /hint >}}

You can specify the `consumer-id` when streaming read table:
```sql
SELECT * FROM t /*+ OPTIONS('consumer-id' = 'myid') */;
```

When stream read Paimon tables, the next snapshot id to be recorded into the file system. This has several advantages:

1. When previous job is stopped, the newly started job can continue to consume from the previous progress without
   resuming from the state. The newly reading will start reading from next snapshot id found in consumer files.
2. When deciding whether a snapshot has expired, Paimon looks at all the consumers of the table in the file system,
   and if there are consumers that still depend on this snapshot, then this snapshot will not be deleted by expiration.
3. When there is no watermark definition, the Paimon table will pass the watermark in the snapshot to the downstream
   Paimon table, which means you can track the progress of the watermark for the entire pipeline.

{{< hint info >}}
NOTE: The consumer will prevent expiration of the snapshot. You can specify 'consumer.expiration-time' to manage the 
lifetime of consumers.
{{< /hint >}}

## Query Optimization

{{< label Batch >}}{{< label Streaming >}}

It is highly recommended to specify partition and primary key filters
along with the query, which will speed up the data skipping of the query.

The filter functions that can accelerate data skipping are:
- `=`
- `<`
- `<=`
- `>`
- `>=`
- `IN (...)`
- `LIKE 'abc%'`
- `IS NULL`

Paimon will sort the data by primary key, which speeds up the point queries
and range queries. When using a composite primary key, it is best for the query
filters to form a [leftmost prefix](https://dev.mysql.com/doc/refman/5.7/en/multiple-column-indexes.html)
of the primary key for good acceleration.

Suppose that a table has the following specification:

```sql
CREATE TABLE orders (
    catalog_id BIGINT,
    order_id BIGINT,
    .....,
    PRIMARY KEY (catalog_id, order_id) NOT ENFORCED -- composite primary key
)
```

The query obtains a good acceleration by specifying a range filter for
the leftmost prefix of the primary key.

```sql
SELECT * FROM orders WHERE catalog_id=1025;

SELECT * FROM orders WHERE catalog_id=1025 AND order_id=29495;

SELECT * FROM orders
  WHERE catalog_id=1025
  AND order_id>2035 AND order_id<6000;
```

However, the following filter cannot accelerate the query well.

```sql
SELECT * FROM orders WHERE order_id=29495;

SELECT * FROM orders WHERE catalog_id=1025 OR order_id=29495;
```