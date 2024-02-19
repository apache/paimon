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

```sql
-- Flink SQL
SET 'execution.runtime-mode' = 'batch';
```

### Batch Time Travel

Paimon batch reads with time travel can specify a snapshot or a tag and read the corresponding data.

{{< tabs "time-travel-example" >}}

{{< tab "Flink (dynamic option)" >}}
```sql
-- read the snapshot with id 1L
SELECT * FROM t /*+ OPTIONS('scan.snapshot-id' = '1') */;

-- read the snapshot from specified timestamp in unix milliseconds
SELECT * FROM t /*+ OPTIONS('scan.timestamp-millis' = '1678883047356') */;

-- read the snapshot from specified timestamp string ,it will be automatically converted to timestamp in unix milliseconds
-- Supported formats include：yyyy-MM-dd, yyyy-MM-dd HH:mm:ss, yyyy-MM-dd HH:mm:ss.SSS, use default local time zone
SELECT * FROM t /*+ OPTIONS('scan.timestamp' = '2023-12-09 23:09:12') */;

-- read tag 'my-tag'
SELECT * FROM t /*+ OPTIONS('scan.tag-name' = 'my-tag') */;
```
{{< /tab >}}

{{< tab "Flink 1.18+" >}}
Flink SQL supports time travel syntax after 1.18.
```sql
-- read the snapshot from specified timestamp
SELECT * FROM t FOR SYSTEM_TIME AS OF TIMESTAMP '2023-01-01 00:00:00';

-- you can also use some simple expressions (see flink document to get supported functions)
SELECT * FROM t FOR SYSTEM_TIME AS OF TIMESTAMP '2023-01-01 00:00:00' + INTERVAL '1' DAY
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

{{< hint warning >}}
If tag's name is a number and equals to a snapshot id, the VERSION AS OF syntax will consider tag first. For example, if 
you have a tag named '1' based on snapshot 2, the statement `SELECT * FROM t VERSION AS OF '1'` actually queries snapshot 2 
instead of snapshot 1.
{{< /hint >}}

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

-- read the snapshot from specified timestamp string ,it will be automatically converted to timestamp in unix milliseconds
-- Supported formats include：yyyy-MM-dd, yyyy-MM-dd HH:mm:ss, yyyy-MM-dd HH:mm:ss.SSS, use default local time zone
SET paimon.scan_timestamp='2023-11-29 14:55:12';
SELECT * FROM t;
```

{{< /tab >}}

{{< tab "Trino 368+" >}}

```sql
-- read the snapshot from specified timestamp
SELECT * FROM t FOR TIMESTAMP AS OF TIMESTAMP '2023-01-01 00:00:00 Asia/Shanghai';

-- read the snapshot with id 1L (use snapshot id as version)
SELECT * FROM t FOR VERSION AS OF 1;
```

{{< /tab >}}

{{< tab "Hive" >}}

Hive requires adding the following configuration parameters to the hive-site.xml file:
```xml
<!--This parameter is used to configure the whitelist of permissible configuration items allowed for use in SQL standard authorization mode.-->
<property>
  <name>hive.security.authorization.sqlstd.confwhitelist</name>
  <value>mapred.*|hive.*|mapreduce.*|spark.*</value>
</property>

<!--This parameter is an additional configuration for hive.security.authorization.sqlstd.confwhitelist. It allows you to add other permissible configuration items to the existing whitelist.-->
<property>
 <name>hive.security.authorization.sqlstd.confwhitelist.append</name>
  <value>mapred.*|hive.*|mapreduce.*|spark.*</value>
</property>
```

```sql
-- read the snapshot with id 1L (use snapshot id as version)
SET paimon.scan.snapshot-id=1
SELECT * FROM t;
SET paimon.scan.snapshot-id=null;

-- read the snapshot from specified timestamp in unix seconds
SET paimon.scan.timestamp-millis=1679486589444;
SELECT * FROM t;
SET paimon.scan.timestamp-millis=null;

-- read the snapshot from specified timestamp string ,it will be automatically converted to timestamp in unix milliseconds
-- Supported formats include：yyyy-MM-dd, yyyy-MM-dd HH:mm:ss, yyyy-MM-dd HH:mm:ss.SSS, use default local time zone
SET paimon.scan.timestamp=2023-12-09 23:09:01;
SELECT * FROM t;
SET paimon.scan.timestamp=null;
    
-- read tag 'my-tag'
set paimon.scan.tag-name=my-tag;
SELECT * FROM t;
set paimon.scan.tag-name=null;
```
{{< /tab >}}

{{< tab "Presto" >}}
```sql
-- read the snapshot with id 1L (use snapshot id as version)
set session paimon.scan_snapshot_id=1;
SELECT * FROM t;

-- Re enter the client
-- read the snapshot from specified timestamp in unix seconds
SET paimon.scan_timestamp_millis=1679486589444;
SELECT * FROM t;

-- Re enter the client
-- read the snapshot from specified timestamp string ,it will be automatically converted to timestamp in unix milliseconds
-- Supported formats include：yyyy-MM-dd, yyyy-MM-dd HH:mm:ss, yyyy-MM-dd HH:mm:ss.SSS, use default local time zone
SET paimon.scan_timestamp='2023-11-29 14:55:12';
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
-- incremental between snapshot ids
SELECT * FROM t /*+ OPTIONS('incremental-between' = '12,20') */;

-- incremental between snapshot time mills
SELECT * FROM t /*+ OPTIONS('incremental-between-timestamp' = '1692169000000,1692169900000') */;
```
{{< /tab >}}

{{< tab "Spark3" >}}

Requires Spark 3.2+.

Paimon supports that use Spark SQL to do the incremental query that implemented by Spark Table Valued Function.
To enable this needs these configs below:

```text
--conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions
```

you can use `paimon_incremental_query` in query to extract the incremental data:

```sql
-- read the incremental data between snapshot id 12 and snapshot id 20.
SELECT * FROM paimon_incremental_query('tableName', 12, 20);
```

{{< /tab >}}

{{< tab "Spark-DF" >}}

```scala
// incremental between snapshot ids
spark.read()
  .format("paimon")
  .option("incremental-between", "12,20")
  .load("path/to/table")

// incremental between snapshot time mills
spark.read()
  .format("paimon")
  .option("incremental-between-timestamp", "1692169000000,1692169900000")
  .load("path/to/table")
```

{{< /tab >}}

{{< tab "Hive" >}}
```sql
-- incremental between snapshot ids
SET paimon.incremental-between='12,20';
SELECT * FROM t;
SET paimon.incremental-between=null;

-- incremental between snapshot time mills
SET paimon.incremental-between-timestamp='1692169000000,1692169900000';
SELECT * FROM t;
SET paimon.incremental-between-timestamp=null;
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

```sql
-- Flink SQL
SET 'execution.runtime-mode' = 'streaming';
```

You can also do streaming read without the snapshot data, you can use `latest` scan mode:

{{< tabs "latest streaming read" >}}
{{< tab "Flink" >}}
```sql
-- Continuously reads latest changes without producing a snapshot at the beginning.
SELECT * FROM t /*+ OPTIONS('scan.mode' = 'latest') */;
```
{{< /tab >}}
{{< /tabs >}}

### Streaming Time Travel

If you only want to process data for today and beyond, you can do so with partitioned filters:

```sql
SELECT * FROM t WHERE dt > '2023-06-26';
```

If it's not a partitioned table, or you can't filter by partition, you can use Time travel's stream read.

{{< tabs "streaming-time-travel" >}}
{{< tab "Flink (dynamic option)" >}}
```sql
-- read changes from snapshot id 1L 
SELECT * FROM t /*+ OPTIONS('scan.snapshot-id' = '1') */;

-- read changes from snapshot specified timestamp
SELECT * FROM t /*+ OPTIONS('scan.timestamp-millis' = '1678883047356') */;

-- read snapshot id 1L upon first startup, and continue to read the changes
SELECT * FROM t /*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id' = '1') */;
```
{{< /tab >}}

{{< tab "Flink 1.18+" >}}
Flink SQL supports time travel syntax after 1.18.
```sql
-- read the snapshot from specified timestamp
SELECT * FROM t FOR SYSTEM_TIME AS OF TIMESTAMP '2023-01-01 00:00:00';

-- you can also use some simple expressions (see flink document to get supported functions)
SELECT * FROM t FOR SYSTEM_TIME AS OF TIMESTAMP '2023-01-01 00:00:00' + INTERVAL '1' DAY
```
{{< /tab >}}

{{< /tabs >}}

Time travel's stream read rely on snapshots, but by default, snapshot only retains data within 1 hour, which can 
prevent you from reading older incremental data. So, Paimon also provides another mode for streaming reads, 
`scan.file-creation-time-millis`, which provides a rough filtering to retain files generated after `timeMillis`.

{{< tabs "file-creation-time-millis" >}}

{{< tab "Flink (dynamic option)" >}}
```sql
SELECT * FROM t /*+ OPTIONS('scan.file-creation-time-millis' = '1678883047356') */;
```
{{< /tab >}}

{{< /tabs >}}

### Consumer ID

You can specify the `consumer-id` when streaming read table:
```sql
SELECT * FROM t /*+ OPTIONS('consumer-id' = 'myid') */;
```

When stream read Paimon tables, the next snapshot id to be recorded into the file system. This has several advantages:

1. When previous job is stopped, the newly started job can continue to consume from the previous progress without
   resuming from the state. The newly reading will start reading from next snapshot id found in consumer files. 
   If you don't want this behavior, you can set `'consumer.ignore-progress'` to true.
2. When deciding whether a snapshot has expired, Paimon looks at all the consumers of the table in the file system,
   and if there are consumers that still depend on this snapshot, then this snapshot will not be deleted by expiration.
3. When there is no watermark definition, the Paimon table will pass the watermark in the snapshot to the downstream
   Paimon table, which means you can track the progress of the watermark for the entire pipeline.

{{< hint info >}}
NOTE: The consumer will prevent expiration of the snapshot. You can specify 'consumer.expiration-time' to manage the 
lifetime of consumers.
{{< /hint >}}

By default, the consumer uses `exactly-once` mode to record consumption progress, which strictly ensures that what is 
recorded in the consumer is the snapshot-id + 1 that all readers have exactly consumed. You can set `consumer.mode` to 
`at-least-once` to allow readers consume snapshots at different rates and record the slowest snapshot-id among all 
readers into the consumer. This mode can provide more capabilities, such as watermark alignment.

{{< hint warning >}}
1. When there is no watermark definition, the consumer in `at-least-once` mode cannot provide the ability to pass the 
watermark in the snapshot to the downstream. 
2. Since the implementation of `exactly-once` mode and `at-least-once` mode are completely different, the state of 
flink is incompatible and cannot be restored from the state when switching modes.
{{< /hint >}}

You can reset a consumer with a given consumer ID and next snapshot ID and delete a consumer with a given consumer ID.

{{< hint info >}}
First, you need to stop the streaming task using this consumer ID, and then execute the reset consumer action job.
{{< /hint >}}

{{< tabs "reset-consumer" >}}

{{< tab "Flink" >}}

Run the following command:

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    reset-consumer \
    --warehouse <warehouse-path> \
    --database <database-name> \ 
    --table <table-name> \
    --consumer_id <consumer-id> \
    [--next_snapshot <next-snapshot-id>] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]]
```

please don't specify --next_snapshot parameter if you want to delete the consumer.

{{< /tab >}}

{{< /tabs >}}

### Read Overwrite

Streaming reading will ignore the commits generated by `INSERT OVERWRITE` by default. If you want to read the
commits of `OVERWRITE`, you can configure `streaming-read-overwrite`.

## Read Parallelism

{{< tabs "Read Parallelism" >}}
{{< tab "Flink" >}}

By default, the parallelism of batch reads is the same as the number of splits, while the parallelism of stream 
reads is the same as the number of buckets, but not greater than `scan.infer-parallelism.max`.

Disable `scan.infer-parallelism`, global parallelism will be used for reads.

You can also manually specify the parallelism from `scan.parallelism`.

<table class="configuration table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 20%">Key</th>
            <th class="text-left" style="width: 15%">Default</th>
            <th class="text-left" style="width: 10%">Type</th>
            <th class="text-left" style="width: 55%">Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>scan.infer-parallelism</h5></td>
            <td style="word-wrap: break-word;">true</td>
            <td>Boolean</td>
            <td>If it is false, parallelism of source are set by global parallelism. Otherwise, source parallelism is inferred from splits number (batch mode) or bucket number(streaming mode).</td>
        </tr>
         <tr>
            <td><h5>scan.infer-parallelism.max</h5></td>
            <td style="word-wrap: break-word;">1024</td>
            <td>Integer</td>
            <td>If scan.infer-parallelism is true, limit the parallelism of source through this option.</td>
        </tr>
        <tr>
            <td><h5>scan.parallelism</h5></td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>Integer</td>
            <td>Define a custom parallelism for the scan source. By default, if this option is not defined, the planner will derive the parallelism for each statement individually by also considering the global configuration. If user enable the scan.infer-parallelism, the planner will derive the parallelism by inferred parallelism.</td>
        </tr>
    </tbody>
</table>

{{< /tab >}}
{{< /tabs >}}

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
);
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