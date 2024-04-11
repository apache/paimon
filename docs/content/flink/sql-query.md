---
title: "SQL Query"
weight: 4
type: docs
aliases:
- /flink/sql-query.html
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

# SQL Query

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

-- read tag 'my-tag'
SELECT * FROM t /*+ OPTIONS('scan.tag-name' = 'my-tag') */;

-- read the snapshot from watermark, will match the first snapshot after the watermark
SELECT * FROM t /*+ OPTIONS('scan.watermark' = '1678883047356') */; 
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

### Batch Incremental

Read incremental changes between start snapshot (exclusive) and end snapshot.

For example:
- '5,10' means changes between snapshot 5 and snapshot 10.
- 'TAG1,TAG3' means changes between TAG1 and TAG3.

```sql
-- incremental between snapshot ids
SELECT * FROM t /*+ OPTIONS('incremental-between' = '12,20') */;

-- incremental between snapshot time mills
SELECT * FROM t /*+ OPTIONS('incremental-between-timestamp' = '1692169000000,1692169900000') */;
```

In Batch SQL, the `DELETE` records are not allowed to be returned, so records of `-D` will be dropped.
If you want see `DELETE` records, you can use audit_log table:

```sql
SELECT * FROM t$audit_log /*+ OPTIONS('incremental-between' = '12,20') */;
```

## Streaming Query

By default, Streaming read produces the latest snapshot on the table upon first startup,
and continue to read the latest changes.

Paimon by default ensures that your startup is properly processed with all data included.

{{< hint warning >}}
Paimon Source in Streaming mode is unbounded, like a queue that never ends.
{{< /hint >}}

```sql
-- Flink SQL
SET 'execution.runtime-mode' = 'streaming';
```

You can also do streaming read without the snapshot data, you can use `latest` scan mode:
```sql
-- Continuously reads latest changes without producing a snapshot at the beginning.
SELECT * FROM t /*+ OPTIONS('scan.mode' = 'latest') */;
```

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

```sql
SELECT * FROM t /*+ OPTIONS('scan.file-creation-time-millis' = '1678883047356') */;
```

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

{{< hint info >}}
NOTE: The consumer will prevent expiration of the snapshot. You can specify 'consumer.expiration-time' to manage the 
lifetime of consumers.
{{< /hint >}}

By default, the consumer uses `exactly-once` mode to record consumption progress, which strictly ensures that what is 
recorded in the consumer is the snapshot-id + 1 that all readers have exactly consumed. You can set `consumer.mode` to 
`at-least-once` to allow readers consume snapshots at different rates and record the slowest snapshot-id among all 
readers into the consumer. This mode can provide more capabilities, such as watermark alignment.

{{< hint warning >}}
Since the implementation of `exactly-once` mode and `at-least-once` mode are completely different, the state of 
flink is incompatible and cannot be restored from the state when switching modes.
{{< /hint >}}

You can reset a consumer with a given consumer ID and next snapshot ID and delete a consumer with a given consumer ID.

{{< hint info >}}
First, you need to stop the streaming task using this consumer ID, and then execute the reset consumer action job.
{{< /hint >}}

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

### Read Overwrite

Streaming reading will ignore the commits generated by `INSERT OVERWRITE` by default. If you want to read the
commits of `OVERWRITE`, you can configure `streaming-read-overwrite`.

## Read Parallelism

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
