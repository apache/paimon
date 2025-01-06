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

-- read the snapshot from specified timestamp string ,it will be automatically converted to timestamp in unix milliseconds
-- Supported formats include：yyyy-MM-dd, yyyy-MM-dd HH:mm:ss, yyyy-MM-dd HH:mm:ss.SSS, use default local time zone
SELECT * FROM t /*+ OPTIONS('scan.timestamp' = '2023-12-09 23:09:12') */;

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

By default, will scan changelog files for the table which produces changelog files. Otherwise, scan newly changed files.
You can also force specifying `'incremental-between-scan-mode'`.

In Batch SQL, the `DELETE` records are not allowed to be returned, so records of `-D` will be dropped.
If you want see `DELETE` records, you can use audit_log table:

```sql
SELECT * FROM t$audit_log /*+ OPTIONS('incremental-between' = '12,20') */;
```

### Batch Incremental between Auto-created Tags

You can use `incremental-between` to query incremental changes between two tags. But for auto-created tag, the tag may
not be created in-time because of data delay.

For example, assume that tags '2024-12-01', '2024-12-02' and '2024-12-04' are auto created daily. Data for 12/03 are delayed
and ingested with data for 12/04. Now if you want to query the incremental changes between tags, and you don't know the tag 
of 12/03 is not created, you will use `incremental-between` with '2024-12-01,2024-12-02', '2024-12-02,2024-12-03' and 
'2024-12-03,2024-12-04' respectively, then you will get an error that the tag '2024-12-03' doesn't exist.

We introduced a new option `incremental-to-auto-tag` for this scenario. You can only specify the end tag, and Paimon will 
find an earlier tag and return changes between them. If the tag doesn't exist or the earlier tag doesn't exist, return empty. 

For example, when you query 'incremental-to-auto-tag=2024-12-01' or 'incremental-to-auto-tag=2024-12-03', the result is 
empty; Query 'incremental-to-auto-tag=2024-12-02', the result is change between 12/01 and 12/02; Query 'incremental-to-auto-tag=2024-12-04', 
the result is change between 12/02 and 12/04.

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
