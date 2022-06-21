---
title: "Write Table"
weight: 3
type: docs
aliases:
- /development/write-table.html
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

# Write Table

```sql
INSERT { INTO | OVERWRITE } [catalog_name.][db_name.]table_name
  [PARTITION part_spec] [column_list] select_statement

part_spec:
  (part_col_name1=val1 [, part_col_name2=val2, ...])

column_list:
  (col_name1 [, column_name2, ...])
```

## Parallelism

It is recommended that the parallelism of sink should be less than or
equal to the number of buckets, preferably equal. You can control the
parallelism of the sink with the `sink.parallelism` option.

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 20%">Option</th>
      <th class="text-center" style="width: 5%">Required</th>
      <th class="text-center" style="width: 5%">Default</th>
      <th class="text-center" style="width: 10%">Type</th>
      <th class="text-center" style="width: 60%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>sink.parallelism</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>Defines the parallelism of the sink operator. By default, the parallelism is determined by the framework using the same parallelism of the upstream chained operator.</td>
    </tr>
    </tbody>
</table>

## Expiring Snapshot

Table Store generates one or two snapshots per commit. To avoid too many snapshots 
that create a lot of small files and redundant storage, Table Store writes defaults
to eliminate expired snapshots:

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 20%">Option</th>
      <th class="text-center" style="width: 5%">Required</th>
      <th class="text-center" style="width: 5%">Default</th>
      <th class="text-center" style="width: 10%">Type</th>
      <th class="text-center" style="width: 60%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>snapshot.time-retained</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">1 h</td>
      <td>Duration</td>
      <td>The maximum time of completed snapshots to retain.</td>
    </tr>
    <tr>
      <td><h5>snapshot.num-retained.min</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">10</td>
      <td>Integer</td>
      <td>The minimum number of completed snapshots to retain.</td>
    </tr>
    <tr>
      <td><h5>snapshot.num-retained.max</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">Integer.MAX_VALUE</td>
      <td>Integer</td>
      <td>The maximum number of completed snapshots to retain.</td>
    </tr>
    </tbody>
</table>

Please note that too short retain time or too small retain number may result in:
- Batch query cannot find the file. For example, the table is relatively large and
  the batch query takes 10 minutes to read, but the snapshot from 10 minutes ago
  expires, at which point the batch query will read a deleted snapshot.
- Continuous reading jobs on FileStore (Without Log System) fail to restart. At the
  time of the job failover, the snapshot it recorded has expired.

## Performance

Table Store uses LSM data structure, which itself has the ability to support a large
number of updates. Update performance and query performance is a tradeoff, the
following parameters control this tradeoff:

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 20%">Option</th>
      <th class="text-center" style="width: 5%">Required</th>
      <th class="text-center" style="width: 5%">Default</th>
      <th class="text-center" style="width: 10%">Type</th>
      <th class="text-center" style="width: 60%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>num-sorted-run.compaction-trigger</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">5</td>
      <td>Integer</td>
      <td>The sorted run number to trigger compaction. Includes level0 files (one file one sorted run) and high-level runs (one level one sorted run).</td>
    </tr>
    </tbody>
</table>

The compaction-trigger determines the frequency of compaction. The smaller the number of
sorted run, the more compaction occurs, and the larger the number of sorted run, the less compaction occurs.

- The larger `num-sorted-run.compaction-trigger`, the less merge cost when updating data, which
  can avoid many invalid merges. However, if this value is too large, more memory 
  will be needed when merging files because each FileReader will take up a lot of
  memory.

- The smaller `num-sorted-run.compaction-trigger`, the better performance when querying, fewer
  files will be merged.

## Write Stalls

The Writer automatically maintains the structure of the LSM, which means that there
will be asynchronous threads constantly compaction, but if write speed is faster than the
compaction speed, write stalls may occur. Writing will be stopping.

If we don't limit writing, we will have the following problems:
- Increasing space scaling, which can lead to running out of disk space.
- Increasing read amplification, which greatly reduces read performance.

The following parameters determine when to stop writing:

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 20%">Option</th>
      <th class="text-center" style="width: 5%">Required</th>
      <th class="text-center" style="width: 5%">Default</th>
      <th class="text-center" style="width: 10%">Type</th>
      <th class="text-center" style="width: 60%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>num-sorted-run.stop-trigger</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">10</td>
      <td>Integer</td>
      <td>The number of sorted-runs that trigger the stopping of writes.</td>
    </tr>
    </tbody>
</table>

## Memory

There are three main places in the Table Store's sink writer that take up memory:
- MemTable's write buffer, which is individually occupied by each partition, each
  bucket, and this memory value can be adjustable by the `write-buffer-size`
  option (default 64 MB).
- The memory consumed by compaction for reading files, it can be adjusted by the
  `num-sorted-run.compaction-trigger` option to change the maximum number of files to be merged.
- The memory consumed by writing file, which is not adjustable.


## Scale Bucket

Since the LSM trees are built against each bucket, the number of total buckets dramatically influences the performance.
Table Store allows users to tune bucket numbers by `ALTER TABLE` command and reorganize data layout by `INSERT OVERWRITE` 
without recreating the table/partition. When executing overwrite jobs, the framework will automatically scan the data with
the bucket number recorded in manifest file and hash the record according to the current bucket numbers.

#### Rescale Overwrite
```sql
-- scale number of total buckets
ALTER TABLE table_dentifier SET ('bucket' = '...')

-- reorganize data layout of table/partition
INSERT OVERWRITE table_identifier [PARTITION (part_spec)]
SELECT ... 
FROM table_identifier
[WHERE part_spec]
``` 

Please beware that
- `ALTER TABLE` only modifies the table's metadata and will **NOT** reorganize or reformat existing data. 
  Reorganize exiting data must be achieved by `INSERT OVERWRITE`.
- Scale bucket number does not influence the read and running write jobs.
- Once the bucket number is changed, any new `INSERT INTO` jobs without reorganize table/partition 
  will throw a `TableException` with message like 
  ```text
  Try to write table/partition ... with a new bucket num ..., 
  but the previous bucket num is ... Please switch to batch mode, 
  and perform INSERT OVERWRITE to rescale current data layout first.
  ```


{{< hint info >}}
__Note:__ Currently, scale bucket is only supported for tables without enabling log system.
{{< /hint >}}

#### Use Case

Suppose there is a daily streaming ETL task to sync transaction data. The table's DDL and pipeline
are listed as follows.

```sql
-- table DDL
CREATE TABLE verified_orders (
    trade_order_id BIGINT,
    item_id BIGINT,
    item_price DOUBLE
    dt STRING
    PRIMARY KEY (dt, trade_order_id, item_id) NOT ENFORCED 
) PARTITIONED BY (dt)
WITH (
    'bucket' = '16'
);

-- streaming insert as bucket num = 16
INSERT INTO verified_orders
SELECT trade_order_id,
       item_id,
       item_price,
       DATE_FORMAT(gmt_create, 'yyyy-MM-dd') AS dt
FROM raw_orders
WHERE order_status = 'verified'
```
The pipeline has been running well for the past four weeks. However, the data volume has grown fast recently, 
and the job's latency keeps increasing. A possible workaround is to create a new table with a larger bucket number 
(thus the parallelism can be increased accordingly) and sync data to this new table.

However, there is a better solution with four steps.

- First, suspend the streaming job.

- Increase the bucket number.
  ```sql
  -- scaling out bucket number
  ALTER TABLE verified_orders SET ('bucket' = '32')
  ```

- Switch to batch mode and `INSER OVERWRITE` the partition.
  ```sql
  -- reorganize the data layout as bucket num = 32
  INSERT OVERWRITE verified_orders PARTITION (dt = 'yyyy-MM-dd')
  SELECT trade_order_id,
         item_id,
         item_price
  FROM verified_orders
  WHERE dt = 'yyyy-MM-dd' AND order_status = 'verified'
  ```

- Recompile the streaming job and restore from the latest checkpoint.

