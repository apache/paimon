---
title: "Rescale Bucket"
sidebar_position: 7
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

# Rescale Bucket

Since the number of total buckets dramatically influences the performance, Paimon allows users to 
tune bucket numbers by `ALTER TABLE` command and reorganize data layout by `INSERT OVERWRITE` 
without recreating the table/partition. When executing overwrite jobs, the framework will automatically 
scan the data with the old bucket number and hash the record according to the current bucket number.

## Rescale Overwrite
```sql
-- rescale number of total buckets
ALTER TABLE table_identifier SET ('bucket' = '...');

-- reorganize data layout of table/partition
INSERT OVERWRITE table_identifier [PARTITION (part_spec)]
SELECT ... 
FROM table_identifier
[WHERE part_spec];
``` 

Please note that
- `ALTER TABLE` only modifies the table's metadata and will **NOT** reorganize or reformat existing data. 
  Reorganize existing data must be achieved by `INSERT OVERWRITE`.
- Rescale bucket number does not influence the read and running write jobs.
- For **partitioned tables**, it is possible to have different bucket numbers for different partitions.
  This requires setting `'bucket.per-partition-count-enabled' = 'true'` on the table; otherwise every
  partition uses the single table-level bucket count. *E.g.*
  ```sql
  ALTER TABLE my_table SET ('bucket.per-partition-count-enabled' = 'true');

  ALTER TABLE my_table SET ('bucket' = '4');
  INSERT OVERWRITE my_table PARTITION (dt = '2022-01-01')
  SELECT * FROM ...;
  
  ALTER TABLE my_table SET ('bucket' = '8');
  INSERT OVERWRITE my_table PARTITION (dt = '2022-01-02')
  SELECT * FROM ...;
  ```
  After these operations, partition `dt=2022-01-01` uses 4 buckets, `dt=2022-01-02` uses 8 buckets, and any
  new partitions will use the latest table-level default (8 buckets in this case).
  Each partition retains its own bucket count from its data files,
  and the new bucket count only applies to newly created partitions or partitions that
  have been reorganized with `INSERT OVERWRITE`.

:::info
  Per-partition bucket counts are disabled by default. Set `'bucket.per-partition-count-enabled' = 'true'`
  to let partitions keep their own bucket count and be rescaled independently. When it is disabled, all
  partitions share the table-level `bucket` value.

  Note that enabling this option adds an extra manifest scan on write (to resolve each partition's bucket
  count), so only enable it when you actually need different bucket counts across partitions.
:::

:::warning
  Per-partition bucket counts are currently supported by the **Flink** engine only. The **Spark** writer
  still derives the bucket from the single table-level bucket count, so when partitions have different
  bucket counts (for example, after changing the table-level `bucket` while existing partitions keep
  their previous count), Spark may route rows to buckets that do not belong to the partition and corrupt
  the per-partition layout. Until Spark support is added, use Flink to write to tables that have
  per-partition bucket counts, or perform a full-table rescale so every partition shares the same bucket
  count before writing with Spark.
:::
- **Unpartitioned tables** require a full rescale before writing. If you change the bucket number and attempt
  to write without reorganizing the data first, a `RuntimeException` will be thrown:
  ```text
  Try to write table/partition ... with a new bucket num ...,
  but the previous bucket num is ... Please switch to batch mode,
  and perform INSERT OVERWRITE to rescale current data layout first.
  ```
- During overwrite period, make sure there are no other jobs writing the same table/partition.
- **Streaming jobs must be restarted after rescaling a partition.** The per-partition bucket mapping
  is loaded once when the streaming job starts (from the manifest files at that point in time). If a
  partition is rescaled while the streaming job is running, the job will continue routing rows using
  the old bucket count for that partition, which can cause rows to land in wrong buckets and lead to
  data correctness issues. The recommended workflow is: suspend the streaming job with a savepoint →
  perform the rescale overwrite → restart from the savepoint.


## Use Case

Rescale bucket helps to handle sudden spikes in throughput. Suppose there is a daily streaming ETL task to sync transaction data. The table's DDL and pipeline
are listed as follows.

```sql
-- table DDL
CREATE TABLE verified_orders (
    trade_order_id BIGINT,
    item_id BIGINT,
    item_price DOUBLE,
    dt STRING,
    PRIMARY KEY (dt, trade_order_id, item_id) NOT ENFORCED 
) PARTITIONED BY (dt)
WITH (
    'bucket' = '16'
);

-- like from a kafka table 
CREATE temporary TABLE raw_orders(
    trade_order_id BIGINT,
    item_id BIGINT,
    item_price BIGINT,
    gmt_create STRING,
    order_status STRING
) WITH (
    'connector' = 'kafka',
    'topic' = '...',
    'properties.bootstrap.servers' = '...',
    'format' = 'csv'
    ...
);

-- streaming insert as bucket num = 16
INSERT INTO verified_orders
SELECT trade_order_id,
       item_id,
       item_price,
       DATE_FORMAT(gmt_create, 'yyyy-MM-dd') AS dt
FROM raw_orders
WHERE order_status = 'verified';
```
The pipeline has been running well for the past few weeks. However, the data volume has grown fast recently,
and the job's latency keeps increasing. To improve the data freshness, users can
- Suspend the streaming job with a savepoint ( see
  [Suspended State](https://nightlies.apache.org/flink/flink-docs-stable/docs/internals/job_scheduling/) and
  [Stopping a Job Gracefully Creating a Final Savepoint](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/cli/#terminating-a-job) )
  ```bash
  $ ./bin/flink stop \
        --savepointPath /tmp/flink-savepoints \
        $JOB_ID
   ```
- Increase the bucket number
  ```sql
  -- scaling out
  ALTER TABLE verified_orders SET ('bucket' = '32');
  ```
- Switch to the batch mode and overwrite the current partition(s) to which the streaming job is writing
  ```sql
  SET 'execution.runtime-mode' = 'batch';
  -- suppose today is 2022-06-22
  -- case 1: there is no late event which updates the historical partitions, thus overwrite today's partition is enough
  INSERT OVERWRITE verified_orders PARTITION (dt = '2022-06-22')
  SELECT trade_order_id,
         item_id,
         item_price
  FROM verified_orders
  WHERE dt = '2022-06-22';
  
  -- case 2: there are late events updating the historical partitions, but the range does not exceed 3 days
  INSERT OVERWRITE verified_orders
  SELECT trade_order_id,
         item_id,
         item_price,
         dt
  FROM verified_orders
  WHERE dt IN ('2022-06-20', '2022-06-21', '2022-06-22');
  ```
- After overwrite job has finished, switch back to streaming mode. And now, the parallelism can be increased alongside with bucket number to restore the streaming job from the savepoint
  ( see [Start a SQL Job from a savepoint](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sqlclient/#start-a-sql-job-from-a-savepoint) )
  ```sql
  SET 'execution.runtime-mode' = 'streaming';
  SET 'execution.savepoint.path' = <savepointPath>;

  INSERT INTO verified_orders
  SELECT trade_order_id,
       item_id,
       item_price,
       DATE_FORMAT(gmt_create, 'yyyy-MM-dd') AS dt
  FROM raw_orders
  WHERE order_status = 'verified';
  ```