---
title: "Scale Bucket"
weight: 5
type: docs
aliases:
- /development/scale-bucket.html
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

# Scale Bucket

Since the LSM trees are built against each bucket, the number of total buckets dramatically influences the performance.
Table Store allows users to tune bucket numbers by `ALTER TABLE` command and reorganize data layout by `INSERT OVERWRITE` 
without recreating the table/partition. When executing overwrite jobs, the framework will automatically scan the data with
the bucket number recorded in manifest file and hash the record according to the current bucket numbers.

## Rescale Overwrite
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

## Use Case

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

