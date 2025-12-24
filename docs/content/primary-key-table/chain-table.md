---
title: "Chain Table"
weight: 9
type: docs
aliases:
- /primary-key-table/chain-table.html
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

# Chain Table

Chain table is a new capability for primary key tables that transforms how you process incremental data.
Imagine a scenario where you periodically store a full snapshot of data (for example, once a day), even 
though only a small portion changes between snapshots. ODS binlog dump is a typical example of this pattern.

Taking a daily binlog dump job as an example. A batch job merges yesterday’s full dataset with today’s 
incremental changes to produce a new full dataset. This approach has two clear drawbacks:
* Full computation: Merge operation includes all data, and it will involve shuffle, which results in poor performance.
* Full storage: Store a full set of data every day, and the changed data usually accounts for a very small proportion.

Paimon addresses this problem by directly consuming only the changed data and performing merge-on-read. 
In this way, full computation and storage are turned into incremental mode:
* Incremental computation: The offline ETL daily job only needs to consume the changed data of the current day and do not require merging all data.
* Incremental Storage: Only store the changed data each day, and asynchronously compact it periodically (e.g., weekly) to build a global chain table within the lifecycle.
  {{< img src="/img/chain-table.png">}}

Based on the regular table, chain table introduces snapshot and delta branches to represent full and incremental 
data respectively. When writing, you specify the branch to write full or incremental data. When reading, paimon 
automatically chooses the appropriate strategy based on the read mode, such as full, incremental, or hybrid.

To enable chain table, you must config `chain-table.enabled` to true in the table options when creating the
table, and the snapshot and delta branch need to be created as well. Consider an example via Spark SQL:

```sql
CREATE TABLE default.t (
    `t1` string ,
    `t2` string ,
    `t3` string
) PARTITIONED BY (`date` string)
TBLPROPERTIES (
  'chain-table.enabled' = 'true',
  -- props about primary key table  
  'primary-key' = 'date,t1',
  'sequence.field' = 't2',
  'bucket-key' = 't1',
  'bucket' = '2',
  -- props about partition
  'partition.timestamp-pattern' = '$date', 
  'partition.timestamp-formatter' = 'yyyyMMdd'
);

CALL sys.create_branch('default.t', 'snapshot');

CALL sys.create_branch('default.t', 'delta');

ALTER TABLE default.t SET tblproperties 
    ('scan.fallback-snapshot-branch' = 'snapshot', 
     'scan.fallback-delta-branch' = 'delta');
 
ALTER TABLE `default`.`t$branch_snapshot` SET tblproperties
    ('scan.fallback-snapshot-branch' = 'snapshot',
     'scan.fallback-delta-branch' = 'delta');

ALTER TABLE `default`.`t$branch_delta` SET tblproperties 
    ('scan.fallback-snapshot-branch' = 'snapshot',
     'scan.fallback-delta-branch' = 'delta');
```

Notice that:
- Chain table is only supported for primary key table, which means you should define `bucket` and `bucket-key` for the table.
- Chain table should ensure that the schema of each branch is consistent.
- Only spark support now, flink will be supported later.
- Chain compact is not supported for now, and it will be supported later.

After creating a chain table, you can read and write data in the following ways.

- Full Write: Write data to t$branch_snapshot.
```sql
insert overwrite `default`.`t$branch_snapshot` partition (date = '20250810') 
    values ('1', '1', '1'); 
```

- Incremental Write: Write data to t$branch_delta.
```sql
insert overwrite `default`.`t$branch_delta` partition (date = '20250811') 
    values ('2', '1', '1');
```

- Full Query: If the snapshot branch has full partition, read it directly; otherwise, read on chain merge mode.
```sql
select t1, t2, t3 from default.t where date = '20250811'
```
you will get the following result:
```text
+---+----+-----+ 
| t1|  t2|   t3| 
+---+----+-----+ 
| 1 |   1|   1 |           
| 2 |   1|   1 |               
+---+----+-----+ 
```

- Incremental Query: Read the incremental partition from t$branch_delta
```sql
select t1, t2, t3 from `default`.`t$branch_delta` where date = '20250811'
```
you will get the following result:
```text
+---+----+-----+ 
| t1|  t2|   t3| 
+---+----+-----+      
| 2 |   1|   1 |               
+---+----+-----+ 
```

- Hybrid Query: Read both full and incremental data simultaneously.
```sql
select t1, t2, t3 from default.t where date = '20250811'
union all
select t1, t2, t3 from `default`.`t$branch_delta` where date = '20250811'
```
you will get the following result:
```text
+---+----+-----+ 
| t1|  t2|   t3| 
+---+----+-----+ 
| 1 |   1|   1 |           
| 2 |   1|   1 |  
| 2 |   1|   1 |               
+---+----+-----+ 
```
