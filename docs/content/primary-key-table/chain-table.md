---
title: "Chain Table"
weight: 6
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

Chain table is a new feature for Primary Key tables that revolutionizes how you process incremental data.
Consider such a type of scenario: store a full set of data periodically, such as every day, but most of it
is redundant, with only a small amount of changed data every day. ODS Binlog Dump is just such a typical
scene.

Taking a daily binlog dump job as an example, the previous day's full data and the new incremental data
are merged to generate a new full dataset by a daily task. There are two obvious drawbacks in this way:
* Full computation: Merge operation includes all data, and it will involve shuffle, which results in poor performance.
* Full storage: Store a full set of data every day, and the changed data usually accounts for a very small proportion (e.g., 1%).

Paimon can solve this problem by consuming the changed data directly and merge on read, in this way, the
aforementioned full computation and storage can be optimized into incremental mode.
* Incremental computation: The offline ETL daily job only needs to consume the changed data of the current day and do not require merging all data.
* Incremental Storage: Only store the changed data each day, and asynchronously compact it periodically (e.g., weekly) to build a global chain table within the lifecycle.
  {{< img src="/img/chain-table.png">}}

Based on the regular paimon table, snapshot and delta branches are introduced to describe the full and incremental data
respectively. When writing, you can specify branch to write full or incremental data, and when reading, the
appropriate reading strategy is automatically selected based on the reading mode, such as full, incremental,
or hybrid reading.

To enable chain table, you must config `chain-table.enabled` to true in the table options when creating the
table, and the snapshot and delta branch need to be created as well. Consider an example via Spark SQL:

```sql
CREATE TABLE default.t (
    `t1` string ,
    `t2` string ,
    `t3` string
) PARTITIONED BY (`date` string)
TBLPROPERTIES (
  'chain-table.enabled' = 'true'
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
