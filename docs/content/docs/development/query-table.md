---
title: "Query Table"
weight: 4
type: docs
aliases:
- /development/query-table.html
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

# Query Table

The Table Store is streaming batch unified, you can read full
and incremental data depending on the runtime execution mode:

```sql
-- Batch mode, read latest snapshot
SET 'execution.runtime-mode' = 'batch';
SELECT * FROM MyTable;

-- Streaming mode, read incremental snapshot, read the snapshot first, then read the incremental
SET 'execution.runtime-mode' = 'streaming';
SELECT * FROM MyTable;

-- Streaming mode, read latest incremental
SET 'execution.runtime-mode' = 'streaming';
SELECT * FROM MyTable /*+ OPTIONS ('log.scan'='latest') */;
```

## Query Optimization

It is highly recommended to take partition and primary key filters
along with the query, which will speed up the data skipping of the query.

Supported filter functions are:
- `=`
- `<>`
- `<`
- `<=`
- `>`
- `>=`
- `in`
- starts with `like`

## Streaming Real-time

By default, data is only visible after the checkpoint, which means
that the streaming reading has transactional consistency.

If you want the data to be immediately visible, you need to set the following options:

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 20%">Table Option</th>
      <th class="text-center" style="width: 5%">Default</th>
      <th class="text-center" style="width: 60%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>`log.system` = `kafka`</h5></td>
      <td>No</td>
      <td>You need to enable log system because the FileStore's continuous consumption only provides checkpoint-based visibility.</td>
    </tr>
    <tr>
      <td><h5>`log.consistency` = `eventual`</h5></td>
      <td>No</td>
      <td>This means that writes are visible without using LogSystem's transaction mechanism.</td>
    </tr>
    </tbody>
</table>

Note: All tables need to have the primary key defined because only then can the
data be de-duplicated by the normalizing node of the downstream job.

## Streaming Low Cost

By default, for the table with the primary key, the records in the table store only
contain `INSERT`, `UPDATE_AFTER`, and `DELETE`. The downstream consuming job will
generate a normalized node, and it stores all processed key-value to produce the
`UPDATE_BEFORE` message, which will bring extra overhead.

If you want to remove downstream normalized node (It's costly) or see the all
changes of this table, you can configure:
- 'log.changelog-mode' = 'all'
- 'log.consistency' = 'transactional' (default)

The inserted query written to the table store must contain all message types with
`UPDATE_BEFORE`, otherwise the planner will throw an exception. It means that Planner
expects the inserted query to produce a real changelog, otherwise the data would
be wrong.
