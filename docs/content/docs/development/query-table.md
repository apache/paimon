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

-- Streaming mode, read incremental snapshot, read the snapshot first, then read the increment
SET 'execution.runtime-mode' = 'streaming';
SELECT * FROM MyTable;

-- Streaming mode, read latest incremental
SET 'execution.runtime-mode' = 'streaming';
SELECT * FROM MyTable /*+ OPTIONS ('log.scan'='latest') */;
```

## Query Optimization

It is highly recommended taking partition and primary key filters
in the query, which will speed up the data skipping of the query.

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

If you want the data to be immediately visible, you need to:
- 'log.system' = 'kafka', you can't use the FileStore's continuous consumption
  capability because the FileStore only provides checkpoint-based visibility.
- 'log.consistency' = 'eventual', this means that writes are visible without 
  using LogSystem's transaction mechanism.
- All tables need to have primary key defined, because only then can the
  data be de-duplicated by normalize node of downstream job.

## Streaming Low Cost

By default, for the table with primary key, the records in the table store only
contains INSERT, UPDATE_AFTER, DELETE. No UPDATE_BEFORE. A normalized node is
generated in downstream consuming job, the node will store all key-value for
producing UPDATE_BEFORE message.

If you want to remove downstream normalized node (It's costly) or see the all
changes of this table, you can configure:
- 'log.changelog-mode' = 'all'
- 'log.consistency' = 'transactional' (default)

The query written to the table store must contain all message types with
UPDATE_BEFORE, otherwise the planner will throw an exception.
