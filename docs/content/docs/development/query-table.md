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

It is highly recommended to specify partition and primary key filters
along with the query, which will speed up the data skipping of the query.
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

## Real-time Streaming Consumption

By default, data is only visible after the checkpoint, which means
that the streaming reading has transactional consistency.

Immediate data visibility is configured via
`log.consistency` = `eventual`.

In this at-least-once mode, records are sent to downstream jobs ahead of time,
which means that duplicate data may be sent at job failover, and you may need to
manually de-duplicate data to achieve final consistency.
