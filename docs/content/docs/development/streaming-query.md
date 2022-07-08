---
title: "Streaming Query"
weight: 5
type: docs
aliases:
- /development/streaming-query.html
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

# Streaming Query

Currently, only Flink supports streaming query.

The Table Store is streaming batch unified, you can read full
and incremental data depending on the runtime execution mode:

```sql
-- Batch mode, read latest snapshot
SET 'execution.runtime-mode' = 'batch';
SELECT * FROM MyTable;

-- Streaming mode, streaming reading, read incremental snapshot, read the snapshot first, then read the incremental
SET 'execution.runtime-mode' = 'streaming';
SELECT * FROM MyTable;

-- Streaming mode, streaming reading, read latest incremental
SET 'execution.runtime-mode' = 'streaming';
SELECT * FROM MyTable /*+ OPTIONS ('log.scan'='latest') */;
```

Different `log.scan` mode will result in different consuming behavior under streaming mode.
<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 20%">Scan Mode</th>
      <th class="text-center" style="width: 5%">Default</th>
      <th class="text-center" style="width: 60%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>FULL</h5></td>
      <td>Yes</td>
      <td>FULL scan mode performs a hybrid reading with a snapshot scan and the streaming incremental scan.</td>
    </tr>
    <tr>
      <td><h5>LATEST</h5></td>
      <td>No</td>
      <td>LATEST scan mode only reads incremental data from the latest offset.</td>
    </tr>
    </tbody>
</table>

## Streaming Query on Files

You can incrementally consume tables directly on the lake store files. This mode has
a lower cost compared to Kafka, but the latency will be bigger, depending on the
checkpoint interval of the writing stream job.

By default, the downstream streaming consumption is a disordered (ordered within the key)
stream of upsert data. If you expect an ordered CDC data stream, you can configure it
as follows (recommended):

```sql
CREATE TABLE T (...)
WITH (
    'changelog-file' = 'true',
    'log.changelog-mode' = 'all'
)
```

## Streaming Query on Kafka

You can configure the Kafka topic for the table, and the data written will be
double-written to Kafka, and streaming reads will be incremental snapshots
reading on hybrid storage of files and Kafka.

```sql
CREATE TABLE T (...)
WITH (
    'log.system' = 'kafka',
    'kafka.bootstrap.servers' = '...',
    'kafka.topic' = '...'
)
```
The partition of the Kafka topic needs to be the same as the number of buckets.

By default, data is only visible after the checkpoint, which means
that the streaming reading has transactional consistency.

Immediate data visibility is configured via
`log.consistency` = `eventual`.

Due to the tradeoff between data freshness and completeness, immediate data visibility is barely
accomplished under exactly-once semantics. Nevertheless, users can relax the constraint to use
at-least-once mode to achieve it. Note that records may be sent to downstream jobs ahead of the committing
(since no barrier alignment is required), which may lead to duplicate data during job failover. As a result,
users may need to manually de-duplicate data to achieve final consistency.
