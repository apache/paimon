---
title: "Streaming Recovery"
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

# Streaming Recovery

Spark checkpoints and Paimon Consumers solve different parts of recovery. Keep the checkpoint
for precise micro-batch recovery; use a Consumer to record a conservative table-side position
and protect data needed by that reader.

![Spark checkpoints restore micro-batches, while a Paimon Consumer records a conservative snapshot position and protects retained history.](/img/spark-streaming-recovery.svg)

## Configure a Consumer

You can assign a Paimon Consumer to a streaming query. The Consumer records a
table-side, snapshot-level recovery position and acts as a fence during normal
snapshot expiration. For tables with a decoupled changelog lifecycle, enabling
`consumer.changelog-only` makes the Consumer protect long-lived changelogs
instead of snapshots.

Configure the Consumer lifetime as a table property before starting the query:

```sql
ALTER TABLE table_name SET TBLPROPERTIES (
  'consumer.expiration-time' = '1 d'
);
```

```scala
val query = spark.readStream
  .format("paimon")
  .option("consumer-id", "my-consumer")
  .table("table_name")
  .writeStream
  .format("console")
  .option("checkpointLocation", "/path/to/spark/checkpoint")
  .start()
```

## Recovery Position

Spark checkpoint and Paimon Consumer progress have different granularities:

- When the Spark checkpoint is available, Spark uses it for precise micro-batch
  recovery.
- A new query without that checkpoint starts from the Consumer position. This
  recovery is conservative and may replay a completed snapshot if the query
  failed after processing it but before updating the Consumer.

Paimon updates the Consumer only from Spark's successful micro-batch commit
callback. When a micro-batch ends partway through a delta snapshot, the Consumer
may be created or refreshed at that snapshot so that the whole snapshot remains
protected and can be replayed. The Consumer advances past a snapshot only after
the micro-batch containing its last split is committed. A Consumer update failure
is propagated, fails the running query, and leaves the Consumer at its previous
conservative position.

Spark never advances Consumer progress past an incomplete snapshot. An incomplete
initial full snapshot does not create a Consumer because Consumer recovery from
that snapshot would use a delta scan. `consumer.mode` does not select a different
Spark source implementation.

Offsets restored from an older Spark checkpoint do not contain snapshot
completion metadata. Spark logs a warning and leaves the Consumer unchanged for
those offsets rather than advancing it without proof that the snapshot finished.

If the Consumer does not exist, the configured startup options are used. For
example, the default `latest-full` mode first reads the full snapshot and creates
the Consumer only after that snapshot is completely committed. If the Consumer
already exists, its next snapshot takes precedence over startup options and is
read incrementally, unless `consumer.ignore-progress` is enabled.

:::note

Spark can invoke the source commit callback while constructing a later
micro-batch. Therefore the Consumer may temporarily lag behind the latest
successful batch, especially while a query is idle or after its final available
batch. This is safe and can only cause conservative replay.

:::

## Retention and Query Ownership

Consumer files are scoped to the branch being read. Use one active Spark query
for each `(table, branch, consumer-id)` combination. Concurrent queries sharing
the same Consumer are unsupported because a faster query could move the shared
position past data still needed by a slower query.

Configure `consumer.expiration-time` according to the longest expected snapshot
processing time, failure recovery time, and idle interval. Eligible Spark source
commit callbacks refresh the Consumer file, but there is no separate timer-based
Spark heartbeat. Expired Consumer files are cleaned by non-write-only table
commits. A write-only writer does not perform Consumer expiration, so stale
Consumer IDs must be managed explicitly or by a separate non-write-only
maintenance process.

An initial full scan has no Consumer retention fence until it completes, so
snapshot retention should be long enough for that scan. If its snapshot expires
first, a restart without a Spark checkpoint performs a new initial full scan
according to the startup options.

When expired data would force recovery of a logged Spark batch to switch between
a full and an incremental scan, Paimon fails the query instead of combining the
two plans. Start a new query with a new checkpoint to apply the startup options
again.

## Manage Consumer Records

See [`reset_consumer`](./procedures/metadata#reset_consumer) and
[`clear_consumers`](./procedures/metadata#clear_consumers) for explicit maintenance.
Return to [Structured Streaming](./structured-streaming) for startup modes and trigger examples.
