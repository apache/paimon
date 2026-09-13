---
title: "Changelog Producer"
sidebar_position: 5
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

# Changelog Producer

The `changelog-producer` option controls the changes available to streaming readers. It is
separate from the [merge engine](./merge-engine/), which defines the table's logical rows, and
from the [table mode](./table-mode), which determines how those rows are stored and read.

A full changelog includes the old row needed to retract an update. For example, changing an
amount from `4` to `5` requires a downstream sum to subtract `4` and add `5`. An upsert containing
only `5` requires the consumer to remember the previous value.

## Choose a Producer

| Producer | Where old values come from | When changes are available | Use when |
| --- | --- | --- | --- |
| `none` (default) | Consumer state, if needed | Incremental snapshot reads | The consumer accepts upserts or can normalize them |
| `input` | Complete changelog supplied by the source | Committed input changelog files | The upstream already supplies the required before/after records |
| `lookup` | Lookup of existing rows during compaction | After lookup compaction is committed | The input has no before images and consumers need complete changes |
| `full-compaction` | Difference between full-compaction results | After a full compaction is committed | Consumers can wait for periodic full compactions |

The diagram follows one existing key whose value changes from `4` to `5`. `-U` is
`UPDATE_BEFORE`, and `+U` is `UPDATE_AFTER`.

![For an update from 4 to 5, none leaves old-state reconstruction to the consumer; input, lookup, and full-compaction obtain the before image at different stages.](/img/primary-key-changelog-producers.svg)

Deletion-vector tables support `none`, `input`, and `lookup`; they do not support the
`full-compaction` producer.

Producing extra changelog files adds work and storage. Choose the least expensive producer that
satisfies the consumer's contract. Check merge-engine restrictions as well:
[Partial Update](./merge-engine/partial-update), [Aggregation](./merge-engine/aggregation), and
[First Row](./merge-engine/first-row). [Managed BLOB storage](./blob-storage#requirements-and-limitations)
requires `none`.

## None

With `changelog-producer = none`, the writer creates no separate full changelog. Incremental
reads expose changes without complete before images; they are not an audit log of every input
record. A downstream upsert sink can replace its stored value by key.

Flink can add a stateful normalize operator when downstream processing needs old values. The
state and checkpoint cost depend on the number of keys and the workload. Do not remove this
operator with `scan.remove-normalize` unless the downstream computation remains correct without
before images.

[Nullable primary keys](./#nullable-primary-keys) cannot be exposed as a Flink SQL primary-key
constraint, so Flink cannot normalize an updating stream from such a table. Use a suitable full
changelog producer for that case.

## Input

```sql
'changelog-producer' = 'input'
```

Paimon saves the incoming records in separate changelog files and forwards them to streaming
readers. It does not reconstruct missing before images or convert partial input rows into the
final merged row.

Use this producer with a complete upstream changelog, such as suitable database CDC output or
Flink stateful computation. Verify that the source actually supplies the old values required by
your consumer.

## Lookup

```sql
'changelog-producer' = 'lookup'
```

Lookup compaction reads the existing value for a key, applies the incoming changes, and generates
the resulting changelog. By default, writers wait for lookup compaction before committing, unless
writer compaction is disabled with `write-only`. [Asynchronous Compaction](./compaction#asynchronous-compaction)
can improve write throughput at the cost of later changelog availability.

Lookup uses memory and local disk caches:

| Option | Default | Purpose |
| --- | --- | --- |
| `lookup.cache-file-retention` | `1 h` | Retain cached files; expired files may need to be fetched and indexed again |
| `lookup.cache-max-disk-size` | Unlimited | Bound local disk usage |
| `lookup.cache-max-memory-size` | `256 mb` | Bound in-memory cache usage |

In Flink, `execution.checkpointing.max-concurrent-checkpoints` can also affect throughput when
checkpoint completion waits for compaction. Tune it with checkpoint duration and resource usage.

`lookup` is incompatible with `full-compaction.delta-commits`. For periodic full compaction with
changelog generation, use `full-compaction` instead.

## Full Compaction

```sql
'changelog-producer' = 'full-compaction'
```

Paimon compares successive full-compaction results and emits their differences. Intermediate
updates between those results may be collapsed; this is a changelog of table-state changes,
not a copy of every source event.

`full-compaction.delta-commits` controls the number of delta commits between synchronous full
compactions. In Flink streaming writes with this producer, the interval defaults to one checkpoint
when it is not explicitly configured. Increase the interval when the consumer can tolerate a
longer delay and full compaction is too expensive.

For example, consumers with a latency budget of tens of minutes may use periodic full compaction.
For lower-latency generated changelogs, evaluate `lookup`.

## Filter Generated Changes

The `lookup` and `full-compaction` producers support:

| Option | Effect |
| --- | --- |
| `changelog-producer.row-deduplicate` | Avoid an update pair when the old and new rows are equal |
| `changelog-producer.ignore-update-before` | Omit `UPDATE_BEFORE` (`-U`) records |
| `changelog-producer.ignore-delete` | Omit `DELETE` (`-D`) records |

Dropping before images or deletes changes the consumer contract. Enable these filters only when
the consumer can handle the resulting stream; downstream retracting aggregations need those
records to remain correct.

## Changelog Merging

For `input`, `lookup`, and `full-compaction`, short Flink checkpoint intervals combined with many
buckets can produce numerous small changelog files.

Set `precommit-compact = true` to merge them before commit. This adds a compaction coordinator
and worker after the writer. The default is `false`. This file-consolidation step is distinct
from choosing how the changelog records are generated.
