---
title: "Compaction"
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

# Compaction

Compaction combines sorted runs and applies the [merge engine](./merge-engine/) to records with
the same key. It reduces file and merge overhead for reads, while consuming CPU and storage I/O
on the write side. Paimon's default strategy selects runs using a universal compaction policy.

![Compaction merges several overlapping sorted runs into a new run, while old files remain available to retained snapshots.](/img/primary-key-compaction.svg)

Depending on the table options, compaction also generates a
[changelog](./changelog-producer), maintains [deletion vectors](./table-mode#merge-on-write) and
[indexes](./global-index#maintenance-and-coverage), or applies record-level expiration.
Compaction itself does not mean that old files are immediately deleted: snapshot, tag, and
partition retention have separate lifecycles. See [Maintenance](../maintenance/).

## Choose a Compaction Strategy

| Need | Configuration or operation | Trade-off |
| --- | --- | --- |
| Control overlapping runs | Default background compaction and sorted-run thresholds | Lower thresholds spend more write resources to reduce read work |
| Fresh lookup changelogs or MOW rows | Lookup compaction with the default wait behavior | Commit latency includes required compaction work |
| Favor write throughput | [Asynchronous Compaction](#asynchronous-compaction) | More pending files and potentially older query results |
| Isolate compaction resources or coordinate writers | [Dedicated compaction job](#dedicated-compaction-job) | Requires a separately operated job |
| Refresh a read-optimized view | `compaction.optimization-interval` | Freshness follows completed full compactions |
| Fully merge every N delta commits | `full-compaction.delta-commits` | Synchronous full compaction increases write amplification |

## Asynchronous Compaction

Writers normally run compaction in background threads. They can still wait when too many sorted
runs accumulate, or when lookup compaction is needed before a commit.

The following table options illustrate a configuration that relaxes those waits:

```properties
num-sorted-run.stop-trigger = 2147483647
sort-spill-threshold = 10
lookup-wait = false
```

This effectively removes the sorted-run write-stall limit and allows pending work to accumulate.
It is a throughput-oriented example, not a default recommendation. Size the spill storage and
monitor file counts, compaction backlog, and read latency before using it.

By default, MOW and `first-row` batch reads exclude pending Level-0 data until lookup compaction
publishes it. MOW batch readers can opt into merging pending data with
`deletion-vectors.merge-on-read`; see [MOW visibility](./table-mode#merge-on-write).
For `changelog-producer = lookup`, generated changelogs are also delayed. A compactor that cannot
keep up with sustained input will keep falling behind; relaxing waits does not add capacity.

## Dedicated compaction job

Set `write-only = true` on ingest writers and run a
[dedicated compaction job](../maintenance/dedicated-compaction#dedicated-compaction-job) when
compaction needs separate resources or multiple writers need a single compaction owner.
Avoid overlapping compaction jobs for the same partition, which can cause commit conflicts.

This does not lift [dynamic-bucket](./data-distribution#dynamic-bucket) restrictions on concurrent
writers to the same partition.

## Record-Level expire

Configure `record-level.expire-time` for the retention duration and `record-level.time-field` for
the field used to evaluate each record's age.

Expiration happens when compaction processes the records, so it has no strict wall-clock deadline.
A manual full compaction can process records that ordinary compaction has not reached. This is
separate from expiring snapshots or entire partitions.

## Full Compaction

Full compaction merges all runs of a bucket into its highest level. The default strategy can
select a full compaction as data accumulates. To request one regularly:

| Option | Scheduling | Use |
| --- | --- | --- |
| `compaction.optimization-interval` | Time-based optimization compaction | Keep the [read-optimized table](../concepts/system-tables#read-optimized-table) reasonably fresh |
| `full-compaction.delta-commits` | Synchronous compaction after a number of delta commits | COW behavior or periodic full-compaction changelogs |

`full-compaction.delta-commits` is incompatible with `changelog-producer = lookup`. See
[Full Compaction Changelogs](./changelog-producer#full-compaction) for producer-specific defaults.

## Lookup Compaction

Paimon uses lookup compaction for the `lookup` changelog producer, the `first-row` merge engine,
and tables with deletion vectors. It can also be enabled explicitly with `force-lookup = true`.
Lookup compaction reconciles Level-0 records with existing rows and promotes them to higher
levels, making processed rows or generated changelogs available to readers.

The following options control forced Level-0 promotion for these lookup scenarios. Ordinary
MOR tables can merge Level-0 data during reads and do not enable this strategy by default.
Non-lookup tables can opt into immediate forced promotion separately with
`compaction.force-up-level-0 = true`.

| Option | Behavior |
| --- | --- |
| `lookup-compact = radical` (default) | Immediately select Level-0 files for promotion when the universal strategy selects no compaction work |
| `lookup-compact = gentle` | Allow the universal strategy to select work, with forced Level-0 promotion after the effective interval |
| `lookup-compact.max-interval` | Control the forced-promotion interval in `gentle` mode; count only compaction-selection attempts where the universal strategy selects no work |

Setting `lookup-compact = gentle` alone already defers forced Level-0 promotion. When
`lookup-compact.max-interval` is unset, its effective runtime default is
`2 * num-sorted-run.compaction-trigger`: **10** with the default trigger of **5**. An explicitly
configured interval is clamped to at least `num-sorted-run.compaction-trigger`; for example,
configuring `3` with a trigger of `5` gives an effective interval of `5`.

The interval counts selection attempts, not elapsed time or commits. The universal strategy can
still select compaction work before this interval is reached.

Gentle mode can reduce compaction frequency, but pending Level-0 files can delay the visibility
of DV/`first-row` data and lookup changelogs. Choose the interval together with `lookup-wait` and
the [visibility requirements](#asynchronous-compaction) of your table mode.

## Compaction Options

### Number of Sorted Runs to Pause Writing

`num-sorted-run.stop-trigger` limits the backlog at which writers wait for compaction. If unset,
it defaults to `num-sorted-run.compaction-trigger + 3`.

Increasing the limit can reduce write stalls but leaves more overlapping runs for readers. Use
`sort-spill-threshold` to allow merge readers to spill when their number exceeds the configured
threshold. This option has no explicit configured default; spilling trades memory pressure for
local disk I/O. Size it for the available memory and disk resources.

### Number of Sorted Runs to Trigger Compaction

`num-sorted-run.compaction-trigger` defaults to `5`. Each Level-0 file counts as one sorted run;
each occupied higher level counts as one run.

A larger value usually makes compaction less frequent and leaves more merge work for reads.
A smaller value spends more write resources keeping reads efficient. Tune it alongside the
stop threshold, rather than increasing either threshold to hide a persistent compaction backlog.

For the complete option reference, see [Configurations](../maintenance/configurations#coreoptions).
