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
and tables with deletion vectors. It reconciles Level-0 records with existing rows.

| Option | Behavior |
| --- | --- |
| `lookup-compact = radical` (default) | Force new Level-0 files into higher levels at compaction triggers |
| `lookup-compact = gentle` | Use the universal strategy with a configurable forced-compaction interval |
| `lookup-compact.max-interval` | Number of compaction-selection attempts without universal compaction work before forcing Level-0 compaction; only used in `gentle` mode |

To defer forced Level-0 compaction, use `gentle` together with an explicit
`lookup-compact.max-interval`. The interval has no default value; leaving it unset still forces
Level-0 compaction immediately when the universal strategy selects no work. It counts selection
attempts, not elapsed time.

Deferring compaction can reduce resource use, but pending files can reduce freshness. Choose the
interval together with `lookup-wait` and the visibility requirements of your table mode.

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
