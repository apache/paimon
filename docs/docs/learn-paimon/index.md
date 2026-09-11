---
title: "Learn Paimon"
sidebar_position: 100
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

# Learn Paimon

Learn how to choose a table design, follow changes from a writer to storage, and diagnose the
files that remain after an update or compaction. These guides connect Paimon's concepts through
small examples; the linked reference pages cover individual features in detail.

## Choose a Learning Path

| Your question | Start here | What you will learn |
| --- | --- | --- |
| Which table fits my workload? | [Scenario Guide](./scenario-guide) | Choose row semantics, distribution, and streaming output separately. |
| How do I store payloads and update AI features? | [AI Data Pipelines](./ai-pipelines) | Combine BLOBs, column updates, vector indexes, and Python processing. |
| What does a write actually change on disk? | [Understand Files](./understand-files) | Trace schema creation, inserts, deletes, compaction, and expiration. |
| Why do I have so many small files? | [Streaming Writes and Small Files](./small-files) | Separate write fragmentation, compaction backlog, and retained history. |

For a first read, follow the pages in that order. If you already operate a table, start with the
file walkthrough or the small-file diagnosis table.

## Before You Begin

Read [Basic Concepts](../concepts/basic-concepts) for snapshots, manifests, partitions, and buckets.
To run examples, configure a Paimon catalog using the [Flink quick start](../flink/quick-start),
[Spark quick start](../spark/quick-start), or [PyPaimon quick start](../pypaimon/quick-start).
Each guide identifies the engine and assumptions for its examples.

## Keep These Distinctions in Mind

- **Logical rows and physical records:** a primary-key query can return one row while several
  versions of that key remain in data files.
- **Commit and compaction:** a commit publishes a snapshot; compaction reorganizes files and
  publishes its result through a commit.
- **Compaction and expiration:** retiring a file from the current snapshot does not immediately
  remove it from storage. Retained history can still need it.
- **Storage and indexes:** storing a vector or text value does not automatically build a search
  index. Index coverage has its own lifecycle.

## Continue with the References

Use [Primary-Key Tables](../primary-key-table/) for merge and changelog behavior,
[Append Tables](../append-table/) for batch and ordered append layouts, and
[Multimodal Tables](../multimodal-table/) for column storage and search. For ongoing operations,
see [Maintenance](../maintenance/) and [System Tables](../concepts/system-tables).
