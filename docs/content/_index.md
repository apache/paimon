---
title: Apache Paimon
type: docs
bookToc: false
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

# Apache Paimon

Apache Paimon is a lake format for building Lakehouse Architecture for both streaming and batch
operations. Paimon provides large-scale data lake storage for analytics, realtime streaming updates
powered by LSM (Log-structured merge-tree) structure, and multimodal data management for AI workloads
— all in a single unified format.

## Large-Scale Data Lake

Paimon is built for huge analytic datasets. A single table can contain tens of petabytes of data, and even
these huge tables can be read efficiently without a distributed SQL engine.

- **Time travel** enables reproducible queries that use exactly the same table snapshot, or lets users easily
  examine changes. Version rollback allows users to quickly correct problems by resetting tables to a good state.
- **Scan planning is fast** — data files are pruned with partition and column-level stats, using table metadata. 
  File Index (BloomFilter, Bitmap, Range Bitmap) and aggregate push-down further accelerate queries.
- **Schema evolution** supports add, drop, update, or rename columns, and has no side-effects.
- **Rich ecosystem** — adds tables to compute engines including Flink, Spark, Hive, Trino, Presto, StarRocks, and
  Doris, working just like a SQL table.
- **Incremental Clustering** with z-order/hilbert/order sorting to optimize data layout at low cost.

## Realtime Data Lake

Paimon's Primary Key Table brings realtime streaming updates into the lake architecture, powered by the LSM
(Log-structured merge-tree) structure.

- **Large-scale streaming updates** with very high performance, typically through Flink Streaming.
- **Multiple Merge Engines**: Deduplicate to keep last row, Partial Update to progressively complete records, 
  Aggregation to aggregate values, or First Row to keep the earliest record — update records however you like.
- **Multiple Table Modes**: Merge On Read (MOR), Copy On Write (COW), and Merge On Write (MOW) with Deletion Vectors
  for flexible read/write trade-offs.
- **Changelog Producers** (None, Input, Lookup, Full Compaction) produce correct and complete changelog for merge
  engines, simplifying your streaming analytics.
- **CDC Ingestion** from MySQL, Kafka, MongoDB, Pulsar, PostgreSQL, and Flink CDC with schema evolution support.

## Multimodal Data Lake

Paimon is a multimodal lakehouse for AI. Keep multimodal data, metadata, and embeddings in the same table and query
them via vector search, full-text search, or SQL.

- **Data Evolution** for efficient row-level updates and partial column changes without rewriting entire files — add
  new features (columns) as your application evolves, without copying existing data.
- **Blob Table** for storing multimodal data (images, videos, audio, documents, model weights) with separated storage
  layout — blob data is stored in dedicated `.blob` files while metadata stays in standard columnar files.
- **Global Index** with BTree Index for high-performance scalar lookups and Vector Index (DiskANN) for approximate
  nearest neighbor search.
- **PyPaimon** native Python SDK with no JDK dependency, seamlessly integrating with the Python AI ecosystem
  including Ray, PyTorch, Pandas and PyArrow for data loading, training, and inference workflows.

{{< columns >}}

## Try Paimon

If you're interested in playing around with Paimon, check out our
quick start guide with [Flink]({{< ref "flink/quick-start" >}}) or [Spark]({{< ref "spark/quick-start" >}}). It provides a step by
step introduction to the APIs and guides you through real applications.

<--->

## Get Help with Paimon

If you get stuck, you can subscribe User Mailing List (user-subscribe@paimon.apache.org),
Paimon tracks issues in GitHub and prefers to receive contributions as pull requests. You can also create an issue.

{{< /columns >}}
