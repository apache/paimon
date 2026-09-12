---
title: "Program API"
sidebar_position: 95
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

# Program API

Use Paimon's program APIs to manage catalogs, embed table reads and writes in an application,
or build a connector for a processing engine. Start with the interface that matches your application.

## Choose an API

| What you want to do | Start here |
| --- | --- |
| Read and write tables in a standalone Java application | [Java API](java-api) |
| Create databases and tables, or change schemas | [Catalog API](catalog-api) |
| Build a Flink DataStream job or ingest records with schema evolution | [Flink API](flink-api) |
| Call a REST catalog from a lightweight Java client | [REST Java Client](rest-api) |
| Integrate a native C++ engine | [C++ API](cpp-api) |
| Access Paimon from Rust | [Rust API](rust-api) |
| Work with Python, Arrow, or AI datasets | [PyPaimon](../pypaimon/) |
| Reduce repeated file reads | [Local Cache](file-cache) |

For SQL applications, start with [Flink](../flink/quick-start) or [Spark](../spark/quick-start).
These integrations handle execution, data distribution, and recovery for you.

![Program APIs: catalogs manage metadata, table APIs plan reads and prepare writes, and engines coordinate execution.](/img/program-api-overview.svg)

## Follow the Java workflow

1. **Set up the client.** Add the [dependency and create a catalog](java-api#dependency).
2. **Create or load a table.** Define its schema through the [Catalog API](catalog-api).
3. **Read or write data.** Follow [Java Reads](java-reading) or [Java Writes](java-writing)
   for batch and streaming examples.
4. **Integrate with your runtime.** Distribute splits and writer input, close resources, and
   coordinate checkpoints and commits. Use [types and predicates](java-types) to convert records
   and construct filters.

## Understand the boundaries

A **catalog** resolves table names and manages metadata. A **table** supplies builders for reads
and writes. A **scan** plans splits; readers consume those splits. Writers prepare file changes;
a committer publishes them in snapshots.

The low-level Java API exposes these building blocks. A custom distributed application must provide
scheduling, writer routing, and recovery. The [Flink builders](flink-api) connect Paimon to Flink's runtime.
The [REST Java client](rest-api) handles catalog requests; use a table API to read or write rows.
