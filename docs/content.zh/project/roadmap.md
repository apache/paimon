---
title: "Roadmap"
weight: 1
type: docs
aliases:
- /project/roadmap.html
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

# Roadmap

Paimon's long-term goal is to become the better data lake platform for building the Streaming Lakehouse. Paimon will
invest in real-time, ecology, and data warehouse integrity for a long time.

If you have other requirements, please contact us.

## What’s Next?

### Core
- Lookup Changelog-Producer to produce changelog in real-time
- Enhance Flink Lookup Join from True Lookup
- Provide stable Java Programing API
- Savepoint support
- More Metrics, such as the busyness of compaction thread
- Multi table consistency for real-time materialized views

### Ingestion
- Schema Evolution Synchronization from Flink CDC
- Entire Database Synchronization from Flink CDC
- Integration with Apache Seatunnel

### Compute Engines
- Flink DELETE/UPDATE support
- More management via Flink/Spark `CALL` procedures. 
- Hive Writer
- Spark Writer supports `INSERT OVERWRITE`
- Spark Time Traveling
- Presto Reader
- Doris Reader
