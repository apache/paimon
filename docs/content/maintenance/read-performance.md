---
title: "Read Performance"
weight: 2
type: docs
aliases:
- /maintenance/read-performance.html
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

# Read Performance

There are many ways to improve read performance when reading Paimon data with Flink.

## Acceleration In Flink client
When use Flink batch job scan Paimon table, if you don't add option `scan.parallelism`, Paimon will infer the parallelism by read manifest files which take lots of time. So you can read Paimon table with the `scan.parallelism` table property.

## Read Compact Snapshot
You can read snapshot which commitKind = Compact, Compared to read other commitKind snapshot, reading this snapshot requires fewer merge operations, resulting in higher performance.

## Modify Compaction Config
You can reduce the option value of configuration `num-sorted-run.compaction-trigger` for higher performance. But this is a trade-off between writing and query performance.

## Modify DataFile's Compression Rates
You can add option `file.compression.zstd-level` to adjust DataFile's compression rates, the smaller the value, the higher the read performance.

## DeletionVector + Vectorized Computing Engine
You can set `deletion-vectors.enabled` = true, and use a computing engine with vectorized ability to read Paimon table. This way can fully utilize the SIMD capability of the CPU to accelerate data processing.

# Avoid OOM
Reading data often leads to OOM due to various reasons. Below are several methods to reduce the probability of OOM.

## Reduce The Concurrency Of Reading Manifest
The parallelism of scanning manifest files, default value is the size of cpu processor. We can control it by `scan.manifest.parallelism`, the smaller the value, the lower probability of OOM.

## Reduce The Stats In DataFileMeta
If your Paimon table has lots of columns, and you just want field B to use dense stat, other columns close stats. You can control this by follow options.

`metadata.stats-dense-store` = true

`metadata.stats-mode` = none

`fields.b.stats-mode` = truncate(16)

## Reduce The Record In Memory
If your Paimon table has some lager records which will occupy a lot of memory. Your can reduce the option `read.batch-size`, the default value is 1024.

## Optimize Memory When MOR
When merge on read in a sortedRun which include lots of DataFile, too many readers will consume too much memory and causing OOM.
Your can add option `sort-spill-threshold` to spill some DataFile before MOR.


