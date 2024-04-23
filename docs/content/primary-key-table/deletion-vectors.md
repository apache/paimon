---
title: "Deletion Vectors"
weight: 6
type: docs
aliases:
- /primary-key-table/deletion-vectors.html
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

# Deletion Vectors

## Overview

The Deletion Vectors mode is designed to takes into account both data reading and writing efficiency.

In this mode, additional overhead (looking up LSM Tree and generating the corresponding Deletion File) will be introduced during writing,
but during reading, data can be directly retrieved by employing data with deletion vectors, avoiding additional merge costs between different files.

Furthermore, data reading concurrency is no longer limited, and non-primary key columns can also be used for filter push down.
Generally speaking, in this mode, we can get a huge improvement in read performance without losing too much write performance.

{{< img src="/img/deletion-vectors-overview.png">}}

## Usage

By specifying `'deletion-vectors.enabled' = 'true'`, the Deletion Vectors mode can be enabled.

## Limitation

- `changelog-producer` needs to be `none` or `lookup`.
- `changelog-producer.lookup-wait` can't be `false`.
- `merge-engine` can't be `first-row`, because the read of first-row is already no merging, deletion vectors are not needed.
- This mode will filter the data in level-0, so when using time travel to read `APPEND` snapshot, there will be data delay.
