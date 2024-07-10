---
title: "Overview"
weight: 1
type: docs
aliases:
- /append-table/overview.html
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

# Overview

If a table does not have a primary key defined, it is an append table. Compared to the primary key table, it does not
have the ability to directly receive changelogs. It cannot be directly updated with data through upsert. It can only
receive incoming data from append data.

{{< tabs "create-append-table" >}}
{{< tab "Flink" >}}
```sql
CREATE TABLE my_table (
    product_id BIGINT,
    price DOUBLE,
    sales BIGINT
) WITH (
    -- 'target-file-size' = '256 MB',
    -- 'file.format' = 'parquet',
    -- 'file.compression' = 'zstd',
    -- 'file.compression.zstd-level' = '3'
);
```
{{< /tab >}}
{{< /tabs >}}

Batch write and batch read in typical application scenarios, similar to a regular Hive partition table, but compared to
the Hive table, it can bring:

1. Object storage (S3, OSS) friendly
2. Time Travel and Rollback
3. DELETE / UPDATE with low cost
4. Automatic small file merging in streaming sink
5. Streaming read & write like a queue
6. High performance query with order and index
