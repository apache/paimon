---
title: "PK Clustering Override"
weight: 10
type: docs
aliases:
- /primary-key-table/pk-clustering-override.html
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

# PK Clustering Override

By default, data files in a primary key table are physically sorted by the primary key. This is optimal for point
lookups but can hurt scan performance when queries filter on non-primary-key columns.

**PK Clustering Override** mode changes the physical sort order of data files from the primary key to user-specified
clustering columns. This significantly improves scan performance for queries that filter or group by clustering columns,
while still maintaining primary key uniqueness through deletion vectors.

## Quick Start

```sql
CREATE TABLE my_table (
    id BIGINT,
    dt STRING,
    city STRING,
    amount DOUBLE,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'pk-clustering-override' = 'true',
    'clustering.columns' = 'city',
    'deletion-vectors.enabled' = 'true',
    'bucket' = '4'
);
```

For `first-row` merge engine, deletion vectors are already built-in, so you don't need to enable them explicitly:

```sql
CREATE TABLE my_table (
    id BIGINT,
    dt STRING,
    city STRING,
    amount DOUBLE,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'pk-clustering-override' = 'true',
    'clustering.columns' = 'city',
    'merge-engine' = 'first-row',
    'bucket' = '4'
);
```

After this, data files within each bucket will be physically sorted by `city` instead of `id`. Queries like
`SELECT * FROM my_table WHERE city = 'Beijing'` can skip irrelevant data files by checking their min/max statistics
on the clustering column.

## Requirements

| Option | Requirement |
|--------|-------------|
| `pk-clustering-override` | `true` |
| `clustering.columns` | Must be set (one or more non-primary-key columns) |
| `deletion-vectors.enabled` | Must be `true` (not required for `first-row` merge engine) |
| `merge-engine` | `deduplicate` (default) or `first-row` only |

## When to Use

PK Clustering Override is beneficial when:

- Analytical queries frequently filter or aggregate on non-primary-key columns (e.g., `WHERE city = 'Beijing'`).
- The table uses `deduplicate` or `first-row` merge engine.

{{< hint info >}}
Although data files are no longer sorted by the primary key, filtering on bucket-key fields (which default to the
primary key) still benefits from bucket pruning. The query engine can skip entire buckets that do not contain matching
values, so queries like `WHERE id = 12345` remain efficient.
{{< /hint >}}

**Unsupported modes**:

- Merge engine: `partial-update` or `aggregation`.
- Changelog producer: `lookup` or `full-compaction`.
- Configue: `sequence.fields` or `record-level.expire-time`.
