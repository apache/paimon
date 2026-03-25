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

After this, data files within each bucket will be physically sorted by `city` instead of `id`. Queries like
`SELECT * FROM my_table WHERE city = 'Beijing'` can skip irrelevant data files by checking their min/max statistics
on the clustering column.
s
## How It Works

PK Clustering Override replaces the default LSM compaction with a two-phase clustering compaction:

**Phase 1 — Sort by Clustering Columns**: Newly flushed (level 0) files are read, sorted by the configured clustering
columns, and rewritten as sorted (level 1) files. A key index tracks each primary key's file and row position to
maintain uniqueness.

**Phase 2 — Merge Overlapping Sections**: Sorted files are grouped into sections based on clustering column range
overlap. Overlapping sections are merged together. Adjacent small sections are also consolidated to reduce file count
and IO amplification. Non-overlapping large files are left untouched.

During both phases, deduplication is handled via deletion vectors:

- **Deduplicate mode**: When a key already exists in an older file, the old row is marked as deleted.
- **First-row mode**: When a key already exists, the new row is marked as deleted, keeping the first-seen value.

When the number of files to merge exceeds `sort-spill-threshold`, smaller files are first spilled to row-based
temporary files to reduce memory consumption, preventing OOM during multi-way merge.

## Requirements

| Option | Requirement |
|--------|-------------|
| `pk-clustering-override` | `true` |
| `clustering.columns` | Must be set (one or more non-primary-key columns) |
| `deletion-vectors.enabled` | Must be `true` |
| `merge-engine` | `deduplicate` (default) or `first-row` only |

## When to Use

PK Clustering Override is beneficial when:

- Analytical queries frequently filter or aggregate on non-primary-key columns (e.g., `WHERE city = 'Beijing'`).
- The table uses `deduplicate` or `first-row` merge engine.
- You want data files physically co-located by a business dimension rather than the primary key.

It is **not** suitable when:

- Point lookups by primary key are the dominant access pattern (default LSM sort is already optimal).
- You need `partial-update` or `aggregation` merge engine.
- `sequence.fields` or `record-level.expire-time` is required.
- Changelog producer`lookup` or `full-compaction` is required.
