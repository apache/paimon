---
title: "First Row"
sidebar_position: 4
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

# First Row

Set `merge-engine = first-row` to keep the first row for each primary key and ignore subsequent
rows for that key. This is useful for event or log deduplication. Unlike `deduplicate`, later
values do not replace the retained row.

## Create a Table

```sql
CREATE TABLE events (
    event_id BIGINT,
    payload STRING,
    PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
    'merge-engine' = 'first-row',
    'changelog-producer' = 'lookup'
);
```

Inputs `(1, 'first')` and `(1, 'later')` leave `(1, 'first')` in the table.

## Streaming Reads

The engine supports `none` and `lookup` changelog producers. Use
[`lookup`](../changelog-producer#lookup) for streaming reads that must emit a key only once:
lookup compaction checks existing keys and produces an insert-only changelog of newly retained
rows. `none` does not provide the same cross-commit streaming deduplication.

[Managed BLOB storage](../blob-storage#first-row-with-blob-fields) requires `none`, so it cannot
use this lookup-changelog streaming pattern.

## Ordering and Deletes

- User-defined [sequence fields](../sequence-rowkind#sequence-field) are not supported.
- `DELETE` and `UPDATE_BEFORE` records are rejected by default. Set `ignore-delete = true` to
  discard them when the source may emit retractions.
- The retained row follows ingestion ordering, not the smallest value of a business timestamp.

## Compaction and Visibility

By default, batch reads expose Level-0 data only after lookup compaction. Writers wait for lookup
compaction by default; asynchronous compaction can delay visibility. See
[Lookup Compaction](../compaction#lookup-compaction).

Do not enable deletion vectors for an ordinary `first-row` table. The
[PK Clustering Override](../pk-clustering-override) layout is a special case with its own requirements.
