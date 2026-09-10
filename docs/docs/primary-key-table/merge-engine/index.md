---
title: "Merge Engine"
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

<a id="overview"></a>

# Merge Engines

A merge engine defines how records with the same primary key form one logical row. Set the
`merge-engine` table option when creating a table. The choice affects the meaning of updates,
null values, and retractions; it is separate from the [storage mode](../table-mode).

## Choose a Merge Engine

| Engine | Result for the same key | Typical input | Details |
| --- | --- | --- | --- |
| `deduplicate` (default) | Keep the latest row; a latest retract removes it | Complete replacement rows, such as CDC updates | [Deduplicate](#deduplicate) |
| `partial-update` | Update non-null fields; sequence groups can also explicitly set nulls | Updates to different columns of an entity | [Partial Update](./partial-update) |
| `aggregation` | Aggregate each value field using its configured function | Contributions such as sums or maxima | [Aggregation](./aggregation) |
| `first-row` | Keep the first row and ignore later rows for the key | Deduplicating events or logs | [First Row](./first-row) |

"Latest" follows Paimon's record ordering. Configure a
[sequence field](../sequence-rowkind#sequence-field) if arrival order does not represent the
business order. Partial updates from independent streams can use per-column
[sequence groups](./partial-update#sequence-group).

:::info Flink SQL input ordering

Set `table.exec.sink.upsert-materialize` to `NONE` in the Flink SQL TableConfig for these merge
pipelines. The sink materializer can reorder records before Paimon receives them. Use sequence
fields to express the required update order explicitly.

:::

## Deduplicate

`deduplicate` keeps the latest complete row. A null value in that row replaces an earlier non-null
value; use `partial-update` if null should mean "leave this field unchanged".

For example, two inputs `(1, 'open', 12.00)` and `(1, 'closed', 15.00)`, where the first column is
the key, produce `(1, 'closed', 15.00)`.

```sql
CREATE TABLE orders (
    order_id BIGINT,
    status STRING,
    amount DECIMAL(12, 2),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'merge-engine' = 'deduplicate'
);
```

If the latest record is `DELETE` or `UPDATE_BEFORE`, the logical row is removed. Set
`ignore-delete = true` only when the pipeline should discard these retract records.

## Streaming Results

The merge engine defines stored table state; the
[changelog producer](../changelog-producer) defines what streaming readers receive. For example,
`input` forwards the original input records, which may be partial updates rather than complete
merged rows. Check the streaming requirements on each engine's page before building a downstream
aggregation or sink.
