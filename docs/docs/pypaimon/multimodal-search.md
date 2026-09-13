---
title: "Vector and Full-text Search"
description: "Build indexes and retrieve candidates using vector, full-text, or hybrid queries."
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

# Vector and Full-text Search

Build indexes and retrieve candidates using vector, full-text, or hybrid queries. Start with the `docs` table from [Multimodal Tables](./multimodal-tables#create), including its `content` and `embedding` columns. Examples with additional vector or partition columns require a table with those fields.

## Prerequisites

The index implementations used below require optional packages:

```shell
python -m pip install 'pypaimon[vindex,full-text]'
```

Use Python 3.9 or newer for this combination. Index creation requires data in the
target columns; replace the small illustrative vectors with data appropriate for
the chosen index's training requirements. In the examples, `docs` is the table,
`pm` is `pypaimon.multimodal`, and query vectors must match their column dimensions.

## Create Index

Use `create_index` to create the global indexes used by search APIs. The
`index_type` argument is required; the multimodal API does not choose a default
index implementation. Use `full-text` for full-text indexes.

```python
docs.create_index("embedding", index_type="ivf-pq")
docs.create_index("content", index_type="full-text")
```

## Search

Use `search` for one vector query or one full-text query.

If the table has exactly one vector column, `column` can be omitted for vector
search. A string query is shorthand for a full-text match query when the table
has exactly one text column. To target a specific text column, pass `column`.

Use `pre_filter` to prune search candidates before ranking. Use `where()` to
filter the rows read from the search result. Both `pre_filter` and `where()`
accept SQL-like predicate strings. For full-text search, `pre_filter` must only
reference partition columns.

```python
neighbors = (
    docs.search(
        [0.1, 0.2, 0.3],
        column="embedding",
        pre_filter="category = 'lake'",
    )
    .limit(10)
    .to_arrow()
)

matches = (
    docs.search("paimon vector", column="content")
    .limit(10)
    .to_pandas()
)
```

```python
matches = (
    docs.search(
        '{"match":{"query":"paimon vector","operator":"And"}}',
        column="content",
    )
    .limit(10)
    .to_list()
)
```

## Search Hybrid

Use `search_hybrid` to combine vector and full-text routes, then rerank the
merged candidates. Create the indexes required by the routes you use.

Pass route specs to `search_hybrid`. Each route can set its own candidate
`limit`, `weight`, and vector index `options`. String text routes infer the
text column when the table has exactly one text column. To target a specific
text column, pass `column` to `pm.text_route`.

`pre_filter` is applied before ranking. It accepts a SQL-like predicate string.
When a hybrid query has a full-text route, `pre_filter` must only reference
partition columns.

```python
# This example assumes the table is partitioned by dt.
hybrid = (
    docs.search_hybrid(
        [
            pm.vector_route("embedding", query_vector, limit=50),
            pm.text_route("paimon vector", column="content", weight=0.2),
        ],
        pre_filter="dt = '2026-07-01'",
    )
    .rerank("rrf")
    .limit(10)
    .to_list()
)
```

For multiple vector fields, add multiple vector routes:

```python
hybrid = (
    docs.search_hybrid(
        [
            pm.vector_route(
                "image_embedding",
                image_vector,
                weight=0.7,
                limit=50,
                options={"nprobe": "8"},
            ),
            pm.vector_route(
                "text_embedding",
                text_vector,
                weight=0.3,
                limit=50,
            ),
            pm.text_route("paimon vector", column="content", weight=0.2),
        ],
        ranker="weighted_score",
    )
    .limit(10)
    .to_list()
)
```

Dictionary route specs are also accepted:

```python
hybrid = docs.search_hybrid(
    [
        {
            "column": "image_embedding",
            "vector": image_vector,
            "weight": 0.7,
            "limit": 50,
            "options": {"nprobe": "8"},
        },
        {
            "column": "text_embedding",
            "vector": text_vector,
            "weight": 0.3,
            "limit": 50,
        },
    ],
)
```

Dictionary vector routes also accept `anns_field`, `data`, and `param` aliases.

## Search Vectors

Use `search_vectors` for multiple query vectors against one vector column. It
returns one result set for each input vector, preserving input order.

```python
batch_neighbors = (
    docs.search_vectors(
        [
            [0.1, 0.2, 0.3],
            [0.4, 0.5, 0.6],
        ],
        column="embedding",
        pre_filter="category = 'lake'",
    )
    .limit(10)
    .to_list()
)
```
