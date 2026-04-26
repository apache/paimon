---
title: "Global Index"
weight: 6
type: docs
aliases:
- /pypaimon/global-index.html
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

# Global Index

PyPaimon supports querying global indexes built on Data Evolution (append) tables. Three index types are available:

- **BTree Index**: B-tree based index for scalar column lookups. Supports equality, IN, range, and combined predicates.
- **Vector Index (Lumina)**: Approximate nearest neighbor (ANN) index for vector similarity search.
- **Full-Text Index (Tantivy)**: Full-text search index for text retrieval with relevance scoring.

> Global indexes must be built beforehand (e.g., via Spark or Flink). See [Global Index]({{< ref "append-table/global-index" >}}) for how to create indexes.

## BTree Index

BTree index is automatically used during scan when a filter predicate matches the indexed column. No special API is needed — just set a filter on the read builder.

```python
import pypaimon

catalog = pypaimon.create_catalog(...)
table = catalog.get_table("db.my_table")

# BTree index is used automatically when filtering on indexed columns
read_builder = table.new_read_builder()
read_builder = read_builder.with_filter(
    pypaimon.PredicateBuilder(table.fields)
    .in_("name", ["a200", "a300"])
)

scan = read_builder.new_scan()
read = read_builder.new_read()
splits = scan.plan().splits
data = read.to_arrow(splits)
```

Supported predicates: `equal`, `not_equal`, `less_than`, `less_or_equal`, `greater_than`, `greater_or_equal`, `in_`, `not_in`, `between`, `is_null`, `is_not_null`.

## Vector Index (Lumina)

Use `VectorSearchBuilder` to perform approximate nearest neighbor search on a vector column, then read the matched rows.

```python
table = catalog.get_table("db.my_table")

# Step 1: vector search to get matching row IDs
builder = table.new_vector_search_builder()
index_result = (
    builder
    .with_vector_column("embedding")
    .with_query_vector([1.0, 2.0, 3.0, ...])
    .with_limit(10)
    .execute_local()
)

# Step 2: read actual data for matched rows
read_builder = table.new_read_builder()
scan = read_builder.new_scan()
scan.with_global_index_result(index_result)
read = read_builder.new_read()
data = read.to_arrow(scan.plan().splits)
```

You can also add a scalar filter to pre-filter rows before vector search:

```python
predicate = (
    pypaimon.PredicateBuilder(table.fields)
    .equal("category", "electronics")
)

index_result = (
    table.new_vector_search_builder()
    .with_vector_column("embedding")
    .with_query_vector([1.0, 2.0, 3.0, ...])
    .with_limit(10)
    .with_filter(predicate)
    .execute_local()
)

read_builder = table.new_read_builder()
scan = read_builder.new_scan()
scan.with_global_index_result(index_result)
read = read_builder.new_read()
data = read.to_arrow(scan.plan().splits)
```

## Full-Text Index (Tantivy)

Use `FullTextSearchBuilder` to perform full-text search on a text column, then read the matched rows.

```python
table = catalog.get_table("db.my_table")

# Step 1: full-text search to get matching row IDs
builder = table.new_full_text_search_builder()
index_result = (
    builder
    .with_text_column("content")
    .with_query_text("search keywords")
    .with_limit(20)
    .execute_local()
)

# Step 2: read actual data for matched rows
read_builder = table.new_read_builder()
scan = read_builder.new_scan()
scan.with_global_index_result(index_result)
read = read_builder.new_read()
data = read.to_arrow(scan.plan().splits)
```

For better performance when reading from remote storage, consider enabling the [Local Disk Cache]({{< ref "program-api/file-cache" >}}).
