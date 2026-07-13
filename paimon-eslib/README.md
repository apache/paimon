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

# paimon-eslib: ES Index (Multi-Column Global Index)

`paimon-eslib` provides the `es-index` global index: a **Lucene-backed, multi-column, multi-modal**
index. A single `es-index` file indexes a **primary column** (typically a vector) together with
optional **companion columns** — full-text, keyword, scalar, and date — so vector search and
full-text search can be served from the same index, and companion columns stay available for scoring
and filtering inside one index definition.

Unlike the single-purpose vector and full-text global indexes — each of which indexes one column with
one dedicated engine — `es-index` groups a primary column plus optional companion columns into one
index. The primary column determines the main modality (usually a vector); companion columns are
indexed according to their data type and per-field options.

> **Plugin required.** `es-index` is provided by this `paimon-eslib` module. Make sure `paimon-eslib`
> is on the classpath of your Spark or Flink job (and of any reader) before building or querying an
> `es-index`.

See the general [Global Index](../docs/docs/multimodal-table/global-index.mdx) documentation for the
required Data Evolution table properties, coverage/freshness behavior, and shared build options.

## How columns map to index types

For each indexed column, `es-index` decides the sub-index type from the column data type, unless you
override it with `fields.<field>.type`:

| Column data type | Default sub-index | Override with `type` |
|---|---|---|
| `VECTOR<FLOAT>` / `ARRAY<FLOAT>` | Vector (ANN) | `vector` |
| `STRING` | Full-text primary + keyword sub-field | `fulltext` or `keyword` |
| `TIMESTAMP` / `DATE` | Long scalar | `date` |
| supported numeric scalars | Scalar | — |

Text columns always provide both capabilities. A `fulltext` primary field gets a `<column>.keyword`
exact-match sub-field; a `keyword` primary field gets a `<column>.fulltext` analyzed sub-field.

## Prerequisites

Create a Data Evolution table with the required properties:

```sql
CREATE TABLE my_table (
    id INT,
    embedding ARRAY<FLOAT>,
    content STRING,
    category STRING,
    price INT
) TBLPROPERTIES (
    'bucket' = '-1',
    'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true',
    'global-index.enabled' = 'true'
);
```

## Build ES Index

Build the index with `sys.create_global_index` and `index_type => 'es-index'`. To index several
columns in one `es-index`, pass a **comma-separated** `index_column` list: the **first** column is
the primary column, the rest are companion columns. Per-column behavior is configured through options
under the `global-index.es-index.` prefix.

### Spark SQL

```sql
-- Single vector column with an HNSW index
CALL sys.create_global_index(
    table => 'db.my_table',
    index_column => 'embedding',
    index_type => 'es-index',
    options => 'global-index.es-index.fields.embedding.algorithm=hnsw,global-index.es-index.fields.embedding.dimension=768,global-index.es-index.fields.embedding.metric=cosine'
);

-- Multi-column: vector (primary) + full-text + keyword + scalar in one index.
-- 'content' becomes full-text because an analyzer is set; 'category' stays keyword; 'price' scalar.
CALL sys.create_global_index(
    table => 'db.my_table',
    index_column => 'embedding,content,category,price',
    index_type => 'es-index',
    options => 'global-index.es-index.fields.embedding.algorithm=diskbbq,global-index.es-index.fields.embedding.dimension=768,global-index.es-index.fields.embedding.metric=cosine,global-index.es-index.fields.content.analyzer=standard,global-index.es-index.fields.category.type=keyword'
);
```

### Flink SQL

```sql
-- Single vector column with an HNSW index
CALL sys.create_global_index(
    `table` => 'db.my_table',
    index_column => 'embedding',
    index_type => 'es-index',
    options => 'global-index.es-index.fields.embedding.algorithm=hnsw,global-index.es-index.fields.embedding.dimension=768,global-index.es-index.fields.embedding.metric=cosine'
);

-- Multi-column: vector (primary) + full-text + keyword + scalar in one index.
CALL sys.create_global_index(
    `table` => 'db.my_table',
    index_column => 'embedding,content,category,price',
    index_type => 'es-index',
    options => 'global-index.es-index.fields.embedding.algorithm=diskbbq,global-index.es-index.fields.embedding.dimension=768,global-index.es-index.fields.embedding.metric=cosine,global-index.es-index.fields.content.analyzer=standard,global-index.es-index.fields.category.type=keyword'
);
```

You can also set the same options in `TBLPROPERTIES` at table-creation time instead of passing them
per build. Options are resolved with field-level keys taking precedence over index-type-level keys:
`global-index.es-index.fields.<field>.<key>` overrides `global-index.es-index.<key>`.

## Options

Index-type-level keys use the `global-index.es-index.<key>` prefix and apply to every column of that
kind; field-level keys use `global-index.es-index.fields.<field>.<key>` and override them for one
column.

| Key | Applies to | Default | Description |
|---|---|---|---|
| `type` | any column | inferred from data type | Force the sub-index type: `vector`, `fulltext`, `keyword`, `date`. |
| `algorithm` | vector | `hnsw` | Vector algorithm: `hnsw` or `diskbbq`. `native` is not supported by paimon-eslib. |
| `dimension` | vector | inferred for `VECTOR<FLOAT>` | Vector dimension (1–4096). Required for `ARRAY<FLOAT>`; taken from the type for `VECTOR<FLOAT>`. |
| `metric` | vector | `cosine` | Distance metric (for example `cosine`, `l2`, `dot_product`). Use the same metric at build and query time. |
| `m` | vector (`hnsw`) | engine default | HNSW graph out-degree; valid range: 1–512. |
| `ef_construction` | vector (`hnsw`) | engine default | HNSW construction search width; valid range: 1–3200. |
| `vectors_per_cluster` | vector (`diskbbq`) | engine default | Target vectors per cluster for DiskBBQ; valid range: 64–65536. |
| `centroids_per_parent_cluster` | vector (`diskbbq`) | engine default | Number of child centroids per parent cluster for DiskBBQ; valid range: 2–384. |
| `analyzer` | string | `standard` | Text analyzer used by the full-text primary field or `.fulltext` sub-field: `standard`, `whitespace`, `simple`, or `keyword`. |

## Query

`es-index` implements the same search API as the other global indexes, so querying does not depend on
which engine built the index — only the build step differs.

### Spark SQL

```sql
-- Vector search: top-5 nearest neighbors on the primary vector column
SELECT * FROM vector_search('my_table', 'embedding', array(1.0f, 2.0f, 3.0f), 5);

-- Full-text search on a companion full-text column, using the JSON query DSL
SELECT * FROM full_text_search(
    'my_table',
    'content',
    '{"match":{"query":"paimon lake format"}}',
    10
);
```

Vector routes and full-text routes over an `es-index` can also be combined with the
`hybrid_search(...)` table-valued function.

### Flink SQL

Flink exposes vector search as a `CALL` procedure. The procedure returns JSON-serialized rows as
strings.

```sql
-- Vector search: top-5 nearest neighbors on the primary vector column
CALL sys.vector_search(
    `table` => 'db.my_table',
    vector_column => 'embedding',
    query_vector => '1.0,2.0,3.0',
    top_k => 5
);

-- Optionally restrict the returned columns
CALL sys.vector_search(
    `table` => 'db.my_table',
    vector_column => 'embedding',
    query_vector => '1.0,2.0,3.0',
    top_k => 5,
    projection => 'id,content'
);
```

> **Note.** Flink SQL currently exposes vector search only. For full-text search and hybrid search
> over an `es-index`, use Spark SQL (the `full_text_search` / `hybrid_search` table-valued functions)
> or the Java API (`Table.newFullTextSearchBuilder()` / `Table.newHybridSearchBuilder()`).

## Drop ES Index

### Spark SQL

```sql
CALL sys.drop_global_index(
    table => 'db.my_table',
    index_column => 'embedding',
    index_type => 'es-index'
);
```

### Flink SQL

```sql
CALL sys.drop_global_index(
    `table` => 'db.my_table',
    index_column => 'embedding',
    index_type => 'es-index'
);
```
