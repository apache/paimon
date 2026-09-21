---
title: "Indexes and Search"
sidebar_position: 5
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

# Indexes and Search

Create or remove global indexes, query them, or rewrite file indexes.

See [Procedures](../procedures) for Flink version requirements, argument conventions, and catalog selection.

## create_global_index

To create a global index on a table for accelerating queries. Arguments:

- `table` (required): the target table identifier.

- `index_column` (required): the column name to build index on.

- `index_type` (required): the type of global index, supported types include 'btree', 'bitmap', 'fm', 'ivf-flat', 'ivf-pq', 'ivf-sq', 'ivf-rq', 'diskann', and 'full-text'.

- `partitions` (optional): partition filter for selective index creation.

- `options` (optional): additional dynamic options for index creation.

**Syntax**

```sql
CALL [catalog.]sys.create_global_index(
    `table` => 'table',
    `index_column` => 'columnName',
    `index_type` => 'indexType',
    `partitions` => 'partitions',
    `options` => 'key1=value1,key2=value2');
```

**Example**

```sql
-- Create btree index
CALL sys.create_global_index(
    `table` => 'default.T',
    `index_column` => 'name',
    `index_type` => 'btree');

-- Create bitmap index
CALL sys.create_global_index(
    `table` => 'default.T',
    `index_column` => 'tag',
    `index_type` => 'bitmap',
    `options` => 'sorted-index.records-per-range=1000000');

-- Create exact FM contains index
CALL sys.create_global_index(
    `table` => 'default.T',
    `index_column` => 'content',
    `index_type` => 'fm');

-- Create index for specific partitions
CALL sys.create_global_index(
    `table` => 'default.T',
    `index_column` => 'name',
    `index_type` => 'btree',
    `partitions` => 'pt=p1;pt=p2');

-- Create native full-text index with ngram tokenizer
CALL sys.create_global_index(
    `table` => 'default.T',
    `index_column` => 'content',
    `index_type` => 'full-text',
    `options` => 'full-text.tokenizer=ngram,full-text.ngram.min-gram=2,full-text.ngram.max-gram=2');

-- Create native full-text index with jieba tokenizer
CALL sys.create_global_index(
    `table` => 'default.T',
    `index_column` => 'content',
    `index_type` => 'full-text',
    `options` => 'full-text.tokenizer=jieba');

-- Create native full-text index with a custom analyzer
CALL sys.create_global_index(
    `table` => 'default.T',
    `index_column` => 'content',
    `index_type` => 'full-text',
    `options` => 'full-text.tokenizer=simple,full-text.stem=true,full-text.remove-stop-words=true');
```

## drop_global_index

To drop global index files from a table. Arguments:

- `table` (required): the target table identifier.

- `index_column` (required): the column name for which to drop the index.

- `index_type` (required): the type of global index to drop, e.g., 'btree'.

- `partitions` (optional): partition specification for selective index deletion.

- `dry_run` (optional): when true, report how many index files would be dropped without committing any change. Default is false.

**Syntax**

```sql
CALL [catalog.]sys.drop_global_index(
    `table` => 'table',
    `index_column` => 'columnName',
    `index_type` => 'indexType',
    `partitions` => 'partitions',
    `dry_run` => dryRun);
```

**Example**

```sql
-- Drop all btree indexes for column 'name'
CALL sys.drop_global_index(
    `table` => 'default.T',
    `index_column` => 'name',
    `index_type` => 'btree');

-- Drop indexes only for specific partitions
CALL sys.drop_global_index(
    `table` => 'default.T',
    `index_column` => 'name',
    `index_type` => 'btree',
    `partitions` => 'pt=p1;pt=p2');

-- Preview what would be dropped without deleting
CALL sys.drop_global_index(
    `table` => 'default.T',
    `index_column` => 'name',
    `index_type` => 'btree',
    `dry_run` => true);
```

## full_text_search

To perform full-text search on a table and return deterministically ordered JSON rows. Arguments:

- `table` (required): the target table identifier.

- `column` (required): the character column to search.

- `query` (required): native full-text query JSON.

- `top_k` (required): the maximum number of results, from 1 through 10,000.

- `projection` (optional): comma-separated result columns. Add `__paimon_search_score` to return the search relevance score.

- `options` (optional): additional dynamic options of the table. The query authorization setting cannot be overridden.

**Syntax**

```sql
CALL [catalog.]sys.full_text_search(
    `table` => 'identifier',
    `column` => 'columnName',
    query => 'queryJson',
    top_k => topK,
    projection => 'col1,col2,__paimon_search_score',
    options => 'key1=value1;key2=value2');
```

**Example**

```sql
CALL sys.full_text_search(
    `table` => 'default.articles',
    `column` => 'content',
    query => '{"match":{"query":"paimon lake"}}',
    top_k => 10,
    projection => 'id,content,__paimon_search_score');
```

## vector_search

To perform vector similarity search on a table with a global vector index. Returns JSON-serialized rows. Arguments:

- `table` (required): the target table identifier.

- `vector_column` (required): the name of the vector column to search.

- `query_vector` (required): comma-separated float values representing the query vector, e.g. '1.0,2.0,3.0'.

- `top_k` (required): the number of nearest neighbors to return.

- `projection` (optional): comma-separated column names to include in the result. If omitted, all columns are returned.

- `options` (optional): additional dynamic options of the table.

- `where` (optional): a SQL predicate applied before Top-K.

- `partitions` (optional): semicolon-separated specs for a partitioned table.

**Syntax**

```sql
CALL [catalog.]sys.vector_search(
    `table` => 'identifier',
    vector_column => 'columnName',
    query_vector => 'v1,v2,...',
    top_k => topK,
    projection => 'col1,col2,__paimon_search_score',
    options => 'key1=value1;key2=value2',
    `where` => 'predicate',
    partitions => 'pt1=v1,pt2=v2;pt1=v3,pt2=v4');
```

**Example**

```sql
CALL sys.vector_search(
    `table` => 'default.T',
    vector_column => 'embedding',
    query_vector => '1.0,2.0,3.0',
    top_k => 5);

CALL sys.vector_search(
    `table` => 'default.T',
    vector_column => 'embedding',
    query_vector => '1.0,2.0,3.0',
    top_k => 5,
    projection => 'id,name,__paimon_search_score',
    options => 'vector-search.distribute.enabled=true;ivf.nprobe=32',
    `where` => 'status = ''active''');
```

## rewrite_file_index

Rewrite the file index for the table. Argument:

- `table`: `databaseName.tableName`.

- `partitions`: specific partitions.

**Syntax**

```sql
-- Use named argument
CALL [catalog.]sys.rewrite_file_index(<`table` => identifier> [, <partitions => partitions>]);

-- Use indexed argument
CALL [catalog.]sys.rewrite_file_index(<identifier> [, <partitions>]);
```

**Example**

```sql
-- rewrite the file index for the whole table
CALL sys.rewrite_file_index(`table` => 'test_db.T');

-- rewrite the file index for the specified partition in the table
CALL sys.rewrite_file_index(`table` => 'test_db.T', partitions => 'pt=a');
```
