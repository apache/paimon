---
title: "Global Index"
weight: 8
type: docs
aliases:
- /append-table/global-index.html
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

## Overview

Global Index is a powerful indexing mechanism for Data Evolution (append) tables. It enables efficient row-level lookups and filtering
without full-table scans. Paimon supports multiple global index types:

- **BTree Index**: A B-tree based index for scalar column lookups. Supports equality, IN, range predicates, and can be combined across multiple columns with AND/OR logic.
- **Vector Index**: An approximate nearest neighbor (ANN) index powered by DiskANN for vector similarity search.
- **Full-Text Index**: A full-text search index powered by Tantivy for text retrieval. Supports term matching and relevance scoring.

Global indexes work on top of Data Evolution tables. To use global indexes, your table **must** have:

- `'bucket' = '-1'` (unaware-bucket mode)
- `'row-tracking.enabled' = 'true'`
- `'data-evolution.enabled' = 'true'`

> Global index queries may not be exact when the index only covers part of the table data. If a query predicate matches the index, Paimon returns only the results from the indexed portion. Matching records in data that has not been indexed yet will not be returned.

## Prerequisites

Create a table with the required properties:

```sql
CREATE TABLE my_table (
    id INT,
    name STRING,
    embedding ARRAY<FLOAT>,
    content STRING
) TBLPROPERTIES (
    'bucket' = '-1',
    'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true',
    'global-index.enabled' = 'true'
);
```

## BTree Index

BTree index builds a logical B-tree structure over SST files, enabling efficient point lookups and range queries on scalar columns.

**Build BTree Index**

```sql
-- Create BTree index on 'name' column
CALL sys.create_global_index(
    table => 'db.my_table',
    index_column => 'name',
    index_type => 'btree'
);
```

**Query with BTree Index**

Once a BTree index is built, it is automatically used during scan when a filter predicate matches the indexed column.

```sql
SELECT * FROM my_table WHERE name IN ('a200', 'a300');
```

## Vector Index

Vector Index provides approximate nearest neighbor (ANN) search based on the DiskANN algorithm. It is suitable for
vector similarity search scenarios such as recommendation systems, image retrieval, and RAG (Retrieval Augmented
Generation) applications.

**Build Vector Index**

```sql
-- Create Lumina vector index on 'embedding' column
CALL sys.create_global_index(
    table => 'db.my_table',
    index_column => 'embedding',
    index_type => 'lumina-vector-ann',
    options => 'lumina.index.dimension=128'
);
```

**Vector Search**

{{< tabs "vector-search" >}}

{{< tab "Spark SQL" >}}
```sql
-- Search for top-5 nearest neighbors
SELECT * FROM vector_search('my_table', 'embedding', array(1.0f, 2.0f, 3.0f), 5);
```
{{< /tab >}}

{{< tab "Flink SQL (Procedure)" >}}

Unlike Spark's table-valued function, Flink uses a `CALL` procedure to perform vector search.
The procedure returns JSON-serialized rows as strings.

```sql
-- Search for top-5 nearest neighbors
CALL sys.vector_search(
    `table` => 'db.my_table',
    vector_column => 'embedding',
    query_vector => '1.0,2.0,3.0',
    top_k => 5
);

-- With projection (only return specific columns)
CALL sys.vector_search(
    `table` => 'db.my_table',
    vector_column => 'embedding',
    query_vector => '1.0,2.0,3.0',
    top_k => 5,
    projection => 'id,name'
);
```

{{< /tab >}}

{{< tab "Java API" >}}
```java
Table table = catalog.getTable(identifier);

// Step 1: Build vector search
float[] queryVector = {1.0f, 2.0f, 3.0f};
GlobalIndexResult result = table.newVectorSearchBuilder()
        .withVector(queryVector)
        .withLimit(5)
        .withVectorColumn("embedding")
        .executeLocal();

// Step 2: Read matching rows using the search result
ReadBuilder readBuilder = table.newReadBuilder();
TableScan.Plan plan = readBuilder.newScan().withGlobalIndexResult(result).plan();
try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
    reader.forEachRemaining(row -> {
        System.out.println("id=" + row.getInt(0) + ", name=" + row.getString(1));
    });
}
```
{{< /tab >}}

{{< /tabs >}}

## Full-Text Index

Full-Text Index provides text search capabilities powered by Tantivy. It is suitable for text retrieval scenarios
such as document search, log analysis, and content-based filtering.

**Build Full-Text Index**

```sql
-- Create full-text index on 'content' column
CALL sys.create_global_index(
    table => 'db.my_table',
    index_column => 'content',
    index_type => 'tantivy-fulltext'
);
```

**Full-Text Search**

{{< tabs "fulltext-search" >}}

{{< tab "Spark SQL" >}}
```sql
-- Search for top-10 documents matching the query
SELECT * FROM full_text_search('my_table', 'content', 'paimon lake format', 10);
```
{{< /tab >}}

{{< tab "Java API" >}}
```java
Table table = catalog.getTable(identifier);

// Step 1: Build full-text search
GlobalIndexResult result = table.newFullTextSearchBuilder()
        .withQueryText("paimon lake format")
        .withLimit(10)
        .withTextColumn("content")
        .executeLocal();

// Step 2: Read matching rows using the search result
ReadBuilder readBuilder = table.newReadBuilder();
TableScan.Plan plan = readBuilder.newScan().withGlobalIndexResult(result).plan();
try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
    reader.forEachRemaining(row -> {
        System.out.println("id=" + row.getInt(0) + ", content=" + row.getString(1));
    });
}
```
{{< /tab >}}

{{< tab "Python SDK" >}}
```python
table = catalog.get_table('db.my_table')

# Step 1: Build full-text search
builder = table.new_full_text_search_builder()
builder.with_text_column('content')
builder.with_query_text('paimon lake format')
builder.with_limit(10)
result = builder.execute_local()

# Step 2: Read matching rows using the search result
read_builder = table.new_read_builder()
scan = read_builder.new_scan().with_global_index_result(result)
plan = scan.plan()
table_read = read_builder.new_read()
pa_table = table_read.to_arrow(plan.splits())
print(pa_table)
```
{{< /tab >}}

{{< /tabs >}}
