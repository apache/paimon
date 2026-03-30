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

Global indexes work on top of Data Evolution tables. To use global indexes, your table **must** have:

- `'bucket' = '-1'` (unaware-bucket mode)
- `'row-tracking.enabled' = 'true'`
- `'data-evolution.enabled' = 'true'`

## Prerequisites

Create a table with the required properties:

```sql
CREATE TABLE my_table (
    id INT,
    name STRING,
    embedding ARRAY<FLOAT>
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
