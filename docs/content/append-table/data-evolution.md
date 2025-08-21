---
title: "Data Evolution"
weight: 6
type: docs
aliases:
- /append-table/data-evolution.html
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

# Data Evolution

Paimon supports complete Schema Evolution, allowing you to freely add, modify, or delete column schema. But how to
backfill newly added columns or update column data.

Data Evolution Mode is a new feature for Append tables that revolutionizes how you handle data evolution, 
particularly when adding new columns. This mode allows you to update partial columns without rewriting entire data
files. Instead, it writes new column data to separate files and intelligently merges them with the original data
during read operations.

The data evolution mode offers significant advantages for your data lake architecture:

* Efficient Partial Column Updates: With this mode, you can use Spark's MERGE INTO statement to update a subset of columns. This avoids the high I/O cost of rewriting the whole file, as only the updated columns are written.

* Reduced File Rewrites: In scenarios with frequent schema changes, such as adding new columns, the traditional method requires constant file rewriting. Data evolution mode eliminates this overhead by appending new column data to dedicated files. This approach is much more efficient and reduces the burden on your storage system.

* Optimized Read Performance: The new mode is designed for seamless data retrieval. During query execution, Paimon's engine efficiently combines the original data with the new column data, ensuring that read performance remains uncompromised. The merge process is highly optimized, so your queries run just as fast as they would on a single, consolidated file.

To enable data evolution, you must enable row-tracking and set the `row-tracking.enabled` and `data-evolution.enabled` property to `true` when creating an append table. This ensures that the table is ready for efficient schema evolution operations.

Use Spark Sql as an example:

```sql
CREATE TABLE target (a INT, b INT, c STRING) TBLPROPERTIES (
    'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true'
)
```

Now we could only support spark 'MERGE INTO' statement to update partial columns.

```sql
MERGE INTO t
USING s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET t.b = s.b
WHEN NOT MATCHED THEN INSERT (id, b, c) VALUES (id, b, 11)
```

This statement updates only the `b` column in the target table `t` based on the matching records from the source table
`s`. The `id` column and `c` column remain unchanged, and new records are inserted with the specified values.

Note that: 
* Data Evolution Table does not support 'Delete' statement yet.
* Merge Into for Data Evolution Table does not support 'WHEN NOT MATCHED BY SOURCE' clause.
* Only Spark version greater than 3.5.0 is supported for Data Evolution Table.
