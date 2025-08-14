---
title: "Query Performance"
weight: 8
type: docs
aliases:
- /primary-key-table/query-performance.html
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

# Query Performance

## Table Mode

The table schema has the greatest impact on query performance. See [Table Mode]({{< ref "primary-key-table/table-mode" >}}).

For Merge On Read table, the most important thing you should pay attention to is the number of buckets, which will limit
the concurrency of reading data.

For MOW (Deletion Vectors) or COW table or [Read Optimized]({{< ref "concepts/system-tables#read-optimized-table" >}}) table,
there is no limit to the concurrency of reading data, and they can also utilize some filtering conditions for non-primary-key columns.

## Aggregate push down

Table with Deletion Vectors Enabled supports aggregate push down:

```sql
SELECT COUNT(*) FROM TABLE WHERE DT = '20230101';
```

This query can be accelerated during compilation and returns very quickly.

For Spark SQL, table with default `metadata.stats-mode` can be accelerated:

```sql
SELECT MIN(a), MAX(b) FROM TABLE WHERE DT = '20230101';
```

Min max query can be also accelerated during compilation and returns very quickly.

## Data Skipping By Primary Key Filter

For a regular bucketed table (For example, bucket = 5), the filtering conditions of the primary key will greatly
accelerate queries and reduce the reading of a large number of files.

## Bucketed Join

Fixed Bucketed table (e.g. bucket = 10) can be used to avoid shuffle if necessary in batch query, for example, you can
use the following Spark SQL to read a Paimon table:

```sql
SET spark.sql.sources.v2.bucketing.enabled = true;

CREATE TABLE FACT_TABLE (order_id INT, f1 STRING) TBLPROPERTIES ('bucket'='10', 'primary-key' = 'order_id');

CREATE TABLE DIM_TABLE (order_id INT, f2 STRING) TBLPROPERTIES ('bucket'='10', 'primary-key' = 'order_id');

SELECT * FROM FACT_TABLE JOIN DIM_TABLE on t1.order_id = t4.order_id;
```

The `spark.sql.sources.v2.bucketing.enabled` config is used to enable bucketing for V2 data sources. When turned on,
Spark will recognize the specific distribution reported by a V2 data source through SupportsReportPartitioning, and
will try to avoid shuffle if necessary.

The costly join shuffle will be avoided if two tables have the same bucketing strategy and same number of buckets.
