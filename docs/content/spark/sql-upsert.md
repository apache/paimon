---
title: "SQL Upsert"
weight: 10
type: docs
aliases:
- /spark/sql-upsert.html
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

# SQL Upsert

For table without primary key, Paimon supports upsert write mode: If the row with the same upsert key already exists, perform update; otherwise, perform insert.

## Usage

Specify the following table properties when creating the table

* `upsert-key`: Defines the key columns used for upsert, cannot be used together with primary key. 
Unlike primary key, the upsert key value can be `null`, and null-equality matching is supported.
Multiple columns separated by commas.

* `sequence.field` (optional): When new record share the same upsert key, the row with the larger `sequence.field` value is kept as the merge result.
And it will also deduplicate the data being written. 
If `sequence.field` is not set, new record share the same upsert key simply update the existing one and no deduplication is performed.
Multiple columns separated by commas.

## Example

Create table:

```sql
CREATE TABLE t (k1 INT, k2 INT, ts1 INT, ts2 INT, v STRING)
TBLPROPERTIES ('upsert-key' = 'k1,k2', 'sequence.field' = 'ts1,ts2')
```

Insert data1:

```sql
INSERT INTO t values
(null, null, 2, 1, 'v1'),
(null, null, 2, 2, 'v4'),
(1, null, 1, 1, 'v1'),
(1, 2, 1, 1, 'v1'),
(1, 2, 2, 1, 'v2')
```

Query result:

```sql
SELECT * FROM t ORDER BY k1, k2

-- null, null, 2, 2, "v4"
-- 1, null, 1, 1, "v1"
-- 1, 2, 2, 1, "v2"
```

Insert data2:

```sql
INSERT INTO t values
(null, null, 2, 1, 'v5'),
(null, 1, 1, 1, 'v1'),
(1, null, 2, 1, 'v2'),
(1, 1, 1, 1, 'v1'),
(1, 2, 2, 0, 'v3')
```

Query result:

```sql
SELECT * FROM t ORDER BY k1, k2

-- null, null, 2, 2, "v4"
-- null, 1, 1, 1, "v1"
-- 1, null, 2, 1, "v2"
-- 1, 1, 1, 1, "v1"
-- 1, 2, 2, 1, "v2"
```
