---
title: "Default Value"
weight: 8
type: docs
aliases:
- /flink/default-value.html
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

# Default Value

Paimon allows specifying default values for columns. When users write to these tables without explicitly providing
values for certain columns, Paimon automatically generates default values for these columns.

## Create Table

Flink SQL does not have native support for default values, so we can only create a table without default values:

```sql
CREATE TABLE my_table (
    a BIGINT,
    b STRING,
    c INT
);
```

We support the procedure of modifying column default values in Flink. You can add default value definitions after
creating the table:

```sql
CALL sys.alter_column_default_value('default.my_table', 'b', 'my_value');
CALL sys.alter_column_default_value('default.my_table', 'c', '5');
```

## Insert Table

For SQL commands that execute table writes, such as the `INSERT`, `UPDATE`, and `MERGE` commands, `NULL` value is
parsed into the default value specified for the corresponding column.

For example:

```sql
INSERT INTO my_table (a) VALUES (1), (2);

SELECT * FROM my_table;
-- result: [[1, 5, my_value], [2, 5, my_value]]
```
