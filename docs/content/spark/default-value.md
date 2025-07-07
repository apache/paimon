---
title: "Default Value"
weight: 8
type: docs
aliases:
- /spark/default-value.html
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

You can create a table with columns with default values using the following SQL:

```sql
CREATE TABLE my_table (
    a BIGINT,
    b STRING DEFAULT 'my_value',
    c INT DEFAULT 5 
);
```

## Insert Table

For SQL commands that execute table writes, such as the `INSERT`, `UPDATE`, and `MERGE` commands, the `DEFAULT` keyword
or `NULL` value is parsed into the default value specified for the corresponding column.

## Alter Default Value

Paimon supports alter column default value.

For example:

```sql
CREATE TABLE T (a INT, b INT DEFAULT 2);

INSERT INTO T (a) VALUES (1);
-- result: [[1, 2]]

ALTER TABLE T ALTER COLUMN b SET DEFAULT 3;

INSERT INTO T (a) VALUES (2);
-- result: [[1, 2], [2, 3]]
```

The default value of `'b'` column has been changed to 3 from 2.

## Limitation

Not support alter table add column with default value, for example: `ALTER TABLE T ADD COLUMN d INT DEFAULT 5;`.
