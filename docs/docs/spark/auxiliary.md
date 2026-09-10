---
title: "Inspect and Maintain Tables"
sidebar_position: 7
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

# Inspect and Maintain Tables

Inspect schemas, partitions, statistics, and cached metadata with these Spark SQL statements.
For file maintenance and retention operations, see [Procedures](./procedures).

## Set / Reset

See [Configuration](./configuration#set-and-reset-session-options) for `SET` / `RESET`,
option scopes, table-specific overrides, and connector defaults.

## Describe table
DESCRIBE TABLE statement returns the basic metadata information of a table or view. The metadata information includes column name, column type and column comment.

```sql
-- describe table or view
DESCRIBE TABLE my_table;

-- describe table or view with additional metadata
DESCRIBE TABLE EXTENDED my_table;
```

## Show create table
SHOW CREATE TABLE returns the CREATE TABLE statement or CREATE VIEW statement that was used to create a given table or view.

```sql
SHOW CREATE TABLE my_table;
```

## Show columns
Returns the list of columns in a table. If the table does not exist, an exception is thrown.

```sql
SHOW COLUMNS FROM my_table;
```

## Show partitions
The SHOW PARTITIONS statement is used to list partitions of a table. An optional partition spec may be specified to return the partitions matching the supplied partition spec.

```sql
-- Lists all partitions for my_table
SHOW PARTITIONS my_table;

-- Lists partitions matching the supplied partition spec for my_table
SHOW PARTITIONS my_table PARTITION (dt='20230817');
```

## Show table extended
The SHOW TABLE EXTENDED statement is used to list table or partition information.

```sql
-- Lists tables that satisfy regular expressions
SHOW TABLE EXTENDED IN db_name LIKE 'test*';

-- Lists the specified partition information for the table
SHOW TABLE EXTENDED IN db_name LIKE 'table_name' PARTITION(pt = '2024');
```

## Show views
The SHOW VIEWS statement returns all the views for an optionally specified database.

```sql
-- Lists all views
SHOW VIEWS;

-- Lists all views that satisfy regular expressions
SHOW VIEWS LIKE 'test*';
```

## Analyze table

The ANALYZE TABLE statement collects statistics about the table, that are to be used by the query optimizer to find a better query execution plan.
Paimon supports collecting table-level statistics and column statistics through analyze.

```sql
-- collect table-level statistics
ANALYZE TABLE my_table COMPUTE STATISTICS;

-- collect table-level statistics and column statistics for col1
ANALYZE TABLE my_table COMPUTE STATISTICS FOR COLUMNS col1;

-- collect table-level statistics and column statistics for all columns
ANALYZE TABLE my_table COMPUTE STATISTICS FOR ALL COLUMNS;
```

On a Format Table with catalog-managed partitions the statement means something narrower: it
measures the table's partitions and supports `PARTITION (...)` and `NOSCAN`, while the
`FOR COLUMNS` forms above are not supported, see
[Manage Format Table Partitions](./sql-ddl#manage-format-table-partitions).

## Refresh table

The REFRESH TABLE statement invalidates the cached entries, which include data and metadata of the given table.

In particular, when the caching catalog is enabled, Paimon will automatically cache the table's metadata. In multi-session scenarios, after a table is recreated in one session, this command must be used in another session to clear the cache.

```sql
REFRESH TABLE table_identifier;
```
