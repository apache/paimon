---
title: "Auxiliary"
weight: 7
type: docs
aliases:
- /spark/auxiliary.html
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

# Auxiliary Statements

## Set / Reset
The SET command sets a property, returns the value of an existing property or returns all SQLConf properties with value and meaning.
The RESET command resets runtime configurations specific to the current session which were set via the SET command to their default values.
To set dynamic options globally, you need add the `spark.paimon.` prefix. You can also set dynamic table options at this format: 
`spark.paimon.${catalogName}.${dbName}.${tableName}.${config_key}`. The catalogName/dbName/tableName can be `*`, which means matching all 
the specific parts. Dynamic table options will override global options if there are conflicts.

```sql
-- set spark conf
SET spark.sql.sources.partitionOverwriteMode=dynamic;

-- set paimon conf
SET spark.paimon.file.block-size=512M;

-- reset conf
RESET spark.paimon.file.block-size;

-- set catalog
USE paimon;

-- set scan.snapshot-id=1 for the table default.T in any catalogs
SET spark.paimon.*.default.T.scan.snapshot-id=1;
SELECT * FROM default.T;

-- set scan.snapshot-id=1 for the table T in any databases and catalogs
SET spark.paimon.*.*.T.scan.snapshot-id=1;
SELECT * FROM default.T;

-- set scan.snapshot-id=2 for the table default.T1 in any catalogs and scan.snapshot-id=1 on other tables
SET spark.paimon.scan.snapshot-id=1;
SET spark.paimon.*.default.T1.scan.snapshot-id=2;
SELECT * FROM default.T1 JOIN default.T2 ON xxxx;
```

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
SHOW PARTITIONS my_table PARTITION (dt=20230817);
```

## Show Table Extended
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
