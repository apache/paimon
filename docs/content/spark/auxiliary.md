---
title: "Auxiliary"
weight: 7
type: docs
aliases:
- /spark/sql-alter.html
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
To set paimon configs specifically, you need add the `spark.paimon.` prefix.

```sql
-- set spark conf
set spark.sql.sources.partitionOverwriteMode=dynamic;
 
-- set paimon conf
SET spark.paimon.file.block-size=512M;

-- reset conf
RESET spark.paimon.file.block-size;
```

## Describe table
DESCRIBE TABLE statement returns the basic metadata information of a table. The metadata information includes column name, column type and column comment.

```sql
-- describe table
DESCRIBE TABLE my_table;

-- describe table with additional metadata
DESCRIBE TABLE EXTENDED my_table;
```

## Show create table
SHOW CREATE TABLE returns the CREATE TABLE statement that was used to create a given table.

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
