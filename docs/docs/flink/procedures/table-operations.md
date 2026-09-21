---
title: "Table Operations"
sidebar_position: 4
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

# Table Operations

Merge records, migrate or clone tables, and manage defaults and partitions.

See [Procedures](../procedures) for Flink version requirements, argument conventions, and catalog selection.

## merge_into

To perform "MERGE INTO" syntax. See [merge_into action](../action-jars#merging-into-table) for details of arguments.

**Syntax**

```sql
-- for Flink 1.18
CALL [catalog.]sys.merge_into('identifier','targetAlias',
    'sourceSqls','sourceTable','mergeCondition',
    'matchedUpsertCondition','matchedUpsertSetting',
    'notMatchedInsertCondition','notMatchedInsertValues',
    'matchedDeleteCondition');

-- for Flink 1.19 and later
CALL [catalog.]sys.merge_into(
    target_table => 'identifier',
    target_alias => 'targetAlias',
    source_sqls => 'sourceSqls',
    source_table => 'sourceTable',
    merge_condition => 'mergeCondition',
    matched_upsert_condition => 'matchedUpsertCondition',
    matched_upsert_setting => 'matchedUpsertSetting',
    not_matched_insert_condition => 'notMatchedInsertCondition',
    not_matched_insert_values => 'notMatchedInsertValues',
    matched_delete_condition => 'matchedDeleteCondition',
    not_matched_by_source_upsert_condition => 'notMatchedBySourceUpsertCondition',
    not_matched_by_source_upsert_setting => 'notMatchedBySourceUpsertSetting',
    not_matched_by_source_delete_condition => 'notMatchedBySourceDeleteCondition');
```

**Example**

```sql
-- for matched order rows,
-- increase the price,
-- and if there is no match,
-- insert the order from
-- the source table
-- for Flink 1.18
CALL sys.merge_into('default.T','','','default.S','T.id=S.order_id','','price=T.price+20','','*','');

-- for Flink 1.19 and later
CALL sys.merge_into(
    target_table => 'default.T',
    source_table => 'default.S',
    merge_condition => 'T.id=S.order_id',
    matched_upsert_setting => 'price=T.price+20',
    not_matched_insert_values => '*');
```

## data_evolution_merge_into

To perform "MERGE INTO" syntax specially implemented for data-evolution tables. Please see [data evolution](../../multimodal-table/data-evolution) for more information.

**Syntax**

```sql
-- Use indexed argument
CALL [catalog.]sys.data_evolution_merge_into('targetTable','targetAlias',
    'sourceSqls','sourceTable','mergeCondition','matchedUpdateSet',sinkParallelism);

-- Use named argument
CALL [catalog.]sys.data_evolution_merge_into(
    target_table => 'identifier',
    target_alias => 'targetAlias',
    source_sqls => 'sourceSqls',
    source_table => 'sourceTable',
    merge_condition => 'mergeCondition',
    matched_update_set => 'matchedUpdateSet',
    sink_parallelism => sinkParallelism);
```

**Example**

```sql
-- for Flink 1.18
CALL [catalog.]sys.data_evolution_merge_into('default.T', '', '', 'S', 'T.id=S.id', 'name=S.name', 2);

-- for Flink 1.19 and later
CALL [catalog.]sys.data_evolution_merge_into(
    target_table => 'default.T',
    source_table => 'S',
    merge_condition => 'T.id=S.id',
    matched_update_set => 'name=S.name',
    sink_parallelism => 2);
```

## migrate_database

To migrate all hive tables in database to paimon table. Argument:

- `connector`: the origin database's type to be migrated, such as hive. Cannot be empty.

- `source_database`: name of the origin database to be migrated. Cannot be empty.

- `options`: the table options of the paimon table to migrate.

- `parallelism`: the parallelism for migrate process, default is core numbers of machine.

**Syntax**

```sql
-- for Flink 1.18
-- migrate all hive tables in database to paimon tables.
CALL [catalog.]sys.migrate_database('connector', 'dbIdentifier', 'options'[, <parallelism>]);

-- for Flink 1.19 and later
-- migrate all hive tables in database to paimon tables.
CALL [catalog.]sys.migrate_database(
    connector => 'connector',
    source_database => 'dbIdentifier',
    options => 'options'[,
    <parallelism => parallelism>]
);
```

**Example**

```sql
-- for Flink 1.18
CALL sys.migrate_database('hive', 'db01', 'file.format=parquet', 6);

-- for Flink 1.19 and later
CALL sys.migrate_database(
    connector => 'hive',
    source_database => 'db01',
    options => 'file.format=parquet',
    parallelism => 6
);
```

## migrate_table

To migrate hive table to a paimon table. Argument:

- `connector`: the origin table's type to be migrated, such as hive. Cannot be empty.

- `source_table`: name of the origin table to be migrated. Cannot be empty.

- `target_table`: name of the target paimon table to migrate. If not set would keep the same name with origin table

- `options`: the table options of the paimon table to migrate.

- `parallelism`: the parallelism for migrate process, default is core numbers of machine.

- `delete_origin`: If had set target_table, can set delete_origin to decide whether delete the origin table metadata from hms after migrate. Default is true

**Syntax**

```sql
-- migrate hive table to a paimon table.
CALL [catalog.]sys.migrate_table(
    connector => 'connector',
    source_table => 'tableIdentifier',
    options => 'options'[,
    <parallelism => parallelism>]
);
```

**Example**

```sql
CALL sys.migrate_table(
    connector => 'hive',
    source_table => 'db01.t1',
    options => 'file.format=parquet',
    parallelism => 6
);
```

## clone

Clone a table or a database. Arguments:

- `database and table`: source database and optional source table.

- `catalog_conf and target_catalog_conf`: source and target catalog options.

- `target_database and target_table`: target database and optional target table.

- `clone_from`: optional source type, either `hive` or `paimon`.

- `parallelism, where, included_tables, excluded_tables, prefer_file_format, meta_only, clone_if_exists and target_table_conf`: optional clone controls.

**Syntax**

```sql
CALL [catalog.]sys.clone(
    database => 'sourceDatabase',
    `table` => 'sourceTable',
    target_database => 'targetDatabase',
    target_table => 'targetTable'[,
    catalog_conf => 'key=value'][,
    target_catalog_conf => 'key=value'][,
    parallelism => parallelism][,
    `where` => 'predicate'][,
    included_tables => 'table1,table2'][,
    excluded_tables => 'table3'][,
    prefer_file_format => 'parquet'][,
    clone_from => 'hive-or-paimon'][,
    meta_only => true][,
    clone_if_exists => true][,
    target_table_conf => 'key=value']
);
```

**Example**

```sql
CALL sys.clone(
    database => 'source_db',
    `table` => 'source_t',
    target_database => 'target_db',
    target_table => 'target_t',
    clone_from => 'hive'
);
```

## copy_files

Deprecated. This procedure is supported only by `FileSystemCatalog` and does not commit a standard catalog snapshot. Do not use it for normal table-copy workflows; use `clone` instead. It copies files from a source table to a target table. Arguments:

- `warehouse, database, table and catalog_conf`: optional source catalog configuration.

- `target_warehouse`: the target warehouse. Cannot be empty.

- `target_database, target_table and target_catalog_conf`: optional target table and catalog configuration.

- `parallelism`: optional copy job parallelism.

**Syntax**

```sql
CALL [catalog.]sys.copy_files(
    warehouse => 'sourceWarehouse',
    database => 'sourceDatabase',
    `table` => 'sourceTable',
    catalog_conf => 'key=value',
    target_warehouse => 'targetWarehouse',
    target_database => 'targetDatabase',
    target_table => 'targetTable',
    target_catalog_conf => 'key=value',
    parallelism => parallelism
);
```

**Example**

```sql
CALL sys.copy_files(
    warehouse => 'hdfs:///source',
    database => 'default',
    `table` => 'T',
    target_warehouse => 'hdfs:///target',
    target_database => 'default',
    target_table => 'T'
);
```

## alter_column_default_value

Update a column default value. Arguments:

- `table`: the target table identifier.

- `column`: the column name; nested columns are separated by dots.

- `default_value`: the new default value.

**Syntax**

```sql
CALL [catalog.]sys.alter_column_default_value(
    `table` => 'identifier',
    `column` => 'columnName',
    default_value => 'value'
);
```

**Example**

```sql
CALL sys.alter_column_default_value(`table` => 'default.T', `column` => 'status', default_value => 'active');
```

## drop_partition

Drop one or more partitions. This procedure is deprecated; use `ALTER TABLE DROP PARTITION` instead.

**Syntax**

```sql
CALL [catalog.]sys.drop_partition('identifier', 'partition1'[, 'partition2', ...]);
```

**Example**

```sql
CALL sys.drop_partition('default.T', 'dt=2024-07-01');
```

## mark_partition_done

Mark partitions as done. Arguments:

- `table`: the target table identifier. Cannot be empty.

- `partitions`: semicolon-separated partition specs.

**Syntax**

```sql
CALL [catalog.]sys.mark_partition_done(`table` => 'identifier', partitions => 'partition1;partition2');
```

**Example**

```sql
CALL sys.mark_partition_done(`table` => 'default.T', partitions => 'day=2024-07-01;day=2024-07-02');
```
