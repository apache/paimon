---
title: "Action Jars"
weight: 98
type: docs
aliases:
- /flink/procedures.html
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

# Action Jars

After the Flink Local Cluster has been started, you can execute the action jar by using the following command.

```
<FLINK_HOME>/bin/flink run \
 /path/to/paimon-flink-action-{{< version >}}.jar \
 <action>
 <args>
``` 

The following command is used to compact a table.

```
<FLINK_HOME>/bin/flink run \
 /path/to/paimon-flink-action-{{< version >}}.jar \
 compact \
 --path <TABLE_PATH>
```

## Merging into table

Paimon supports "MERGE INTO" via submitting the 'merge_into' job through `flink run`.

{{< hint info >}}
Important table properties setting:
1. Only [primary key table]({{< ref "primary-key-table/overview" >}}) supports this feature.
2. The action won't produce UPDATE_BEFORE, so it's not recommended to set 'changelog-producer' = 'input'.
   {{< /hint >}}

The design referenced such syntax:
```sql
MERGE INTO target-table
  USING source_table | source-expr AS source-alias
  ON merge-condition
  WHEN MATCHED [AND matched-condition]
    THEN UPDATE SET xxx
  WHEN MATCHED [AND matched-condition]
    THEN DELETE
  WHEN NOT MATCHED [AND not_matched_condition]
    THEN INSERT VALUES (xxx)
  WHEN NOT MATCHED BY SOURCE [AND not-matched-by-source-condition]
    THEN UPDATE SET xxx
  WHEN NOT MATCHED BY SOURCE [AND not-matched-by-source-condition]
    THEN DELETE
```
The merge_into action use "upsert" semantics instead of "update", which means if the row exists,
then do update, else do insert. For example, for non-primary-key table, you can update every column,
but for primary key table, if you want to update primary keys, you have to insert a new row which has
different primary keys from rows in the table. In this scenario, "upsert" is useful.

Run the following command to submit a 'merge_into' job for the table.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    merge_into \
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table <target-table> \
    [--target_as <target-table-alias>] \
    --source_table <source_table-name> \
    [--source_sql <sql> ...]\
    --on <merge-condition> \
    --merge_actions <matched-upsert,matched-delete,not-matched-insert,not-matched-by-source-upsert,not-matched-by-source-delete> \
    --matched_upsert_condition <matched-condition> \
    --matched_upsert_set <upsert-changes> \
    --matched_delete_condition <matched-condition> \
    --not_matched_insert_condition <not-matched-condition> \
    --not_matched_insert_values <insert-values> \
    --not_matched_by_source_upsert_condition <not-matched-by-source-condition> \
    --not_matched_by_source_upsert_set <not-matched-upsert-changes> \
    --not_matched_by_source_delete_condition <not-matched-by-source-condition> \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]]
    
You can pass sqls by '--source_sql <sql> [, --source_sql <sql> ...]' to config environment and create source table at runtime.
    
-- Examples:
-- Find all orders mentioned in the source table, then mark as important if the price is above 100 
-- or delete if the price is under 10.
./flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    merge_into \
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table T \
    --source_table S \
    --on "T.id = S.order_id" \
    --merge_actions \
    matched-upsert,matched-delete \
    --matched_upsert_condition "T.price > 100" \
    --matched_upsert_set "mark = 'important'" \
    --matched_delete_condition "T.price < 10" 
    
-- For matched order rows, increase the price, and if there is no match, insert the order from the 
-- source table:
./flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    merge_into \
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table T \
    --source_table S \
    --on "T.id = S.order_id" \
    --merge_actions \
    matched-upsert,not-matched-insert \
    --matched_upsert_set "price = T.price + 20" \
    --not_matched_insert_values * 

-- For not matched by source order rows (which are in the target table and does not match any row in the
-- source table based on the merge-condition), decrease the price or if the mark is 'trivial', delete them:
./flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    merge_into \
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table T \
    --source_table S \
    --on "T.id = S.order_id" \
    --merge_actions \
    not-matched-by-source-upsert,not-matched-by-source-delete \
    --not_matched_by_source_upsert_condition "T.mark <> 'trivial'" \
    --not_matched_by_source_upsert_set "price = T.price - 20" \
    --not_matched_by_source_delete_condition "T.mark = 'trivial'"
    
-- A --source_sql example: 
-- Create a temporary view S in new catalog and use it as source table
./flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    merge_into \
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table T \
    --source_sql "CREATE CATALOG test_cat WITH (...)" \
    --source_sql "CREATE TEMPORARY VIEW test_cat.`default`.S AS SELECT order_id, price, 'important' FROM important_order" \
    --source_table test_cat.default.S \
    --on "T.id = S.order_id" \
    --merge_actions not-matched-insert\
    --not_matched_insert_values *
```

The term 'matched' explanation:
1. matched: changed rows are from target table and each can match a source table row based on
   merge-condition and optional matched-condition (source ∩ target).
2. not matched: changed rows are from source table and all rows cannot match any target table
   row based on merge-condition and optional not_matched_condition (source - target).
3. not matched by source: changed rows are from target table and all row cannot match any source
   table row based on merge-condition and optional not-matched-by-source-condition (target - source).

Parameters format:
1. matched_upsert_changes:\
   col = \<source_table>.col | expression [, ...] (Means setting \<target_table>.col with given value. Do not
   add '\<target_table>.' before 'col'.)\
   Especially, you can use '*' to set columns with all source columns (require target table's
   schema is equal to source's).
2. not_matched_upsert_changes is similar to matched_upsert_changes, but you cannot reference
   source table's column or use '*'.
3. insert_values:\
   col1, col2, ..., col_end\
   Must specify values of all columns. For each column, you can reference \<source_table>.col or
   use an expression.\
   Especially, you can use '*' to insert with all source columns (require target table's schema
   is equal to source's).
4. not_matched_condition cannot use target table's columns to construct condition expression.
5. not_matched_by_source_condition cannot use source table's columns to construct condition expression.

{{< hint warning >}}
1. Target alias cannot be duplicated with existed table name.
2. If the source table is not in the current catalog and current database, the source-table-name must be
   qualified (database.table or catalog.database.table if created a new catalog).
   For examples:\
   (1) If source table 'my_source' is in 'my_db', qualify it:\
   \--source_table "my_db.my_source"\
   (2) Example for sqls:\
   When sqls changed current catalog and database, it's OK to not qualify the source table name:\
   \--source_sql "CREATE CATALOG my_cat WITH (...)"\
   \--source_sql "USE CATALOG my_cat"\
   \--source_sql "CREATE DATABASE my_db"\
   \--source_sql "USE my_db"\
   \--source_sql "CREATE TABLE S ..."\
   \--source_table S\
   but you must qualify it in the following case:\
   \--source_sql "CREATE CATALOG my_cat WITH (...)"\
   \--source_sql "CREATE TABLE my_cat.\`default`.S ..."\
   \--source_table my_cat.default.S\
   You can use just 'S' as source table name in following arguments.
3. At least one merge action must be specified.
4. If both matched-upsert and matched-delete actions are present, their conditions must both be present too
   (same to not-matched-by-source-upsert and not-matched-by-source-delete). Otherwise, all conditions are optional.
5. All conditions, set changes and values should use Flink SQL syntax. To ensure the whole command runs normally
   in Shell, please quote them with \"\" to escape blank spaces and use '\\' to escape special characters in statement.
   For example:\
   \--source_sql "CREATE TABLE T (k INT) WITH ('special-key' = '123\\!')"

{{< /hint >}}

For more information of 'merge_into', see

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    merge_into --help
```

## Deleting from table

In Flink 1.16 and previous versions, Paimon only supports deleting records via submitting the 'delete' job through `flink run`.

Run the following command to submit a 'delete' job for the table.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    delete \
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table <table-name> \
    --where <filter_spec> \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]]
    
filter_spec is equal to the 'WHERE' clause in SQL DELETE statement. Examples:
    age >= 18 AND age <= 60
    animal <> 'cat'
    id > (SELECT count(*) FROM employee)
```

For more information of 'delete', see

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    delete --help
```

## Drop Partition

Run the following command to submit a 'drop_partition' job for the table.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    drop_partition \
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table <table-name> \
    [--partition <partition_spec> [--partition <partition_spec> ...]] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]]

partition_spec:
key1=value1,key2=value2...
```

For more information of 'drop_partition', see

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    drop_partition --help
```

## Rewrite File Index

Run the following command to submit a 'rewrite_file_index' job for the table.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    rewrite_file_index \
    --warehouse <warehouse-path> \
    --identifier <database.table> \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]]
```

For more information of 'rewrite_file_index', see

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    rewrite_file_index --help
```
