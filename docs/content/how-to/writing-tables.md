---
title: "Writing Tables"
weight: 4
type: docs
aliases:
- /how-to/writing-tables.html
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

# Writing Tables

You can use the `INSERT` statement to inserts new rows into a table 
or overwrites the existing data in the table. The inserted rows can 
be specified by value expressions or result from a query.

## Syntax

```sql
INSERT { INTO | OVERWRITE } table_identifier [ part_spec ] [ column_list ] { value_expr | query };
```
- part_spec

    An optional parameter that specifies a comma-separated list of key and value pairs for partitions. 
    Note that one can use a typed literal (e.g., date’2019-01-02’) in the partition spec.

    Syntax: PARTITION ( partition_col_name = partition_col_val [ , ... ] )

- column_list

    An optional parameter that specifies a comma-separated list of columns belonging to the 
    table_identifier table.
    
    Syntax: (col_name1 [, column_name2, ...])
    
    {{< hint info >}}

    All specified columns should exist in the table and not be duplicated from each other.
    It includes all columns except the static partition columns.
      
    The size of the column list should be exactly the size of the data from VALUES clause or query.
    
    {{< /hint >}}

- value_expr

    Specifies the values to be inserted. Either an explicitly specified value or a NULL can be 
    inserted. A comma must be used to separate each value in the clause. More than one set of 
    values can be specified to insert multiple rows.

    Syntax: VALUES ( { value | NULL } [ , … ] ) [ , ( … ) ]

    {{< hint info >}}

    Currently, Flink doesn't support use NULL directly, so the NULL should be cast to actual 
    data type by `CAST (NULL AS data_type)`.

    {{< /hint >}}

For more information, please check the syntax document:

[Flink INSERT Statement](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/insert/)

[Spark INSERT Statement](https://spark.apache.org/docs/latest/sql-ref-syntax-dml-insert-table.html)

### Write Nullable field to Not-null field

We cannot insert into a non-null column of one table with a nullable column of another table. Assume that, 
we have a column key1 in table A which is primary key, primary key cannot be null. We have a column key2 in table B,
which is nullable. If we run a sql like this:

``` sql
INSERT INTO A key1 SELECT key2 FROM B
```
We will catch an exception,
- In spark: "Cannot write nullable values to non-null column 'key1'."
- In flink: "Column 'key1' is NOT NULL, however, a null value is being written into it. "

Other engines will throw respective exception to announce this. We can use function "NVL" or "COALESCE" to work around, 
turn a nullable column into a non-null column to escape exception:

```sql
INSERT INTO A key1 SELECT COALESCE(key2, <non-null expression>) FROM B;
```

## Applying Records/Changes to Tables

{{< tabs "insert-into-example" >}}

{{< tab "Flink" >}}

Use `INSERT INTO` to apply records and changes to tables.

```sql
INSERT INTO MyTable SELECT ...
```

Paimon supports shuffle data by partition and bucket in sink phase.

{{< /tab >}}

{{< tab "Spark3" >}}

Use `INSERT INTO` to apply records and changes to tables.

```sql
INSERT INTO MyTable SELECT ...
```

{{< /tab >}}

{{< /tabs >}}

## Overwriting 
Note :If `spark.sql.sources.partitionOverwriteMode` is set to `dynamic` by default in Spark, 
in order to ensure that the insert overwrite function of the Paimon table can be used normally,
`spark.sql.extensions` should be set to `org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions`.
### Overwriting the Whole Table

For unpartitioned tables, Paimon supports overwriting the whole table.

Use `INSERT OVERWRITE` to overwrite the whole unpartitioned table.

{{< tabs "insert-overwrite-unpartitioned-example" >}}

{{< tab "Flink" >}}

```sql
INSERT OVERWRITE MyTable SELECT ...
```

{{< /tab >}}

{{< tab "Spark" >}}

```sql
INSERT OVERWRITE MyTable SELECT ...
```

{{< /tab >}}

{{< /tabs >}}

### Overwriting a Partition

For partitioned tables, Paimon supports overwriting a partition.

Use `INSERT OVERWRITE` to overwrite a partition.

{{< tabs "insert-overwrite-partitioned-example" >}}

{{< tab "Flink" >}}

```sql
INSERT OVERWRITE MyTable PARTITION (key1 = value1, key2 = value2, ...) SELECT ...
```

{{< /tab >}}

{{< tab "Spark" >}}

```sql
INSERT OVERWRITE MyTable PARTITION (key1 = value1, key2 = value2, ...) SELECT ...
```

{{< /tab >}}

{{< /tabs >}}

### Dynamic Overwrite

{{< tabs "dynamic-overwrite" >}}

{{< tab "Flink" >}}

Flink's default overwrite mode is dynamic partition overwrite (that means Paimon only deletes the partitions
appear in the overwritten data). You can configure `dynamic-partition-overwrite` to change it to static overwritten.

```sql
-- MyTable is a Partitioned Table

-- Dynamic overwrite
INSERT OVERWRITE MyTable SELECT ...

-- Static overwrite (Overwrite whole table)
INSERT OVERWRITE MyTable /*+ OPTIONS('dynamic-partition-overwrite' = 'false') */ SELECT ...
```

{{< /tab >}}

{{< tab "Spark" >}}

Spark's default overwrite mode is static partition overwrite. To enable dynamic overwritten needs these configs below:

```text
--conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions
```

```sql
-- MyTable is a Partitioned Table

-- Static overwrite (Overwrite whole table)
INSERT OVERWRITE MyTable SELECT ...

-- Dynamic overwrite
SET spark.sql.sources.partitionOverwriteMode=dynamic;
INSERT OVERWRITE MyTable SELECT ...
```

{{< /tab >}}

{{< /tabs >}}

## Truncate tables

{{< tabs "truncate-tables-syntax" >}}

{{< tab "Flink 1.17-" >}}

You can use `INSERT OVERWRITE` to purge tables by inserting empty value.

```sql
INSERT OVERWRITE MyTable /*+ OPTIONS('dynamic-partition-overwrite'='false') */ SELECT * FROM MyTable WHERE false;
```

{{< /tab >}}

{{< tab "Flink 1.18" >}}

```sql
TRUNCATE TABLE MyTable;
```

{{< /tab >}}

{{< tab "Spark" >}}

```sql
TRUNCATE TABLE MyTable;
```

{{< /tab >}}


{{< /tabs >}}

## Purging Partitions

Currently, Paimon supports two ways to purge partitions.

1. Like purging tables, you can use `INSERT OVERWRITE` to purge data of partitions by inserting empty value to them.

2. Method #1 does not support to drop multiple partitions. In case that you need to drop multiple partitions, you can submit the drop_partition job through `flink run`.

{{< tabs "purge-partitions-syntax" >}}

{{< tab "Flink" >}}

```sql
-- Syntax
INSERT OVERWRITE MyTable /*+ OPTIONS('dynamic-partition-overwrite'='false') */ 
PARTITION (key1 = value1, key2 = value2, ...) SELECT selectSpec FROM MyTable WHERE false;

-- The following SQL is an example:
-- table definition
CREATE TABLE MyTable (
    k0 INT,
    k1 INT,
    v STRING
) PARTITIONED BY (k0, k1);

-- you can use
INSERT OVERWRITE MyTable /*+ OPTIONS('dynamic-partition-overwrite'='false') */ 
PARTITION (k0 = 0) SELECT k1, v FROM MyTable WHERE false;

-- or
INSERT OVERWRITE MyTable /*+ OPTIONS('dynamic-partition-overwrite'='false') */ 
PARTITION (k0 = 0, k1 = 0) SELECT v FROM MyTable WHERE false;
```

{{< /tab >}}

{{< tab "Flink Job" >}}

Run the following command to submit a drop_partition job for the table.

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

For more information of drop_partition, see

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    drop_partition --help
```

{{< /tab >}}

{{< /tabs >}}

## Updating tables

{{< hint info >}}
Important table properties setting:
1. Only [primary key table]({{< ref "concepts/primary-key-table" >}}) supports this feature.
2. [MergeEngine]({{< ref "concepts/primary-key-table#merge-engines" >}}) needs to be [deduplicate]({{< ref "concepts/primary-key-table#deduplicate" >}}) or [partial-update]({{< ref "concepts/primary-key-table#partial-update" >}}) to support this feature.
{{< /hint >}}

{{< hint warning >}}
Warning: we do not support updating primary keys.
{{< /hint >}}

{{< tabs "update-table-syntax" >}}

{{< tab "Flink" >}}

Currently, Paimon supports updating records by using `UPDATE` in Flink 1.17 and later versions. You can perform `UPDATE` in Flink's `batch` mode.

```sql
-- Syntax
UPDATE table_identifier SET column1 = value1, column2 = value2, ... WHERE condition;

-- The following SQL is an example:
-- table definition
CREATE TABLE MyTable (
	a STRING,
	b INT,
	c INT,
	PRIMARY KEY (a) NOT ENFORCED
) WITH ( 
	'merge-engine' = 'deduplicate' 
);

-- you can use
UPDATE MyTable SET b = 1, c = 2 WHERE a = 'myTable';
```

{{< /tab >}}

{{< tab "Spark" >}}

To enable update needs these configs below:

```text
--conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions
```

spark supports update PrimitiveType and StructType, for example:

```sql
-- Syntax
UPDATE table_identifier SET column1 = value1, column2 = value2, ... WHERE condition;

CREATE TABLE T (
  id INT, 
  s STRUCT<c1: INT, c2: STRING>, 
  name STRING)
TBLPROPERTIES (
  'primary-key' = 'id', 
  'merge-engine' = 'deduplicate'
);

-- you can use
UPDATE T SET name = 'a_new' WHERE id = 1;
UPDATE T SET s.c2 = 'a_new' WHERE s.c1 = 1;
```

{{< /tab >}}

{{< /tabs >}}

## Deleting from table

{{< tabs "delete-from-table" >}}

{{< tab "Flink 1.16-" >}}

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

{{< /tab >}}

{{< tab "Flink 1.17+" >}}
{{< hint info >}}
Important table properties setting:
1. Only primary key tables support this feature.
2. If the table has primary keys, [MergeEngine]({{< ref "concepts/primary-key-table#merge-engines" >}}) needs to be [deduplicate]({{< ref "concepts/primary-key-table#deduplicate" >}}) to support this feature.
   {{< /hint >}}

{{< hint warning >}}
Warning: we do not support deleting from table in streaming mode.
{{< /hint >}}

```sql
-- Syntax
DELETE FROM table_identifier WHERE conditions;

-- The following SQL is an example:
-- table definition
CREATE TABLE MyTable (
    id BIGINT NOT NULL,
    currency STRING,
    rate BIGINT,
    dt String,
    PRIMARY KEY (id, dt) NOT ENFORCED
) PARTITIONED BY (dt) WITH ( 
    'merge-engine' = 'deduplicate' 
);

-- you can use
DELETE FROM MyTable WHERE currency = 'UNKNOWN';
```

{{< /tab >}}

{{< tab "Spark" >}}
{{< hint info >}}
Important table properties setting:
1. Only primary key tables support this feature.
2. If the table has primary keys, [MergeEngine]({{< ref "concepts/primary-key-table#merge-engines" >}}) needs to be [deduplicate]({{< ref "concepts/primary-key-table#deduplicate" >}}) to support this feature.
   {{< /hint >}}

To enable delete needs these configs below:

```text
--conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions
```

```sql
DELETE FROM MyTable WHERE currency = 'UNKNOWN';
```

{{< /tab >}}

{{< /tabs >}}

## Merging into table

Paimon supports "MERGE INTO" via submitting the 'merge_into' job through `flink run`.

{{< hint info >}}
Important table properties setting:
1. Only [primary key table]({{< ref "concepts/primary-key-table" >}}) supports this feature.
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

{{< tabs "merge_into" >}}

{{< tab "Flink Job" >}}

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
    --merge_actions <matched-upsert,matched-delete,not-matched-insert,not_matched_by_source_upsert,not_matched_by_source_delete> \
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
    matched_upsert,matched_delete \
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
    matched_upsert,not_matched_insert \
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
    not_matched_by_source_upsert,not_matched_by_source_delete \
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
    --merge_actions not_matched_insert\
    --not_matched_insert_values *
```

The term 'matched' explanation:
1. matched: changed rows are from target table and each can match a source table row based on 
merge-condition and optional matched-condition (source ∩ target).
2. not-matched: changed rows are from source table and all rows cannot match any target table 
row based on merge-condition and optional not_matched_condition (source - target).
3. not-matched-by-source: changed rows are from target table and all row cannot match any source 
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
(same to not_matched_by_source_upsert and not_matched_by_source_delete). Otherwise, all conditions are optional.
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
{{< /tab >}}

{{< /tabs >}}