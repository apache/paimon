---
title: "SQL Write"
weight: 2
type: docs
aliases:
- /spark/sql-write.html
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

# SQL Write

## Insert Table

The `INSERT` statement inserts new rows into a table or overwrites the existing data in the table. The inserted rows can be specified by value expressions or result from a query.

**Syntax**

```sql
INSERT { INTO | OVERWRITE } table_identifier [ part_spec ] [ column_list ] { value_expr | query };
```
**Parameters**

- **table_identifier**: Specifies a table name, which may be optionally qualified with a database name. 

- **part_spec**: An optional parameter that specifies a comma-separated list of key and value pairs for partitions.

- **column_list**: An optional parameter that specifies a comma-separated list of columns belonging to the table_identifier table. Spark will reorder the columns of the input query to match the table schema according to the specified column list.

  Note: Since Spark 3.4, INSERT INTO commands with explicit column lists comprising fewer columns than the target table will automatically add the corresponding default values for the remaining columns (or NULL for any column lacking an explicitly-assigned default value). In Spark 3.3 or earlier, column_list's size must be equal to the target table's column size, otherwise these commands would have failed.

- **value_expr** ( { value | NULL } [ , … ] ) [ , ( … ) ]: Specifies the values to be inserted. Either an explicitly specified value or a NULL can be inserted. A comma must be used to separate each value in the clause. More than one set of values can be specified to insert multiple rows.

For more information, please check the syntax document: [Spark INSERT Statement](https://spark.apache.org/docs/latest/sql-ref-syntax-dml-insert-table.html)

### Insert Into

Use `INSERT INTO` to apply records and changes to tables.

```sql
INSERT INTO my_table SELECT ...
```

### Insert Overwrite

Use `INSERT OVERWRITE` to overwrite the whole table.

```sql
INSERT OVERWRITE my_table SELECT ...
```

#### Insert Overwrite Partition

Use `INSERT OVERWRITE` to overwrite a partition.

```sql
INSERT OVERWRITE my_table PARTITION (key1 = value1, key2 = value2, ...) SELECT ...
```

#### Dynamic Overwrite Partition

Spark's default overwrite mode is `static` partition overwrite. To enable dynamic overwritten you need to set the Spark session configuration `spark.sql.sources.partitionOverwriteMode` to `dynamic`

For example:

```sql
CREATE TABLE my_table (id INT, pt STRING) PARTITIONED BY (pt);
INSERT INTO my_table VALUES (1, 'p1'), (2, 'p2');

-- Static overwrite (Overwrite the whole table)
INSERT OVERWRITE my_table VALUES (3, 'p1');

SELECT * FROM my_table;
/*
+---+---+
| id| pt|
+---+---+
|  3| p1|
+---+---+
*/

-- Dynamic overwrite (Only overwrite pt='p1')
SET spark.sql.sources.partitionOverwriteMode=dynamic;
INSERT OVERWRITE my_table VALUES (3, 'p1');

SELECT * FROM my_table;
/*
+---+---+
| id| pt|
+---+---+
|  2| p2|
|  3| p1|
+---+---+
*/
```

## Truncate Table

The `TRUNCATE TABLE` statement removes all the rows from a table or partition(s).

```sql
TRUNCATE TABLE my_table;
```

## Update Table

spark supports update PrimitiveType and StructType, for example:

```sql
-- Syntax
UPDATE table_identifier SET column1 = value1, column2 = value2, ... WHERE condition;

CREATE TABLE t (
  id INT, 
  s STRUCT<c1: INT, c2: STRING>, 
  name STRING)
TBLPROPERTIES (
  'primary-key' = 'id', 
  'merge-engine' = 'deduplicate'
);

-- you can use
UPDATE t SET name = 'a_new' WHERE id = 1;
UPDATE t SET s.c2 = 'a_new' WHERE s.c1 = 1;
```

## Delete From Table

```sql
DELETE FROM my_table WHERE currency = 'UNKNOWN';
```

## Merge Into Table

Paimon currently supports Merge Into syntax in Spark 3+, which allow a set of updates, insertions and deletions based on a source table in a single commit.

{{< hint into >}}
1. In update clause, to update primary key columns is not supported.
2. `WHEN NOT MATCHED BY SOURCE` syntax is not supported.
{{< /hint >}}

**Example: One**

This is a simple demo that, if a row exists in the target table update it, else insert it.

```sql

-- Here both source and target tables have the same schema: (a INT, b INT, c STRING), and a is a primary key.

MERGE INTO target
USING source
ON target.a = source.a
WHEN MATCHED THEN
UPDATE SET *
WHEN NOT MATCHED
THEN INSERT *

```

**Example: Two**

This is a demo with multiple, conditional clauses.

```sql

-- Here both source and target tables have the same schema: (a INT, b INT, c STRING), and a is a primary key.

MERGE INTO target
USING source
ON target.a = source.a
WHEN MATCHED AND target.a = 5 THEN
   UPDATE SET b = source.b + target.b      -- when matched and meet the condition 1, then update b;
WHEN MATCHED AND source.c > 'c2' THEN
   UPDATE SET *    -- when matched and meet the condition 2, then update all the columns;
WHEN MATCHED THEN
   DELETE      -- when matched, delete this row in target table;
WHEN NOT MATCHED AND c > 'c9' THEN
   INSERT (a, b, c) VALUES (a, b * 1.1, c)      -- when not matched but meet the condition 3, then transform and insert this row;
WHEN NOT MATCHED THEN
INSERT *      -- when not matched, insert this row without any transformation;

```

## Streaming Write

{{< hint info >}}

Paimon currently supports Spark 3+ for streaming write.

Paimon Structured Streaming only supports the two `append` and `complete` modes.

{{< /hint >}}

```scala
// Create a paimon table if not exists.
spark.sql(s"""
           |CREATE TABLE T (k INT, v STRING)
           |TBLPROPERTIES ('primary-key'='k', 'bucket'='3')
           |""".stripMargin)

// Here we use MemoryStream to fake a streaming source.
val inputData = MemoryStream[(Int, String)]
val df = inputData.toDS().toDF("k", "v")

// Streaming Write to paimon table.
val stream = df
  .writeStream
  .outputMode("append")
  .option("checkpointLocation", "/path/to/checkpoint")
  .format("paimon")
  .start("/path/to/paimon/sink/table")
```

## Schema Evolution

Schema evolution is a feature that allows users to easily modify the current schema of a table to adapt to existing data, or new data that changes over time, while maintaining data integrity and consistency.

Paimon supports automatic schema merging of source data and current table data while data is being written, and uses the merged schema as the latest schema of the table, and it only requires configuring `write.merge-schema`.

```scala
data.write
  .format("paimon")
  .mode("append")
  .option("write.merge-schema", "true")
  .save(location)
```

When enable `write.merge-schema`, Paimon can allow users to perform the following actions on table schema by default:
- Adding columns
- Up-casting the type of column(e.g. Int -> Long)

Paimon also supports explicit type conversions between certain types (e.g. String -> Date, Long -> Int), it requires an explicit configuration `write.merge-schema.explicit-cast`.

Schema evolution can be used in streaming mode at the same time.

```scala
val inputData = MemoryStream[(Int, String)]
inputData
  .toDS()
  .toDF("col1", "col2")
  .writeStream
  .format("paimon")
  .option("checkpointLocation", "/path/to/checkpoint")
  .option("write.merge-schema", "true")
  .option("write.merge-schema.explicit-cast", "true")
  .start(location)
```

Here list the configurations.

<table class="configuration table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 20%">Scan Mode</th>
            <th class="text-left" style="width: 60%">Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>write.merge-schema</h5></td>
            <td>If true, merge the data schema and the table schema automatically before write data.</td>
        </tr>
        <tr>
            <td><h5>write.merge-schema.explicit-cast</h5></td>
            <td>If true, allow to merge data types if the two types meet the rules for explicit casting.</td>
        </tr>
    </tbody>
</table>
