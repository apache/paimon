---
title: "Merge Engine"
weight: 3
type: docs
aliases:
- /primary-key-table/merge-engine.html
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

# Merge Engine

When Paimon sink receives two or more records with the same primary keys, it will merge them into one record to keep primary keys unique. By specifying the `merge-engine` table property, users can choose how records are merged together.

{{< hint info >}}
Always set `table.exec.sink.upsert-materialize` to `NONE` in Flink SQL TableConfig, sink upsert-materialize may
result in strange behavior. When the input is out of order, we recommend that you use
[Sequence Field]({{< ref "primary-key-table/sequence-rowkind#sequence-field" >}}) to correct disorder.
{{< /hint >}}

{{< hint info >}}
Some compute engines support row level update and delete in batch mode but not all merge engines support them.
- Support batch update merge engines: `deduplicate` and `first-row`.
- Support batch delete merge engines: `deduplicate`.
{{< /hint >}}

## Deduplicate

`deduplicate` merge engine is the default merge engine. Paimon will only keep the latest record and throw away other records with the same primary keys.

Specifically, if the latest record is a `DELETE` record, all records with the same primary keys will be deleted.  You can config `ignore-delete` to ignore it.

## Partial Update

By specifying `'merge-engine' = 'partial-update'`,
Users have the ability to update columns of a record through multiple updates until the record is complete. This is achieved by updating the value fields one by one, using the latest data under the same primary key. However, null values are not overwritten in the process.

For example, suppose Paimon receives three records:
- `<1, 23.0, 10, NULL>`-
- `<1, NULL, NULL, 'This is a book'>`
- `<1, 25.2, NULL, NULL>`

Assuming that the first column is the primary key, the final result would be `<1, 25.2, 10, 'This is a book'>`.

{{< hint info >}}
For streaming queries, `partial-update` merge engine must be used together with `lookup` or `full-compaction`
[changelog producer]({{< ref "primary-key-table/changelog-producer" >}}). ('input' changelog producer is also supported, but only returns input records.)
{{< /hint >}}

{{< hint info >}}
By default, Partial update can not accept delete records, you can choose one of the following solutions:
- Configure 'ignore-delete' to ignore delete records.
- Configure 'sequence-group's to retract partial columns.
  {{< /hint >}}

### Sequence Group

A sequence-field may not solve the disorder problem of partial-update tables with multiple stream updates, because
the sequence-field may be overwritten by the latest data of another stream during multi-stream update.

So we introduce sequence group mechanism for partial-update tables. It can solve:

1. Disorder during multi-stream update. Each stream defines its own sequence-groups.
2. A true partial-update, not just a non-null update.

See example:

```sql
CREATE TABLE t (
    k INT,
    a INT,
    b INT,
    g_1 INT,
    c INT,
    d INT,
    g_2 INT,
    PRIMARY KEY (k) NOT ENFORCED
) WITH (
    'merge-engine'='partial-update',
    'fields.g_1.sequence-group'='a,b',
    'fields.g_2.sequence-group'='c,d'
);

INSERT INTO t VALUES (1, 1, 1, 1, 1, 1, 1);

-- g_2 is null, c, d should not be updated
INSERT INTO t VALUES (1, 2, 2, 2, 2, 2, CAST(NULL AS INT));

SELECT * FROM t; -- output 1, 2, 2, 2, 1, 1, 1

-- g_1 is smaller, a, b should not be updated
INSERT INTO t VALUES (1, 3, 3, 1, 3, 3, 3);

SELECT * FROM t; -- output 1, 2, 2, 2, 3, 3, 3
```

For `fields.<field-name>.sequence-group`, valid comparative data types include: DECIMAL, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, DATE, TIME, TIMESTAMP, and TIMESTAMP_LTZ.

### Aggregation For Partial Update

You can specify aggregation function for the input field, all the functions in the [Aggregation]({{< ref "primary-key-table/merge-engine#aggregation" >}}) are supported.

See example:

```sql
CREATE TABLE t (
          k INT,
          a INT,
          b INT,
          c INT,
          d INT,
          PRIMARY KEY (k) NOT ENFORCED
) WITH (
     'merge-engine'='partial-update',
     'fields.a.sequence-group' = 'b',
     'fields.b.aggregate-function' = 'first_value',
     'fields.c.sequence-group' = 'd',
     'fields.d.aggregate-function' = 'sum'
 );
INSERT INTO t VALUES (1, 1, 1, CAST(NULL AS INT), CAST(NULL AS INT));
INSERT INTO t VALUES (1, CAST(NULL AS INT), CAST(NULL AS INT), 1, 1);
INSERT INTO t VALUES (1, 2, 2, CAST(NULL AS INT), CAST(NULL AS INT));
INSERT INTO t VALUES (1, CAST(NULL AS INT), CAST(NULL AS INT), 2, 2);


SELECT * FROM t; -- output 1, 2, 1, 2, 3
```

## Aggregation

{{< hint info >}}
NOTE: Always set `table.exec.sink.upsert-materialize` to `NONE` in Flink SQL TableConfig.
{{< /hint >}}

Sometimes users only care about aggregated results. The `aggregation` merge engine aggregates each value field with the latest data one by one under the same primary key according to the aggregate function.

Each field not part of the primary keys can be given an aggregate function, specified by the `fields.<field-name>.aggregate-function` table property, otherwise it will use `last_non_null_value` aggregation as default. For example, consider the following table definition.

{{< tabs "aggregation-merge-engine-example" >}}

{{< tab "Flink" >}}

```sql
CREATE TABLE my_table (
    product_id BIGINT,
    price DOUBLE,
    sales BIGINT,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'merge-engine' = 'aggregation',
    'fields.price.aggregate-function' = 'max',
    'fields.sales.aggregate-function' = 'sum'
);
```

{{< /tab >}}

{{< /tabs >}}

Field `price` will be aggregated by the `max` function, and field `sales` will be aggregated by the `sum` function. Given two input records `<1, 23.0, 15>` and `<1, 30.2, 20>`, the final result will be `<1, 30.2, 35>`.

Current supported aggregate functions and data types are:

* `sum`:
  The sum function aggregates the values across multiple rows.
  It supports DECIMAL, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, and DOUBLE data types.

* `product`:
  The product function can compute product values across multiple lines.
  It supports DECIMAL, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, and DOUBLE data types.

* `count`:
  The count function counts the values across multiple rows.
  It supports INTEGER, BIGINT data types.

* `max`:
  The max function identifies and retains the maximum value.
  It supports CHAR, VARCHAR, DECIMAL, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, DATE, TIME, TIMESTAMP, and TIMESTAMP_LTZ data types.

* `min`:
  The min function identifies and retains the minimum value.
  It supports CHAR, VARCHAR, DECIMAL, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, DATE, TIME, TIMESTAMP, and TIMESTAMP_LTZ data types.

* `last_value`:
  The last_value function replaces the previous value with the most recently imported value.
  It supports all data types.

* `last_non_null_value`:
  The last_non_null_value function replaces the previous value with the latest non-null value.
  It supports all data types.

* `listagg`:
  The listagg function concatenates multiple string values into a single string.
  It supports STRING data type.

* `bool_and`:
  The bool_and function evaluates whether all values in a boolean set are true.
  It supports BOOLEAN data type.

* `bool_or`:
  The bool_or function checks if at least one value in a boolean set is true.
  It supports BOOLEAN data type.

* `first_value`:
  The first_value function retrieves the first null value from a data set.
  It supports all data types.

* `first_non_null_value`:
  The first_non_null_value function selects the first non-null value in a data set.
  It supports all data types.

* `nested_update`:
  The nested_update function collects multiple rows into one array<row> (so-called 'nested table'). It supports ARRAY<ROW> data types.

  Use `fields.<field-name>.nested-key=pk0,pk1,...` to specify the primary keys of the nested table. If no keys, row will be appended to array<row>.

  An example:

  {{< tabs "nested_update-example" >}}

  {{< tab "Flink" >}}

  ```sql
  -- orders table
  CREATE TABLE orders (
    order_id BIGINT PRIMARY KEY NOT ENFORCED,
    user_name STRING,
    address STRING
  );
  
  -- sub orders that have the same order_id 
  -- belongs to the same order
  CREATE TABLE sub_orders (
    order_id BIGINT,
    sub_order_id INT,
    product_name STRING,
    price BIGINT,
    PRIMARY KEY (order_id, sub_order_id) NOT ENFORCED
  );
  
  -- wide table
  CREATE TABLE order_wide (
    order_id BIGINT PRIMARY KEY NOT ENFORCED,
    user_name STRING,
    address STRING,
    sub_orders ARRAY<ROW<sub_order_id BIGINT, product_name STRING, price BIGINT>>
  ) WITH (
    'merge-engine' = 'aggregation',
    'fields.sub_orders.aggregate-function' = 'nested_update',
    'fields.sub_orders.nested-key' = 'sub_order_id'
  );
  
  -- widen
  INSERT INTO order_wide
  
  SELECT 
    order_id, 
    user_name,
    address, 
    CAST (NULL AS ARRAY<ROW<sub_order_id BIGINT, product_name STRING, price BIGINT>>) 
  FROM orders
  
  UNION ALL 
    
  SELECT 
    order_id, 
    CAST (NULL AS STRING), 
    CAST (NULL AS STRING), 
    ARRAY[ROW(sub_order_id, product_name, price)] 
  FROM sub_orders;
  
  -- query using UNNEST
  SELECT order_id, user_name, address, sub_order_id, product_name, price 
  FROM order_wide, UNNEST(sub_orders) AS so(sub_order_id, product_name, price)
  ```

  {{< /tab >}}

  {{< /tabs >}}

* `collect`:
  The collect function collects elements into an Array. You can set `fields.<field-name>.distinct=true` to deduplicate elements.
  It only supports ARRAY type.

* `merge_map`:
  The merge_map function merge input maps. It only supports MAP type.

{{< hint info >}}
For streaming queries, `aggregation` merge engine must be used together with `lookup` or `full-compaction`
[changelog producer]({{< ref "primary-key-table/changelog-producer" >}}). ('input' changelog producer is also supported, but only returns input records.)
{{< /hint >}}

### Retract

Only `sum`, `product`, `count`, `collect`, `merge_map`, `nested_update`, `last_value` and `last_non_null_value` supports retraction (`UPDATE_BEFORE` and `DELETE`), others aggregate functions do not support retraction.
If you allow some functions to ignore retraction messages, you can configure:
`'fields.${field_name}.ignore-retract'='true'`.

The `last_value` and `last_non_null_value` just set field to null when accept retract messages.

The `collect` and `merge_map` make a best-effort attempt to handle retraction messages, but the results are not 
guaranteed to be accurate. The following behaviors may occur when processing retraction messages:

1. It might fail to handle retraction messages if records are disordered. For example, the table uses `collect`, and the 
upstreams send `+I['A', 'B']` and `-U['A']` respectively. If the table receives `-U['A']` first, it can do nothing; then it receives
`+I['A', 'B']`, the merge result will be `+I['A', 'B']` instead of `+I['B']`.

2. The retract message from one upstream will retract the result merged from multiple upstreams. For example, the table 
uses `merge_map`, and one upstream sends `+I[1->A]`, another upstream sends `+I[1->B]`, `-D[1->B]` later. The table will 
merge two insert values to `+I[1->B]` first, and then the `-D[1->B]` will retract the whole result, so the final result 
is an empty map instead of `+I[1->A]`

## First Row

By specifying `'merge-engine' = 'first-row'`, users can keep the first row of the same primary key. It differs from the
`deduplicate` merge engine that in the `first-row` merge engine, it will generate insert only changelog.

{{< hint info >}}
1. `first-row` merge engine must be used together with `lookup` [changelog producer]({{< ref "primary-key-table/changelog-producer" >}}).
2. You can not specify `sequence.field`.
3. Not accept `DELETE` and `UPDATE_BEFORE` message. You can config `ignore-delete` to ignore these two kinds records.
   {{< /hint >}}

This is of great help in replacing log deduplication in streaming computation.
