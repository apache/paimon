---
title: "Aggregation"
weight: 3
type: docs
aliases:
- /primary-key-table/merge-engin/aggregation.html
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

# Aggregation

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

## Aggregation Functions

Current supported aggregate functions and data types are:

### sum

  The sum function aggregates the values across multiple rows.
  It supports DECIMAL, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, and DOUBLE data types.

### product
  The product function can compute product values across multiple lines.
  It supports DECIMAL, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, and DOUBLE data types.

### count
  In scenarios where counting rows that match a specific condition is required, you can use the SUM function to achieve this. By expressing a condition as a Boolean value (TRUE or FALSE) and converting it into a numerical value, you can effectively count the rows. In this approach, TRUE is converted to 1, and FALSE is converted to 0.

  For example, if you have a table orders and want to count the number of rows that meet a specific condition, you can use the following query:
  ```sql
  SELECT SUM(CASE WHEN condition THEN 1 ELSE 0 END) AS count
  FROM orders;
  ```


### max
  The max function identifies and retains the maximum value.
  It supports CHAR, VARCHAR, DECIMAL, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, DATE, TIME, TIMESTAMP, and TIMESTAMP_LTZ data types.

### min
  The min function identifies and retains the minimum value.
  It supports CHAR, VARCHAR, DECIMAL, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, DATE, TIME, TIMESTAMP, and TIMESTAMP_LTZ data types.

### last_value
  The last_value function replaces the previous value with the most recently imported value.
  It supports all data types.

### last_non_null_value
  The last_non_null_value function replaces the previous value with the latest non-null value.
  It supports all data types.

### listagg
  The listagg function concatenates multiple string values into a single string.
  It supports STRING data type.
  Each field not part of the primary keys can be given a list agg delimiter, specified by the fields.<field-name>.list-agg-delimiter table property, otherwise it will use "," as default.

### bool_and
  The bool_and function evaluates whether all values in a boolean set are true.
  It supports BOOLEAN data type.

### bool_or
  The bool_or function checks if at least one value in a boolean set is true.
  It supports BOOLEAN data type.

### first_value
  The first_value function retrieves the first null value from a data set.
  It supports all data types.

### first_non_null_value
  The first_non_null_value function selects the first non-null value in a data set.
  It supports all data types.

### rbm32
  The rbm32 function aggregates multiple serialized 32-bit RoaringBitmap into a single RoaringBitmap.
  It supports VARBINARY data type.

### rbm64
  The rbm64 function aggregates multiple serialized 64-bit Roaring64Bitmap into a single Roaring64Bitmap.
  It supports VARBINARY data type.

### nested_update
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

### collect
  The collect function collects elements into an Array. You can set `fields.<field-name>.distinct=true` to deduplicate elements.
  It only supports ARRAY type.

### merge_map
  The merge_map function merge input maps. It only supports MAP type.

### Types of cardinality sketches

 Paimon uses the [Apache DataSketches](https://datasketches.apache.org/) library of stochastic streaming algorithms to implement sketch modules. The DataSketches library includes various types of sketches, each one designed to solve a different sort of problem. Paimon supports HyperLogLog (HLL) and Theta cardinality sketches.

#### HyperLogLog

 The HyperLogLog (HLL) sketch aggregator is a very compact sketch algorithm for approximate distinct counting.  You can also use the HLL aggregator to calculate a union of HLL sketches. 

#### Theta

 The Theta sketch is a sketch algorithm for approximate distinct counting with set operations. Theta sketches let you count the overlap between sets, so that you can compute the union, intersection, or set difference between sketch objects.

#### Choosing a sketch type

  HLL and Theta sketches both support approximate distinct counting; however, the HLL sketch produces more accurate results and consumes less storage space. Theta sketches are more flexible but require significantly more memory.

When choosing an approximation algorithm for your use case, consider the following:

If your use case entails distinct counting and merging sketch objects, use the HLL sketch.
If you need to evaluate union, intersection, or difference set operations, use the Theta sketch.
You cannot merge HLL sketches with Theta sketches.

#### hll_sketch

The hll_sketch function aggregates multiple serialized Sketch objects into a single Sketch.
It supports VARBINARY data type.

An example:

{{< tabs "hll_sketch-example" >}}

{{< tab "Flink" >}}

  ```sql
  -- source table
  CREATE TABLE VISITS (
    id INT PRIMARY KEY NOT ENFORCED,
    user_id STRING
  );
  
  -- agg table
  CREATE TABLE UV_AGG (
    id INT PRIMARY KEY NOT ENFORCED,
    uv VARBINARY
  ) WITH (
    'merge-engine' = 'aggregation',
    'fields.uv.aggregate-function' = 'hll_sketch'
  );
  
  -- Register the following class as a Flink function with the name "HLL_SKETCH" 
  -- for example: create TEMPORARY function HLL_SKETCH as  'HllSketchFunction';
  -- which is used to transform input to sketch bytes array:
  --
  -- public static class HllSketchFunction extends ScalarFunction {
  --   public byte[] eval(String user_id) {
  --     HllSketch hllSketch = new HllSketch();
  --     hllSketch.update(user_id);
  --     return hllSketch.toCompactByteArray();
  --   }
  -- }
  --
  INSERT INTO UV_AGG SELECT id, HLL_SKETCH(user_id) FROM VISITS;

  -- Register the following class as a Flink function with the name "HLL_SKETCH_COUNT"
  -- for example: create TEMPORARY function HLL_SKETCH_COUNT as  'HllSketchCountFunction';
  -- which is used to get cardinality from sketch bytes array:
  -- 
  -- public static class HllSketchCountFunction extends ScalarFunction { 
  --   public Double eval(byte[] sketchBytes) {
  --     if (sketchBytes == null) {
  --       return 0d; 
  --     } 
  --     return HllSketch.heapify(sketchBytes).getEstimate(); 
  --   } 
  -- }
  --
  -- Then we can get user cardinality based on the aggregated field.
  SELECT id, HLL_SKETCH_COUNT(UV) as uv FROM UV_AGG;
  ```

{{< /tab >}}

{{< /tabs >}}


#### theta_sketch
  The theta_sketch function aggregates multiple serialized Sketch objects into a single Sketch.
  It supports VARBINARY data type.

  An example:

  {{< tabs "theta_sketch-example" >}}

  {{< tab "Flink" >}}

  ```sql
  -- source table
  CREATE TABLE VISITS (
    id INT PRIMARY KEY NOT ENFORCED,
    user_id STRING
  );
  
  -- agg table
  CREATE TABLE UV_AGG (
    id INT PRIMARY KEY NOT ENFORCED,
    uv VARBINARY
  ) WITH (
    'merge-engine' = 'aggregation',
    'fields.uv.aggregate-function' = 'theta_sketch'
  );
  
  -- Register the following class as a Flink function with the name "THETA_SKETCH" 
  -- for example: create TEMPORARY function THETA_SKETCH as  'ThetaSketchFunction';
  -- which is used to transform input to sketch bytes array:
  --
  -- public static class ThetaSketchFunction extends ScalarFunction {
  --   public byte[] eval(String user_id) {
  --     UpdateSketch updateSketch = UpdateSketch.builder().build();
  --     updateSketch.update(user_id);
  --     return updateSketch.compact().toByteArray();
  --   }
  -- }
  --
  INSERT INTO UV_AGG SELECT id, THETA_SKETCH(user_id) FROM VISITS;

  -- Register the following class as a Flink function with the name "THETA_SKETCH_COUNT"
  -- for example: create TEMPORARY function THETA_SKETCH_COUNT as  'ThetaSketchCountFunction';
  -- which is used to get cardinality from sketch bytes array:
  -- 
  -- public static class ThetaSketchCountFunction extends ScalarFunction { 
  --   public Double eval(byte[] sketchBytes) {
  --     if (sketchBytes == null) {
  --       return 0d; 
  --     } 
  --     return Sketches.wrapCompactSketch(Memory.wrap(sketchBytes)).getEstimate(); 
  --   } 
  -- }
  --
  -- Then we can get user cardinality based on the aggregated field.
  SELECT id, THETA_SKETCH_COUNT(UV) as uv FROM UV_AGG;
  ```

  {{< /tab >}}

  {{< /tabs >}}

{{< hint info >}}
For streaming queries, `aggregation` merge engine must be used together with `lookup` or `full-compaction`
[changelog producer]({{< ref "primary-key-table/changelog-producer" >}}). ('input' changelog producer is also supported, but only returns input records.)
{{< /hint >}}

## Retraction

Only `sum`, `product`, `collect`, `merge_map`, `nested_update`, `last_value` and `last_non_null_value` supports retraction (`UPDATE_BEFORE` and `DELETE`), others aggregate functions do not support retraction.
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
