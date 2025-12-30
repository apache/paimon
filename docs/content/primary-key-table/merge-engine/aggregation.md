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
  You can use `fields.<field-name>.distinct=true` to deduplicate values split by the `fields.<field-name>.list-agg-delimiter`.

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
  It supports VARBINARY data type which must be serialized 32-bit RoaringBitmap.

  RoaringBitmap is a compressed bitmap that efficiently represents sets of integers. The rbm32 aggregator is useful for scenarios where you need to merge multiple bitmap sets or use case requiring set union operations on large integer datasets.

  **Example:**

  ```sql
  -- Create a table to store user visit data with bitmap aggregation
  CREATE TABLE user_visits (
    user_id INT PRIMARY KEY NOT ENFORCED,
    visit_bitmap VARBINARY
  ) WITH (
    'merge-engine' = 'aggregation',
    'fields.visit_bitmap.aggregate-function' = 'rbm32'
  );

  -- Register a UDF to create RoaringBitmap from user IDs
  -- CREATE TEMPORARY FUNCTION TO_BITMAP AS 'BitmapUDF';

  -- Insert data with bitmap representations
  INSERT INTO user_visits VALUES
    (1, TO_BITMAP(100, 101, 102)),  -- User 1 visited pages 100, 101, 102
    (2, TO_BITMAP(101, 103)),       -- User 2 visited pages 101, 103
    (3, TO_BITMAP(102, 104));       -- User 3 visited pages 102, 104

  -- When the same user_id is inserted again, the bitmaps will be merged
  INSERT INTO user_visits VALUES
    (1, TO_BITMAP(103, 105)),       -- User 1 also visited pages 103, 105
    (2, TO_BITMAP(104, 106));       -- User 2 also visited pages 104, 106

  -- The final result will have merged bitmaps for each user
  -- User 1: pages 100, 101, 102, 103, 105
  -- User 2: pages 101, 103, 104, 106
  -- User 3: pages 102, 104
  ```

### rbm64
  The rbm64 function aggregates multiple serialized 64-bit Roaring64Bitmap into a single Roaring64Bitmap.
  It supports VARBINARY data type which must be serialized 64-bit RoaringBitmap.

  Similar to rbm32, but supports 64-bit integers, making it suitable for scenarios with very large integer values
  or when you need to represent sets with values beyond the 32-bit range (up to 2^31-1).

  **Example:**

  ```sql
  -- Create a table to store large-scale user interaction data
  CREATE TABLE user_interactions (
    session_id BIGINT PRIMARY KEY NOT ENFORCED,
    interaction_bitmap VARBINARY
  ) WITH (
    'merge-engine' = 'aggregation',
    'fields.interaction_bitmap.aggregate-function' = 'rbm64'
  );

  -- Register a UDF to create Roaring64Bitmap from large user IDs
  -- CREATE TEMPORARY FUNCTION TO_BITMAP64 AS 'Bitmap64UDF';

  -- Insert data with 64-bit bitmap representations
  INSERT INTO user_interactions VALUES
    (1001, TO_BITMAP64(1000000001L, 1000000002L, 1000000003L)),
    (1002, TO_BITMAP64(1000000002L, 1000000004L)),
    (1003, TO_BITMAP64(1000000003L, 1000000005L));

  -- Merge additional interactions
  INSERT INTO user_interactions VALUES
    (1001, TO_BITMAP64(1000000004L, 1000000006L)),
    (1002, TO_BITMAP64(1000000005L, 1000000007L));

  -- The final result will have merged 64-bit bitmaps for each session
  ```

The `rbm32` and `rbm64` aggregators work by:
1. Deserializing the input VARBINARY data into RoaringBitmap objects
2. Performing bitwise OR operations to merge the bitmaps
3. Serializing the result back to VARBINARY format

**Working with RoaringBitmap Functions:**

Paimon currently does not provide built-in Flink UDFs for bitmap creation. You have two options:

1. Create bitmaps programmatically: in your application code and insert serialized bytes
2. Create custom Flink UDFs: to convert raw integers to serialized bitmap format

Here are examples of both approaches:

**Option 1: Programmatic Bitmap Creation (Java/Scala)**

```java
// Create bitmaps programmatically
RoaringBitmap32 bitmap1 = RoaringBitmap32.bitmapOf(100, 101, 102);
RoaringBitmap32 bitmap2 = RoaringBitmap32.bitmapOf(101, 103);
RoaringBitmap32 bitmap3 = RoaringBitmap32.bitmapOf(102, 104);

byte[] serialized1 = bitmap1.serialize();
byte[] serialized2 = bitmap2.serialize();
byte[] serialized3 = bitmap3.serialize();

INSERT INTO user_page_visits VALUES
    (1, CAST(x'...' AS VARBINARY)),  -- serialized1 bytes
    (2, CAST(x'...' AS VARBINARY)),  -- serialized2 bytes
    (3, CAST(x'...' AS VARBINARY));  -- serialized3 bytes
```

**Option 2: Custom Flink UDFs**

```sql
-- Create the aggregation table
CREATE TABLE user_page_visits (
    user_id INT PRIMARY KEY NOT ENFORCED,
    page_visits VARBINARY
) WITH (
    'merge-engine' = 'aggregation',
    'fields.page_visits.aggregate-function' = 'rbm32'
);

-- Register custom UDFs (you need to implement these)

-- Use the UDFs
INSERT INTO user_page_visits VALUES
    (1, TO_BITMAP(100, 101, 102)),
    (2, TO_BITMAP(101, 103)),
    (3, TO_BITMAP(102, 104));

-- Query with UDFs
SELECT user_id, FROM_BITMAP(page_visits) as unique_pages
FROM user_page_visits;

SELECT user_id,
       BITMAP_CONTAINS(page_visits, 101) as visited_page_101
FROM user_page_visits
WHERE user_id = 1;
```

**Sample UDF Implementation (Java):**

```java
// BitmapUDF - Converts integers to serialized RoaringBitmap
public static class BitmapUDF extends ScalarFunction {
    public byte[] eval(Integer... values) {
        RoaringBitmap32 bitmap = new RoaringBitmap32();
        for (Integer value : values) {
            if (value != null) {
                bitmap.add(value);
            }
        }
        return bitmap.serialize();
    }
}

// BitmapCountUDF - Gets cardinality from serialized RoaringBitmap
public static class BitmapCountUDF extends ScalarFunction {
    public Long eval(byte[] bitmapBytes) {
        if (bitmapBytes == null) {
            return 0L;
        }
        try {
            RoaringBitmap32 bitmap = new RoaringBitmap32();
            bitmap.deserialize(ByteBuffer.wrap(bitmapBytes));
            return bitmap.getCardinality();
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize bitmap", e);
        }
    }
}

// BitmapContainsUDF - Checks if value exists in bitmap
public static class BitmapContainsUDF extends ScalarFunction {
    public Boolean eval(byte[] bitmapBytes, Integer value) {
        if (bitmapBytes == null || value == null) {
            return false;
        }
        try {
            RoaringBitmap32 bitmap = new RoaringBitmap32();
            bitmap.deserialize(ByteBuffer.wrap(bitmapBytes));
            return bitmap.contains(value);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize bitmap", e);
        }
    }
}
```

### nested_update
  The nested_update function collects multiple rows into one array<row> (so-called 'nested table'). It supports ARRAY<ROW> data types.

  Use `fields.<field-name>.nested-key=pk0,pk1,...` to specify the primary keys of the nested table. If no keys, row will be appended to array<row>.

  Use `fields.<field-name>.count-limit=<Interger>` to specify the maximum number of rows in the nested table. When no nested-key, it will select data
  sequentially up to limit; but if nested-key is specified, it cannot guarantee the correctness of the aggregation result. This option can be used to
  avoid abnormal input.

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

### nested_partial_update
  The nested_partial_update function collects multiple rows into one array<row> (so-called 'nested table'). It supports
  ARRAY<ROW> data types. You need to use `fields.<field-name>.nested-key=pk0,pk1,...` to specify the primary keys of the
  nested table. The values in each row are written by partial updating some columns.

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

The `product` will return null for retraction message when accumulator is null.

The `collect` and `merge_map` make a best-effort attempt to handle retraction messages, but the results are not
guaranteed to be accurate. The following behaviors may occur when processing retraction messages:

1. It might fail to handle retraction messages if records are disordered. For example, the table uses `collect`, and the
   upstreams send `+I['A', 'B']` and `-U['A']` respectively. If the table receives `-U['A']` first, it can do nothing; then it receives
   `+I['A', 'B']`, the merge result will be `+I['A', 'B']` instead of `+I['B']`.

2. The retract message from one upstream will retract the result merged from multiple upstreams. For example, the table
   uses `merge_map`, and one upstream sends `+I[1->A]`, another upstream sends `+I[1->B]`, `-D[1->B]` later. The table will
   merge two insert values to `+I[1->B]` first, and then the `-D[1->B]` will retract the whole result, so the final result
   is an empty map instead of `+I[1->A]`
