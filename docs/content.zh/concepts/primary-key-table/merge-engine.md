---
title: "Merge Engine"
weight: 3
type: docs
aliases:
- /concepts/primary-key-table/merge-engine.html
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

# 合并引擎

当 Paimon Sink 接收到具有相同主键的两个或多个记录时，它将合并它们成为一个记录以保持主键的唯一性。通过指定 `merge-engine` 表属性，用户可以选择如何将记录合并在一起。

{{< hint info >}} 在 Flink SQL TableConfig 中始终将 `table.exec.sink.upsert-materialize` 设置为 `NONE`，sink upsert-materialize 可能会导致奇怪的行为。当输入无序时，我们建议您使用 [序列字段]({{< ref "concepts/primary-key-table/sequence-rowkind#sequence-field" >}}) 来纠正无序。 {{< /hint >}}

## 去重

`deduplicate` 合并引擎是默认的合并引擎。Paimon 只会保留最新的记录，并丢弃具有相同主键的其他记录。

具体来说，如果最新的记录是 `DELETE` 记录，则所有具有相同主键的记录都将被删除。

## 部分更新

通过指定 `'merge-engine' = 'partial-update'`， 用户可以通过多次更新记录的值字段，直到记录完整为止。这是通过逐个更新值字段，使用相同主键下的最新数据来实现的。但是，在这个过程中不会覆盖空值。

例如，假设 Paimon 接收到三条记录：

* `<1, 23.0, 10, NULL>`-
* `<1, NULL, NULL, '这是一本书'>`
* `<1, 25.2, NULL, NULL>`

假设第一列是主键，则最终结果将是 `<1, 25.2, 10, '这是一本书'>`。

{{< hint info >}} 对于流查询，必须将 `partial-update` 合并引擎与 `lookup` 或 `full-compaction` [变更日志生成器]({{< ref "concepts/primary-key-table/changelog-producer" >}}) 一起使用。 （'input' 变更日志生成器也受支持，但只返回输入记录。） {{< /hint >}}

{{< hint info >}} 默认情况下，部分更新无法接受删除记录，您可以选择以下解决方案之一：

* 配置 'partial-update.ignore-delete' 以忽略删除记录。
* 配置 'sequence-group' 以撤销部分列。 {{< /hint >}}

### 序列组

序列字段可能无法解决具有多个流更新的部分更新表的无序问题，因为在多流更新期间，序列字段可能会被另一个流的最新数据覆盖。

因此，我们为部分更新表引入了序列组机制。它可以解决以下问题：

1. 多流更新期间的无序问题。每个流都定义了自己的序列组。
2. 真正的部分更新，而不仅仅是非空更新。

请参阅示例：

```sql
CREATE TABLE T (
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

INSERT INTO T VALUES (1, 1, 1, 1, 1, 1, 1);

-- g_2 为空，不应更新 c 和 d
INSERT INTO T VALUES (1, 2, 2, 2, 2, 2, CAST(NULL AS INT));

SELECT * FROM T; -- 输出 1, 2, 2, 2, 1, 1, 1

-- g_1 较小，不应更新 a 和 b
INSERT INTO T VALUES (1, 3, 3, 1, 3, 3, 3);

SELECT * FROM T; -- 输出 1, 2, 2, 2, 3, 3, 3
```

对于 fields.<fieldName>.sequence-group，有效的比较数据类型包括：DECIMAL、TINYINT、SMALLINT、INTEGER、BIGINT、FLOAT、DOUBLE、DATE、TIME、TIMESTAMP 和 TIMESTAMP_LTZ。

### 部分更新的聚合

您可以为输入字段指定聚合函数，[聚合函数]({{< ref "concepts/primary-key-table/merge-engine#aggregation" >}}) 中支持所有函数。

请参阅示例：

```sql
CREATE TABLE T (
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
INSERT INTO T VALUES (1, 1, 1, CAST(NULL AS INT), CAST(NULL AS INT));
INSERT INTO T VALUES (1, CAST(NULL AS INT), CAST(NULL AS INT), 1, 1);
INSERT INTO T VALUES (1, 2, 2, CAST(NULL AS INT), CAST(NULL AS INT));
INSERT INTO T VALUES (1, CAST(NULL AS INT), CAST(NULL AS INT), 2, 2);


SELECT * FROM T; -- 输出 1, 2, 1, 2, 3
```

## 聚合

{{< hint info >}} 注意：在 Flink SQL TableConfig 中始终将 `table.exec.sink.upsert-materialize` 设置为 `NONE`。 {{< /hint >}}

有时用户只关心聚合结果。`aggregation` 合并引擎会根据聚合函数逐个将相同主键下的每个值字段与最新数据聚合在一起。

不属于主键的每个字段都可以指定一个聚合函数，由 `fields.<field-name>.aggregate-function` 表属性指定，否则它将使用 `last_non_null_value` 聚合作为默认值。例如，考虑以下表定义。

{{< tabs "aggregation-merge-engine-example" >}}

{{< tab "Flink" >}}

```sql
CREATE TABLE MyTable (
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

字段 `price` 将由 `max` 函数进行聚合，字段 `sales` 将由 `sum` 函数进行聚合。给定两个输入记录 `<1, 23.0, 15>` 和 `<1, 30.2, 20>`，最终结果将是 `<1, 30.2, 35>`。

当前支持的聚合函数和数据类型有：

* `sum`： sum 函数聚合多行的值。 它支持 DECIMAL、TINYINT、SMALLINT、INTEGER、BIGINT、FLOAT 和 DOUBLE 数据类型。
    
* `product`： product 函数可以计算多行的乘积值。 它支持 DECIMAL、TINYINT、SMALLINT、INTEGER、BIGINT、FLOAT 和 DOUBLE 数据类型。
    
* `count`： count 函数计算多行的值。 它支持 INTEGER 和 BIGINT 数据类型。
    
* `max`： max 函数识别并保留最大值。 它支持 CHAR、VARCHAR、DECIMAL、TINYINT、SMALLINT、INTEGER、BIGINT、FLOAT、DOUBLE、DATE、TIME、TIMESTAMP 和 TIMESTAMP_LTZ 数据类型。
    
* `min`： min 函数识别并保留最小值。 它支持 CHAR、VARCHAR、DECIMAL、TINYINT、SMALLINT、INTEGER、BIGINT、FLOAT、DOUBLE、DATE、TIME、TIMESTAMP 和 TIMESTAMP_LTZ 数据类型。
    
* `last_value`： last_value 函数用最新导入的值替换先前的值。 它支持所有数据类型。
    
* `last_non_null_value`： last_non_null_value 函数用最新的非空值替换先前的值。 它支持所有数据类型。
    
* `listagg`： listagg 函数将多个字符串值连接成单个字符串。 它支持 STRING 数据类型。
    
* `bool_and`： bool_and 函数评估布尔集合中的所有值是否为 true。 它支持 BOOLEAN 数据类型。
    
* `bool_or`： bool_or 函数检查布尔集合中是否至少有一个值为 true。 它支持 BOOLEAN 数据类型。
    
* `first_value`： first_value 函数从数据集中检索第一个空值。 它支持所有数据类型。
    
* `first_not_null_value`： first_not_null_value 函数从数据集中选择第一个非空值。 它支持所有数据类型。
    
* `nested_update`： nested_update 函数将多行收集到一个数组<row>（称为 '嵌套表'）。它支持 ARRAY<ROW> 数据类型。
    
    使用 `fields.<field-name>.nested-key=pk0,pk1,...` 来指定嵌套表的主键。如果没有主键，行将附加到数组<row>。
    
    一个示例：
    
    {{< tabs "nested_update-example" >}}
    
    {{< tab "Flink" >}}
    
    ```sql
    -- 订单表
    CREATE TABLE orders (
      order_id BIGINT PRIMARY KEY NOT ENFORCED,
      user_name STRING,
      address STRING
    );
    
    -- 具有相同 order_id 的子订单属于同一订单
    CREATE TABLE sub_orders (
      order_id BIGINT,
      sub_order_id INT,
      product_name STRING,
      price BIGINT,
      PRIMARY KEY (order_id, sub_order_id) NOT ENFORCED
    );
    
    -- 宽表
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
    
    -- 扩展
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
    
    -- 使用 UNNEST 进行查询
    SELECT order_id, user_name, address, sub_order_id, product_name, price 
    FROM order_wide, UNNEST(sub_orders) AS so(sub_order_id, product_name, price)
    ```
    
    {{< /tab >}}
    
    {{< /tabs >}}
    
* `collect`： collect 函数将元素收集到数组中。您可以设置 `fields.<field-name>.distinct=true` 以去重元素。 它仅支持 ARRAY 类型。
    
* `merge_map`： merge_map 函数合并输入映射。它仅支持 MAP 类型。
    

只有 `sum` 和 `product` 支持撤回 (`UPDATE_BEFORE` 和 `DELETE`)，其他聚合函数不支持撤回。 如果允许某些函数忽略撤回消息，您可以配置： `'fields.${field_name}.ignore-retract'='true'`。

{{< hint info >}} 对于流查询，`aggregation` 合并引擎必须与 `lookup` 或 `full-compaction` [变更日志生成器]({{< ref "concepts/primary-key-table/changelog-producer" >}}) 一起使用。 （'input' 变更日志生成器也受支持，但只返回输入记录。） {{< /hint >}}

## 第一行

通过指定 `'merge-engine' = 'first-row'`，用户可以保留相同主键的第一行。它与 `deduplicate` 合并引擎不同，在 `first-row` 合并引擎中，它将生成仅插入的变更日志。

{{< hint info >}}

1. `first-row` 合并引擎必须与 `lookup` [变更日志生成器]({{< ref "concepts/primary-key-table/changelog-producer" >}}).
2. 不能指定 `sequence.field`。
3. 不接受 `DELETE` 和 `UPDATE_BEFORE` 消息。您可以配置 `first-row.ignore-delete` 来忽略这两种记录。 {{< /hint >}}

这对于在流式计算中替代日志去重非常有帮助。
