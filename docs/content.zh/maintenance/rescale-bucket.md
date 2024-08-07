---
title: "Rescale Bucket"
weight: 7
type: docs
aliases:
- /maintenance/rescale-bucket.html
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

# 调整存储桶

由于总存储桶数量极大地影响性能，Paimon允许用户通过`ALTER TABLE`命令调整存储桶数量，并通过`INSERT OVERWRITE`重新组织数据布局，而无需重新创建表/分区。在执行覆盖作业时，框架将自动使用旧的存储桶数量扫描数据，并根据当前的存储桶数量对记录进行哈希处理。

## 调整覆盖

```sql
-- 调整总存储桶数量
ALTER TABLE table_identifier SET ('bucket' = '...');

-- 重新组织表/分区的数据布局
INSERT OVERWRITE table_identifier [PARTITION (part_spec)]
SELECT ... 
FROM table_identifier
[WHERE part_spec];
```

请注意：

* `ALTER TABLE`仅修改表的元数据，不会重新组织或重新格式化现有数据。重新组织现有数据必须通过`INSERT OVERWRITE`来实现。
* 调整存储桶数量不会影响读取和正在运行的写作业。
* 一旦存储桶数量更改，任何新计划的`INSERT INTO`作业都将写入未重新组织的现有表/分区，并抛出类似以下消息的`TableException`：
    
    ```text
    尝试使用新的存储桶编号...来写入表/分区...，但以前的存储桶编号是...。请切换到批处理模式，并首先执行INSERT OVERWRITE以重新组织当前的数据布局。
    ```
    
* 对于分区表，不同分区可以具有不同的存储桶数量。例如：
    
    ```sql
    ALTER TABLE my_table SET ('bucket' = '4');
    INSERT OVERWRITE my_table PARTITION (dt = '2022-01-01')
    SELECT * FROM ...;
    
    ALTER TABLE my_table SET ('bucket' = '8');
    INSERT OVERWRITE my_table PARTITION (dt = '2022-01-02')
    SELECT * FROM ...;
    ```
    
* 在覆盖期间，请确保没有其他作业写入相同的表/分区。

{{< hint info >}} **注意：** 对于启用了日志系统（例如Kafka）的表，请同时调整主题的分区以保持一致性。 {{< /hint >}}

## 应用案例

调整存储桶有助于处理吞吐量突然增加的情况。假设有一个每日的流式ETL任务用于同步交易数据。表的DDL和流水线如下所示。

```sql
-- 表的DDL
CREATE TABLE verified_orders (
    trade_order_id BIGINT,
    item_id BIGINT,
    item_price DOUBLE,
    dt STRING,
    PRIMARY KEY (dt, trade_order_id, item_id) NOT ENFORCED 
) PARTITIONED BY (dt)
WITH (
    'bucket' = '16'
);

-- 来自Kafka表
CREATE TEMPORARY TABLE raw_orders(
    trade_order_id BIGINT,
    item_id BIGINT,
    item_price BIGINT,
    gmt_create STRING,
    order_status STRING
) WITH (
    'connector' = 'kafka',
    'topic' = '...',
    'properties.bootstrap.servers' = '...',
    'format' = 'csv'
    ...
);

-- 以16个存储桶的流式插入
INSERT INTO verified_orders
SELECT trade_order_id,
       item_id,
       item_price,
       DATE_FORMAT(gmt_create, 'yyyy-MM-dd') AS dt
FROM raw_orders
WHERE order_status = 'verified';
```

过去几周，这个流水线一直运行得很好。然而，最近数据量增长迅速，作业的延迟不断增加。为了提高数据的新鲜度，用户可以：

* 暂停流式作业并创建一个保存点（请参见[Suspended State](https://nightlies.apache.org/flink/flink-docs-stable/docs/internals/job_scheduling/)和[Gracefully Creating a Final Savepoint](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/cli/) ）
    
    ```bash
    $ ./bin/flink stop \
          --savepointPath /tmp/flink-savepoints \
          $JOB_ID
    ```
    
* 增加存储桶数量
    
    ```sql
    -- 扩展存储桶
    ALTER TABLE verified_orders SET ('bucket' = '32');
    ```
    
* 切换到批处理模式，并覆盖流式作业正在写入的当前分区（们）
    
    ```sql
    SET 'execution.runtime-mode' = 'batch';
    -- 假设今天是2022-06-22
    -- 情况1：没有更新历史分区的延迟事件，因此仅覆盖今天的分区足够
    INSERT OVERWRITE verified_orders PARTITION (dt = '2022-06-22')
    SELECT trade_order_id,
           item_id,
           item_price
    FROM verified_orders
    WHERE dt = '2022-06-22';
    
    -- 情况2：有延迟事件更新历史分区，但范围不超过3天
    INSERT OVERWRITE verified_orders
    SELECT trade_order_id,
           item_id,
           item_price,
           dt
    FROM verified_orders
    WHERE dt IN ('2022-06-20', '2022-06-21', '2022-06-22');
    ```
    
* 在覆盖作业完成后，切换回流式模式。现在，可以增加并行度以及存储桶数量，从保存点恢复流式作业 （请参见[从保存点启动SQL作业](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sqlclient/#start-a-sql-job-from-a-savepoint) ）
    
    ```sql
    SET 'execution.runtime-mode' = 'streaming';
    SET 'execution.savepoint.path' = <savepointPath>;
    
    INS
    ```