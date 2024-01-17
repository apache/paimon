---
title: "Overview"
weight: 1
type: docs
aliases:
- /cdc-ingestion/overview.html
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

# 概述

Paimon支持多种方式将数据导入Paimon表并进行模式演化。这意味着新增的列会实时同步到Paimon表，并且不会为此目的重新启动同步作业。

我们目前支持以下同步方式：

1. MySQL表同步：将一个或多个MySQL表同步到一个Paimon表中。
2. MySQL数据库同步：将整个MySQL数据库同步到一个Paimon数据库中。
3. [程序API同步]({{< ref "/program-api/flink-api#cdc-ingestion-table" >}})：将自定义的DataStream输入同步到一个Paimon表中。
4. Kafka表同步：将一个Kafka主题的表同步到一个Paimon表中。
5. Kafka数据库同步：将包含多个表的一个Kafka主题或包含一个表的多个主题同步到一个Paimon数据库中。
6. MongoDB集合同步：将MongoDB中的一个集合同步到一个Paimon表中。
7. MongoDB数据库同步：将整个MongoDB数据库同步到一个Paimon数据库中。
8. Pulsar表同步：将一个Pulsar主题的表同步到一个Paimon表中。
9. Pulsar数据库同步：将包含多个表的一个Pulsar主题或包含一个表的多个主题同步到一个Paimon数据库中。

## 什么是模式演化

假设我们有一个名为`tableA`的MySQL表，它有三个字段：`field_1`、`field_2`、`field_3`。当我们想要将这个MySQL表加载到Paimon时，我们可以在Flink SQL中执行此操作，或者使用[MySqlSyncTableAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/mysql/MySqlSyncTableAction)。

**Flink SQL:**

在Flink SQL中，如果在导入后更改了MySQL表的表模式，则表模式更改不会同步到Paimon。

{{< img src="/img/cdc-ingestion-flinksql.png">}}

**MySqlSyncTableAction:**

在[MySqlSyncTableAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/mysql/MySqlSyncTableAction)中， 如果在导入后更改了MySQL表的表模式，则表模式更改将同步到Paimon，并且新添加的`field_4`的数据也将同步到Paimon。

{{< img src="/img/cdc-ingestion-schema-evolution.png">}}

## 模式变更演化

Cdc Ingestion支持有限数量的模式更改。目前，该框架无法重命名表或删除列，因此`RENAME TABLE`和`DROP COLUMN`的行为将被忽略，而`RENAME COLUMN`将添加一个新列。目前支持的模式更改包括：

* 添加列。

* Altering column types. More specifically,

    * 从字符串类型（char、varchar、text）更改为另一种具有更长长度的字符串类型，
    * 从二进制类型（binary、varbinary、blob）更改为具有更长长度的另一种二进制类型，
    * 从整数类型（tinyint、smallint、int、bigint）更改为具有更宽范围的另一种整数类型，
    * 从浮点类型（float、double）更改为具有更宽范围的另一种浮点类型，
    
    都得到支持。

## 计算函数

`--computed_column`是计算列的定义。参数字段来自源表字段名称。支持的表达式包括：

{{< generated/compute_column >}}

## 特殊数据类型映射

1. MySQL的TINYINT(1)类型将默认映射为布尔类型。如果您想要像MySQL一样在其中存储数字（-128~127），可以指定类型映射选项 `tinyint1-not-bool`（使用`--type_mapping`），然后该列将映射为Paimon表中的TINYINT。
2. 您可以使用类型映射选项 `to-nullable`（使用`--type_mapping`）来忽略所有非NULL约束（除了主键）。
3. 您可以使用类型映射选项 `to-string`（使用`--type_mapping`）将所有MySQL数据类型映射为STRING。
4. 您可以使用类型映射选项 `char-to-string`（使用`--type_mapping`）将MySQL CHAR(length)/VARCHAR(length)类型映射为STRING。
5. 您可以使用类型映射选项 `longtext-to-bytes`（使用`--type_mapping`）将MySQL LONGTEXT类型映射为BYTES。
6. MySQL的`BIGINT UNSIGNED`、`BIGINT UNSIGNED ZEROFILL`、`SERIAL`将默认映射为`DECIMAL(20, 0)`。您可以使用类型映射选项 `bigint-unsigned-to-bigint`（使用`--type_mapping`）将这些类型映射为Paimon的`BIGINT`，但由于`BIGINT UNSIGNED`最多可以存储20位整数值，而Paimon的`BIGINT`只能存储最多19位整数值，所以在使用此选项时应确保不会发生溢出。
7. MySQL的BIT(1)类型将映射为布尔类型。
8. 当使用Hive目录时，MySQL的TIME类型将映射为STRING。
9. MySQL的BINARY将映射为Paimon的VARBINARY。这是因为二进制值以字节形式传递在binlog中，因此应该映射为字节类型（BYTES或VARBINARY）。我们选择VARBINARY是因为它可以保留长度信息。

## 自定义作业设置

### 检查点

使用`-Dexecution.checkpointing.interval=<interval>`来启用检查点并设置间隔。对于0.7及更高版本，如果您没有启用检查点，Paimon将默认启用检查点并将检查点间隔设置为180秒。

### 作业名称

使用`-Dpipeline.name=<job-name>`来设置自定义同步作业名称。