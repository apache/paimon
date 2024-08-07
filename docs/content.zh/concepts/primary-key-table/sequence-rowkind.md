---
title: "Sequence & Rowkind"
weight: 5
type: docs
aliases:
- /concepts/primary-key-table/sequence-rowkind.html
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

# 序列和行种类

在创建表格时，您可以通过指定字段来确定更新顺序，以指定 `'sequence.field'`， 或者您可以指定 `'rowkind.field'` 来确定记录的变更种类。

## 序列字段

默认情况下，主键表格根据输入顺序来确定合并顺序（最后输入的记录将是最后合并的）。然而，在分布式计算中， 会出现一些导致数据无序的情况。这时，您可以使用一个时间字段作为 `sequence.field`，例如：

{{< tabs "sequence.field" >}} {{< tab "Flink" >}}

```sql
CREATE TABLE MyTable (
    pk BIGINT PRIMARY KEY NOT ENFORCED,
    v1 DOUBLE,
    v2 BIGINT,
    dt TIMESTAMP
) WITH (
    'sequence.field' = 'dt'
);
```

{{< /tab >}} {{< /tabs >}}

具有最大 `sequence.field` 值的记录将是最后合并的，而不管输入顺序如何。

**序列自动填充**：

当记录被更新或删除时，`sequence.field` 必须增大，不能保持不变。 对于 -U 和 +U，它们的序列字段必须不同。如果您无法满足此要求，Paimon 提供了自动填充序列字段的选项。

1. `'sequence.auto-padding' = 'row-kind-flag'`：如果您对 -U 和 +U 使用相同的值，就像 Mysql Binlog 中的 "`op_ts`" （数据库中进行更改的时间）一样。建议使用自动填充行种类标志，它将自动区分 -U（-D）和 +U（+I）。
    
2. 精度不足：如果提供的 `sequence.field` 不符合精度，例如大约秒或毫秒，您可以将 `sequence.auto-padding` 设置为 `second-to-micro` 或 `millis-to-micro`， 以使序列号的精度补充到微秒级别。
    
3. 复合模式：例如，"second-to-micro,row-kind-flag"，首先，将微秒添加到秒钟，然后填充行种类标志。
    

## 行种类字段

默认情况下，主键表格根据输入行来确定行种类。您还可以定义 `'rowkind.field'` 来使用字段来提取行种类。

有效的行种类字符串应为 `'+I'`、`'-U'`、`'+U'` 或 `'-D'`。