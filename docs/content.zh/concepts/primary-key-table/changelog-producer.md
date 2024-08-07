---
title: "Changelog Producer"
weight: 4
type: docs
aliases:
- /concepts/primary-key-table/changelog-producer.html
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

# 变更日志生产者

流式查询将持续产生最新的更改。

通过在创建表时指定 `changelog-producer` 表属性，用户可以选择从表文件中生成的更改模式。

{{< hint info >}}

`changelog-producer` 表属性仅影响来自表文件的更改日志。它不影响外部日志系统。

{{< /hint >}}

## None

默认情况下，不会应用额外的更改日志生产者到表的写入者。Paimon源只能看到跨快照合并的更改，例如哪些键被删除以及某些键的新值是什么。

然而，这些合并的更改不能形成完整的更改日志，因为我们无法直接从中读取键的旧值。合并的更改需要消费者“记住”每个键的值，并在不看到旧值的情况下重新写入这些值。然而，一些消费者需要旧值来确保正确性或效率。

考虑一个只计算某些分组键上的总和的消费者（可能不等于主键）。如果消费者只看到一个新值 `5`，它无法确定应将哪些值添加到总和结果中。例如，如果旧值是 `4`，则应将 `1` 添加到结果中。但如果旧值是 `6`，则应将 `1` 从结果中减去。对于这些类型的消费者来说，旧值非常重要。

总之，`none` 更改日志生产者最适合诸如数据库系统之类的消费者。Flink还有一个内置的 "normalize" 运算符，它会将每个键的值持久化到状态中。正如大家可以轻松看出的那样，这个运算符将非常昂贵，应该尽量避免使用（可以通过 `'scan.remove-normalize'` 强制删除 "normalize" 运算符）。

{{< img src="/img/changelog-producer-none.png">}}

## Input

通过指定 `'changelog-producer' = 'input'`，Paimon写入者依赖它们的输入作为完整更改日志的来源。所有输入记录将保存在单独的 [更改日志文件]({{< ref "concepts/file-layouts" >}}) 中，并将由Paimon源提供给消费者。

`input` 更改日志生产者可用于当Paimon写入者的输入是完整更改日志时，例如来自数据库CDC的输入，或由Flink状态计算生成的输入。

{{< img src="/img/changelog-producer-input.png">}}

## Lookup

如果您的输入无法生成完整的更改日志，但仍希望摆脱昂贵的标准化运算符，您可以考虑使用 `'lookup'` 更改日志生产者。

通过指定 `'changelog-producer' = 'lookup'`，Paimon将在提交数据写入之前通过 `'lookup'` 生成更改日志。

{{< img src="/img/changelog-producer-lookup.png">}}

查找将缓存数据在内存和本地磁盘上，您可以使用以下选项来调整性能：

<table class="table table-bordered"> <thead> <tr> <th class="text-left" style="width: 20%">选项</th> <th class="text-left" style="width: 5%">默认</th> <th class="text-left" style="width: 10%">类型</th> <th class="text-left" style="width: 60%">描述</th> </tr> </thead> <tbody> <tr> <td><h5>lookup.cache-file-retention</h5></td> <td style="word-wrap: break-word;">1小时</td> <td>持续时间</td> <td>查找缓存文件的保留时间。文件过期后，如果需要访问，将从DFS中重新读取以在本地磁盘上构建索引。</td> </tr> <tr> <td><h5>lookup.cache-max-disk-size</h5></td> <td style="word-wrap: break-word;">不限</td> <td>内存大小</td> <td>查找缓存的最大磁盘大小，您可以使用此选项来限制本地磁盘的使用。</td> </tr> <tr> <td><h5>lookup.cache-max-memory-size</h5></td> <td style="word-wrap: break-word;">256 MB</td> <td>内存大小</td> <td>查找缓存的最大内存大小。</td> </tr> </tbody> </table>

查找更改日志生产者支持 `changelog-producer.row-deduplicate` 以避免为相同记录生成 -U、+U 更改日志。

（注意：请增加 `'execution.checkpointing.max-concurrent-checkpoints'` Flink配置，这对性能非常重要）。

## Full-compaction

如果您认为 'lookup' 的资源消耗太大，可以考虑使用 'full-compaction' 更改日志生产者，它可以将数据写入和更改日志生成分开，并且更适合具有高延迟的场景（例如，10分钟）。

通过指定 `'changelog-producer' = 'full-compaction'`，Paimon将比较完全压缩的结果，并将差异生成为更改日志。更改日志的延迟受到完全压缩频率的影响。

通过指定 `full-compaction.delta-commits` 表属性，完全压缩将在增量提交（检查点）后不断触发。默认情况下，此值设置为 1，因此每个检查点将进行完全压缩并生成更改日志。

{{< img src="/img/changelog-producer-full-compaction.png">}}

{{< hint info >}}

完全压缩更改日志生产者可以为任何类型的来源生成完整的更改日志。但它不像输入更改日志生产者那样高效，生成更改日志的延迟可能较高。

{{< /hint >}}

完全压缩更改日志生产者支持 `changelog-producer.row-deduplicate` 以避免为相同记录生成 -U、+U 更改日志。

（注意：请增加 `'execution.checkpointing.max-concurrent-checkpoints'` Flink配置，这对性能非常重要）。