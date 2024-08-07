---
title: "Dedicated Compaction"
weight: 3
type: docs
aliases:
- /maintenance/dedicated-compaction.html
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

# Dedicated Compaction

Paimon的快照管理支持多个写入者。

{{< hint info >}} 对于类似S3的对象存储，它的 `'RENAME'` 操作没有原子语义。我们需要配置Hive元存储并启用 `'lock.enabled'` 选项以用于目录。 {{< /hint >}}

默认情况下，Paimon支持并发写入到不同的分区。建议的模式是流式作业将记录写入Paimon的最新分区；同时批处理作业（覆盖）将记录写入历史分区。

{{< img src="/img/multiple-writers.png">}}

到目前为止，一切都运行得很顺利，但如果您需要多个写入者将记录写入同一个分区，那将会有点复杂。例如，您不想使用 `UNION ALL`，您有多个流式作业要将记录写入 `'partial-update'` 表中。请参考下面的 `'Dedicated Compaction作业'`。

## Dedicated Compaction作业

默认情况下，Paimon的写入者在写入记录时会根据需要执行压缩操作。这对于大多数用例来说已经足够了，但有两个缺点：

* 这可能会导致不稳定的写入吞吐量，因为在执行压缩操作时吞吐量可能会暂时下降。
* 压缩操作将一些数据文件标记为 "已删除"（实际上并未删除，详见 expiring-snapshots 了解更多信息）。如果多个写入者标记同一文件，提交更改时将会发生冲突。Paimon将自动解决冲突，但这可能会导致作业重新启动。

为了避免这些缺点，用户还可以选择在写入者中跳过压缩操作，只运行专用的压缩作业。由于压缩操作仅由专用作业执行，写入者可以持续写入记录而无需暂停，也不会发生冲突。

要在写入者中跳过压缩操作，请将以下表属性设置为 `true`。

<table class="table table-bordered"> <thead> <tr> <th class="text-left" style="width: 20%">选项</th> <th class="text-left" style="width: 5%">必需</th> <th class="text-left" style="width: 5%">默认</th> <th class="text-left" style="width: 10%">类型</th> <th class="text-left" style="width: 60%">描述</th> </tr> </thead> <tbody> <tr> <td><h5>write-only</h5></td> <td>否</td> <td style="word-wrap: break-word;">false</td> <td>布尔</td> <td>如果设置为true，将跳过压缩操作和快照过期。此选项与Dedicated Compaction作业一起使用。</td> </tr> </tbody> </table>

要运行专用的压缩作业，请按照以下说明进行操作。

{{< tabs "dedicated-compaction-job" >}}

{{< tab "Flink" >}}

当前，Flink SQL不支持与压缩操作相关的语句，因此我们必须通过 `flink run` 提交压缩作业。

运行以下命令以提交表的压缩作业。

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    compact \
    --warehouse <warehouse-path> \
    --database <database-name> \ 
    --table <table-name> \
    [--partition <partition-name>] \
    [--table_conf <table_conf>] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]] \
```

示例：压缩表

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    compact \
    --warehouse s3:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --partition dt=20221126,hh=08 \
    --partition dt=20221127,hh=09 \
    --table_conf sink.parallelism=10 \
    --catalog_conf s3.endpoint=https://****.com \
    --catalog_conf s3.access-key=***** \
    --catalog_conf s3.secret-key=*****
```

您可以使用 `-D execution.runtime-mode=batch` 或 `-yD execution.runtime-mode=batch`（用于ON-YARN场景）来控制批处理或流式模式。如果提交批处理作业，将压缩所有当前表文件。如果提交流式作业，作业将持续监视表的新更改并根据需要执行压缩操作。

有关压缩操作的更多用法，请参见

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    compact --help
```

{{< /tab >}}

{{< /tabs >}}

## 数据库压缩作业

您可以运行以下命令来提交多个数据库的压缩作业。

{{< tabs "database-compaction-job" >}}

{{< tab "Flink" >}}

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    compact_database \
    --warehouse <warehouse-path> \
    --including_databases <database-name|name-regular-expr> \ 
    [--including_tables <paimon-table-name|name-regular-expr>] \
    [--excluding_tables <paimon-table-name|name-regular-expr>] \
    [--mode <compact-mode>] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]] \
    [--table_conf <paimon-table_conf> [--table_conf <paimon-table_conf> ...]]
```

* `--including_databases` 用于指定要压缩的数据库。在压缩模式下，您需要指定数据库名称，在压缩数据库模式下，您可以指定多个数据库，支持正则表达式。
* `--including_tables` 用于指定要压缩的源表，您必须使用 '|' 分隔多个表，格式为 `databaseName.tableName`，支持正则表达式。例如，指定 "--including_tables db1.t1|db2.+" 意味着压缩表 'db1.t1' 和 db2 数据库中的所有表。
* `--excluding_tables` 用于指定不压缩的源表。使用方式与 "--including_tables" 相同。如果同时指定了两者，"--excluding_tables" 的优先级高于 "--including_tables"。
* `mode` 用于指定压缩模式。可能的值：
    * "divided"（如果您没有指定模式，默认模式）：为每个表启动一个独立的sink，新表的压缩需要重新启动作业。
    * "combined"：为所有表启动一个单独的合并sink，新表将自动进行压缩。
* `--catalog_conf` 是Paimon目录的配置。每个配置应以 `key=value` 格式指定。请参阅[此处]({{< ref "maintenance/configurations" >}})以获取完整的目录配置列表。
* `--table_conf` 是压缩的配置。每个配置应以 `key=value` 格式指定。关键配置如下：

| 键 | 默认值 | 类型 | 描述 |
| --- | --- | --- | --- |
| continuous.discovery-interval | 10 秒 | 持续时间 | 持续读取的发现间隔。 |
| sink.parallelism | (无) | 整数 | 为sink定义自定义并行性。如果未定义此选项，规划器将通过考虑全局配置来为每个语句单独推导并行性。 |

您可以使用 `-D execution.runtime-mode=batch` 来控制批处理或流式模式。如果提交批处理作业，将压缩所有当前表文件。如果提交流式作业，作业将持续监视表的新更改并根据需要执行压缩操作。

{{< hint info >}}

如果您只想提交压缩作业并且不想等待作业完成，您应该以[分离模式](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/cli/#submitting-a-job)提交。

{{< /hint >}}

{{< hint info >}}

您可以设置 `--mode combined` 以启用在不重新启动作业的情况下压缩新添加的表。

{{< /hint >}}

示例1：压缩数据库

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    compact_database \
    --warehouse s3:///path/to/warehouse \
    --including_databases test_db \
    --catalog_conf s3.endpoint=https://****.com \
    --catalog_conf s3.access-key=***** \
    --catalog_conf s3.secret-key=*****
```

示例2：以合并模式压缩数据库

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    compact_database \
    --warehouse s3:///path/to/warehouse \
    --including_databases test_db \
    --mode combined \
    --catalog_conf s3.endpoint=https://****.com \
    --catalog_conf s3.access-key=***** \
    --catalog_conf s3.secret-key=***** \
    --table_conf continuous.discovery-interval=*****
```

有关 `compact_database` 操作的更多用法，请参见

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    compact_database --help
```

{{< /tab >}}

{{< /tabs >}}

## 排序压缩

如果您的表配置了 dynamic bucket primary key table 或 unaware bucket append table， 您可以触发具有指定列排序的压缩以加速查询。

```bash
<FLINK_HOME>/bin/flink run \
    -D execution.runtime-mode=batch \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    compact \
    --warehouse <warehouse-path> \
    --database <database-name> \ 
    --table <table-name> \
    --order_strategy <orderType> \
    --order_by <col1,col2,...>
    [--partition <partition-name>] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]] \
    [--table_conf <paimon-table-dynamic-conf> [--table_conf <paimon-table-dynamic-conf>] ...]
```

在 `排序压缩` 中有两个新的配置项 {{< generated/sort-compact >}}

排序并行性与sink并行性相同，您可以通过添加配置 `--table_conf sink.parallelism=<value>` 来动态指定它。