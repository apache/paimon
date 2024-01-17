---
title: "Mysql CDC"
weight: 2
type: docs
aliases:
- /cdc-ingestion/mysql-cdc.html
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

# MySQL CDC

Paimon支持使用变更数据捕获（CDC）从不同的数据库同步更改。此功能需要Flink及其 [CDC连接器](https://ververica.github.io/flink-cdc-connectors/)。

## Prepare CDC Bundled Jar

```
flink-sql-connector-mysql-cdc-*.jar
```

## 同步表

通过在Flink DataStream作业中使用[MySqlSyncTableAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/mysql/MySqlSyncTableAction)或直接通过`flink run`，用户可以将一个或多个表格从MySQL同步到一个Paimon表格中。

要通过`flink run`使用此功能，请运行以下shell命令。

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mysql_sync_table
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table <table-name> \
    [--partition_keys <partition_keys>] \
    [--primary_keys <primary-keys>] \
    [--type_mapping <option1,option2...>] \
    [--computed_column <'column-name=expr-name(args[, ...])'> [--computed_column ...]] \
    [--metadata_column <metadata-column>] \
    [--mysql_conf <mysql-cdc-source-conf> [--mysql_conf <mysql-cdc-source-conf> ...]] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]] \
    [--table_conf <paimon-table-sink-conf> [--table_conf <paimon-table-sink-conf> ...]]
```

{{< generated/mysql_sync_table >}}

如果您指定的Paimon表格不存在，此操作将自动创建该表格。其架构将根据所有指定的MySQL表格派生而来。如果Paimon表格已存在，其架构将与所有指定的MySQL表格的架构进行比较。


Example 1: 将表格同步到一个Paimon表格中

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mysql_sync_table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --partition_keys pt \
    --primary_keys pt,uid \
    --computed_column '_year=year(age)' \
    --mysql_conf hostname=127.0.0.1 \
    --mysql_conf username=root \
    --mysql_conf password=123456 \
    --mysql_conf database-name='source_db' \
    --mysql_conf table-name='source_table1|source_table2' \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4
```

如示例所示，mysql_conf的table-name支持正则表达式以监视满足正则表达式的多个表格。所有表格的架构将合并为一个Paimon表格架构。

示例2：将分片同步到一个Paimon表格中

您还可以将'database-name'设置为正则表达式，以捕获多个数据库。一个典型的情况是，一个表格'source_table'被分割成数据库'source_db1'，'source_db2'，...，然后您可以将所有'source_table'的数据同步到一个Paimon表格中。

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mysql_sync_table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --partition_keys pt \
    --primary_keys pt,uid \
    --computed_column '_year=year(age)' \
    --mysql_conf hostname=127.0.0.1 \
    --mysql_conf username=root \
    --mysql_conf password=123456 \
    --mysql_conf database-name='source_db.+' \
    --mysql_conf table-name='source_table' \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4
```

## 同步数据库

通过在Flink DataStream作业中使用[MySqlSyncDatabaseAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/mysql/MySqlSyncDatabaseAction)或直接通过`flink run`，用户可以将整个MySQL数据库同步到一个Paimon数据库中。

要通过`flink run`使用此功能，请运行以下shell命令。

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mysql_sync_database
    --warehouse <warehouse-path> \
    --database <database-name> \
    [--ignore_incompatible <true/false>] \
    [--merge_shards <true/false>] \
    [--table_prefix <paimon-table-prefix>] \
    [--table_suffix <paimon-table-suffix>] \
    [--including_tables <mysql-table-name|name-regular-expr>] \
    [--excluding_tables <mysql-table-name|name-regular-expr>] \
    [--mode <sync-mode>] \
    [--metadata_column <metadata-column>] \
    [--type_mapping <option1,option2...>] \
    [--mysql_conf <mysql-cdc-source-conf> [--mysql_conf <mysql-cdc-source-conf> ...]] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]] \
    [--table_conf <paimon-table-sink-conf> [--table_conf <paimon-table-sink-conf> ...]]
```

{{< generated/mysql_sync_database >}}

只有具有主键的表格将被同步。

要同步的每个MySQL表格，如果相应的Paimon表格不存在，此操作将自动创建该表格。其架构将从所有指定的MySQL表格派生而来。如果Paimon表格已经存在，其架构将与所有指定的MySQL表格的架构进行比较。

示例1：同步整个数据库

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mysql_sync_database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --mysql_conf hostname=127.0.0.1 \
    --mysql_conf username=root \
    --mysql_conf password=123456 \
    --mysql_conf database-name=source_db \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4
```

示例2：同步新增的数据库表格

假设一开始一个Flink作业正在同步数据库`source_db`下的表格[product, user, address]。提交作业的命令如下所示：

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mysql_sync_database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --mysql_conf hostname=127.0.0.1 \
    --mysql_conf username=root \
    --mysql_conf password=123456 \
    --mysql_conf database-name=source_db \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4 \
    --including_tables 'product|user|address'
```

在以后的某个时候，我们希望该作业还同步包含历史数据的表格[order, custom]。我们可以通过从作业的先前快照中恢复并重用作业的现有状态来实现这一点。恢复的作业将首先快照新添加的表格，然后自动从先前位置读取变更日志。

从以前的快照中恢复并添加新表格以进行同步的命令如下所示：


```bash
<FLINK_HOME>/bin/flink run \
    --fromSavepoint savepointPath \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mysql_sync_database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --mysql_conf hostname=127.0.0.1 \
    --mysql_conf username=root \
    --mysql_conf password=123456 \
    --mysql_conf database-name=source_db \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --including_tables 'product|user|address|order|custom'
```

{{< hint info >}}
您可以设置`--mode combined`以在不重新启动作业的情况下启用同步新增的表格。
{{< /hint >}}

示例3：同步和合并多个分片

假设您有多个数据库分片`db1`，`db2`，...，每个数据库都有表格`tbl1`，`tbl2`，....您可以通过以下命令将所有`db.+.tbl.+`同步到表格`test_db.tbl1`，`test_db.tbl2` ... 中：

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mysql_sync_database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --mysql_conf hostname=127.0.0.1 \
    --mysql_conf username=root \
    --mysql_conf password=123456 \
    --mysql_conf database-name='db.+' \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4 \
    --including_tables 'tbl.+'
```

通过将database-name设置为正则表达式，同步作业将捕获匹配的数据库下的所有表格，并将具有相同名称的表格合并为一个表格。

{{< hint info >}}
您可以设置`--merge_shards false`以防止合并分片。同步的表格将命名为'databaseName_tableName'，以避免潜在的名称冲突。
{{< /hint >}}

## 常见问题解答

1. 从MySQL中摄取的记录中的中文字符显示乱码。

* 尝试在`flink-conf.yaml`中设置`env.java.opts: -Dfile.encoding=UTF-8`（自Flink-1.17以来，该选项已更改为`env.java.opts.all`）。