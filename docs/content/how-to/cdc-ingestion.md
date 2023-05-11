---
title: "CDC Ingestion"
weight: 7
type: docs
aliases:
- /how-to/cdc-ingestion.html
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

# CDC Ingestion

Paimon supports synchronizing changes from different databases using change data capture (CDC). This feature requires Flink and its [CDC connectors](https://ververica.github.io/flink-cdc-connectors/).

## MySQL

### Prepare Flink SQL Connector MySQL CDC Bundled Jar

```
flink-sql-connector-mysql-cdc-*.jar
```

### Synchronizing Tables

By using [MySqlSyncTableAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/mysql/MySqlSyncTableAction) in a Flink DataStream job or directly through `flink run`, users can synchronize one or multiple tables from MySQL into one Paimon table.

To use this feature through `flink run`, run the following shell command.

```bash
<FLINK_HOME>/bin/flink run \
    -c org.apache.paimon.flink.action.FlinkActions \
    /path/to/paimon-flink-**-{{< version >}}.jar \
    mysql-sync-table
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table <table-name> \
    [--partition-keys <partition-keys>] \
    [--primary-keys <primary-keys>] \
    [--computed-column <'column-name=expr-name(args[, ...])'> [--computed-column ...]] \
    [--mysql-conf <mysql-cdc-source-conf> [--mysql-conf <mysql-cdc-source-conf> ...]] \
    [--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]] \
    [--table-conf <paimon-table-sink-conf> [--table-conf <paimon-table-sink-conf> ...]]
```

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 3%">Configuration</th>
      <th class="text-left" style="width: 10%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>`--warehouse`</code></td>
      <td><code>The path to Paimon warehouse.</code></td>
    </tr>
    <tr>
      <td><code>`--database`</code></td>
      <td><code>The database name in Paimon catalog.</code></td>
    </tr>
    <tr>
      <td><code>`--table`</code></td>
      <td><code>The Paimon table name.</code></td>
    </tr>
    <tr>
      <td><code>`--partition-keys`</code></td>
      <td><code>The partition keys for Paimon table. If there are multiple partition keys, connect them with comma, for example `dt,hh,mm`.</code></td>
    </tr>
    <tr>
      <td><code>`--primary-keys`</code></td>
      <td><code>The primary keys for Paimon table. If there are multiple primary keys, connect them with comma, for example `buyer_id,seller_id`.</code></td>
    </tr>
    <tr>
      <td><code>`--computed-column`</code></td>
      <td><code>The definitions of computed columns. The argument field is from MySQL table field name. Supported expressions are: * year(date-column): Extract year from a DATE, DATETIME or TIMESTAMP. Output is an INT value represent the year.</code></td>
    </tr>
    <tr>
      <td><code>`--mysql-conf`</code></td>
      <td><code>The configuration for Flink CDC MySQL table sources. Each configuration should be specified in the format `key=value`. `hostname`, `username`, `password`, `database-name` and `table-name` are required configurations, others are optional. See its [document](https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mysql-cdc.html#connector-options) for a complete list of configurations.</code></td>
    </tr>
    <tr>
      <td><code>`--catalog-conf`</code></td>
      <td><code>The configuration for Paimon catalog. Each configuration should be specified in the format `key=value`. See [here]({{< ref "maintenance/configurations" >}}) for a complete list of catalog configurations.</code></td>
    </tr>
    <tr>
      <td><code>`--table-conf`</code></td>
      <td><code>The configuration for Paimon table sink. Each configuration should be specified in the format `key=value`. See [here]({{< ref "maintenance/configurations" >}}) for a complete list of table configurations. </code></td>
    </tr>
    </tbody>
</table>


If the Paimon table you specify does not exist, this action will automatically create the table. Its schema will be derived from all specified MySQL tables. If the Paimon table already exists, its schema will be compared against the schema of all specified MySQL tables.

This action supports a limited number of schema changes. Currently, the framework can not drop columns, so the behaviors of `DROP` will be ignored, `RENAME` will add a new column. Currently supported schema changes includes:

* Adding columns.

* Altering column types. More specifically,

  * altering from a string type (char, varchar, text) to another string type with longer length,
  * altering from a binary type (binary, varbinary, blob) to another binary type with longer length,
  * altering from an integer type (tinyint, smallint, int, bigint) to another integer type with wider range,
  * altering from a floating-point type (float, double) to another floating-point type with wider range,
  
  are supported. 

Example

```bash
<FLINK_HOME>/bin/flink run \
    -c org.apache.paimon.flink.action.FlinkActions \
    /path/to/paimon-flink-**-{{< version >}}.jar \
    mysql-sync-table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --partition-keys pt \
    --primary-keys pt,uid \
    --computed-columns '_year=year(age)' \
    --mysql-conf hostname=127.0.0.1 \
    --mysql-conf username=root \
    --mysql-conf password=123456 \
    --mysql-conf database-name=source_db \
    --mysql-conf table-name='source_table_.*' \
    --catalog-conf metastore=hive \
    --catalog-conf uri=thrift://hive-metastore:9083 \
    --table-conf bucket=4 \
    --table-conf changelog-producer=input \
    --table-conf sink.parallelism=4
```

### Synchronizing Databases

By using [MySqlSyncDatabaseAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/mysql/MySqlSyncDatabaseAction) in a Flink DataStream job or directly through `flink run`, users can synchronize the whole MySQL database into one Paimon database.

To use this feature through `flink run`, run the following shell command.

```bash
<FLINK_HOME>/bin/flink run \
    -c org.apache.paimon.flink.action.FlinkActions \
    /path/to/paimon-flink-**-{{< version >}}.jar \
    mysql-sync-database
    --warehouse <warehouse-path> \
    --database <database-name> \
    [--ignore-incompatible <true/false>] \
    [--table-prefix <paimon-table-prefix>] \
    [--table-suffix <paimon-table-suffix>] \
    [--including-tables <mysql-table-name|name-regular-expr>] \
    [--excluding-tables <mysql-table-name|name-regular-expr>] \
    [--mysql-conf <mysql-cdc-source-conf> [--mysql-conf <mysql-cdc-source-conf> ...]] \
    [--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]] \
    [--table-conf <paimon-table-sink-conf> [--table-conf <paimon-table-sink-conf> ...]]
```

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 3%">Configuration</th>
      <th class="text-left" style="width: 10%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>`--warehouse`</code></td>
      <td><code>The path to Paimon warehouse.</code></td>
    </tr>
    <tr>
      <td><code>`--database`</code></td>
      <td><code>The database name in Paimon catalog.</code></td>
    </tr>
    <tr>
      <td><code>`--ignore-incompatible`</code></td>
      <td><code>It is default false, in this case, if MySQL table name exists in Paimon and their schema is incompatible,an exception will be thrown. You can specify it to true explicitly to ignore the incompatible tables and exception.</code></td>
    </tr>
    <tr>
      <td><code>`--table-prefix`</code></td>
      <td><code>The prefix of all Paimon tables to be synchronized. For example, if you want all synchronized tables to have "ods_" as prefix, you can specify `--table-prefix ods_`.</code></td>
    </tr>
    <tr>
      <td><code>`--table-suffix`</code></td>
      <td><code>The suffix of all Paimon tables to be synchronized. The usage is same as `--table-prefix`.</code></td>
    </tr>
    <tr>
      <td><code>`--including-tables`</code></td>
      <td><code>It is used to specify which source tables are to be synchronized. You must use '|' to separate multiple tables.Because '|' is a special character, a comma is required, for example: 'a|b|c'.Regular expression is supported, for example, specifying `--including-tables test|paimon.*` means to synchronize table 'test' and all tables start with 'paimon'.</code></td>
    </tr>
    <tr>
      <td><code>`--excluding-tables`</code></td>
      <td><code>It is used to specify which source tables are not to be synchronized. The usage is same as `--including-tables`.`--excluding-tables` has higher priority than `--including-tables` if you specified both.</code></td>
    </tr>
   <tr>
      <td><code>`--mysql-conf`</code></td>
      <td><code>The configuration for Flink CDC MySQL table sources. Each configuration should be specified in the format `key=value`. `hostname`, `username`, `password`, `database-name` and `table-name` are required configurations, others are optional. See its [document](https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mysql-cdc.html#connector-options) for a complete list of configurations.</code></td>
    </tr>
    <tr>
      <td><code>`--catalog-conf`</code></td>
      <td><code>The configuration for Paimon catalog. Each configuration should be specified in the format `key=value`. See [here]({{< ref "maintenance/configurations" >}}) for a complete list of catalog configurations.</code></td>
    </tr>
    <tr>
      <td><code>`--table-conf`</code></td>
      <td><code>The configuration for Paimon table sink. Each configuration should be specified in the format `key=value`. See [here]({{< ref "maintenance/configurations" >}}) for a complete list of table configurations. </code></td>
    </tr>
    </tbody>
</table>

Only tables with primary keys will be synchronized.

For each MySQL table to be synchronized, if the corresponding Paimon table does not exist, this action will automatically create the table. Its schema will be derived from all specified MySQL tables. If the Paimon table already exists, its schema will be compared against the schema of all specified MySQL tables.

This action supports a limited number of schema changes. Currently, the framework can not drop columns, so the behaviors of `DROP` will be ignored, `RENAME` will add a new column. Currently supported schema changes includes:

* Adding columns.

* Altering column types. More specifically,

  * altering from a string type (char, varchar, text) to another string type with longer length,
  * altering from a binary type (binary, varbinary, blob) to another binary type with longer length,
  * altering from an integer type (tinyint, smallint, int, bigint) to another integer type with wider range,
  * altering from a floating-point type (float, double) to another floating-point type with wider range,
  
  are supported.

Example

```bash
<FLINK_HOME>/bin/flink run \
    -c org.apache.paimon.flink.action.FlinkActions \
    /path/to/paimon-flink-**-{{< version >}}.jar \
    mysql-sync-database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --mysql-conf hostname=127.0.0.1 \
    --mysql-conf username=root \
    --mysql-conf password=123456 \
    --mysql-conf database-name=source_db \
    --catalog-conf metastore=hive \
    --catalog-conf uri=thrift://hive-metastore:9083 \
    --table-conf bucket=4 \
    --table-conf changelog-producer=input \
    --table-conf sink.parallelism=4
```
