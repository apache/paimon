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

### Prepare CDC Bundled Jar

```
flink-sql-connector-mysql-cdc-*.jar
```

### Synchronizing Tables

By using [MySqlSyncTableAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/mysql/MySqlSyncTableAction) in a Flink DataStream job or directly through `flink run`, users can synchronize one or multiple tables from MySQL into one Paimon table.

To use this feature through `flink run`, run the following shell command.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
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

{{< generated/mysql_sync_table >}}

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
    /path/to/paimon-flink-action-{{< version >}}.jar \
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
    /path/to/paimon-flink-action-{{< version >}}.jar \
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

{{< generated/mysql_sync_database >}}

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
    /path/to/paimon-flink-action-{{< version >}}.jar \
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

## Kafka

### Prepare Kafka Bundled Jar

```
flink-sql-connector-kafka-*.jar
```
### Supported Formats
Flink provides several Kafka CDC formats :canal-json、debezium-json,ogg-json,maxwell-json.
If a message in a Kafka topic is a change event captured from another database using the Change Data Capture (CDC) tool, then you can use the Paimon Kafka CDC. Write the INSERT, UPDATE, DELETE messages parsed into the paimon table.
<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Formats</th>
        <th class="text-left">Supported</th>
      </tr>
    </thead>
    <tbody>
        <tr>
         <td><a href="https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/connectors/table/formats/canal/">Canal CDC</a></td>
          <td>True</td>
        </tr>
        <tr>
         <td><a href="https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/connectors/table/formats/debezium/">Debezium CDC</a></td>
         <td>False</td>
        </tr>
        <tr>
         <td><a href="https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/connectors/table/formats/maxwell/ >}}">Maxwell CDC</a></td>
        <td>False</td>
        </tr>
        <tr>
         <td><a href="https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/connectors/table/formats/ogg/">OGG CDC</a></td>
        <td>False</td>
        </tr>
    </tbody>
</table>

### Synchronizing Tables

By using [KafkaSyncTableAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/kafka/KafkaSyncTableAction) in a Flink DataStream job or directly through `flink run`, users can synchronize one or multiple tables from Kafka's one topic into one Paimon table.

To use this feature through `flink run`, run the following shell command.

```bash
<FLINK_HOME>/bin/flink run \
    -c org.apache.paimon.flink.action.FlinkActions \
    /path/to/paimon-flink-**-{{< version >}}.jar \
    kafka-sync-table
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table <table-name> \
    [--partition-keys <partition-keys>] \
    [--primary-keys <primary-keys>] \
    [--computed-column <'column-name=expr-name(args[, ...])'> [--computed-column ...]] \
    [--kafka-conf <kafka-source-conf> [--kafka-conf <kafka-source-conf> ...]] \
    [--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]] \
    [--table-conf <paimon-table-sink-conf> [--table-conf <paimon-table-sink-conf> ...]]
```

* `--warehouse` is the path to Paimon warehouse.
* `--database` is the database name in Paimon catalog.
* `--table` is the Paimon table name.
* `--partition-keys` are the partition keys for Paimon table. If there are multiple partition keys, connect them with comma, for example `dt,hh,mm`.
* `--primary-keys` are the primary keys for Paimon table. If there are multiple primary keys, connect them with comma, for example `buyer_id,seller_id`.
* `--kafka-conf` is the configuration for Flink Kafka sources. Each configuration should be specified in the format `key=value`. `properties.bootstrap.servers`, `topic`, `properties.group.id`,  and `value.format` are required configurations, others are optional. See its [document](https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/connectors/table/kafka/#%e8%bf%9e%e6%8e%a5%e5%99%a8%e5%8f%82%e6%95%b0) for a complete list of configurations.
* `--catalog-conf` is the configuration for Paimon catalog. Each configuration should be specified in the format `key=value`. See [here]({{< ref "maintenance/configurations" >}}) for a complete list of catalog configurations.
* `--table-conf` is the configuration for Paimon table sink. Each configuration should be specified in the format `key=value`. See [here]({{< ref "maintenance/configurations" >}}) for a complete list of table configurations.

If the Paimon table you specify does not exist, this action will automatically create the table. Its schema will be derived from all specified Kafka topic's tables,it gets the earliest non-DDL data parsing schema from topic. If the Paimon table already exists, its schema will be compared against the schema of all specified Kafka topic's tables.

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
    kafka-sync-table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --partition-keys pt \
    --primary-keys pt,uid \
    --computed-columns '_year=year(age)' \
    --kafka-conf properties.bootstrap.servers=127.0.0.1:9020 \
    --kafka-conf topic=order \
    --kafka-conf properties.group.id=123456 \
    --kafka-conf value.format=canal-json \
    --catalog-conf metastore=hive \
    --catalog-conf uri=thrift://hive-metastore:9083 \
    --table-conf bucket=4 \
    --table-conf changelog-producer=input \
    --table-conf sink.parallelism=4
```

## Computed Functions
* `--computed-column` are the definitions of computed columns. The argument field is from Kafka topic's table field name. Supported expressions are:
  * year(date-column): Extract year from a DATE, DATETIME or TIMESTAMP. Output is an INT value represent the year.
  * substring(column,beginInclusive): Get column.substring(beginInclusive). Output is a STRING.
  * substring(column,beginInclusive,endExclusive): Get column.substring(beginInclusive,endExclusive). Output is a STRING.
  * truncate(column,width): truncate column by width. Output type is same with column.
    * If the column is a STRING, truncate(column,width) will truncate the string to width characters, namely `value.substring(0, width)`.
    * If the column is an INT or LONG, truncate(column,width) will truncate the number with the algorithm `v - (((v % W) + W) % W)`. The `redundant` compute part is to keep the result always positive.
    * If the column is a DECIMAL, truncate(column,width) will truncate the decimal with the algorithm: let `scaled_W = decimal(W, scale(v))`, then return `v - (v % scaled_W)`.