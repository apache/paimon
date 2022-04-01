---
title: "Create Table"
weight: 2
type: docs
aliases:
- /development/create-table.html
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

# CREATE statement

```sql
CREATE TABLE [IF NOT EXISTS] [catalog_name.][db_name.]table_name
  (
    { <physical_column_definition> | <computed_column_definition> }[ , ...n]
    [ <watermark_definition> ]
    [ <table_constraint> ][ , ...n]
  )
  [PARTITIONED BY (partition_column_name1, partition_column_name2, ...)]
  WITH (key1=val1, key2=val2, ...)
   
<physical_column_definition>:
  column_name column_type [ <column_constraint> ] [COMMENT column_comment]
  
<column_constraint>:
  [CONSTRAINT constraint_name] PRIMARY KEY NOT ENFORCED

<table_constraint>:
  [CONSTRAINT constraint_name] PRIMARY KEY (column_name, ...) NOT ENFORCED

<computed_column_definition>:
  column_name AS computed_column_expression [COMMENT column_comment]

<watermark_definition>:
  WATERMARK FOR rowtime_column_name AS watermark_strategy_expression
```

{{< hint info >}}
__Note:__ To ensure the uniqueness of the primary key, the
primary key must contain the partition field.
{{< /hint >}}

{{< hint info >}}
__Note:__ Metadata column is not supported yet.
{{< /hint >}}

Table options that do not contain the 'connector' key and value
represent a managed table. Creating a table will create the
corresponding physical storage.

When the corresponding physical storage already exists,
such as a file directory or kafka topic:
- If you want to reuse it, use `CREATE TABLE IF NOT EXISTS`
- If you don't want to reuse it, `DROP TABLE IF EXISTS`
  or delete it yourself.

It is recommended that you use a persistent catalog, such as
`HiveCatalog`, otherwise make sure you create the table with
the same options each time.

## Session Options

To create a managed table, you need to set the required
session options in advance. Session options are only valid
when creating a table, not interfering with reading or
writing the table.

You can set session options in the following two ways:
- Edit `flink-conf.yaml`.
- Via `TableEnvironment.getConfig().set`.

The difference between session options and table options
is that the session option needs to be prefixed with
`table-store`. Take `bucket` option for example:
- set as session level: `SET 'table-store.bucket' = '4';`
- set as per table level: `CREATE TABLE ... WITH ('bucket' = '4')`

Important options include the following:

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 20%">Option</th>
      <th class="text-center" style="width: 5%">Required</th>
      <th class="text-center" style="width: 5%">Default</th>
      <th class="text-center" style="width: 10%">Type</th>
      <th class="text-center" style="width: 60%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>file.path</h5></td>
      <td>Yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The root file path of the table store in the filesystem.</td>
    </tr>
    <tr>
      <td><h5>bucket</h5></td>
      <td>Yes</td>
      <td style="word-wrap: break-word;">1</td>
      <td>Integer</td>
      <td>The bucket number for table store.</td>
    </tr>
    <tr>
      <td><h5>log.system</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The log system used to keep changes of the table, supports 'kafka'.</td>
    </tr>
    <tr>
      <td><h5>log.kafka.bootstrap.servers</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Required Kafka server connection string for log store.</td>
    </tr>
    <tr>
      <td><h5>log.retention</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Duration</td>
      <td>The duration to keep a log file before deleting it. The default value is from the log system cluster.</td>
    </tr>
    </tbody>
</table>

## Physical storage

Creating a table will create the corresponding physical storage:
- The table's FileStore directory will be created under:
  `${file.path}/${catalog_name}.catalog/${database_name}.db/${table_name}`
- If `log.system` is configured as Kafka, a Topic named
  "${catalog_name}.${database_name}.${table_name}" will be created
  automatically when the table is created.
