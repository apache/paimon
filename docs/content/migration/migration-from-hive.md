---
title: "Migration From Hive"
weight: 1
type: docs
aliases:
- /migration/migration-from-hive.html
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

# Hive Table Migration

Apache Hive supports ORC, Parquet file formats that could be migrated to Paimon. 
When migrating data to a paimon table, the origin table will be permanently disappeared. So please back up your data if you
still need the original table. The migrated table will be [append table]({{< ref "append-table/overview" >}}).

Now, we can use paimon hive catalog with Migrate Table Procedure to totally migrate a table from hive to paimon.
At the same time, you can use paimon hive catalog with Migrate Database Procedure to fully synchronize all tables in the database to paimon.

* Migrate Table Procedure: Paimon table does not exist, use the procedure upgrade hive table to paimon table. Hive table will disappear after action done.
* Migrate Database Procedure: Paimon table does not exist, use the procedure upgrade all hive tables in database to paimon table. All hive tables will disappear after action done.

These three actions now support file format of hive "orc" and "parquet" and "avro".

<span style="color: red; "> **We highly recommend to back up hive table data before migrating, because migrating action is not atomic. If been interrupted while migrating, you may lose your data.** </span>

## Migrate Hive Table

{{< tabs "migrate table" >}}
{{< tab "Flink SQL" >}}
```sql
CREATE CATALOG PAIMON WITH (
   'type'='paimon',
   'metastore' = 'hive',
   'uri' = 'thrift://localhost:9083',
   'warehouse'='/path/to/warehouse/');

USE CATALOG PAIMON;

CALL sys.migrate_table(
    connector => 'hive',
    source_table => 'default.hivetable',
    -- You can specify the target table, and if the target table already exists
    -- the file will be migrated directly to it
    -- target_table => 'default.paimontarget',
    -- You can specify delete_origin is false, this won't delete hivetable
    -- delete_origin => false,
    options => 'file.format=orc');
```
{{< /tab >}}
{{< tab "Flink Action" >}}
```bash
<FLINK_HOME>/flink run ./paimon-flink-action-{{< version >}}.jar \
migrate_table \
--warehouse /path/to/warehouse \
--catalog_conf uri=thrift://localhost:9083 \
--catalog_conf metastore=hive \
--source_type hive \
--table default.hive_or_paimon
```
{{< /tab >}}
{{< /tabs >}}

After invoke, "hivetable" will totally convert to paimon format. Writing and reading the table by old "hive way" will fail.

## Migrate Hive Database

{{< tabs "migrate database" >}}
{{< tab "Flink SQL" >}}
```sql
CREATE CATALOG PAIMON WITH (
   'type'='paimon', 
   'metastore' = 'hive', 
   'uri' = 'thrift://localhost:9083', 
   'warehouse'='/path/to/warehouse/');

USE CATALOG PAIMON;

CALL sys.migrate_database(
    connector => 'hive',
    source_database => 'default',
    options => 'file.format=orc');
```
{{< /tab >}}
{{< tab "Flink Action" >}}
```bash
<FLINK_HOME>/bin/flink run \
/path/to/paimon-flink-action-{{< version >}}.jar \
migrate_databse \
--warehouse <warehouse-path> \
--source_type hive \
--database <database> \
[--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]] \
[--options <paimon-table-conf  [,paimon-table-conf ...]> ]
```

Example:
```bash
<FLINK_HOME>/flink run ./paimon-flink-action-{{< version >}}.jar migrate_table \
--warehouse /path/to/warehouse \
--catalog_conf uri=thrift://localhost:9083 \
--catalog_conf metastore=hive \
--source_type hive \
--database default
```
{{< /tab >}}
{{< /tabs >}}

After invoke, all tables in "default" database will totally convert to paimon format. Writing and reading the table by old "hive way" will fail.
