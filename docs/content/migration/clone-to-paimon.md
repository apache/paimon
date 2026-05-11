---
title: "Clone To Paimon"
weight: 5
type: docs
aliases:
- /migration/clone.html
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

# Clone To Paimon

Clone supports cloning tables to Paimon tables.

1. Clone is `OVERWRITE` semantic that will overwrite the partitions of the target table according to the data.
2. Clone is reentrant, but it requires existing tables to contain all fields from the source table and have the
   same partition fields.

Currently, clone supports clone Hive tables in Hive Catalog to Paimon Catalog, supports Parquet, ORC, Avro formats,
target table will be append table.

## Clone Hive Table

```bash
<FLINK_HOME>/flink run ./paimon-flink-action-{{< version >}}.jar \
clone \
--database default \
--table hivetable \
--catalog_conf metastore=hive \
--catalog_conf uri=thrift://localhost:9088 \
--target_database test \
--target_table test_table \
--target_catalog_conf warehouse=my_warehouse \
--parallelism 10 \
--where <filter_spec>
```

You can use filter spec to specify the filtering condition for the partition.

## Clone Hive Database

```bash
<FLINK_HOME>/flink run ./paimon-flink-action-{{< version >}}.jar \
clone \
--database default \
--catalog_conf metastore=hive \
--catalog_conf uri=thrift://localhost:9088 \
--target_database test \
--parallelism 10 \
--target_catalog_conf warehouse=my_warehouse
--included_tables <included_tables_spec> \
--excluded_tables <excluded_tables_spec>
```
"--included_tables" and "--excluded_tables" are optional parameters, which are used to specify the tables that need or don't need to be cloned.
The format is `<database1>.<table1>,<database2>.<table2>,<database3>.<table3>`.
"--excluded_tables" has higher priority than "--included_tables" if you specified both.

## Clone Hive Catalog

```bash
<FLINK_HOME>/flink run ./paimon-flink-action-{{< version >}}.jar \
clone \
--catalog_conf metastore=hive \
--catalog_conf uri=thrift://localhost:9088 \
--parallelism 10 \
--target_catalog_conf warehouse=my_warehouse \
--included_tables <included_tables_spec> \
--excluded_tables <excluded_tables_spec>
```
"--included_tables" and "--excluded_tables" are optional parameters, which are used to specify the tables that need or don't need to be cloned. 
The format is `<database1>.<table1>,<database2>.<table2>,<database3>.<table3>`.
"--excluded_tables" has higher priority than "--included_tables" if you specified both.
