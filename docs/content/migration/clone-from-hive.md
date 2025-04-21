---
title: "Clone From Hive"
weight: 5
type: docs
aliases:
- /migration/clone-from-hive.html
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

# Clone Hive Table

Clone Hive Table supports cloning hive tables with parquet, orc and avro formats. The cloned table will be
[append table]({{< ref "append-table/overview" >}}).

## Clone Hive Table

```bash
<FLINK_HOME>/flink run ./paimon-flink-action-{{< version >}}.jar \
clone_hive \
--database default \
--table hivetable \
--catalog_conf metastore=hive \
--catalog_conf uri=thrift://localhost:9088 \
--target_database test \
--target_table test_table \
--target_catalog_conf warehouse=my_warehouse \
--where <filter_spec>
```

You can use filter spec to specify the filtering condition for the partition.

## Clone Hive Database

```bash
<FLINK_HOME>/flink run ./paimon-flink-action-{{< version >}}.jar \
clone_hive \
--database default \
--catalog_conf metastore=hive \
--catalog_conf uri=thrift://localhost:9088 \
--target_database test \
--target_catalog_conf warehouse=my_warehouse
```
