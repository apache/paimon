---
title: "Clone To Paimon"
sidebar_position: 5
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

## Clone Paimon Full History

Full-history clone physically copies a complete Paimon table to mapped storage paths. It preserves
all retained schemas, snapshots, tags, branches, long-lived changelogs, data files, extra files, and
indexes. It does not create or register a table in the target catalog.
The source catalog must expose the complete retained history through Paimon's filesystem metadata
layout.

Stop writes to the source table for the entire initial run and any retry. The action checks a source
metadata fingerprint and fails if the retained metadata roots change.

```bash
<FLINK_HOME>/flink run ./paimon-flink-action-@@VERSION@@.jar \
clone \
--clone_from paimon \
--clone_mode full-history \
--database default \
--table source_table \
--catalog_conf metastore=hive \
--catalog_conf uri=thrift://localhost:9088 \
--target_catalog_conf warehouse=dfs://target-cluster/warehouse \
--path_mapping dfs://source-cluster/warehouse=dfs://target-cluster/warehouse \
--path_mapping dfs://source-cluster/external-data=dfs://target-cluster/external-data \
--parallelism 100
```

Every reachable source path must match one `path_mapping`. The mapped source table root is the
physical target root. `target_database` and `target_table` are optional logical identifiers and do
not change that path or register the table. Mapping and source paths must use the same explicit
filesystem scheme; for example, `file:/path` does not match `/path`.

The target root must initially be absent or empty. A failed clone may be resumed with
`--clone_if_exists true`; existing same-size files are skipped and conflicting sizes fail. Resume is
accepted only when the ownership marker matches and `_SUCCESS` is absent. A completed clone cannot
be resumed. Run at most one full-history clone job for a target root at a time; `clone_if_exists`
is a failed-job resume protocol, not a concurrent execution protocol.

Mapped external-data and external-index target locations must be dedicated to this clone and must
not be changed while the initial run or a retry is active. Payload files are published through
two-phase output so an interrupted transfer does not expose a partial target. Existing-file
validation compares sizes, not checksums.

Full-history clone does not currently support blob descriptors or blob views, Iceberg compatibility
metadata, filtered clone, format conversion, or metadata-only clone.

## Clone Hive Table

```bash
<FLINK_HOME>/flink run ./paimon-flink-action-@@VERSION@@.jar \
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
<FLINK_HOME>/flink run ./paimon-flink-action-@@VERSION@@.jar \
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
<FLINK_HOME>/flink run ./paimon-flink-action-@@VERSION@@.jar \
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
