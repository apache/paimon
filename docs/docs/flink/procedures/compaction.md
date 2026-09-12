---
title: "Compaction and Layout"
sidebar_position: 1
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

# Compaction and Layout

Compact data files and manifests, rescale buckets, or maintain row IDs.

See [Procedures](../procedures) for Flink version requirements, argument conventions, and catalog selection.

## compact

To compact a table. Arguments:

- `table` (required): the target table identifier.

- `partitions` (optional): partition filter.

- `order_strategy` (optional): 'order' or 'zorder' or 'hilbert' or 'none'.

- `order_by` (optional): the columns need to be sort. Left empty if 'order_strategy' is 'none'.

- `options` (optional): additional dynamic options of the table. It prioritizes higher than original `tableProp` and lower than `procedureArg`.

- `where` (optional): partition predicate(Can't be used together with "partitions"). Note: as where is a keyword,a pair of backticks need to add around like `where`.

- `partition_idle_time` (optional): this is used to do a full compaction for partition which had not received any new data for 'partition_idle_time'. And only these partitions will be compacted. This argument can not be used with order compact.

- `compact_strategy` (optional): this determines how to pick files to be merged, the default is determined by the runtime execution mode. 'full' strategy only supports batch mode. All files will be selected for merging. 'minor' strategy: Pick the set of files that need to be merged based on specified conditions.

**Syntax**

```sql
-- Use named argument
CALL [catalog.]sys.compact(
    `table` => 'table',
    partitions => 'partitions',
    order_strategy => 'order_strategy',
    order_by => 'order_by',
    options => 'options',
    `where` => 'where',
    partition_idle_time => 'partition_idle_time',
    compact_strategy => 'compact_strategy');

-- Use indexed argument
CALL [catalog.]sys.compact('table');

CALL [catalog.]sys.compact('table', 'partitions');

CALL [catalog.]sys.compact('table', 'order_strategy', 'order_by');

CALL [catalog.]sys.compact('table', 'partitions', 'order_strategy', 'order_by');

CALL [catalog.]sys.compact('table', 'partitions', 'order_strategy', 'order_by', 'options');

CALL [catalog.]sys.compact('table', 'partitions', 'order_strategy', 'order_by', 'options', 'where');

CALL [catalog.]sys.compact(
    'table',
    'partitions',
    'order_strategy',
    'order_by',
    'options',
    'where',
    'partition_idle_time'
);

CALL [catalog.]sys.compact(
    'table',
    'partitions',
    'order_strategy',
    'order_by',
    'options',
    'where',
    'partition_idle_time',
    'compact_strategy'
);
```

**Example**

```sql
-- use partition filter
CALL sys.compact(
    `table` => 'default.T',
    partitions => 'p=0',
    order_strategy => 'zorder',
    order_by => 'a,b',
    options => 'sink.parallelism=4'
);

-- use partition predicate
CALL sys.compact(
    `table` => 'default.T',
    `where` => 'dt>10 and h<20',
    order_strategy => 'zorder',
    order_by => 'a,b',
    options => 'sink.parallelism=4'
);
```

## compact_database

To compact databases. Arguments:

- `includingDatabases`: to specify databases. You can use regular expression.

- `mode`: compact mode. "divided" (default): start a sink for each table, detecting the new table requires restarting the job; "combined": start a single combined sink for all tables, the new table will be automatically detected.

- `includingTables`: to specify tables. You can use regular expression.

- `excludingTables`: to specify tables that are not compacted. You can use regular expression.

- `tableOptions`: additional dynamic options of the table.

- `partition_idle_time`: this is used to do a full compaction for partition which had not received any new data for 'partition_idle_time'. And only these partitions will be compacted.

- `compact_strategy` (optional): this determines how to pick files to be merged, the default is determined by the runtime execution mode. 'full' strategy only supports batch mode. All files will be selected for merging. 'minor' strategy: Pick the set of files that need to be merged based on specified conditions.

**Syntax**

```sql
-- Use named argument
CALL [catalog.]sys.compact_database(
    including_databases => 'includingDatabases',
    mode => 'mode',
    including_tables => 'includingTables',
    excluding_tables => 'excludingTables',
    table_options => 'tableOptions',
    partition_idle_time => 'partitionIdleTime',
    compact_strategy => 'compact_strategy');

-- Use indexed argument
CALL [catalog.]sys.compact_database();

CALL [catalog.]sys.compact_database('includingDatabases');

CALL [catalog.]sys.compact_database('includingDatabases', 'mode');

CALL [catalog.]sys.compact_database('includingDatabases', 'mode', 'includingTables');

CALL [catalog.]sys.compact_database('includingDatabases', 'mode', 'includingTables', 'excludingTables');

CALL [catalog.]sys.compact_database(
    'includingDatabases',
    'mode',
    'includingTables',
    'excludingTables',
    'tableOptions'
);

CALL [catalog.]sys.compact_database(
    'includingDatabases',
    'mode',
    'includingTables',
    'excludingTables',
    'tableOptions',
    'partitionIdleTime'
);

CALL [catalog.]sys.compact_database(
    'includingDatabases',
    'mode',
    'includingTables',
    'excludingTables',
    'tableOptions',
    'partitionIdleTime',
    'compact_strategy'
);
```

**Example**

```sql
CALL sys.compact_database(
    including_databases => 'db1|db2',
    mode => 'combined',
    including_tables => 'table_.*',
    excluding_tables => 'ignore',
    table_options => 'sink.parallelism=4',
    compact_strategy => 'full');
```

## compact_chain_table

To compact chain table by merging snapshot and delta branches into the snapshot branch. Arguments:

- `table`: the target chain table identifier. Cannot be empty.

- `partition`: partition specification format (e.g., 'dt=20250810,hour=22'). Cannot be empty.

- `overwrite`: whether to overwrite if the partition already exists in the snapshot branch. Default is false. Optional.

**Syntax**

```sql
-- Use named argument
CALL [catalog.]sys.compact_chain_table(
    `table` => 'table',
    partition => 'partition',
    overwrite => overwrite);

-- Use indexed argument
CALL [catalog.]sys.compact_chain_table('table', 'partition');

CALL [catalog.]sys.compact_chain_table('table', 'partition', overwrite);
```

**Example**

```sql
CALL sys.compact_chain_table(`table` => 'default.T', partition => 'dt=20250810,hour=22');

CALL sys.compact_chain_table('default.T', 'dt=20250810,hour=22', true);
```

## compact_manifest

To compact_manifest the manifests. Arguments:

- `table`: the target table identifier. Cannot be empty.

- `options`: the additional dynamic options of the table. It prioritizes higher than original `tableProp` and lower than `procedureArg`.

- dry_run (Boolean, optional): when true, returns manifest metadata statistics without actually compacting. When manifest sort is enabled, the result also contains the number of manifest files in each level built by manifest sort.

- manifest_sort_enabled (Boolean, optional): whether to use manifest sort rewrite for this invocation.

- manifest_sort_partition_field (String, optional): partition field used to sort manifest entries. Defaults to the first partition field.

- manifest_sort_max_rewrite_size (String, optional): maximum manifest size rewritten by one sort pass.

**Syntax**

```sql
CALL [catalog.]sys.compact_manifest(`table` => 'identifier');

CALL [catalog.]sys.compact_manifest(`table` => 'identifier', 'options' => 'key1=value1,key2=value2');

CALL [catalog.]sys.compact_manifest(`table` => 'identifier', `dry_run` => true);

CALL [catalog.]sys.compact_manifest(
    `table` => 'identifier',
    `manifest_sort_enabled` => true,
    `manifest_sort_partition_field` => 'dt',
    `manifest_sort_max_rewrite_size` => '1 gb'
);
```

**Example**

```sql
CALL sys.compact_manifest(`table` => 'default.T');

CALL sys.compact_manifest(`table` => 'default.T', `dry_run` => true);

CALL sys.compact_manifest(
    `table` => 'default.T',
    `manifest_sort_enabled` => true,
    `manifest_sort_partition_field` => 'dt',
    `manifest_sort_max_rewrite_size` => '1 gb'
);
```

## rescale

Rescale one partition of a table. Arguments:

- `table`: The target table identifier. Cannot be empty.

- `bucket_num`: Resulting bucket number after rescale. The default value of argument bucket_num is the current bucket number of the table. Cannot be empty for postpone bucket tables.

- `partition`: What partition to rescale. For partitioned table this argument cannot be empty.

- `scan_parallelism`: Parallelism of source operator. The default value is the current bucket number of the partition.

- `sink_parallelism`: Parallelism of sink operator. The default value is equal to bucket_num.

**Syntax**

```sql
CALL [catalog.]sys.rescale(
    `table` => 'identifier',
    `bucket_num` => bucket_num,
    `partition` => 'partition',
    `scan_parallelism` => scan_parallelism,
    `sink_parallelism` => sink_parallelism
);
```

**Example**

```sql
CALL sys.rescale(`table` => 'default.T', `bucket_num` => 16, `partition` => 'dt=20250217,hh=08');
```

## materialize_deletion_vectors

Applies deletion vectors to the latest state of an unaware-bucket Data Evolution table and assigns new row IDs to surviving rows. Affected global indexes are dropped. One invocation processes one bounded batch with a soft target of 100,000 deletion vectors; an overlapping row-ID component is not split and can exceed the target. Invoke the procedure repeatedly until an invocation makes no changes. Historical snapshots and tags can retain the replaced files until snapshot expiration. Arguments:

- `table` (required): the target table identifier.

- `partitions` (optional): partition filter.

- `options` (optional): additional dynamic table options.

- `where` (optional): partition predicate (cannot be used together with partitions).

**Syntax**

```sql
-- Use named argument
CALL [catalog.]sys.materialize_deletion_vectors(
    `table` => 'table',
    partitions => 'partitions',
    options => 'options',
    `where` => 'where');

-- Use indexed argument
CALL [catalog.]sys.materialize_deletion_vectors('table');

CALL [catalog.]sys.materialize_deletion_vectors('table', 'partitions', 'options', 'where');
```

**Example**

```sql
CALL sys.materialize_deletion_vectors(`table` => 'default.T');

CALL sys.materialize_deletion_vectors(`table` => 'default.T', partitions => 'dt=2026-08-12');
```

## reassign_row_id

Reassign row IDs for a data evolution table by rewriting metadata. Argument:

- `table`: `databaseName.tableName`.

- `partitions`: specific partitions.

**Syntax**

```sql
-- Use named argument
CALL [catalog.]sys.reassign_row_id(<`table` => identifier> [, <partitions => partitions>]);

-- Use indexed argument
CALL [catalog.]sys.reassign_row_id(<identifier> [, <partitions>]);
```

**Example**

```sql
-- reassign row IDs for the whole table
CALL sys.reassign_row_id(`table` => 'test_db.T');

-- reassign row IDs for the specified partition in the table
CALL sys.reassign_row_id(`table` => 'test_db.T', partitions => 'pt=a');
```
