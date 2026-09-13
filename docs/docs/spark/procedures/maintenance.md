---
title: "Compaction and Cleanup"
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

# Compaction and Cleanup

Compact data and manifests, expire unused history, or repair file metadata.
See [Dedicated Compaction](../../maintenance/dedicated-compaction) and
[Manage Snapshots](../../maintenance/manage-snapshots) for operational context.

For catalog selection and invocation syntax, see [Procedures](../procedures).

## compact

Compact files.

**Arguments**

- `table` (`STRING`, required): the target table identifier.
- `partitions` (`STRING`, optional): partition filter. a comma (",") represents AND; a semicolon (";") represents OR. If you want to compact one partition with date=01 and day=01, you need to write 'date=01,day=01'. Omit for all partitions. (Can't be used together with "where")
- `where` (`STRING`, optional): partition predicate. Omit for all partitions. (Can't be used together with "partitions")
- `order_strategy` (`STRING`, optional): 'order' or 'zorder' or 'hilbert' or 'none'. Omit for 'none'.
- `order_by` (`STRING`, optional): the columns to sort. Omit if 'order_strategy' is 'none'.
- `options` (`STRING`, optional): additional dynamic options of the table. These override stored table properties and are overridden by explicit procedure arguments.
- `partition_idle_time` (`STRING`, optional): this is used to do a full compaction for partition which had not received any new data for 'partition_idle_time'. And only these partitions will be compacted. This argument can not be used with order compact.
- `compact_strategy` (`STRING`, optional): `full` selects all candidate files in the chosen scope; `minor` selects files according to the compaction policy. Defaults to `minor` when incremental clustering is enabled, otherwise `full`.
- `buckets` (`STRING`, optional): fixed-bucket IDs, comma-separated IDs, or inclusive ranges, for example `'0-3,7'`. Omit to include all buckets. Valid only for fixed-bucket tables without sort compaction.

```sql
SET spark.sql.shuffle.partitions=10; --set the compact parallelism

CALL sys.compact(table => 'T', partitions => 'p=0;p=1',  order_strategy => 'zorder', order_by => 'a,b');

CALL sys.compact(table => 'T', where => 'p>0 and p<3', order_strategy => 'zorder', order_by => 'a,b');

CALL sys.compact(
  table => 'T',
  where => 'dt>10 and h<20',
  order_strategy => 'zorder',
  order_by => 'a,b',
  options => 'target-file-size=128m'
);

CALL sys.compact(table => 'T', partition_idle_time => '60s');

CALL sys.compact(table => 'T', compact_strategy => 'minor');

-- Limit full compaction to these buckets of a fixed-bucket table with at least 8 buckets.
CALL sys.compact(table => 'T', compact_strategy => 'full', buckets => '0-3,7');
```

## compact_database

Compact all tables across one or more databases.

**Arguments**

- `including_databases` (`STRING`, optional): regular expression to match databases to compact. Omit to match all databases (i.e. '.*').
- `including_tables` (`STRING`, optional): regular expression to match table identifiers (in 'db.table' form) to compact. Omit to match all tables (i.e. '.*').
- `excluding_tables` (`STRING`, optional): regular expression to match table identifiers to exclude from compaction.
- `options` (`STRING`, optional): additional dynamic options of the table. These override stored table properties and are overridden by explicit procedure arguments.

```sql
-- compact all databases
CALL sys.compact_database();

-- compact some databases (accept regular expression)
CALL sys.compact_database(including_databases => 'db1|db2');

-- compact some tables (accept regular expression)
CALL sys.compact_database(including_databases => 'db1', including_tables => 'db1.table1|db1.table2');

-- exclude some tables (accept regular expression)
CALL sys.compact_database(
  including_databases => 'db1',
  including_tables => '.*',
  excluding_tables => '.*ignore_table'
);

-- set table options
CALL sys.compact_database(including_databases => 'db1', options => 'target-file-size=128m');
```

## compact_chain_table

Compact chain table by merging snapshot and delta branches into the snapshot branch.

**Arguments**

- `table` (`STRING`, required): The target chain table identifier.
- `partition` (`STRING`, required): Partition specification format (e.g., 'dt="20250810",hour="22"').
- `overwrite` (`BOOLEAN`, optional): Whether to overwrite if the partition already exists in the snapshot branch. Default is false.

```sql
CALL sys.compact_chain_table(table => 'default.T', partition => 'dt="20250810",hour="22"');
```

## compact_manifest

Compact manifest files.

**Arguments**

- `table` (`STRING`, required): the target table identifier.
- `options` (`STRING`, optional): the additional dynamic options of the table. These override stored table properties and are overridden by explicit procedure arguments.
- `dry_run` (`BOOLEAN`, optional): when true, logs manifest metadata statistics without actually compacting. When manifest sort is enabled, the log also contains the number of manifest files in each level built by manifest sort. The result is printed to the application log; the SQL return value is still `true`.
- `manifest_sort_enabled` (`BOOLEAN`, optional): whether to use manifest sort rewrite for this invocation.
- `manifest_sort_partition_field` (`STRING`, optional): partition field used to sort manifest entries. Defaults to the first partition field.
- `manifest_sort_max_rewrite_size` (`STRING`, optional): maximum manifest size rewritten by one sort pass.

```sql
CALL sys.compact_manifest(`table` => 'default.T');

CALL sys.compact_manifest(`table` => 'default.T', dry_run => true);

CALL sys.compact_manifest(
  `table` => 'default.T',
  manifest_sort_enabled => true,
  manifest_sort_partition_field => 'dt',
  manifest_sort_max_rewrite_size => '1 gb'
);
```

## materialize_deletion_vectors

Applies deletion vectors to the latest state of an unaware-bucket Data Evolution table and assigns
new row IDs to surviving rows. Affected global indexes are dropped. Spark processes bounded batches
until all matching deletion vectors are materialized. Historical snapshots and tags can retain the
replaced files until snapshot expiration.

**Arguments**

- `table` (`STRING`, required): the target table identifier.
- `partitions` (`STRING`, optional): partition filter. Cannot be used together with where.
- `options` (`STRING`, optional): additional dynamic table options.
- `where` (`STRING`, optional): partition predicate. Cannot be used together with partitions.

```sql
CALL sys.materialize_deletion_vectors(table => 'T');

CALL sys.materialize_deletion_vectors(table => 'T', partitions => 'dt=2026-08-12');
```

## rescale

Rescale partitions of a table by changing the bucket number.

**Arguments**

- `table` (`STRING`, required): the target table identifier.
- `bucket_num` (`INT`, optional): resulting bucket number after rescale. The default value is the current bucket number of the table. Cannot be empty for postpone bucket tables.
- `partitions` (`STRING`, optional): partition filter. Omit for all partitions. (Can't be used together with "where")
- `where` (`STRING`, optional): partition predicate. Omit for all partitions. (Can't be used together with "partitions")

```sql
CALL sys.rescale(
  table => 'default.T',
  bucket_num => 16,
  partitions => 'dt=20250217,hh=08;dt=20250217,hh=09'
);
```

## expire_snapshots

Expire snapshots.

**Arguments**

- `table` (`STRING`, required): the target table identifier.
- `retain_max` (`INT`, optional): the maximum number of completed snapshots to retain.
- `retain_min` (`INT`, optional): the minimum number of completed snapshots to retain.
- `older_than` (`STRING`, optional): timestamp before which snapshots will be removed.
- `max_deletes` (`INT`, optional): the maximum number of snapshots that can be deleted at once.
- `options` (`STRING`, optional): the additional dynamic options of the table. These override stored table properties and are overridden by explicit procedure arguments.

```sql
CALL sys.expire_snapshots(table => 'default.T', retain_max => 10, options => 'snapshot.expire.limit=1');
```

## expire_partitions

Expire partitions.

**Arguments**

- `table` (`STRING`, required): the target table identifier.
- `expiration_time` (`STRING`, optional): the expiration interval of a partition. A partition will be expired if it's lifetime is over this value. Partition time is extracted from the partition value.
- `timestamp_formatter` (`STRING`, optional): the formatter to format timestamp from string.
- `timestamp_pattern` (`STRING`, optional): the pattern to get a timestamp from partitions.
- `expire_strategy` (`STRING`, optional): specifies the expiration strategy for partition expiration, possible values: 'values-time' or 'update-time' , 'values-time' as default.
- `max_expires` (`INT`, optional): The maximum of limited expired partitions, it is optional.
- `options` (`STRING`, optional): the additional dynamic options of the table. These override stored table properties and are overridden by explicit procedure arguments.

```sql
CALL sys.expire_partitions(
  table => 'default.T',
  expiration_time => '1 d',
  timestamp_formatter => 'yyyy-MM-dd',
  timestamp_pattern => '$dt',
  expire_strategy => 'values-time',
  options => 'partition.expiration-max-num=2'
);
```

## remove_orphan_files

Remove the orphan data files and metadata files.

**Arguments**

- `table` (`STRING`, required): the target table identifier. Use `database_name.*` to process the whole database.
- `older_than` (`STRING`, optional): to avoid deleting newly written files, this procedure only deletes orphan files older than 1 day by default. This argument can modify the interval.
- `dry_run` (`BOOLEAN`, optional): when true, view only orphan files, don't actually remove files. Default is false.
- `parallelism` (`INT`, optional): The maximum number of concurrent deleting files. By default is the number of processors available to the Java virtual machine.
- `mode` (`STRING`, optional): The mode of remove orphan clean procedure (local or distributed) . By default is distributed.

```sql
CALL sys.remove_orphan_files(table => 'default.T', older_than => '2023-10-31 12:00:00');

CALL sys.remove_orphan_files(table => 'default.*', older_than => '2023-10-31 12:00:00');

CALL sys.remove_orphan_files(table => 'default.T', older_than => '2023-10-31 12:00:00', dry_run => true);

CALL sys.remove_orphan_files(
  table => 'default.T',
  older_than => '2023-10-31 12:00:00',
  dry_run => true,
  parallelism => 5
);

CALL sys.remove_orphan_files(
  table => 'default.T',
  older_than => '2023-10-31 12:00:00',
  dry_run => true,
  parallelism => 5,
  mode => 'local'
);
```

## remove_unexisting_files

Procedure to remove unexisting data files from manifest entries. See [Java docs](https://paimon.apac
he.org/docs/master/api/java/org/apache/paimon/flink/action/RemoveUnexistingFilesAction.html) for
detailed use cases. Note that user is on his own risk using this procedure, which may cause data
loss when used outside from the use cases listed in Java docs.

**Arguments**

- `table` (`STRING`, required): the target table identifier. Use `database_name.*` to process the whole database.
- `dry_run` (`BOOLEAN`, optional): only check what files will be removed, but not really remove them. Default is false.
- `parallelism` (`INT`, optional): number of parallelisms to check files in the manifests.

```sql
-- remove unexisting data files in the table `mydb.myt`
CALL sys.remove_unexisting_files(table => 'mydb.myt');

-- only check what files will be removed, but not really remove them (dry run)
CALL sys.remove_unexisting_files(table => 'mydb.myt', dry_run => true);
```

## purge_files

Clear table with purge files.

**Arguments**

- `table` (`STRING`, required): the target table identifier.

```sql
CALL sys.purge_files(table => 'default.T');
```

## repair

Synchronize information from the file system to Metastore.

**Arguments**

- `database_or_table` (`STRING`, required): empty or the target database name or the target table identifier, if you specify multiple tags, delimiter is ','

```sql
CALL sys.repair('test_db.T');

CALL sys.repair('test_db.T,test_db01,test_db.T2');
```

## repair_earliest_snapshot

Repair the earliest snapshot hint for a table.

**Arguments**

- `table` (`STRING`, required): the target table identifier.
- `snapshot_id` (`BIGINT`, required): the snapshot ID to set as the earliest snapshot.

```sql
CALL sys.repair_earliest_snapshot(table => 'test_db.T', snapshot_id => 10);
```
