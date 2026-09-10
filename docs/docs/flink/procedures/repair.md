---
title: "Cleanup and Repair"
sidebar_position: 7
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

# Cleanup and Repair

Inspect orphan files or repair missing files, manifests, and table metadata.

See [Procedures](../procedures) for Flink version requirements, argument conventions, and catalog selection.

## remove_orphan_files

To remove the orphan data files and metadata files. Arguments:

- `table`: the target table identifier. Cannot be empty, you can use database_name.* to clean whole database.

- `olderThan`: to avoid deleting newly written files, this procedure only deletes orphan files older than 1 day by default. This argument can modify the interval.

- `dryRun`: when true, view only orphan files, don't actually remove files. Default is false.

- `parallelism`: The maximum number of concurrent deleting files. By default is the number of processors available to the Java virtual machine.

- `mode`: The mode of remove orphan clean procedure (local or distributed) . By default is distributed.

**Syntax**

```sql
-- Use named argument
CALL [catalog.]sys.remove_orphan_files(
    `table` => 'identifier',
    older_than => 'olderThan',
    dry_run => 'dryRun',
    mode => 'mode'
);

-- Use indexed argument
CALL [catalog.]sys.remove_orphan_files('identifier');

CALL [catalog.]sys.remove_orphan_files('identifier', 'olderThan');

CALL [catalog.]sys.remove_orphan_files('identifier', 'olderThan', 'dryRun');

CALL [catalog.]sys.remove_orphan_files('identifier', 'olderThan', 'dryRun','parallelism');

CALL [catalog.]sys.remove_orphan_files('identifier', 'olderThan', 'dryRun','parallelism','mode');
```

**Example**

```sql
CALL sys.remove_orphan_files(`table` => 'default.T', older_than => '2023-10-31 12:00:00');

CALL sys.remove_orphan_files(`table` => 'default.*', older_than => '2023-10-31 12:00:00');

CALL sys.remove_orphan_files(`table` => 'default.T', older_than => '2023-10-31 12:00:00', dry_run => true);

CALL sys.remove_orphan_files(
    `table` => 'default.T',
    older_than => '2023-10-31 12:00:00',
    dry_run => false,
    parallelism => 5
);

CALL sys.remove_orphan_files(
    `table` => 'default.T',
    older_than => '2023-10-31 12:00:00',
    dry_run => false,
    parallelism => 5,
    mode => 'local'
);
```

## remove_unexisting_files

Procedure to remove unexisting data files from manifest entries. See [Java docs](https://paimon.apache.org/docs/master/api/java/org/apache/paimon/flink/action/RemoveUnexistingFilesAction.html) for detailed use cases. Arguments:

- `table`: the target table identifier. Cannot be empty, you can use database_name.* to clean whole database.

- `dry_run` (optional): only check what files will be removed, but not really remove them. Default is false.

- `parallelism` (optional): number of parallelisms to check files in the manifests.

Note that user is on his own risk using this procedure, which may cause data loss when used outside from the use cases listed in Java docs.

**Syntax**

```sql
-- Use named argument
CALL [catalog.]sys.remove_unexisting_files(
    `table` => 'identifier',
    dry_run => 'dryRun',
    parallelism => parallelism
);

-- Use indexed argument
CALL [catalog.]sys.remove_unexisting_files('identifier');

CALL [catalog.]sys.remove_unexisting_files('identifier', 'dryRun', 'parallelism');
```

**Example**

```sql
-- remove unexisting data files in the table `mydb.myt`
CALL sys.remove_unexisting_files(`table` => 'mydb.myt');

-- only check what files will be removed, but not really remove them (dry run)
CALL sys.remove_unexisting_files(`table` => 'mydb.myt', `dry_run` = true);
```

## remove_unexisting_manifests

Procedure to remove unexisting manifest file from manifset-list. for detailed use cases. Arguments:

- `table`: the target table identifier. Cannot be empty, you can use database.table$branch_xx to remove branch table unexisting manifest file.

Note that user is on his own risk using this procedure, which may cause data loss when used outside from the use cases listed in Java docs.

**Syntax**

```sql
-- Use named argument
CALL [catalog.]sys.remove_unexisting_files(`table` => 'identifier');
```

**Example**

```sql
-- remove unexisting manifest file in the table `mydb.myt`
CALL sys.remove_unexisting_manifests(`table` => 'mydb.myt');

-- remove unexisting manifest file in the branch table `mydb.myt$branch_rt`
CALL sys.remove_unexisting_manifests(`table` => 'mydb.myt$branch_rt');
```

## repair

Synchronize information from the file system to Metastore. Argument:

- `empty`: all databases and tables in catalog.

- `databaseName`: the target database name.

- `tableName`: the target table identifier.

**Syntax**

```sql
-- repair all databases and tables in catalog
CALL [catalog.]sys.repair();

-- repair all tables in a specific database
CALL [catalog.]sys.repair('databaseName');

-- repair a table
CALL [catalog.]sys.repair('databaseName.tableName');

-- repair database and table in a string if you specify multiple tags, delimiter is ','
CALL [catalog.]sys.repair('databaseName01,database02.tableName01,database03');
```

**Example**

```sql
CALL sys.repair(`table` => 'test_db.T');
```

## repair_earliest_snapshot

Repair the earliest snapshot hint for a table. Arguments:

- `table`: the target table identifier. Cannot be empty.

- `snapshot_id`: the snapshot ID to set as the earliest snapshot.

**Syntax**

```sql
CALL [catalog.]sys.repair_earliest_snapshot(`table` => 'identifier', snapshot_id => snapshotId);
```

**Example**

```sql
CALL sys.repair_earliest_snapshot(`table` => 'default.T', snapshot_id => 10);
```
