---
title: "Snapshots and Retention"
sidebar_position: 2
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

# Snapshots and Retention

Expire table history, roll back a table, or purge its files.

See [Procedures](../procedures) for Flink version requirements, argument conventions, and catalog selection.

## expire_snapshots

To expire snapshots. Argument:

- `table`: the target table identifier. Cannot be empty.

- `retain_max`: the maximum number of completed snapshots to retain.

- `retain_min`: the minimum number of completed snapshots to retain.

- `order_than`: timestamp before which snapshots will be removed.

- `max_deletes`: the maximum number of snapshots that can be deleted at once.

- `options`: the additional dynamic options of the table. It prioritizes higher than original `tableProp` and lower than `procedureArg`.

**Syntax**

```sql
-- Use named argument
CALL [catalog.]sys.expire_snapshots(
    `table` => 'identifier',
    retain_max => 'retain_max',
    retain_min => 'retain_min',
    older_than => 'older_than',
    max_deletes => 'max_deletes',
    options => 'key1=value1,key2=value2');

-- Use indexed argument
-- for Flink 1.18
CALL [catalog.]sys.expire_snapshots(table, retain_max);

-- for Flink 1.19 and later
CALL [catalog.]sys.expire_snapshots(table, retain_max, retain_min, older_than, max_deletes);
```

**Example**

```sql
-- for Flink 1.18
CALL sys.expire_snapshots('default.T', 2);

-- for Flink 1.19 and later
CALL sys.expire_snapshots(`table` => 'default.T', retain_max => 2);

CALL sys.expire_snapshots(`table` => 'default.T', older_than => '2024-01-01 12:00:00');

CALL sys.expire_snapshots(`table` => 'default.T', older_than => '2024-01-01 12:00:00', retain_min => 10);

CALL sys.expire_snapshots(
    `table` => 'default.T',
    older_than => '2024-01-01 12:00:00',
    max_deletes => 10,
    options => 'snapshot.expire.limit=1'
);
```

## expire_changelogs

To expire changelogs. Argument:

- `table`: the target table identifier. Cannot be empty.

- `retain_max`: the maximum number of completed changelogs to retain.

- `retain_min`: the minimum number of completed changelogs to retain.

- `order_than`: timestamp before which changelogs will be removed.

- `max_deletes`: the maximum number of changelogs that can be deleted at once.

- `delete_all`: whether to delete all separated changelogs.

**Syntax**

```sql
-- Use named argument
CALL [catalog.]sys.expire_changelogs(
    `table` => 'identifier',
    retain_max => 'retain_max',
    retain_min => 'retain_min',
    older_than => 'older_than',
    max_deletes => 'max_deletes');

    delete_all => 'delete_all');

-- Use indexed argument
-- for Flink 1.18
CALL [catalog.]sys.expire_changelogs(table, retain_max, retain_min, older_than, max_deletes);

CALL [catalog.]sys.expire_changelogs(table, delete_all);

-- for Flink 1.19 and later
CALL [catalog.]sys.expire_changelogs(table, retain_max, retain_min, older_than, max_deletes, delete_all);
```

**Example**

```sql
-- for Flink 1.18
CALL sys.expire_changelogs('default.T', 4, 2, '2024-01-01 12:00:00', 2);

CALL sys.expire_changelogs('default.T', true);

-- for Flink 1.19 and later
CALL sys.expire_changelogs(`table` => 'default.T', retain_max => 2);

CALL sys.expire_changelogs(`table` => 'default.T', older_than => '2024-01-01 12:00:00');

CALL sys.expire_changelogs(`table` => 'default.T', older_than => '2024-01-01 12:00:00', retain_min => 10);

CALL sys.expire_changelogs(`table` => 'default.T', older_than => '2024-01-01 12:00:00', max_deletes => 10);

CALL sys.expire_changelogs(`table` => 'default.T', delete_all => true);
```

## expire_partitions

To expire partitions. Argument:

- `table`: the target table identifier. Cannot be empty.

- `expiration_time`: the expiration interval of a partition. A partition will be expired if it's lifetime is over this value. Partition time is extracted from the partition value.

- `timestamp_formatter`: the formatter to format timestamp from string.

- `timestamp_pattern`: the pattern to get a timestamp from partitions.

- `expire_strategy`: specifies the expiration strategy for partition expiration, possible values: 'values-time' or 'update-time' , 'values-time' as default.

- `max_expires`: The maximum of limited expired partitions, it is optional.

- `options`: the additional dynamic options of the table. It prioritizes higher than original `tableProp` and lower than `procedureArg`.

**Syntax**

```sql
CALL [catalog.]sys.expire_partitions(table, expiration_time, timestamp_formatter, expire_strategy, options);
```

**Example**

```sql
-- for Flink 1.18
CALL sys.expire_partitions('default.T', '1 d', 'yyyy-MM-dd', '$dt', 'values-time');

-- for Flink 1.19 and later
CALL sys.expire_partitions(
    `table` => 'default.T',
    expiration_time => '1 d',
    timestamp_formatter => 'yyyy-MM-dd',
    expire_strategy => 'values-time'
);

CALL sys.expire_partitions(
    `table` => 'default.T',
    expiration_time => '1 d',
    timestamp_formatter => 'yyyy-MM-dd HH:mm',
    timestamp_pattern => '$dt $hm',
    expire_strategy => 'values-time',
    options => 'partition.expiration-max-num=2'
);
```

## rollback_to

To rollback to a specific version of target table. Argument:

- `table`: the target table identifier. Cannot be empty.

- snapshotId (Long): id of the snapshot that will roll back to.

- `tagName`: name of the tag that will roll back to.

**Syntax**

```sql
-- for Flink 1.18
-- rollback to a snapshot
CALL [catalog.]sys.rollback_to('identifier', snapshotId);

-- rollback to a tag
CALL [catalog.]sys.rollback_to('identifier', 'tagName');

-- for Flink 1.19 and later
-- rollback to a snapshot
CALL [catalog.]sys.rollback_to(`table` => 'identifier', snapshot_id => snapshotId);

-- rollback to a tag
CALL [catalog.]sys.rollback_to(`table` => 'identifier', tag => 'tagName');
```

**Example**

```sql
-- for Flink 1.18
CALL sys.rollback_to('default.T', 10);

-- for Flink 1.19 and later
CALL sys.rollback_to(`table` => 'default.T', snapshot_id => 10);
```

## rollback_to_as_latest

To roll a table back to a specific version and materialize it as the latest snapshot, without deleting later snapshots or tags. Batch and time-travel reads are correct; for deletion-vector tables, a rollback whose only difference is a deletion-vector change is not guaranteed to be observed by streaming overwrite readers. Argument:

- `table`: the target table identifier. Cannot be empty.

- snapshotId (Long): id of the snapshot to roll back to.

- `tagName`: name of the tag to roll back to.

**Syntax**

```sql
-- for Flink 1.18
-- roll back to a snapshot as the latest snapshot
CALL [catalog.]sys.rollback_to_as_latest('identifier', cast(null as string), snapshotId);

-- roll back to a tag as the latest snapshot
CALL [catalog.]sys.rollback_to_as_latest('identifier', 'tagName', cast(null as bigint));

-- for Flink 1.19 and later
-- roll back to a snapshot as the latest snapshot
CALL [catalog.]sys.rollback_to_as_latest(`table` => 'identifier', snapshot_id => snapshotId);

-- roll back to a tag as the latest snapshot
CALL [catalog.]sys.rollback_to_as_latest(`table` => 'identifier', tag => 'tagName');
```

**Example**

```sql
-- for Flink 1.18
CALL sys.rollback_to_as_latest('default.T', cast(null as string), 10);

-- for Flink 1.19 and later
CALL sys.rollback_to_as_latest(`table` => 'default.T', snapshot_id => 10);
```

## rollback_to_timestamp

To rollback to the snapshot which earlier or equal than timestamp. Argument:

- `table`: the target table identifier. Cannot be empty.

- timestamp (Long): Roll back to the snapshot which earlier or equal than timestamp.

**Syntax**

```sql
-- for Flink 1.18
-- rollback to the snapshot which earlier or equal than timestamp.
CALL [catalog.]sys.rollback_to_timestamp('identifier', timestamp);

-- for Flink 1.19 and later
-- rollback to the snapshot which earlier or equal than timestamp.
CALL [catalog.]sys.rollback_to_timestamp(`table` => 'default.T', `timestamp` => timestamp);
```

**Example**

```sql
-- for Flink 1.18
CALL sys.rollback_to_timestamp('default.T', 10);

-- for Flink 1.19 and later
CALL sys.rollback_to_timestamp(`table` => 'default.T', timestamp => 1730292023000);
```

## rollback_to_watermark

To rollback to the snapshot which earlier or equal than watermark. Argument:

- `table`: the target table identifier. Cannot be empty.

- watermark (Long): Roll back to the snapshot which earlier or equal than watermark.

**Syntax**

```sql
-- for Flink 1.18
-- rollback to the snapshot which earlier or equal than watermark.
CALL [catalog.]sys.rollback_to_watermark('identifier', watermark);

-- for Flink 1.19 and later
-- rollback to the snapshot which earlier or equal than watermark.
CALL [catalog.]sys.rollback_to_watermark(`table` => 'default.T', `watermark` => watermark);
```

**Example**

```sql
-- for Flink 1.18
CALL sys.rollback_to_watermark('default.T', 1730292023000);

-- for Flink 1.19 and later
CALL sys.rollback_to_watermark(`table` => 'default.T', watermark => 1730292023000);
```

## purge_files

To clear table with purge files. Argument:

- `table`: the target table identifier. Cannot be empty.

**Syntax**

```sql
-- clear table with purge files.
CALL [catalog.]sys.purge_files('identifier');
```

**Example**

```sql
CALL sys.purge_files('default.T');
```
