---
title: "Format Table Partitions"
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

# Format Table Partitions

This page covers Spark partition DDL and statistics for Format Tables. For Paimon snapshot
tables, see [Create Tables, Views, and Tags](./sql-ddl) and [Alter Tables](./sql-alter).

## Catalog-managed Partitions

For a Format Table whose `metastore.partitioned-table` option is `true`, the catalog holds the
partitions and Spark supports the standard partition DDL:

```sql
ALTER TABLE my_table ADD PARTITION (dt='2025-01-01');
ALTER TABLE my_table ADD PARTITION (dt='2024-12-31')
    LOCATION 'oss://archive-bucket/events/dt=2024-12-31';
ALTER TABLE my_table DROP PARTITION (dt='2025-01-01');
MSCK REPAIR TABLE my_table;
SHOW PARTITIONS my_table;
ANALYZE TABLE my_table PARTITION (dt='2025-01-01') COMPUTE STATISTICS NOSCAN;
```

On a Format Table whose partitions are discovered from the filesystem, `ADD PARTITION`,
`DROP PARTITION`, `MSCK REPAIR TABLE` and `ANALYZE TABLE` fail with an error.

`ADD PARTITION` creates the partition directory and registers the partition; querying a newly
added partition before any data is written returns no rows. `DROP PARTITION` unregisters the
partition and deletes its directory.

`ADD PARTITION ... LOCATION` registers a custom absolute URI without moving data and requires a
compatible REST catalog. Paimon can read the partition but does not write, delete, or analyze its
data. `DROP PARTITION` only unregisters it, and `MSCK REPAIR TABLE` leaves it unchanged.

A partition value that is empty or all whitespace is rejected by `ADD PARTITION`, `DROP PARTITION`
and `TRUNCATE PARTITION`. Such a value is written to the partition named by
`partition.default-name` (`__DEFAULT_PARTITION__` unless configured otherwise), the same partition
a `NULL` is written to, so it names that partition rather than one of its own - name it directly
instead. Writing one through `INSERT` is unaffected and still lands there.

`ANALYZE TABLE` measures partitions. A Format Table has no snapshot to carry a table-level
statistic and no column statistics, so `COMPUTE STATISTICS FOR COLUMNS` and `FOR ALL COLUMNS` are
not supported on it; what the statement writes back to the catalog is the file count, byte size,
last file creation time and row count of the partitions it measured. Each measured field replaces
the one the catalog held, so running it twice reports the same numbers as running it once, while a
field it could not measure leaves the stored one as it was. It never adds or removes a partition —
use `MSCK REPAIR TABLE` for that.

`NOSCAN` stops at the directory listing, which gives everything except the row count. Without it,
the row count is read from each file's footer, so it is exact for the formats that carry one
(Parquet, ORC) and a partition holding no files counts as zero, while a format that carries none
(CSV, TEXT, JSON) leaves the row count the catalog already held rather than guessing one. Reading
footers costs one open per file, so it runs on the executors and `NOSCAN` is the cheaper of the
two.

A `PARTITION (...)` clause must give values for a leading run of the partition columns, because
that is the shape the catalog can select on. On a table partitioned by `(dt, hh)`,
`PARTITION (dt='2025-01-01')` and `PARTITION (dt='2025-01-01', hh)` both measure every hour of
that day, while `PARTITION (hh='01')` is rejected rather than widened to every day. Naming a
partition that is not registered is an error too, rather than a statement that reports success for
having measured nothing.

:::info

`metastore.partitioned-table = true` enables catalog-managed partitions, which requires an
internal Format Table in a catalog that supports it (currently the REST catalog) and cannot be
combined with `format-table.implementation = engine`. The REST catalog validates this
combination on `CREATE TABLE`, with catalog-level table defaults
(`spark.sql.catalog.paimon.table-default.*`) participating in the effective options: a default
that makes the combination invalid fails the DDL.

In a REST catalog, asking for catalog-managed partitions on a table that cannot have them — an
external table, or `format-table.implementation = engine` — fails, rather than handing back a
table whose options say one thing and whose partitions come from somewhere else. Remove the option
with `ALTER TABLE my_table UNSET TBLPROPERTIES ('metastore.partitioned-table')`.

In any other catalog the option keeps the meaning it has always had on a Format Table — none. Such
a table loads and reads its partitions from the filesystem, so on a Format Table it
only takes effect in a REST catalog; elsewhere partitions are discovered from the filesystem.
On a REST catalog, an existing Format Table whose partitions were never registered reads as empty
until you register them with `MSCK REPAIR TABLE my_table`.

Mixed-version note: only writers that support catalog-managed partitions register the partitions
they produce. During a rolling upgrade, upgrade all writers before relying on catalog-managed
partitions, or run `MSCK REPAIR TABLE` afterwards, since data written by an older writer is not
visible until its partitions are registered.

For a Format Table, `metastore.partitioned-table` only changes where partitions come from; it does not
change the table's managed/external ownership.

:::

## Writes and Maintenance

See [SQL Writes](./sql-write#insert-overwrite) for overwrite behavior and
[TRUNCATE TABLE](./sql-write#truncate-table) for how data, registrations, and statistics are updated.
