---
title: "DataFrame API"
sidebar_position: 9
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

# DataFrame API

Use the Spark DataFrame API to create tables, write rows, and query data. Start `spark-shell`
with the same JAR, catalog, and extension options as [Quick Start](./quick-start#setup), then run:

```scala
import spark.implicits._
spark.sql("USE paimon.default")
```

The examples below use Scala and an existing Paimon catalog. Follow them in order with a new
`test_tbl`, or adapt the names to your own tables. For streaming DataFrames, see
[Structured Streaming](./structured-streaming).

## Choose a Write Method

| Method | Column alignment | Target |
| --- | --- | --- |
| `saveAsTable(name)` | By name when appending to an existing table | Catalog table; can also create a new table. |
| `insertInto(name)` | By position; source column names are ignored | Existing catalog table. |
| `save(path)` | Depends on the write path; default V1 writes are positional | Existing Paimon table location. Select columns in target order explicitly. |

![A DataFrame with columns b and a is written to a table with columns a and b. insertInto maps positions, while saveAsTable aligns names.](/img/spark-dataframe-alignment.svg)

## Create Table

Use `saveAsTable` to create a table and write its initial rows. Set table properties with
`option` and partition columns with `partitionBy`:

```scala
val initial = Seq((1, "x1", "p1"), (2, "x2", "p2")).toDF("a", "b", "pt")

initial.write.format("paimon")
  .option("primary-key", "a,pt")
  .option("bucket", "1")
  .partitionBy("pt")
  .saveAsTable("test_tbl")
```

A path write with `.save(path)` requires a table schema already stored at that location.
Create the table first; `.save(path)` is not a substitute for this creation step.

## Insert

### Insert Into

Append a batch with columns in a different order. `saveAsTable` aligns them by name:

```scala
val nextBatch = Seq(("p1", "updated", 1), ("p3", "x3", 3)).toDF("pt", "b", "a")

nextBatch.write.format("paimon")
  .mode("append")
  .saveAsTable("test_tbl")
```

Because `test_tbl` is a primary key table, this updates key `(1, p1)` and adds key `(3, p3)`.

For `insertInto`, select columns in the target order first. This is an alternative way to
write the same batch:

```scala
nextBatch.select("a", "b", "pt")
  .write
  .mode("append")
  .insertInto("test_tbl")
```

### Insert Overwrite

For a partitioned catalog table, dynamic overwrite replaces only partitions present in the
input. The following example replaces `p1` and preserves `p2` and `p3`:

```scala
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

val replacement = Seq((4, "x4", "p1")).toDF("a", "b", "pt")
replacement.write
  .mode("overwrite")
  .insertInto("test_tbl")
```

In static mode, an overwrite without a partition filter replaces the whole table. See
[overwrite scope](./sql-write#dynamic-overwrite-partition) for the SQL equivalents.

:::info

Since Spark 3.4, `saveAsTable` with `overwrite` mode on an existing Paimon catalog table preserves
the table definition, including partitions, primary keys, and properties. Configure the Paimon
extensions as in Quick Start. To replace the definition, use
[`CREATE OR REPLACE TABLE ... AS SELECT`](./sql-ddl#replace-table).

Before Spark 3.4, `saveAsTable` with `overwrite` drops and recreates the table. Only partitioning
and options explicitly supplied to the writer are kept, and the DataFrame supplies the new schema.

:::

## Query

Read through the catalog and inspect the result of the preceding writes:

```scala
spark.read.format("paimon")
  .table("paimon.default.test_tbl")
  .orderBy("a")
  .show()
// +---+---+---+
// |  a|  b| pt|
// +---+---+---+
// |  2| x2| p2|
// |  3| x3| p3|
// |  4| x4| p1|
// +---+---+---+
```

Use a fully qualified name to select another catalog or database. Read options can be supplied
per operation, for example to read a retained snapshot:

```scala
spark.read.format("paimon")
  .option("scan.snapshot-id", "1")
  .table("paimon.default.test_tbl")
  .show()
```

Replace `1` with a snapshot ID that exists in your table. See
[SQL Queries](./sql-query) for time travel and incremental read semantics.

## Read or Write by Location

For the local warehouse used in Quick Start, the table above is stored at
`file:/tmp/paimon/default.db/test_tbl`. Use the actual table location for another warehouse:

```scala
val tableLocation = "file:/tmp/paimon/default.db/test_tbl"

spark.read.format("paimon").load(tableLocation).show()

// Explicitly order columns for a path write.
nextBatch.select("a", "b", "pt")
  .write.format("paimon")
  .mode("append")
  .save(tableLocation)
```

For `.save(tableLocation)`, `overwrite` replaces the entire table even when the session's
partition overwrite mode is `dynamic`. Use a catalog write when you need dynamic partition
overwrite.

To associate a location-based read with a catalog identifier, supply all three identifier options:

```scala
spark.read.format("paimon")
  .option("catalog", "paimon")
  .option("database", "default")
  .option("table", "test_tbl")
  .load(tableLocation)
```

For missing or extra columns, see
[Schema Evolution on Write](./schema-evolution#column-alignment-by-write-path).
