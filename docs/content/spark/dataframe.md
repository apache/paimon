---
title: "DataFrame"
weight: 9
type: docs
aliases:
- /spark/dataframe.html
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

# DataFrame

Paimon supports creating table, inserting data, and querying through the Spark DataFrame API.

## Create Table
You can specify table properties with `option` or set partition columns with `partitionBy` if needed.

```scala
val data: DataFrame = Seq((1, "x1", "p1"), (2, "x2", "p2")).toDF("a", "b", "pt")

data.write.format("paimon")
  .option("primary-key", "a,pt")
  .option("k1", "v1")
  .partitionBy("pt")
  .saveAsTable("test_tbl") // or .save("/path/to/default.db/test_tbl")
```

## Insert

### Insert Into
You can achieve INSERT INTO semantics by setting the mode to `append`.

```scala
val data: DataFrame = ...

data.write.format("paimon")
  .mode("append")
  .insertInto("test_tbl") // or .saveAsTable("test_tbl") or .save("/path/to/default.db/test_tbl")
```

Note: `insertInto` ignores the column names and just uses position-based write,
if you need to write by column name, use `saveAsTable` or `save` instead.

### Insert Overwrite
You can achieve INSERT OVERWRITE semantics by setting the mode to `overwrite` with `insertInto`.

It supports dynamic partition overwritten for partitioned table.
To enable dynamic overwritten you need to set the Spark session configuration `spark.sql.sources.partitionOverwriteMode` to `dynamic`.

```scala
val data: DataFrame = ...

data.write.format("paimon")
  .mode("overwrite")
  .insertInto("test_tbl")
```

## Replace Table
You can achieve REPLACE TABLE semantics by setting the mode to `overwrite` with `saveAsTable` or `save`.

It first drops the existing table and then create a new one,
so you need to specify the table’s properties or partition columns if needed.

```scala
val data: DataFrame = ...

data.write.format("paimon")
  .option("primary-key", "a,pt")
  .option("k1", "v1")
  .partitionBy("pt")
  .mode("overwrite")
  .saveAsTable("test_tbl") // or .save("/path/to/default.db/test_tbl")
```

## Query

```scala
spark.read.format("paimon")
  .table("t") // or .load("/path/to/default.db/test_tbl")
  .show()
```

To specify the catalog or database, you can use

```scala
// recommend
spark.read.format("paimon")
  .table("<catalogName>.<databaseName>.<tableName>")

// or
spark.read.format("paimon")
  .option("catalog", "<catalogName>")
  .option("database", "<databaseName>")
  .option("table", "<tableName>")
  .load("/path/to/default.db/test_tbl")
```

You can specify other read configs through option:

```scala
// time travel
spark.read.format("paimon")
  .option("scan.snapshot-id", 1)
  .table("t")
```
