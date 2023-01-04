---
title: "Writing Tables"
weight: 4
type: docs
aliases:
- /how-to/writing-tables.html
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

# Writing Tables

## Applying Records/Changes to Tables

{{< tabs "insert-into-example" >}}

{{< tab "Flink" >}}

Use `INSERT INTO` to apply records and changes to tables.

```sql
INSERT INTO MyTable SELECT ...
```

{{< /tab >}}

{{< tab "Spark3" >}}

Use `INSERT INTO` to apply records and changes to tables.

```sql
INSERT INTO MyTable SELECT ...
```

{{< /tab >}}

{{< /tabs >}}

## Overwriting the Whole Table

For unpartitioned tables, Table Store supports overwriting the whole table.

{{< tabs "insert-overwrite-unpartitioned-example" >}}

{{< tab "Flink" >}}

Use `INSERT OVERWRITE` to overwrite the whole unpartitioned table.

```sql
INSERT OVERWRITE MyTable SELECT ...
```

{{< /tab >}}

{{< /tabs >}}

## Overwriting a Partition

For partitioned tables, Table Store supports overwriting a partition.

{{< tabs "insert-overwrite-partitioned-example" >}}

{{< tab "Flink" >}}

Use `INSERT OVERWRITE` to overwrite a partition.

```sql
INSERT OVERWRITE MyTable PARTITION (key1 = value1, key2 = value2, ...) SELECT ...
```

{{< /tab >}}

{{< /tabs >}}

## Purging tables

You can use `INSERT OVERWRITE` to purge tables by inserting empty value.

{{< tabs "purge-tables-syntax" >}}

{{< tab "Flink" >}}

```sql
INSERT OVERWRITE MyTable SELECT * FROM MyTable WHERE false
```

{{< /tab >}}

{{< /tabs >}}

## Purging a Partition

Particularly, you can use `INSERT OVERWRITE` to purge data of a partition by inserting empty value to the partition:

{{< tabs "purge-partition-syntax" >}}

{{< tab "Flink" >}}

```sql
INSERT OVERWRITE MyTable PARTITION (key1 = value1, key2 = value2, ...) SELECT selectSpec FROM MyTable WHERE false
```

{{< /tab >}}

{{< /tabs >}}

{{< hint warning >}}

The `selectSpec` should includes all the columns in the table except the partition columns behind PARTITION.

{{< /hint >}}

The following SQL is an example:

{{< tabs "purge-partition-example" >}}

{{< tab "Flink" >}}

```sql
-- table definition
CREATE TABLE MyTable (
    k0 INT,
    k1 INT,
    v STRING
) PARTITIONED BY (k0, k1);

-- you can use
INSERT OVERWRITE MyTable PARTITION (k0 = 0) SELECT k1, v FROM MyTable WHERE false

-- or
INSERT OVERWRITE MyTable PARTITION (k0 = 0, k1 = 0) SELECT v FROM MyTable WHERE false
```

{{< /tab >}}

{{< /tabs >}}
