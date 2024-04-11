---
title: "SQL Query"
weight: 4
type: docs
aliases:
- /spark/sql-query.html
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

# SQL Query

Just like all other tables, Paimon tables can be queried with `SELECT` statement.

## Batch Query

Paimon's batch read returns all the data in a snapshot of the table. By default, batch reads return the latest snapshot.

### Batch Time Travel

Paimon batch reads with time travel can specify a snapshot or a tag and read the corresponding data.

Requires Spark 3.3+.

you can use `VERSION AS OF` and `TIMESTAMP AS OF` in query to do time travel:

```sql
-- read the snapshot with id 1L (use snapshot id as version)
SELECT * FROM t VERSION AS OF 1;

-- read the snapshot from specified timestamp 
SELECT * FROM t TIMESTAMP AS OF '2023-06-01 00:00:00.123';

-- read the snapshot from specified timestamp in unix seconds
SELECT * FROM t TIMESTAMP AS OF 1678883047;

-- read tag 'my-tag'
SELECT * FROM t VERSION AS OF 'my-tag';

-- read the snapshot from specified watermark. will match the first snapshot after the watermark
SELECT * FROM t VERSION AS OF 'watermark-1678883047356';

```

{{< hint warning >}}
If tag's name is a number and equals to a snapshot id, the VERSION AS OF syntax will consider tag first. For example, if
you have a tag named '1' based on snapshot 2, the statement `SELECT * FROM t VERSION AS OF '1'` actually queries snapshot 2
instead of snapshot 1.
{{< /hint >}}

### Batch Incremental

Read incremental changes between start snapshot (exclusive) and end snapshot.

For example:
- '5,10' means changes between snapshot 5 and snapshot 10.
- 'TAG1,TAG3' means changes between TAG1 and TAG3.

Requires Spark 3.2+.

Paimon supports that use Spark SQL to do the incremental query that implemented by Spark Table Valued Function.
To enable this needs these configs below:

```text
--conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions
```

you can use `paimon_incremental_query` in query to extract the incremental data:

```sql
-- read the incremental data between snapshot id 12 and snapshot id 20.
SELECT * FROM paimon_incremental_query('tableName', 12, 20);
```

In Batch SQL, the `DELETE` records are not allowed to be returned, so records of `-D` will be dropped.
If you want see `DELETE` records, you can query audit_log table.

## Streaming Query

{{< hint info >}}

Paimon currently supports Spark 3.3+ for streaming read.

{{< /hint >}}

Paimon supports rich scan mode for streaming read. There is a list:
<table class="configuration table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 20%">Scan Mode</th>
            <th class="text-left" style="width: 60%">Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>latest</h5></td>
            <td>For streaming sources, continuously reads latest changes without producing a snapshot at the beginning. </td>
        </tr>
        <tr>
            <td><h5>latest-full</h5></td>
            <td>For streaming sources, produces the latest snapshot on the table upon first startup, and continue to read the latest changes.</td>
        </tr>
        <tr>
            <td><h5>from-timestamp</h5></td>
            <td>For streaming sources, continuously reads changes starting from timestamp specified by "scan.timestamp-millis", without producing a snapshot at the beginning. </td>
        </tr>
        <tr>
            <td><h5>from-snapshot</h5></td>
            <td>For streaming sources, continuously reads changes starting from snapshot specified by "scan.snapshot-id", without producing a snapshot at the beginning. </td>
        </tr>
        <tr>
            <td><h5>from-snapshot-full</h5></td>
            <td>For streaming sources, produces from snapshot specified by "scan.snapshot-id" on the table upon first startup, and continuously reads changes.</td>
        </tr>
        <tr>
            <td><h5>default</h5></td>
            <td>It is equivalent to from-snapshot if "scan.snapshot-id" is specified. It is equivalent to from-timestamp if "timestamp-millis" is specified. Or, It is equivalent to latest-full.</td>
        </tr>
    </tbody>
</table>

A simple example with default scan mode:

```scala
// no any scan-related configs are provided, that will use latest-full scan mode.
val query = spark.readStream
  .format("paimon")
  .load("/path/to/paimon/source/table")
  .writeStream
  .format("console")
  .start()
```

Paimon Structured Streaming also supports a variety of streaming read modes, it can support many triggers and many read limits.

These read limits are supported:

<table class="configuration table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 20%">Key</th>
            <th class="text-left" style="width: 15%">Default</th>
            <th class="text-left" style="width: 10%">Type</th>
            <th class="text-left" style="width: 55%">Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>read.stream.maxFilesPerTrigger</h5></td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>Integer</td>
            <td>The maximum number of files returned in a single batch.</td>
        </tr>
        <tr>
            <td><h5>read.stream.maxBytesPerTrigger</h5></td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>Long</td>
            <td>The maximum number of bytes returned in a single batch.</td>
        </tr>
        <tr>
            <td><h5>read.stream.maxRowsPerTrigger</h5></td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>Long</td>
            <td>The maximum number of rows returned in a single batch.</td>
        </tr>
        <tr>
            <td><h5>read.stream.minRowsPerTrigger</h5></td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>Long</td>
            <td>The minimum number of rows returned in a single batch, which used to create MinRowsReadLimit with read.stream.maxTriggerDelayMs together.</td>
        </tr>
        <tr>
            <td><h5>read.stream.maxTriggerDelayMs</h5></td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>Long</td>
            <td>The maximum delay between two adjacent batches, which used to create MinRowsReadLimit with read.stream.minRowsPerTrigger together.</td>
        </tr>
    </tbody>
</table>

**Example: One**

Use `org.apache.spark.sql.streaming.Trigger.AvailableNow()` and `maxBytesPerTrigger` defined by paimon.

```scala
// Trigger.AvailableNow()) processes all available data at the start
// of the query in one or multiple batches, then terminates the query.
// That set read.stream.maxBytesPerTrigger to 128M means that each
// batch processes a maximum of 128 MB of data.
val query = spark.readStream
  .format("paimon")
  .option("read.stream.maxBytesPerTrigger", "134217728")
  .load("/path/to/paimon/source/table")
  .writeStream
  .format("console")
  .trigger(Trigger.AvailableNow())
  .start()
```

**Example: Two**

Use `org.apache.spark.sql.connector.read.streaming.ReadMinRows`.

```scala
// It will not trigger a batch until there are more than 5,000 pieces of data,
// unless the interval between the two batches is more than 300 seconds.
val query = spark.readStream
  .format("paimon")
  .option("read.stream.minRowsPerTrigger", "5000")
  .option("read.stream.maxTriggerDelayMs", "300000")
  .load("/path/to/paimon/source/table")
  .writeStream
  .format("console")
  .start()
```

Paimon Structured Streaming supports read row in the form of changelog (add rowkind column in row to represent its
change type) in two ways:

- Direct streaming read with the system audit_log table
- Set `read.changelog` to true (default is false), then streaming read with table location

**Example:**

```scala
// Option 1
val query1 = spark.readStream
  .format("paimon")
  .table("`table_name$audit_log`")
  .writeStream
  .format("console")
  .start()

// Option 2
val query2 = spark.readStream
  .format("paimon")
  .option("read.changelog", "true")
  .load("/path/to/paimon/source/table")
  .writeStream
  .format("console")
  .start()

/*
+I   1  Hi
+I   2  Hello
*/
```

## Query Optimization

It is highly recommended to specify partition and primary key filters
along with the query, which will speed up the data skipping of the query.

The filter functions that can accelerate data skipping are:
- `=`
- `<`
- `<=`
- `>`
- `>=`
- `IN (...)`
- `LIKE 'abc%'`
- `IS NULL`

Paimon will sort the data by primary key, which speeds up the point queries
and range queries. When using a composite primary key, it is best for the query
filters to form a [leftmost prefix](https://dev.mysql.com/doc/refman/5.7/en/multiple-column-indexes.html)
of the primary key for good acceleration.

Suppose that a table has the following specification:

```sql
CREATE TABLE orders (
    catalog_id BIGINT,
    order_id BIGINT,
    .....,
) TBLPROPERTIES (
    'primary-key' = 'catalog_id,order_id'
);
```

The query obtains a good acceleration by specifying a range filter for
the leftmost prefix of the primary key.

```sql
SELECT * FROM orders WHERE catalog_id=1025;

SELECT * FROM orders WHERE catalog_id=1025 AND order_id=29495;

SELECT * FROM orders
  WHERE catalog_id=1025
  AND order_id>2035 AND order_id<6000;
```

However, the following filter cannot accelerate the query well.

```sql
SELECT * FROM orders WHERE order_id=29495;

SELECT * FROM orders WHERE catalog_id=1025 OR order_id=29495;
```
