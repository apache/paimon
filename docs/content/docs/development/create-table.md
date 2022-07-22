---
title: "Create Table"
weight: 2
type: docs
aliases:
- /development/create-table.html
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

# Create Table

## Catalog

Table Store uses its own catalog to manage all the databases and tables. Users need to configure the type `table-store` and a root directory `warehouse` to use it.

```sql
CREATE CATALOG my_catalog WITH (
  'type'='table-store',
  'warehouse'='hdfs://nn:8020/warehouse/path'
);

USE CATALOG my_catalog;
```

Table Store catalog supports SQL DDL commands:
- `CREATE TABLE ... PARTITIONED BY`
- `DROP TABLE ...`
- `ALTER TABLE ...`
- `SHOW DATABASES`
- `SHOW TABLES`

## Syntax

```sql
CREATE TABLE [IF NOT EXISTS] [catalog_name.][db_name.]table_name
  (
    { <physical_column_definition> | <computed_column_definition> }[ , ...n]
    [ <watermark_definition> ]
    [ <table_constraint> ][ , ...n]
  )
  [PARTITIONED BY (partition_column_name1, partition_column_name2, ...)]
  WITH (key1=val1, key2=val2, ...)
   
<physical_column_definition>:
  column_name column_type [ <column_constraint> ] [COMMENT column_comment]
  
<column_constraint>:
  [CONSTRAINT constraint_name] PRIMARY KEY NOT ENFORCED

<table_constraint>:
  [CONSTRAINT constraint_name] PRIMARY KEY (column_name, ...) NOT ENFORCED

<computed_column_definition>:
  column_name AS computed_column_expression [COMMENT column_comment]

<watermark_definition>:
  WATERMARK FOR rowtime_column_name AS watermark_strategy_expression
```

{{< hint info >}}
__Note:__ 
- To ensure the uniqueness of the primary key, the primary key must contain the partition field.
- Metadata column is not supported yet.
{{< /hint >}}

This will create a directory under `${warehouse}/${database_name}.db/${table_name}`.

## Table Options

Important options include the following:

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 20%">Option</th>
      <th class="text-center" style="width: 5%">Required</th>
      <th class="text-center" style="width: 5%">Default</th>
      <th class="text-center" style="width: 10%">Type</th>
      <th class="text-center" style="width: 60%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>bucket</h5></td>
      <td>Yes</td>
      <td style="word-wrap: break-word;">1</td>
      <td>Integer</td>
      <td>The bucket number for table store.</td>
    </tr>
    <tr>
      <td><h5>log.system</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The log system used to keep changes of the table, supports 'kafka'.</td>
    </tr>
    <tr>
      <td><h5>kafka.bootstrap.servers</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Required Kafka server connection string for log store.</td>
    </tr>
    <tr>
      <td><h5>kafka.topic</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Topic of this kafka table.</td>
    </tr>
    </tbody>
</table>

## Distribution

The data distribution of Table Store consists of three concepts:
Partition, Bucket, and Primary Key.

```sql
CREATE TABLE MyTable (
  user_id BIGINT,
  item_id BIGINT,
  behavior STRING,
  dt STRING,
  PRIMARY KEY (dt, user_id) NOT ENFORCED
) PARTITIONED BY (dt) WITH (
  'bucket' = '4'
);
```

For example, the `MyTable` table above has its data distribution
in the following order:
- Partition: isolating different data based on partition fields.
- Bucket: Within a single partition, distributed into 4 different
  buckets based on the hash value of the primary key.
- Primary key: Within a single bucket, sorted by primary key to
  build LSM structure.

## Partition

Table Store adopts the same partitioning concept as Apache Hive to
separate data, and thus various operations can be managed by partition
as a management unit.

Partitioned filtering is the most effective way to improve performance,
your query statements should contain partition filtering conditions
as much as possible.

## Bucket

Bucket is the concept of dividing data into more manageable parts for more efficient queries.

With `N` as bucket number, records are falling into `(0, 1, ..., N-1)` buckets. For each record, which bucket 
it belongs is computed by the hash value of one or more columns (denoted as **bucket key**), and mod by bucket number.

```
bucket_id = hash_func(bucket_key) % num_of_buckets
```

Users can specify the bucket key as follows

```sql
CREATE TABLE MyTable (
  catalog_id BIGINT,
  user_id BIGINT,
  item_id BIGINT,
  behavior STRING,
  dt STRING
) WITH (
    'bucket-key' = 'catalog_id'
);
```

{{< hint info >}}
__Note:__
- If users do not specify the bucket key explicitly, the primary key (if present) or the whole row is used as bucket key.
- Bucket key cannot be changed once the table is created. `ALTER TALBE SET ('bucket-key' = ...)` or `ALTER TABLE RESET ('bucket-key')` will throw exception.
{{< /hint >}}


The number of buckets is very important as it determines the
worst-case maximum processing parallelism. But it should not be
too big, otherwise, the system will create a lot of small files.

In general, the desired file size is 128 MB, the recommended data
to be kept on disk in each sub-bucket is about 1 GB.

## Primary Key

The primary key is unique and indexed.

Flink Table Store imposes an ordering of data, which means the system
will sort the primary key within each bucket. All fields will be used
to sort if no primary key is defined. Using this feature, you can
achieve high performance by adding filter conditions on the primary key.

The primary key's choice is critical, especially when setting the composite
primary key. A rule of thumb is to put the most frequently queried field in
the front. For example:

```sql
CREATE TABLE MyTable (
  catalog_id BIGINT,
  user_id BIGINT,
  item_id BIGINT,
  behavior STRING,
  dt STRING,
  ......
);
```

For this table, assuming that the composite primary keys are
the `catalog_id` and `user_id` fields, there are two ways to
set the primary key:
1. PRIMARY KEY (user_id, catalog_id)
2. PRIMARY KEY (catalog_id, user_id)

The two methods do not behave in the same way when querying.
Use approach one if you have a large number of filtered queries
with only `user_id`, and use approach two if you have a large
number of filtered queries with only `catalog_id`.

## Partial Update

You can configure partial update from options:

```sql
CREATE TABLE MyTable (
  product_id BIGINT,
  price DOUBLE,
  number BIGINT,
  detail STRING,
  PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
  'merge-engine' = 'partial-update'
);

INSERT INTO MyTable
SELECT product_id, price, number, NULL FROM Src1 UNION ALL
SELECT product_id, NULL, NULL, detail FROM Src2;
```

The value fields are updated to the latest data one by one
under the same primary key, but null values are not overwritten.

For example, the inputs:
- <1, 23.0, 10,   NULL>
- <1, NULL, NULL, 'This is a book'>
- <1, 25.2, NULL, NULL>

Output:
- <1, 25.2, 10, 'This is a book'>

{{< hint info >}}
__Note:__
- Partial update is only supported for table with primary key.
- Partial update is not supported for streaming consuming.
- It is best not to have NULL values in the fields, NULL will not overwrite data.
{{< /hint >}}

## Append-only Table

Append-only tables are a performance feature that only accepts `INSERT_ONLY` data to append to the storage instead of 
updating or de-duplicating the existing data, and hence suitable for use cases that do not require updates (such as log data synchronization).

### Create Append-only Table

By specifying the core option `'write-mode'` to `'append-only'`, users can create an append-only table as follows.

```sql
CREATE TABLE IF NOT EXISTS T1 (
    f0 INT,
    f1 DOUBLE,
    f2 STRING
) WITH (
    'write-mode' = 'append-only',
    'bucket' = '1' --specify the total number of buckets
)
```
{{< hint info >}}
__Note:__
- By definition, users cannot define primary keys on an append-only table.
- Append-only table is different from a change-log table which does not define primary keys. 
  For the latter, updating or deleting the whole row is accepted, although no primary key is present.
{{< /hint >}}

### Query Append-only Table

Table Store supports reading append-only table with preserved sequential order within each bucket. 

For example, with following write

```sql
INSERT INTO T1 VALUES
(1, 1.0, 'AAA'), (2, 2.0, 'BBB'), 
(3, 3.0, 'CCC'), (1, 1.0, 'AAA')
```

The query 
```sql
SELECT * FROM T1
``` 
will return exactly the order of `(1, 1.0, 'AAA'), (2, 2.0, 'BBB'), (3, 3.0, 'CCC'), (1, 1.0, 'AAA')`
because the total number of bucket is 1, and all records falls into the same bucket.

If we create another table with more than one bucket and specify the bucket key
```sql
CREATE TABLE IF NOT EXISTS T2 (
    f0 INT,
    f1 DOUBLE,
    f2 STRING
) WITH (
    'write-mode' = 'append-only',
    'bucket' = '2',
    'bucket-key' = 'f0'
)
```
The following write will write `(1, 1.0, 'AAA'), (2, 2.0, 'BBB'), (1, 1.0, 'AAA')` to bucket-0 and `(3, 3.0, 'CCC')` to bucket-1
```sql
INSERT INTO T2 VALUES
(1, 1.0, 'AAA'), (2, 2.0, 'BBB'), 
(3, 3.0, 'CCC'), (1, 1.0, 'AAA')
```
The query
```sql
SELECT * FROM T2
``` 
will return either `(1, 1.0, 'AAA'), (2, 2.0, 'BBB'), (1, 1.0, 'AAA'), (3, 3.0, 'CCC')` or `(3, 3.0, 'CCC'), (1, 1.0, 'AAA'), (2, 2.0, 'BBB'), (1, 1.0, 'AAA')`.
