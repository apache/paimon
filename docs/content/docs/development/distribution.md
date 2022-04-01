---
title: "Distribution"
weight: 3
type: docs
aliases:
- /development/distribution.html
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

# Distribution

The data distribution of Table Store consists of three concepts:
Partition, Bucket, and Primary Key.

```sql
CREATE TABLE MyTable (
  user_id BIGINT,
  item_id BIGINT,
  behavior STRING,
  dt STRING,
  PRIMARY KEY (dt, user_id) NOT ENFORCED
) PARTITION BY (dt) WITH (
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

The record is hashed into different buckets according to the
primary key or the whole row (without primary key).

The number of buckets is very important as it determines the
worst-case maximum processing parallelism. But it should not be
too big, otherwise, the system will create a lot of small files.

In general, the desired file size is 128 MB, the recommended data
to be kept on disk in each sub-bucket is about 1 GB.

## Primary Key

The primary key is unique and is indexed.

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
Use approach 1 if you have a large number of filtered queries
with only `user_id`, and use approach 2 if you have a large
number of filtered queries with only `catalog_id`.
