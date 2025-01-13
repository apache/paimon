---
title: "Upsert To Partitioned"
weight: 1
type: docs
aliases:
- /migration/upsert-to-partitioned.html
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

# Upsert To Partitioned

{{< hint warning >}}
__Note:__ Only Hive Engine can be used to query these upsert-to-partitioned tables.
{{< /hint >}}

The [Tag Management]({{< ref "maintenance/manage-tags" >}}) will maintain the manifests and data files of the snapshot.
A typical usage is creating tags daily, then you can maintain the historical data of each day for batch reading.

When using primary key tables, a non-partitioned approach is often used to maintain updates, in order to mirror and
synchronize tables from upstream database tables. This allows users to query the latest data. The tradition of Hive
data warehouses is not like this. Offline data warehouses require an immutable view every day to ensure the idempotence
of calculations. So we created a Tag mechanism to output these views.

However, the traditional use of Hive data warehouses is more accustomed to using partitions to specify the query's Tag,
and is more accustomed to using Hive computing engines.

So, we introduce `'metastore.tag-to-partition'` and `'metastore.tag-to-partition.preview'` to mapping a non-partitioned
primary key table to the partition table in Hive metastore, and mapping the partition field to the name of the Tag to be
fully compatible with Hive.

## Example for Tag to Partition

**Step 1: Create table and tag in Flink SQL**

{{< tabs "Create table and tag in Flink SQL 1" >}}
{{< tab "Flink" >}}
```sql
CREATE CATALOG my_hive WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    -- 'uri' = 'thrift://<hive-metastore-host-name>:<port>', default use 'hive.metastore.uris' in HiveConf
    -- 'hive-conf-dir' = '...', this is recommended in the kerberos environment
    -- 'hadoop-conf-dir' = '...', this is recommended in the kerberos environment
    -- 'warehouse' = 'hdfs:///path/to/table/store/warehouse', default use 'hive.metastore.warehouse.dir' in HiveConf
);

USE CATALOG my_hive;

CREATE TABLE mydb.t (
    pk INT,
    col1 STRING,
    col2 STRING
) WITH (
    'bucket' = '-1',
    'metastore.tag-to-partition' = 'dt'
);

INSERT INTO t VALUES (1, '10', '100'), (2, '20', '200');

-- create tag '2023-10-16' for snapshot 1
CALL sys.create_tag('mydb.t', '2023-10-16', 1);
```

{{< /tab >}}
{{< /tabs >}}

**Step 2: Query table in Hive with Partition Pruning**

{{< tabs "Query table in Hive with Partition Pruning 1" >}}
{{< tab "Hive" >}}
```sql
SHOW PARTITIONS t;
/*
OK
dt=2023-10-16
*/

SELECT * FROM t WHERE dt='2023-10-16';
/*
OK
1 10 100 2023-10-16
2 20 200 2023-10-16
*/
```

{{< /tab >}}
{{< /tabs >}}

## Example for Tag Preview

The above example can only query tags that have already been created, but Paimon is a real-time data lake, and you also
need to query the latest data. Therefore, Paimon provides a preview feature:

**Step 1: Create table and tag in Flink SQL**

{{< tabs "Create table and tag in Flink SQL 2" >}}
{{< tab "Flink" >}}
```sql
CREATE CATALOG my_hive WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    -- 'uri' = 'thrift://<hive-metastore-host-name>:<port>', default use 'hive.metastore.uris' in HiveConf
    -- 'hive-conf-dir' = '...', this is recommended in the kerberos environment
    -- 'hadoop-conf-dir' = '...', this is recommended in the kerberos environment
    -- 'warehouse' = 'hdfs:///path/to/table/store/warehouse', default use 'hive.metastore.warehouse.dir' in HiveConf
);

USE CATALOG my_hive;

CREATE TABLE mydb.t (
    pk INT,
    col1 STRING,
    col2 STRING
) WITH (
    'bucket' = '-1',
    'metastore.tag-to-partition' = 'dt',
    -- preview tag creation mode process-time
    -- paimon will create partitions early based on process-time
    'metastore.tag-to-partition.preview' = 'process-time'
);

INSERT INTO t VALUES (1, '10', '100'), (2, '20', '200');

-- create tag '2023-10-16' for snapshot 1
CALL sys.create_tag('mydb.t', '2023-10-16', 1);

-- new data in '2023-10-17'
INSERT INTO t VALUES (3, '30', '300'), (4, '40', '400');

-- haven't finished writing the data for '2023-10-17' yet, so there's no need to create a tag for now
-- but the data is already visible for Hive
```

{{< /tab >}}
{{< /tabs >}}

**Step 2: Query table in Hive with Partition Pruning**

{{< tabs "Query table in Hive with Partition Pruning 2" >}}
{{< tab "Hive" >}}
```sql
SHOW PARTITIONS t;
/*
OK
dt=2023-10-16
dt=2023-10-17
*/

SELECT * FROM t WHERE dt='2023-10-17';
-- preview tag '2023-10-17'
/*
OK
1 10 100 2023-10-17
2 20 200 2023-10-17
3 30 300 2023-10-17
4 40 400 2023-10-17
*/
```

{{< /tab >}}
{{< /tabs >}}
