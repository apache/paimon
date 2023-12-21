---
title: "Primary Key Table"
weight: 6
type: docs
aliases:
- /concepts/primary-key-table.html
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

# Primary Key Table

Changelog table is the default table type when creating a table. Users can insert, update or delete records in the table.

Primary keys consist of a set of columns that contain unique values for each record. Paimon enforces data ordering by sorting the primary key within each bucket, allowing users to achieve high performance by applying filtering conditions on the primary key.

By [defining primary keys]({{< ref "how-to/creating-tables#tables-with-primary-keys" >}}) on a changelog table, users can access the following features.

## Bucket

A bucket is the smallest storage unit for reads and writes, each bucket directory contains an [LSM tree]({{< ref "concepts/file-layouts#lsm-trees" >}}).

### Fixed Bucket

Configure a bucket greater than 0, using Fixed Bucket mode, according to `Math.abs(key_hashcode % numBuckets)` to compute
the bucket of record.

Rescaling buckets can only be done through offline processes, see [Rescale Bucket]({{< ref "/maintenance/rescale-bucket" >}}).
A too large number of buckets leads to too many small files, and a too small number of buckets leads to poor write performance.

### Dynamic Bucket

Configure `'bucket' = '-1'`. The keys that arrive first will fall into the old buckets, and the new keys will fall into
the new buckets, the distribution of buckets and keys depends on the order in which the data arrives. Paimon maintains
an index to determine which key corresponds to which bucket.

Paimon will automatically expand the number of buckets.

- Option1: `'dynamic-bucket.target-row-num'`: controls the target row number for one bucket.
- Option2: `'dynamic-bucket.initial-buckets'`: controls the number of initialized bucket.

{{< hint info >}}
Dynamic Bucket only support single write job. Please do not start multiple jobs to write to the same partition 
(this can lead to duplicate data). Even if you enable `'write-only'` and start a dedicated compaction job, it won't work.
{{< /hint >}}

#### Normal Dynamic Bucket Mode

When your updates do not cross partitions (no partitions, or primary keys contain all partition fields), Dynamic
Bucket mode uses HASH index to maintain mapping from key to bucket, it requires more memory than fixed bucket mode.

Performance:

1. Generally speaking, there is no performance loss, but there will be some additional memory consumption, **100 million**
   entries in a partition takes up **1 GB** more memory, partitions that are no longer active do not take up memory.
2. For tables with low update rates, this mode is recommended to significantly improve performance.

`Normal Dynamic Bucket Mode` supports sort-compact to speed up queries. See [Sort Compact]({{< ref "maintenance/dedicated-compaction#sort-compact" >}}).

#### Cross Partitions Upsert Dynamic Bucket Mode

{{< hint info >}}
This is an experimental feature.
{{< /hint >}}

When you need cross partition upsert (primary keys not contain all partition fields), Dynamic Bucket mode directly
maintains the mapping of keys to partition and bucket, uses local disks, and initializes indexes by reading all 
existing keys in the table when starting stream write job. Different merge engines have different behaviors:

1. Deduplicate: Delete data from the old partition and insert new data into the new partition.
2. PartialUpdate & Aggregation: Insert new data into the old partition.
3. FirstRow: Ignore new data if there is old value.

Performance: For tables with a large amount of data, there will be a significant loss in performance. Moreover,
initialization takes a long time.

If your upsert does not rely on too old data, you can consider configuring index TTL to reduce Index and initialization time:
- `'cross-partition-upsert.index-ttl'`: The TTL in rocksdb index and initialization, this can avoid maintaining too many
  indexes and lead to worse and worse performance.

But please note that this may also cause data duplication.

## Merge Engines

When Paimon sink receives two or more records with the same primary keys, it will merge them into one record to keep primary keys unique. By specifying the `merge-engine` table property, users can choose how records are merged together.

{{< hint info >}}
Always set `table.exec.sink.upsert-materialize` to `NONE` in Flink SQL TableConfig, sink upsert-materialize may
result in strange behavior. When the input is out of order, we recommend that you use
[Sequence Field]({{< ref "concepts/primary-key-table#sequence-field" >}}) to correct disorder.
{{< /hint >}}

### Deduplicate

`deduplicate` merge engine is the default merge engine. Paimon will only keep the latest record and throw away other records with the same primary keys.

Specifically, if the latest record is a `DELETE` record, all records with the same primary keys will be deleted.

### Partial Update

By specifying `'merge-engine' = 'partial-update'`,
Users have the ability to update columns of a record through multiple updates until the record is complete. This is achieved by updating the value fields one by one, using the latest data under the same primary key. However, null values are not overwritten in the process.

For example, suppose Paimon receives three records:
- `<1, 23.0, 10, NULL>`-
- `<1, NULL, NULL, 'This is a book'>`
- `<1, 25.2, NULL, NULL>`

Assuming that the first column is the primary key, the final result would be `<1, 25.2, 10, 'This is a book'>`.

{{< hint info >}}
For streaming queries, `partial-update` merge engine must be used together with `lookup` or `full-compaction`
[changelog producer]({{< ref "concepts/primary-key-table#changelog-producers" >}}). ('input' changelog producer is also supported, but only returns input records.)
{{< /hint >}}

{{< hint info >}}
By default, Partial update can not accept delete records, you can choose one of the following solutions:
- Configure 'partial-update.ignore-delete' to ignore delete records.
- Configure 'sequence-group's to retract partial columns.
{{< /hint >}}

#### Sequence Group

A sequence-field may not solve the disorder problem of partial-update tables with multiple stream updates, because
the sequence-field may be overwritten by the latest data of another stream during multi-stream update.

So we introduce sequence group mechanism for partial-update tables. It can solve:

1. Disorder during multi-stream update. Each stream defines its own sequence-groups.
2. A true partial-update, not just a non-null update.

See example:

```sql
CREATE TABLE T (
    k INT,
    a INT,
    b INT,
    g_1 INT,
    c INT,
    d INT,
    g_2 INT,
    PRIMARY KEY (k) NOT ENFORCED
) WITH (
    'merge-engine'='partial-update',
    'fields.g_1.sequence-group'='a,b',
    'fields.g_2.sequence-group'='c,d'
);

INSERT INTO T VALUES (1, 1, 1, 1, 1, 1, 1);

-- g_2 is null, c, d should not be updated
INSERT INTO T VALUES (1, 2, 2, 2, 2, 2, CAST(NULL AS INT));

SELECT * FROM T; -- output 1, 2, 2, 2, 1, 1, 1

-- g_1 is smaller, a, b should not be updated
INSERT INTO T VALUES (1, 3, 3, 1, 3, 3, 3);

SELECT * FROM T; -- output 1, 2, 2, 2, 3, 3, 3
```

For fields.<fieldName>.sequence-group, valid comparative data types include: DECIMAL, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, DATE, TIME, TIMESTAMP, and TIMESTAMP_LTZ.

#### Aggregation

You can specify aggregation function for the input field, all the functions in the [Aggregation]({{< ref "concepts/primary-key-table#aggregation-1" >}}) are supported. 

See example:

```sql
CREATE TABLE T (
          k INT,
          a INT,
          b INT,
          c INT,
          d INT,
          PRIMARY KEY (k) NOT ENFORCED
) WITH (
     'merge-engine'='partial-update',
     'fields.a.sequence-group' = 'b',
     'fields.b.aggregate-function' = 'first_value',
     'fields.c.sequence-group' = 'd',
     'fields.d.aggregate-function' = 'sum'
 );
INSERT INTO T VALUES (1, 1, 1, CAST(NULL AS INT), CAST(NULL AS INT));
INSERT INTO T VALUES (1, CAST(NULL AS INT), CAST(NULL AS INT), 1, 1);
INSERT INTO T VALUES (1, 2, 2, CAST(NULL AS INT), CAST(NULL AS INT));
INSERT INTO T VALUES (1, CAST(NULL AS INT), CAST(NULL AS INT), 2, 2);


SELECT * FROM T; -- output 1, 2, 1, 2, 3
```

#### Default Value
If the order of the data cannot be guaranteed and field is written only by overwriting null values,
fields that have not been overwritten will be displayed as null when reading table.

```sql
CREATE TABLE T (
                  k INT,
                  a INT,
                  b INT,
                  c INT,
                  PRIMARY KEY (k) NOT ENFORCED
) WITH (
     'merge-engine'='partial-update'
     );
INSERT INTO T VALUES (1, 1, CAST(NULL AS INT), CAST(NULL AS INT));
INSERT INTO T VALUES (1, CAST(NULL AS INT), CAST(NULL AS INT), 1);

SELECT * FROM T; -- output 1, 1, null, 1
```
If it is expected that fields which have not been overwritten have a default value instead of null when reading table,
'fields.name.default-value' is required.
```sql
CREATE TABLE T (
    k INT,
    a INT,
    b INT,
    c INT,
    PRIMARY KEY (k) NOT ENFORCED
) WITH (
    'merge-engine'='partial-update',
    'fields.b.default-value'='0'
);

INSERT INTO T VALUES (1, 1, CAST(NULL AS INT), CAST(NULL AS INT));
INSERT INTO T VALUES (1, CAST(NULL AS INT), CAST(NULL AS INT), 1);

SELECT * FROM T; -- output 1, 1, 0, 1
```

### Aggregation

{{< hint info >}}
NOTE: Always set `table.exec.sink.upsert-materialize` to `NONE` in Flink SQL TableConfig.
{{< /hint >}}

Sometimes users only care about aggregated results. The `aggregation` merge engine aggregates each value field with the latest data one by one under the same primary key according to the aggregate function.

Each field not part of the primary keys can be given an aggregate function, specified by the `fields.<field-name>.aggregate-function` table property, otherwise it will use `last_non_null_value` aggregation as default. For example, consider the following table definition.

{{< tabs "aggregation-merge-engine-example" >}}

{{< tab "Flink" >}}

```sql
CREATE TABLE MyTable (
    product_id BIGINT,
    price DOUBLE,
    sales BIGINT,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'merge-engine' = 'aggregation',
    'fields.price.aggregate-function' = 'max',
    'fields.sales.aggregate-function' = 'sum'
);
```

{{< /tab >}}

{{< /tabs >}}

Field `price` will be aggregated by the `max` function, and field `sales` will be aggregated by the `sum` function. Given two input records `<1, 23.0, 15>` and `<1, 30.2, 20>`, the final result will be `<1, 30.2, 35>`.

Current supported aggregate functions and data types are:

* `sum`: supports DECIMAL, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT and DOUBLE.
* `min`/`max`: support CHAR, VARCHAR, DECIMAL, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, DATE, TIME, TIMESTAMP and TIMESTAMP_LTZ.
* `last_value` / `last_non_null_value`: support all data types.
* `listagg`: supports STRING data type.
* `bool_and` / `bool_or`: support BOOLEAN data type.
* `first_value` / `first_not_null_value`: support all data types.

Only `sum` supports retraction (`UPDATE_BEFORE` and `DELETE`), others aggregate functions do not support retraction.
If you allow some functions to ignore retraction messages, you can configure:
`'fields.${field_name}.ignore-retract'='true'`.

{{< hint info >}}
For streaming queries, `aggregation` merge engine must be used together with `lookup` or `full-compaction`
[changelog producer]({{< ref "concepts/primary-key-table#changelog-producers" >}}). ('input' changelog producer is also supported, but only returns input records.)
{{< /hint >}}

### First Row

{{< hint info >}}
This is an experimental feature.
{{< /hint >}}

By specifying `'merge-engine' = 'first-row'`, users can keep the first row of the same primary key. It differs from the
`deduplicate` merge engine that in the `first-row` merge engine, it will generate insert only changelog. 

{{< hint info >}}
1. `first-row` merge engine must be used together with `lookup` [changelog producer]({{< ref "concepts/primary-key-table#changelog-producers" >}}).
2. You can not specify `sequence.field`.
3. Not accept `DELETE` and `UPDATE_BEFORE` message. You can config `first-row.ignore-delete` to ignore these two kinds records.
{{< /hint >}}


This is of great help in replacing log deduplication in streaming computation.

## Changelog Producers

Streaming queries will continuously produce the latest changes.

By specifying the `changelog-producer` table property when creating the table, users can choose the pattern of changes produced from table files.

{{< hint info >}}

The `changelog-producer` table property only affects changelog from table files. It does not affect the external log system.

{{< /hint >}}

### None

By default, no extra changelog producer will be applied to the writer of table. Paimon source can only see the merged changes across snapshots, like what keys are removed and what are the new values of some keys.

However, these merged changes cannot form a complete changelog, because we can't read the old values of the keys directly from them. Merged changes require the consumers to "remember" the values of each key and to rewrite the values without seeing the old ones. Some consumers, however, need the old values to ensure correctness or efficiency.

Consider a consumer which calculates the sum on some grouping keys (might not be equal to the primary keys). If the consumer only sees a new value `5`, it cannot determine what values should be added to the summing result. For example, if the old value is `4`, it should add `1` to the result. But if the old value is `6`, it should in turn subtract `1` from the result. Old values are important for these types of consumers.

To conclude, `none` changelog producers are best suited for consumers such as a database system. Flink also has a 
built-in "normalize" operator which persists the values of each key in states. As one can easily tell, this operator
will be very costly and should be avoided. (You can force removing "normalize" operator via `'scan.remove-normalize'`.)

{{< img src="/img/changelog-producer-none.png">}}

### Input

By specifying `'changelog-producer' = 'input'`, Paimon writers rely on their inputs as a source of complete changelog. All input records will be saved in separated [changelog files]({{< ref "concepts/file-layouts" >}}) and will be given to the consumers by Paimon sources.

`input` changelog producer can be used when Paimon writers' inputs are complete changelog, such as from a database CDC, or generated by Flink stateful computation.

{{< img src="/img/changelog-producer-input.png">}}

### Lookup

If your input can’t produce a complete changelog but you still want to get rid of the costly normalized operator, you may consider using the `'lookup'` changelog producer.

By specifying `'changelog-producer' = 'lookup'`, Paimon will generate changelog through `'lookup'` before committing the data writing.

{{< img src="/img/changelog-producer-lookup.png">}}

Lookup will cache data on the memory and local disk, you can use the following options to tune performance:

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 20%">Option</th>
      <th class="text-left" style="width: 5%">Default</th>
      <th class="text-left" style="width: 10%">Type</th>
      <th class="text-left" style="width: 60%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
        <td><h5>lookup.cache-file-retention</h5></td>
        <td style="word-wrap: break-word;">1 h</td>
        <td>Duration</td>
        <td>The cached files retention time for lookup. After the file expires, if there is a need for access, it will be re-read from the DFS to build an index on the local disk.</td>
    </tr>
    <tr>
        <td><h5>lookup.cache-max-disk-size</h5></td>
        <td style="word-wrap: break-word;">unlimited</td>
        <td>MemorySize</td>
        <td>Max disk size for lookup cache, you can use this option to limit the use of local disks.</td>
    </tr>
    <tr>
        <td><h5>lookup.cache-max-memory-size</h5></td>
        <td style="word-wrap: break-word;">256 mb</td>
        <td>MemorySize</td>
        <td>Max memory size for lookup cache.</td>
    </tr>
    </tbody>
</table>

Lookup changelog-producer supports `changelog-producer.row-deduplicate` to avoid generating -U, +U
changelog for the same record.

(Note: Please increase `'execution.checkpointing.max-concurrent-checkpoints'` Flink configuration, this is very
important for performance).

### Full Compaction

If you think the resource consumption of 'lookup' is too large, you can consider using 'full-compaction' changelog producer,
which can decouple data writing and changelog generation, and is more suitable for scenarios with high latency (For example, 10 minutes).

By specifying `'changelog-producer' = 'full-compaction'`, Paimon will compare the results between full compactions and produce the differences as changelog. The latency of changelog is affected by the frequency of full compactions.

By specifying `full-compaction.delta-commits` table property, full compaction will be constantly triggered after delta commits (checkpoints). This is set to 1 by default, so each checkpoint will have a full compression and generate a change log.

{{< img src="/img/changelog-producer-full-compaction.png">}}

{{< hint info >}}

Full compaction changelog producer can produce complete changelog for any type of source. However it is not as efficient as the input changelog producer and the latency to produce changelog might be high.

{{< /hint >}}

Full-compaction changelog-producer supports `changelog-producer.row-deduplicate` to avoid generating -U, +U
changelog for the same record.

(Note: Please increase `'execution.checkpointing.max-concurrent-checkpoints'` Flink configuration, this is very
important for performance).

## Sequence Field

By default, the primary key table determines the merge order according to the input order (the last input record will be the last to merge). However, in distributed computing,
there will be some cases that lead to data disorder. At this time, you can use a time field as `sequence.field`, for example:

{{< tabs "sequence.field" >}}
{{< tab "Flink" >}}
```sql
CREATE TABLE MyTable (
    pk BIGINT PRIMARY KEY NOT ENFORCED,
    v1 DOUBLE,
    v2 BIGINT,
    dt TIMESTAMP
) WITH (
    'sequence.field' = 'dt'
);
```
{{< /tab >}}
{{< /tabs >}}

The record with the largest `sequence.field` value will be the last to merge, regardless of the input order.

**Sequence Auto Padding**:

When the record is updated or deleted, the `sequence.field` must become larger and cannot remain unchanged.
For -U and +U, their sequence-fields must be different. If you cannot meet this requirement, Paimon provides
option to automatically pad the sequence field for you.

1. `'sequence.auto-padding' = 'row-kind-flag'`: If you are using same value for -U and +U, just like "`op_ts`"
(the time that the change was made in the database) in Mysql Binlog. It is recommended to use the automatic
padding for row kind flag, which will automatically distinguish between -U (-D) and +U (+I).

2. Insufficient precision: If the provided `sequence.field` doesn't meet the precision, like a rough second or
millisecond, you can set `sequence.auto-padding` to `second-to-micro` or `millis-to-micro` so that the precision
of sequence number will be made up to microsecond by system.

3. Composite pattern: for example, "second-to-micro,row-kind-flag", first, add the micro to the second, and then
pad the row kind flag.

## Row Kind Field

By default, the primary key table determines the row kind according to the input row. You can also define the 
`'rowkind.field'` to use a field to extract row kind.

The valid row kind string should be `'+I'`, `'-U'`, `'+U'` or `'-D'`.
