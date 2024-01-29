---
title: "Sequence & Rowkind"
weight: 5
type: docs
aliases:
- /concepts/primary-key-table/sequence-rowkind.html
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

# Sequence and Rowkind

When creating a table, you can specify the `'sequence.field'` by specifying fields to determine the order of updates,
or you can specify the `'rowkind.field'` to determine the changelog kind of record.

## Sequence Field

By default, the primary key table determines the merge order according to the input order (the last input record will be the last to merge). However, in distributed computing,
there will be some cases that lead to data disorder. At this time, you can use a time field as `sequence.field`, for example:

{{< tabs "sequence.field" >}}
{{< tab "Flink" >}}
```sql
CREATE TABLE my_table (
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

(Note: If the provided `sequence.field` doesn't meet the precision, like a rough second or millisecond, system will combine `sequence.field` with an internal incrementing sequence number to ensure that even if the same `sequence.field` appears, the built-in incrementing sequence number can distinguish between different records.)

## Row Kind Field

By default, the primary key table determines the row kind according to the input row. You can also define the
`'rowkind.field'` to use a field to extract row kind.

The valid row kind string should be `'+I'`, `'-U'`, `'+U'` or `'-D'`.
