---
title: "Sequence & Rowkind"
weight: 6
type: docs
aliases:
- /primary-key-table/sequence-rowkind.html
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
    update_time TIMESTAMP
) WITH (
    'sequence.field' = 'update_time'
);
```
{{< /tab >}}
{{< /tabs >}}

The record with the largest `sequence.field` value will be the last to merge, if the values are the same, the input
order will be used to determine which one is the last one.

You can define multiple fields for `sequence.field`, for example `'update_time,flag'`, multiple fields will be compared in order.

{{< hint info >}}
User defined sequence fields conflict with features such as `first_row` and `first_value`, which may result in unexpected results.
{{< /hint >}}

## Row Kind Field

By default, the primary key table determines the row kind according to the input row. You can also define the
`'rowkind.field'` to use a field to extract row kind.

The valid row kind string should be `'+I'`, `'-U'`, `'+U'` or `'-D'`.
