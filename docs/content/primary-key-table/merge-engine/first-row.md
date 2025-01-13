---
title: "First Row"
weight: 4
type: docs
aliases:
- /primary-key-table/merge-engin/first-row.html
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

# First Row

By specifying `'merge-engine' = 'first-row'`, users can keep the first row of the same primary key. It differs from the
`deduplicate` merge engine that in the `first-row` merge engine, it will generate insert only changelog.

{{< hint info >}}
`first-row` merge engine only supports `none` and `lookup` changelog producer. 
For streaming queries must be used with the `lookup` [changelog producer]({{< ref "primary-key-table/changelog-producer" >}}).
{{< /hint >}}

{{< hint info >}}
1. You can not specify [sequence.field]({{< ref "primary-key-table/sequence-rowkind#sequence-field" >}}).
2. Not accept `DELETE` and `UPDATE_BEFORE` message. You can config `ignore-delete` to ignore these two kinds records.
3. Visibility guarantee: Tables with First Row engine, the files with level 0 will only be visible after compaction.
   So by default, compaction is synchronous, and if asynchronous is turned on, there may be delays in the data.
{{< /hint >}}

This is of great help in replacing log deduplication in streaming computation.
