---
title: "Overview"
weight: 1
type: docs
aliases:
- /cdc-ingestion/merge-engin/overview.html
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

# Overview

When Paimon sink receives two or more records with the same primary keys, it will merge them into one record to keep
primary keys unique. By specifying the `merge-engine` table property, users can choose how records are merged together.

{{< hint info >}}
Always set `table.exec.sink.upsert-materialize` to `NONE` in Flink SQL TableConfig, sink upsert-materialize may
result in strange behavior. When the input is out of order, we recommend that you use
[Sequence Field]({{< ref "primary-key-table/sequence-rowkind#sequence-field" >}}) to correct disorder.
{{< /hint >}}

## Deduplicate

The `deduplicate` merge engine is the default merge engine. Paimon will only keep the latest record and throw away
other records with the same primary keys.

Specifically, if the latest record is a `DELETE` record, all records with the same primary keys will be deleted.
You can config `ignore-delete` to ignore it.
