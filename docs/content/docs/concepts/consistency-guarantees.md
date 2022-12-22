---
title: "Consistency Guarantees"
weight: 5
type: docs
aliases:
- /concepts/file-layouts.html
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

# Consistency Guarantees

Table Store writers uses two-phase commit protocol to atomically commit a batch of records to the table. Each commit produces at most two [snapshots]({{< ref "docs/concepts/basic-concepts#snapshot" >}}) at commit time.

For any two writers modifying a table at the same time, as long as they do not modify the same bucket, their commits are serializable. If they modify the same bucket, only snapshot isolation is guaranteed. That is, the final table state may be a mix of the two commits, but no changes are lost.
