---
title: "Configuration"
weight: 1
type: docs
aliases:
- /deployment/configuration.html
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

# Configuration

Flink Table Store provides the configuration of LogStore and FileStore for hybrid storage to build dynamic tables.
You can specify the below config options in ‘with’ for the Managed Table to configure the LogStore and FileStore. 

# Common Setup Options

**Table Store Factory**

{{< generated/table_store_factory_configuration >}}

**Log Store**

{{< generated/log_store_configuration >}}
{{< generated/kafka_log_store_configuration >}}

**File Store**

{{< generated/file_store_configuration >}}

**Merge Tree**

{{< generated/merge_tree_configuration >}}
