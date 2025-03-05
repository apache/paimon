---
title: "Upgrade Compatibility"
weight: 1
type: docs
aliases:
- /migration/upgrade-compatibility.html
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

# Upgrade Compatibility

This page will introduce the compatibility issues when upgrading Paimon to a newer version.


| Compatibility Issue                                                   | Issue Link                                   | Introduced Version | Affected Version | Affected Scope                                               | Need Manual Fix | Fix Procedure                                                                                              |
|-----------------------------------------------------------------------|----------------------------------------------|:------------------:|:----------------:|--------------------------------------------------------------|-----------------|------------------------------------------------------------------------------------------------------------|
| Incompatible CommitMessage Serializer/Deserializer                    | https://github.com/apache/paimon/issues/3367 |       < 0.8        |    \>= 0.8.1     | Flink Engine, streaming mode                                 | No              | Paimon will automatically fallback to legacy serializer to resolve this issue.                             |
| Fix the timezone conversion for timestamp_ltz data_type in Orc Format | https://github.com/apache/paimon/issues/5066 |       < 1.1        |     \>= 1.1      | Orc Format table including fields of timestamp_ltz data_type | Yes             | When reading legacy orc format table, user should manually enable `orc.timestamp-ltz.legacy.type` as true. |
