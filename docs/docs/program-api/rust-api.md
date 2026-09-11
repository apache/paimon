---
title: "Rust API"
sidebar_position: 9
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

# Rust API

[Paimon Rust](https://github.com/apache/paimon-rust) provides native access to the Paimon table format
from Rust applications. It has its own release cycle and dependency configuration.

## Get started

Follow the [Rust getting-started guide](https://paimon.apache.org/docs/rust/getting-started/) to add
the crate, select storage features, create a catalog, and read a table. Use the crate version and
feature flags from that guide for your chosen release.

## Choose a guide

| Task | Documentation |
| --- | --- |
| Install the crate and read a table | [Getting Started](https://paimon.apache.org/docs/rust/getting-started/) |
| Explore supported features and integrations | [Rust Documentation](https://paimon.apache.org/docs/rust/) |
| Build from source or contribute | [Paimon Rust Repository](https://github.com/apache/paimon-rust) |

Catalog and table concepts are shared across implementations. See [Catalogs](../concepts/catalog)
and the [Storage Specification](../concepts/spec/) for the format, then check the Rust documentation
for the APIs and features supported by the version you use.
