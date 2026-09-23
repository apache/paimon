---
title: "REST Catalog"
sidebar_position: 5
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

<a id="restcatalog"></a>
<a id="overview"></a>

# REST Catalog

The REST Catalog is a Paimon catalog client that talks to a remote service over HTTP. The service
implements the [REST Catalog API](./rest-api) and owns the backend-specific catalog logic.
Compute engines use the client to discover tables and perform catalog operations.

[![The engine sends catalog requests to a REST service, which manages catalog metadata. The engine reads and writes data files through the storage implementation.](/img/concepts-rest-catalog.svg)](/img/concepts-rest-catalog.svg)

<a id="key-features"></a>

## How It Works

1. The client connects to the service URI and identifies the warehouse to use.
2. The service authenticates requests and handles database, table, and other supported metadata operations.
3. The client obtains table metadata and accesses files through the configured filesystem or object store.
   When supported and enabled, the service can provide temporary data-access credentials.

The catalog service does not need to proxy the contents of every data file. Catalog API access
and storage access are separate parts of the connection. Server capabilities determine which
optional table, permission, policy, and snapshot operations are available.

<a id="token-provider"></a>

## Connect an Engine

Configure `metastore = rest`, the service `uri`, the `warehouse`, and an authentication provider.
For REST Catalog, the warehouse identifies the server-side catalog or instance; use the value
expected by your service rather than assuming it is a filesystem path.

Choose the authentication guide for your service:

| Provider | Guide |
| --- | --- |
| Bearer token (`token.provider = bear`) | [Bearer Token](./bear), including a Flink SQL catalog example |
| Alibaba Cloud DLF (`token.provider = dlf`) | [DLF Token](./dlf), including access keys, STS, and ECS roles |

## Work with Tables and Files

| Task | Guide |
| --- | --- |
| Understand Paimon Tables, Format Tables, and Object Tables | [Tables](./tables) |
| Access files using catalog, database, and table names | [Paimon Virtual Storage](./pvfs) |
| Use the catalog from Java | [REST Java API](../../program-api/rest-api) |

<a id="rest-open-api"></a>

## API References

- [REST Catalog API](./rest-api): the OpenAPI contract for catalog operations.
- [REST Management API](./management-api): permissions, row filters, column masking, and the
  corresponding Spark SQL procedures.

These references describe the client/server contracts. Check that your server implements an
operation before relying on it.
