---
title: "REST API"
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

# REST API

The [REST Catalog OpenAPI specification](/rest-catalog-open-api.yaml) defines the language-neutral
wire contract for catalog servers and clients. Open the YAML specification to inspect request and
response schemas, generate SDK models, or validate an implementation.

For client configuration, start with the [REST Catalog overview](./). For privilege grants, row
filters, and column masks, use the separate [REST Management API](./management-api).

## Connect to a Catalog

1. Configure the service URI and authentication using a [Bearer token](./bear) or [DLF credentials](./dlf).
2. Call `GET /v1/config`, with the `warehouse` query parameter when selecting a catalog instance.
3. Merge server `defaults`, client properties, and server `overrides`, in that order. Later values
   take precedence.
4. Use the resulting `prefix` to address catalog resources. Treat it as an opaque value; it is
   independent of the local catalog alias used by Flink or Spark.

The paths below are relative to the configured service URI. Request parameters, pagination,
payloads, and error responses are defined in the OpenAPI specification.

## Find an Operation

| Resource | Operations | Path family |
| --- | --- | --- |
| Configuration | Discover defaults and overrides. | `/v1/config` |
| Databases | List, create, load, alter, and drop. | `/v1/{prefix}/databases` |
| Tables | List, create, register, load, alter, drop, and rename. | Database-scoped `tables`; catalog-scoped `tables`, `tables/id/{tableId}`, and `tables/rename`. |
| Commits and snapshots | Commit, roll back, and inspect table versions. | Table-scoped `commit`, `rollback`, `rollback-schema`, `snapshot`, and `snapshots`. |
| Data access | Request storage credentials and authorize a query. | Table-scoped `token` and `auth`. |
| Partitions | List, create, drop, and mark partitions done. | Table-scoped `partitions`. |
| Branches and tags | Manage named histories and retained snapshots. | Table-scoped `branches` and `tags`. |
| Consumers | List and reset streaming consumer progress. | Table-scoped `consumers`. |
| Views and functions | Manage reusable SQL and function definitions. | Database- and catalog-scoped `views` and `functions`. |

In this table, **table-scoped** means
`/v1/{prefix}/databases/{database}/tables/{table}`. Catalog-wide listing and detail-listing
endpoints are described in the specification alongside their database-scoped counterparts.

## Partition Compatibility

Partition options use the existing `POST .../partitions` request. `partitionOptions` follows the
order of `partitionSpecs`; use `{}` when a partition has no options. Custom locations use the
`path` option. Before registering custom locations, ensure that the REST server supports partition
options and all readers support custom locations.

For an existing Format Table partition, omitting `path` keeps its location. Naming the partition's
own default directory under the table asks the server to put it back there: the stored location is
dropped, no data is deleted, and the request needs `replaceStatistics=true` with a
`partitionStatistics` entry for the same spec. Any other path under the table location stays
invalid, so a server that does not implement this rejects the request rather than storing it. A
server also rejects additive statistics for a partition that already has a custom location.
