---
title: "Semantic Views"
---

<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Semantic Views

A semantic view is a named catalog object that stores a complete semantic model definition.
The model describes datasets, relationships, dimensions, measures, and related business semantics.
This experimental REST Catalog extension manages the object and its definition. The Java client
preserves the original document text. Servers validate the model formats, syntax versions,
and features they support; consumers provide query execution.

Registration does not imply Spark/Flink execution support or grant access to source data.
This implementation supplies Java clients, wire models, and the
[Catalog OpenAPI contract](/rest-catalog-open-api.yaml). REST server providers must implement
persistence, validation, authorization, and the atomic operations described below.

## REST operations

```text
GET    /v1/{prefix}/databases/{database}/semantic-views
GET    /v1/{prefix}/databases/{database}/semantic-views/{semanticView}
POST   /v1/{prefix}/databases/{database}/semantic-views/{semanticView}
DELETE /v1/{prefix}/databases/{database}/semantic-views/{semanticView}
```

Use the opaque prefix returned by config. Database and object names are independent UTF-8 encoded
path segments, including slashes, percent signs, spaces, Unicode, and dot-only names. Names are
never split on dots, and a catalog name inside model content does not determine the REST prefix.

### Upsert and read

POST creates or atomically replaces one complete definition in an existing database:

```json
{
  "definition": {
    "format": "databricks-yaml",
    "content": "version: '1.1'\nsource: main.sales.orders\nmeasures:\n  - name: revenue\n    expr: SUM(paid_amount)\n"
  },
  "expectedRevision": "r17"
}
```

`format` and `content` are required nonblank strings. `format` identifies both the model syntax
and document encoding, for example `databricks-yaml`, `snowflake-yaml`, or `ossie-yaml`. These are
Paimon format identifiers, not a closed enumeration; each server decides which formats it supports.
The example requires server support for `databricks-yaml`. Model syntax versions and expression
SQL dialects belong inside the content, following the chosen format's specification.
Clients do not parse, normalize, or discard fields in the content.
Content is limited to **1 MiB of UTF-8 bytes**, checked before sending by the Java client.

POST and GET return HTTP 200 with the full object:

```json
{
  "name": "order_metrics",
  "entityName": "sales.order_metrics",
  "definition": {
    "format": "databricks-yaml",
    "content": "version: '1.1'\nsource: main.sales.orders\nmeasures:\n  - name: revenue\n    expr: SUM(paid_amount)\n"
  },
  "revision": "r18"
}
```

`entityName` is a canonical server-generated identity. Use it directly for labels; the dot notation
above is illustrative, not a client-side concatenation rule. `revision` is an opaque concurrency
token, independent of a syntax `version` inside the document. POST returns the committed object.
Subsequent reads must expose the complete committed definition.

Omitting `expectedRevision`, or supplying null, performs unconditional upsert. A nonblank revision
requires an atomic match against an existing object: missing objects return 404 and mismatches
return 409 without changes. Replacement preserves identity, creation metadata, owner, permissions,
and labels. Definition-level comments and synonyms are part of the complete replacement.

The Java client disables automatic replay of conditional POSTs. After a lost response, GET the
object and reconcile its definition and revision before another conditional write. Never silently
remove a revision condition to overcome a conflict. Repeating an unconditional upsert leaves the
same definition state, with the last successful writer taking effect.

### List and delete

List accepts optional `maxResults` (1–1000; omitted uses the server default) and opaque `pageToken`:

```json
{"semanticViews":["order_metrics"],"nextPageToken":"opaque-token"}
```

Lists return visible names only; GET retrieves a full definition. The last page omits the token.
An empty page terminates pagination and must not carry a continuation token. A missing database
returns 404; an existing database with no visible models returns an empty array.

DELETE accepts optional `expectedRevision` **in query parameters**, with no request body. It returns
200 without a body; an absent object returns 404, and a stale revision or blocking dependency returns
409. Revision comparison and deletion must be atomic. Deleting and recreating an object must
produce a different revision, so old requests cannot affect its replacement. Retries retain the
same condition and can report a conflict after an earlier successful deletion.

## Java catalog access

```java
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.management.SemanticViewManagement;
import org.apache.paimon.view.SemanticView;
import org.apache.paimon.view.SemanticViewDefinition;

SemanticViewManagement models = restCatalog.semanticViewManagement();
Identifier id = Identifier.create("sales", "order_metrics");
SemanticViewDefinition definition = new SemanticViewDefinition("databricks-yaml", yamlText);
SemanticView saved = models.upsertSemanticView(id, definition);
SemanticView current = models.getSemanticView(id);
models.upsertSemanticView(id, replacementDefinition, current.getRevision());
models.listSemanticViews("sales"); // Follows all pages.
models.listSemanticViewsPaged("sales", 100, null);
restCatalog.labelManagement().upsertLabel("VIEW", saved.getEntityName(), "domain", "sales");
models.deleteSemanticView(id, models.getSemanticView(id).getRevision());
```

The accessor reuses the catalog's REST client, prefix, authentication, and configured headers.
It is specific to `RESTCatalog`, not the generic `Catalog` interface. `RESTApi` also exposes the same
upsert/get/list/paged-list/delete operations; its read and upsert methods return `GetSemanticViewResponse`.
`SemanticView` does not implement ordinary `View.query()` or `View.rowType()`.

## Server integration contract

- Ordinary SQL views and semantic views share the database's view namespace. Both creation paths
  must reject a cross-type name collision with 409. They cannot implicitly convert each other.
- Existing `/views`, `/view-details`, and global view lists return ordinary SQL views only.
  Existing get/alter/drop SQL view routes treat a semantic view name as a missing ordinary view (404).
  Existing table/function naming rules continue to apply.
- Reuse permission `ResourceType.VIEW` and structured database/view identities. Creation checks
  the parent database's `CREATEVIEW`; replacement checks `ALTER`; deletion checks `DROP`; reads
  check `SELECT`. Discovery follows database `LIST` and object visibility rules. Server resource
  resolution must understand the semantic subtype. Source access is checked separately at execution.
- Labels use `entityType=VIEW` and the returned canonical `entityName`. The label resolver must
  support semantic views. Definition replacement preserves bindings; deletion cleans up direct
  labels and permissions without deleting referenced sources.
- Semantic views count when determining whether a database is empty. Database cascade deletion
  follows existing catalog rules and cleans up semantic metadata and its direct bindings.
- Unsupported model semantics must be rejected without side effects or dropping fields. SQL
  analysis, source validation, dependencies, and execution authorization belong to the server's
  model format adapter. The client exposes no generic executable-validation status.

The HTTP client tests verify wire behavior and error propagation. They do not prove a provider's
storage atomicity, permission enforcement, namespace isolation, or database lifecycle behavior.

## Errors and scope

Errors use `ErrorResponse`; semantic view errors use `resourceType=SEMANTIC_VIEW`.

| HTTP | Meaning | Java client exception |
| --- | --- | --- |
| 400 | Invalid input or unsupported model format, syntax version, or feature | `BadRequestException` |
| 401 / 403 | Authentication or permission failure | `NotAuthorizedException` / `ForbiddenException` |
| 404 | Missing database or model | `NoSuchResourceException` |
| 409 | Name/revision conflict or dependency blocks deletion | `AlreadyExistsException` (existing REST mapping) |
| 413 | Content exceeds the size limit | `RESTException` |
| 501 | Server does not implement semantic views | `NotImplementedException` |

Client-side invalid input throws `IllegalArgumentException` before HTTP. An unsupported endpoint
never falls back to SQL views, labels, or table options. Query compilation, measure rewriting,
materialization, rename, and per-measure or per-field REST resources are outside this version.
