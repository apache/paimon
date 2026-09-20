---
title: "Database Branches and Tags"
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Database Branches and Tags

Database branches and tags extend Paimon's existing table branches and tags to a group of tables.
The catalog server coordinates the table operations and records database membership and table
versions. Table data stays in the existing Paimon storage layout.

:::info Implementation status

The Java client, database-name selector parser, and REST contracts are implemented. A catalog server
must implement database branch/tag storage, table orchestration, retention, and forward execution.
The stateful client fixture is not a production reference-management server.

:::

## Relationship to table branches and tags

Let `D = /v1/{prefix}/databases/{database}` and `T = D/tables/{table}`. Management operations use
physical database names, without a `$branch_` or `$tag_` suffix.

| Operation | Table REST | Database REST | Request / response |
| --- | --- | --- | --- |
| List branches | `GET T/branches` | `GET D/branches` | Shared `ListBranchesResponse`. |
| Create branch | `POST T/branches` | `POST D/branches` | Shared `CreateBranchRequest`; no response body. |
| Drop branch | `DELETE T/branches/{branch}` | `DELETE D/branches/{branch}` | No request or response body. |
| Forward | `POST T/branches/{branch}/forward` | `POST D/branches/{branch}/forward` | Shared empty `ForwardBranchRequest`; no response body. |
| List tags | `GET T/tags` | `GET D/tags` | Shared `ListTagsResponse` and pagination parameters. |
| Create tag | `POST T/tags` | `POST D/tags` | `CreateDatabaseTagRequest`; no response body. |
| Get tag | `GET T/tags/{tag}` | `GET D/tags/{tag}` | `GetDatabaseTagResponse`. |
| Delete tag | `DELETE T/tags/{tag}` | `DELETE D/tags/{tag}` | No request or response body. |

Successful mutations return HTTP `200` with no body, following the table API. Branch listing returns
names; tag listing is paged and accepts `maxResults`, `pageToken`, and `tagNamePrefix`. There is no
combined reference list or `/trees` resource. Database merge is deferred; there is no merge endpoint,
merge mode, or three-way conflict-resolution contract in this version.

The database tag request retains table tag field names `tagName` and `timeRetained`, and uses
`fromBranch` instead of `snapshotId`. Tables have independent snapshot IDs, so one numeric snapshot
ID cannot identify a database version. Getting a database tag returns its name, source branch and
optional creation/retention metadata. Read its table versions through the existing table APIs.

Branch names and tag names belong to separate namespaces, as with table branches and tags. A branch
and a tag may have the same name. Both names match `[A-Za-z0-9][A-Za-z0-9._-]{0,127}`. Database tag
names are database-wide: the server stores each tag's source and backing table versions, so callers
can read `training$tag_train_v1` without remembering the source branch. Native table tags remain
branch-local; the server maps the database tag to the corresponding table pins.

### Initial server MVP

Start with managed native Paimon tables and a fixed set of logical table names. Create and populate
those tables on `main`. Use batch writers and pause writes during branch creation, tag creation and
forward. Invalidate cached tables and load them again after publication.

Branch-local table creation, deletion and rename require versioned namespace storage and can be
deferred. Format Tables, Object Tables, external tables, views, functions and catalog permissions
are outside this initial versioned-table scope. Unsupported scoped operations return `501`.
These restrictions also apply to operations on main when they would affect retained references:
the absence of a database suffix does not permit deleting storage used by a branch or tag.

## Branch management

### Create a branch without data

```http
POST /v1/catalog/databases/training/branches
Content-Type: application/json

{"branch":"experiment"}
```

Like table `createBranch` without `fromTag`, this copies main's table membership, schemas and
properties, with no table snapshots. It does not copy main's current data. The server must preserve
empty tables explicitly, including their schema-only state.

### Create a branch with data

First capture a database tag, then create a branch from it:

```http
POST /v1/catalog/databases/training/tags
Content-Type: application/json

{"tagName":"baseline"}
```

```http
POST /v1/catalog/databases/training/branches
Content-Type: application/json

{"branch":"experiment","fromTag":"baseline"}
```

`fromTag` names a database tag in the same database. The server restores its table membership,
schemas, properties and snapshots. A missing tag returns `404`; an existing branch returns `409`.
There is no generic `source: {type, name}` object.

### List and drop branches

```http
GET /v1/catalog/databases/training/branches
```

```json
{"branches":["main","experiment"]}
```

```http
DELETE /v1/catalog/databases/training/branches/experiment
```

The delete request has no body. The server protects `main` and rejects its deletion with `400`.
Deleting a branch does not authorize removing files or pins still required by a database tag or
another branch.

### Forward a branch to main

```http
POST /v1/catalog/databases/training/branches/experiment/forward
Content-Type: application/json

{}
```

The path names the **source branch**, following Table REST. The database operation publishes it to
`main`. A tag is not a forward source. To publish a frozen tag, first create a temporary branch from
that tag, then forward that branch.

Forward extends table fast-forward to the database's tables. It publishes source versions on main
and can replace target changes; it does not preserve independently changed target tables using
three-way conflict resolution. The first fixed-table server requires matching membership, including
both logical names and table identities, and a snapshot for each source table, as native table
fast-forward requires a populated source. An empty
source table is a `400`; namespace changes the server cannot handle are a `501`. The server validates
all tables before starting publication. Source `main` is invalid.

Pause both source and main writers while forwarding. Preserve retained tags, keep the two branches
independently writable afterwards, and invalidate/reload main tables before resuming work. No public
multi-table transaction or atomic read view is required for this MVP. A successful response means
all planned table operations finished. Interrupted execution needs recoverable server bookkeeping.

## Tag management

### Freeze a branch

```http
POST /v1/catalog/databases/training/tags
Content-Type: application/json

{"tagName":"train_v1","fromBranch":"experiment","timeRetained":"7d"}
```

Omitting `fromBranch`, or setting it to null, selects `main`. `timeRetained` uses the table tag
retention-duration syntax and is optional. Tags freeze membership, schemas, properties, snapshots,
and the empty state of tables without snapshots. They cannot be moved or updated. Expiring a tag
must respect versions still used by other references; native table pins cannot expire independently
while the database tag is valid.

### Inspect and list tags

```http
GET /v1/catalog/databases/training/tags/train_v1
```

```json
{
  "tagName":"train_v1",
  "fromBranch":"experiment",
  "tagCreateTime":1720000000000,
  "tagTimeRetained":"7d"
}
```

`tagCreateTime` is milliseconds since the Unix epoch. `tagCreateTime` and `tagTimeRetained` are
optional. `fromBranch` records creation provenance; the source can later be deleted without making
the tag unreadable. Table snapshots are resolved using the tag-suffixed database name.

```http
GET /v1/catalog/databases/training/tags?maxResults=100&tagNamePrefix=train_
```

```json
{"tags":["train_v1"],"nextPageToken":"next-page"}
```

Pass the returned token unchanged to get the next page. A missing token ends iteration. An absent
or zero `maxResults` uses the server default. Pagination does not create a frozen cross-page view.

### Delete a tag

```http
DELETE /v1/catalog/databases/training/tags/train_v1
```

The request has no body and does not need an expected reference type: the resource path identifies
a tag. A missing tag returns `404`. Logical deletion and physical cleanup can be separate operations.

## Errors

Use the existing `ErrorResponse` and table branch/tag resource types:

| Situation | HTTP behavior |
| --- | --- |
| Missing database, branch or tag | `404`, with `DATABASE`, `BRANCH` or `TAG` resource details. |
| Creating an existing branch or tag | `409`, with the corresponding resource type and name. |
| Invalid name, protected main mutation, or invalid forward source | `400`. |
| Missing table or snapshot during table orchestration | `404`, identifying the affected resource. |
| Authorization failure | `403`. |
| Unsupported operation or scoped DDL | `501`. |

There is no merge-specific exception translation. Creation conflicts use the same
`AlreadyExistsException` as table branch/tag creation. Errors never cause fallback to a different
branch or to a physical database without its selector.

## Reference-scoped table API

A database name can include exactly one reference selector:

| Database name | Meaning |
| --- | --- |
| `training` | The main database branch, also addressed as `training$branch_main`. |
| `training$branch_experiment` | The writable database branch `experiment`. |
| `training$branch_main` | Explicit selection of the database branch `main`. |
| `training$tag_train_v1` | The immutable database tag `train_v1`. |

The selector is carried in the existing database field, including inside `Identifier`. Encode the
complete database name once as one REST path segment. JSON names remain decoded. For example:

```http
GET /v1/catalog/databases/training%24branch_experiment/tables/features
GET /v1/catalog/databases/training%24tag_train_v1/tables/features
POST /v1/catalog/databases/training%24branch_experiment/tables/features/commit
```

Branch and tag management use `/branches` and `/tags` on the physical database name.
Table access uses the existing table resource paths with the selected database name.

Let `D = /v1/{prefix}/databases/{database}` below, where `database` may carry a reference suffix.
These are the existing operations and request/response structures:

| Method and path | Existing request / response | Scope |
| --- | --- | --- |
| `GET D` | `GetDatabaseResponse`. | Validate the database and selected reference; return virtual database metadata. |
| `GET D/tables` | `ListTablesResponse`; existing paging/filter query parameters. | Table membership of the reference. |
| `GET D/table-details` | `ListTableDetailsResponse`; existing paging/filter query parameters. | Table definitions within the reference. |
| `GET D/tables/{table}` | `GetTableResponse`. | Selected schema, storage options and path. |
| `POST D/tables` | `CreateTableRequest`. | Create a table in a branch. |
| `POST D/tables/{table}` | `AlterTableRequest`. | Alter a table in a branch. |
| `DELETE D/tables/{table}` | Existing drop-table response. | Remove a table from a branch. |
| `GET D/tables/{table}/snapshot` | `GetTableSnapshotResponse`. | Current branch snapshot or pinned tag snapshot. |
| `GET D/tables/{table}/snapshots/{version}` | `GetVersionSnapshotResponse`. | Resolve a version within this reference. |
| `GET D/tables/{table}/snapshots` | `ListSnapshotsResponse`; existing pagination. | Snapshot history visible through this reference. |
| `GET D/tables/{table}/schemas/{version}` | `GetSchemaResponse`. | Resolve a schema ID or `LATEST` within this reference. |
| `GET D/tables/{table}/schemas` | `ListSchemasResponse`; existing pagination. | Schema history retained for this reference. |
| `POST D/tables/{table}/commit` | `CommitTableRequest` / `CommitTableResponse`. | Commit a snapshot to the selected branch. |
| `GET D/tables/{table}/token` | `GetTableTokenResponse`. | Credentials for the resolved table version. |
| `POST D/tables/{table}/auth` | `AuthTableQueryRequest` / `AuthTableQueryResponse`. | Authorize a read of the resolved table. |

`GetTableResponse` retains the requested database name including its suffix and the logical table
name, such as `features`. It carries the resolved schema, path and storage options; the server may
supply a physical branch alias through existing schema options. Request identifiers retain the
same full database name. A commit keeps the existing `tableId`, `baseSnapshotUuid`, `snapshot`, and
`statistics` fields. The path selects the reference; request identifiers and table IDs must agree
with the resolved table.

### Database lookup and naming rules

`GET database` must resolve a suffixed name, because SQL engines can check namespace existence
before accessing a table. The response represents the virtual database and retains its full name.
Database listing returns physical database names only; use `/branches` and `/tags` to discover their names.

CREATE, DROP and ALTER DATABASE do not accept reference suffixes. In particular, dropping a
virtual database must never drop its physical database. Create, delete and forward branches or manage tags through
`/databases/training/branches` and `/databases/training/tags` instead. This does not prevent ordinary create/alter/drop **table**
operations from modifying membership or metadata in a writable branch.

The initial server rejects physical `DROP DATABASE` with `400` while any non-main database branch
or database tag exists, even if main has no tables. Remove those references before dropping the
database. The existing database deletion endpoint does not implicitly cascade through references.

### Table creation, alteration and deletion

Table operations keep their existing request and response structures. Namespace changes require
server-side versioned membership; accepting a suffix in the client does not imply that the server
implements them. A fixed-table server returns `501` for unsupported namespace changes.

| Operation | Required server behavior |
| --- | --- |
| Create a table on a branch | Allocate a new table identity and storage, then add its logical name only to that branch after metadata is ready. Do not expose it on main as a side effect of physical creation. |
| Create a table on main after branching | Add it only to main. Existing branches and tags retain their own membership; a later forward can fail with `501` because the table sets differ. |
| Alter a table's schema or properties | Update the selected branch's backing table and recorded state. Other branches and existing tags retain their own definitions. |
| Drop a table on any branch, including main | Remove only that branch's membership entry. Keep metadata and data required by other branches or tags; do not recursively delete the shared table path. Return `501` if the server cannot preserve those references. |
| Recreate a dropped table with the same name | Allocate a new table identity. A same-name table retained on another branch is a different table and does not satisfy fixed-table forward validation. |
| Create, alter or drop through a tag | Return `403`; tag membership and table definitions are immutable. |

For example, if main and experiment initially contain `features` and `labels`, creating
`training$branch_experiment.samples` adds `samples` only to experiment. Creating `training.metrics`
later adds `metrics` only to main. The fixed-table forward operation cannot publish these different
table sets; it rejects the operation before changing any target table.

### Selector validation

The markers `$branch_` and `$tag_` are case-sensitive reserved syntax. The base database must be
nonblank, and the reference follows the name rules above. Missing names, multiple selectors, or
invalid reference names are rejected rather than interpreted as literal database names. Other
uses of `$`, such as `training$archive`, remain literal. Catalogs adopting this contract must resolve
any pre-existing physical database names containing the reserved markers before enabling it;
lookup must not switch between literal and reference meanings based on which object exists.

Caller-supplied table branch suffixes cannot be combined with a database selector. For example,
`training$branch_a.features$branch_b` is rejected. REST storage commits preserve the original logical
table identifier, including when bare main is mapped to a different physical backing branch after
forward. Explicit Table branch identifiers on an unsuffixed database retain their table selector.

### Branch and tag behavior

For a version-enabled database, both main aliases must resolve through the same mapping, including
after forward replaces its backing table branches. Writes through either alias update that mapping.

A branch resolves to its current membership and table versions. A tag resolves to the membership,
schemas, options and snapshots captured when it was created, even after its source branch advances.
Tag snapshot listing exposes only the pinned snapshot. `LATEST` and `EARLIEST` select that snapshot;
other version selectors must resolve to it or return `404`. Schema reads may access the captured
schema and older schemas retained for reading the captured data, but never later source schemas.
Freezing REST responses alone is insufficient: native readers and system tables can read metadata
directly from storage. A server can return a dedicated frozen metadata branch, with read-only
credentials and no later source snapshots/schemas, through the existing path and branch options.
Another implementation must enforce the same boundary in native reads, including time travel and
schema/system-table access. A default scan option that callers can override does not enforce it.

An empty captured table is still returned by `GET table`; snapshot lookup returns `404` with
`resourceType: SNAPSHOT`.

The server rejects content changes through a tag with `403`. Read authorization remains allowed
through `POST .../auth`; HTTP method alone does not determine whether an operation is a write.
Tag credentials must permit reading without allowing mutation of retained metadata or data.

Missing databases, references and tables return `404`. The selector chooses the branch or tag
namespace: `$branch_train_v1` returns `404` if only a tag with that name exists. Malformed selectors
return `400`. Reserve `409` for already-existing resources, following the table APIs.
Unsupported operations on references return `501`. None of these errors permits retrying the
request against the physical database without its suffix.

### Java table usage

Use the same catalog for ordinary databases and any number of database references:

```java
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;

Identifier experiment = Identifier.create("training$branch_experiment", "features");
Identifier trainingTag = Identifier.create("training$tag_train_v1", "features");

Table experimentFeatures = restCatalog.getTable(experiment);
Table trainingFeatures = restCatalog.getTable(trainingTag);
restCatalog.listTables("training$branch_experiment");
restCatalog.getDatabase("training$tag_train_v1");

// Use experimentFeatures with the ordinary Paimon batch write API.
// Use trainingFeatures with the ordinary Paimon read API.
```

`RESTApi` uses these same identifiers with its existing table methods. `Identifier` already retains
the full database name through serialization and in table loaders; no extra reference fields are
stored in RESTCatalog or RESTCatalogLoader. Subsequent snapshot reads, schema changes, commits,
auth and token requests carry the same database name. Caches keyed by full table identifiers distinguish branches and tags. The two main aliases
(`training` and `training$branch_main`) refer to the same state. The REST catalog cache invalidates
both aliases when a table is altered, dropped or explicitly invalidated through either name.
After forward, invalidate each affected main table in every client cache before loading it again;
invalidating either main alias clears both. A repeated cached getTable call is not a reload.

SQL clients can pass the selector as a quoted database name, using their ordinary identifier
quoting rules. For example:

```sql
SELECT * FROM `training$branch_experiment`.features;
SELECT * FROM `training$tag_train_v1`.features;
```

A REST server implementing virtual database lookup and table resolution is required. There is no
new engine catalog option or reference-switch operation.

Rename, register, replace, rollback, partition/consumer endpoints, nested table branch/tag
management, view writes, functions and table policies do not yet accept database reference suffixes
in the Java client. RESTCatalog validates the virtual database for read-only view probes, then
returns empty lists or a missing view so engine table discovery and DROP TABLE can proceed. Global table listing and lookup by table ID retain their physical-catalog meaning;
they have no database selector. Extending those operations to discover or address references is
additional work. Catalog-level permissions and reference management continue to use physical names.

### Server routing and reuse

Decode the database path segment and parse it with `DatabaseIdentifier.parse(name)`. The result
contains the physical database name and an optional typed `DatabaseReference`. Resolve that
reference and the logical table once into a request context with table identity and backing version,
then reuse the existing table handlers. Validate authentication against the actual request path
and authorize access to the resolved table. Preserve the full requested database name in returned
identifiers so follow-up calls stay on the same reference.

Parsing the suffix does not replace reference management: listing still needs the selected
membership, tags need frozen metadata, and commits must update the selected branch's recorded
table state. The additional request cost is a reference/table mapping lookup, which can be cached;
this addressing scheme does not require proxying or copying table data. The storage and forward work
remains the server orchestration described below.

## Java management usage

`RESTCatalog.treeManagement()` shares the catalog's prefix, authentication and HTTP configuration.
Its operations follow the existing table branch/tag method names:

```java
import org.apache.paimon.PagedList;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.management.TreeManagement;
import org.apache.paimon.rest.responses.GetDatabaseTagResponse;

TreeManagement trees = restCatalog.treeManagement();
trees.createTag("training", "baseline", null, null);
trees.createBranch("training", "experiment", "baseline");

// Use ordinary batch writers on these tables.
restCatalog.getTable(Identifier.create("training$branch_experiment", "features"));

// Stop experiment writes while capturing its training inputs.
trees.createTag("training", "train_v1", "experiment", "7d");
GetDatabaseTagResponse tag = trees.getTag("training", "train_v1");
PagedList<String> page = trees.listTagsPaged("training", 100, null, "train_");
restCatalog.getTable(Identifier.create("training$tag_train_v1", "features"));

// Stop main and experiment writers before publishing the experiment's current state.
trees.fastForward("training", "experiment");
// Invalidate cached main tables and reload them before resuming writers.

trees.dropBranch("training", "experiment");
trees.deleteTag("training", "train_v1");
```

`RESTApi` exposes `listDatabaseBranches`, `createDatabaseBranch`, `dropDatabaseBranch`,
`fastForwardDatabase`, `createDatabaseTag`, `getDatabaseTag`, `listDatabaseTagsPaged`, and
`deleteDatabaseTag`. All management methods take the physical database name. Table access keeps
using the suffix in the ordinary `Identifier`.

## Server implementation using table capabilities

### Metadata and bootstrap

Keep a database branch record with membership and logical-table-to-backing-branch mappings. Keep a
separate database tag record with its source branch, retention metadata, frozen membership and each
table's identity, schema, properties and snapshot (or explicit no-snapshot state).

A version-enabled database starts with `main`. An existing database can be initialized while writers
are stopped. Normal database access and `$branch_main` must use the same record. Accepted table
commits and schema changes update its table state. Direct filesystem writes and unmanaged edits of
service-owned table references bypass this bookkeeping and are outside the MVP.

Backing reference names are owned by the service. Reject collisions with unrelated table branches
or tags. Database and native table naming rules differ: a purely numeric database branch name needs
a valid physical alias because native table branches reject it. Name matching alone is not a safe
way to locate a backing version.

### Create branches

Without `fromTag`, call the schema-only table branch operation for each table on main. With
`fromTag`, resolve each frozen table entry and create its backing branch from the corresponding
native table tag. Paimon's `createBranch(name, tagName)` copies the selected snapshot and its schemas;
`createBranch(name)` copies schemas without data.

Preserve captured schema-only changes newer than the snapshot schema. Empty captured tables need
schema-only branches. Publish the database branch only after all table entries are ready. No data
files need to be copied just to create a branch. Failed setup can leave private work to resume or
clean up.

### Capture tags and retain data

Pause source writers, capture table membership and versions, and pin each populated table snapshot.
Native table tags belong to physical table branches, so store that backing branch with each pin.
Empty tables need frozen schema and no-snapshot metadata because native createTag needs a snapshot.
A minimal server can explicitly reject empty-table tagging until that behavior is implemented.

A database tag must remain readable after source writes, schema evolution, source deletion, and
forward. Provide frozen metadata to native table readers, including system tables and explicit
snapshot reads, as described above. Native automatic retention must not delete service-owned pins
before the database tag expires or is deleted.

Physical cleanup must account for all database references. Native dropBranch deletes its metadata
directory, including its tags. Defer physical cleanup in the first MVP, or relocate retained metadata
before deleting a backing branch. Keep the data files referenced by every retained snapshot.

### Execute forward

1. Resolve the source branch and main. Validate the whole fixed-table membership, including logical
   names and table identities, and all source snapshots before changing target state.
2. Resolve each source table version and prepare the corresponding main table state using native
   table snapshot/schema mechanisms.
3. Preserve database tags before applying native fastForward: that operation can remove target
   metadata and tags. An adapter can instead prepare fresh backing branches and publish their mapping.
4. Publish all target entries, keeping source and main independently writable. Do not point both at
   one mutable table branch. Both main aliases resolve the published mapping.
5. Return success after the planned work completes. Keep an operation record for recovery from a
   partial backend failure, and invalidate/reload client caches before writers resume.

This uses table forward semantics, including replacement of target state. It needs no public hash,
reference ID, multi-table transaction endpoint, merge base or merge-mode API. More advanced merge
semantics will be designed separately.

## Validate the fixed-table MVP

Use two populated tables, `features` and `labels`:

1. Capture main as `baseline`, then create `experiment` from that tag. Verify both tables contain
   baseline data. Separately create a branch without `fromTag` and verify the schemas exist with no data.
2. Write both experiment tables through `$branch_experiment`; main must remain unchanged.
3. Capture `train_v1`, advance the source data and schema, and verify `$tag_train_v1` still reads the
   captured versions, including native time travel and system-table boundaries. Tag writes must fail.
4. Stop writers and forward experiment to main. Invalidate/reload both main aliases and verify the
   published tables. Resume independent writes on main and experiment and verify isolation.
5. Delete the experiment branch and verify the retained tag still reads correctly. Delete unused tags
   and verify that cleanup preserves any versions retained by other branches or tags.

The client tests cover shared Table REST payloads, paths, empty mutation responses, pagination,
authentication, error propagation, serialization and ordinary table reads/writes through suffixes.
Production branch/tag lifecycle, retention and forward still require integration tests against an
implementing catalog server. Branch-local namespace changes and additional table kinds are later work.
