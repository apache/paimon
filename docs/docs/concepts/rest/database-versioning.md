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

Database references group the versions of several tables under one branch or tag. A typical
training workflow starts an experiment from `main`, writes derived data on the experiment branch,
freezes the inputs under a tag, and merges accepted changes back into `main`.

This page describes the experimental REST reference and table contracts and a proposed server MVP that
reuses Paimon's existing [table branches](../../maintenance/manage-branches) and
[table tags](../../maintenance/manage-tags).

:::info Implementation status

The Java reference-management client, database-name selector parser, and their wire contracts are
implemented. Ordinary table APIs carry the selector in the database name. Reference storage, table-level orchestration, and database merge execution must be
implemented by the catalog server. The server implementation below is a design, not a claim that
an existing service supports it.

Table operations select a database reference through a `$branch_<name>` or `$tag_<name>` suffix on
the database name. The existing table paths and request/response structures are reused. Callers use
ordinary table names without remembering a tag's source branch. No reference header, catalog
option, or separately bound client is needed.

:::

## Scope and terminology

| Term | Meaning |
| --- | --- |
| Database branch | A writable reference to a database's table membership and table versions. |
| Database tag | An immutable reference to a captured membership and versions. Deleting a tag is allowed; moving it is not. |
| Table branch/tag | The existing Paimon storage and read/write mechanism used behind a database reference. |
| Table membership | The names and identities of the tables visible in a database reference. |
| Merge base | The historical state used to distinguish source changes from target changes. It includes previous merge relationships. |

References belong to one database, not the whole catalog. Branches and tags share a name namespace
within that database. A reference contains `type` (`BRANCH` or `TAG`) and `name`. Names match
`[A-Za-z0-9][A-Za-z0-9._-]{0,127}`. Public references have no hash or reference ID.

### Initial server MVP

Start with managed native Paimon tables and a fixed set of logical table names. Create and populate
those tables on `main` before starting the experiment. Use batch writers and pause writes during
branch creation, tag creation, and merge. Resume with freshly loaded tables after publication.

This scope can demonstrate isolated table writes, multi-table training inputs frozen under a tag,
and table-version merge. It does not require a public multi-table transaction API, public hashes,
row-level conflict resolution, or concurrent streaming publication.

Branch-local table creation, deletion, and rename need reference-aware namespace handling. The
merge contract covers table creation and deletion, but the first fixed-table server can defer those
operations until reference-aware namespace storage is implemented. Their scoped REST routes already
reuse the ordinary table request and response schemas; rename is deferred. Format Tables,
Object Tables, external tables, views, functions, and catalog permissions are outside this initial
versioned-table scope.

## Reference management API

All paths use the configured catalog `prefix`. For brevity, the following table uses
`B = /v1/{prefix}/databases/{database}`. Encode each path segment; names in JSON remain unencoded.

| Method and path | Request | Result |
| --- | --- | --- |
| `GET B/trees` | Optional `type`, `maxResults`, and `pageToken` query parameters. | One page of references. |
| `GET B/trees/{name}` | No body. | One reference. |
| `POST B/trees` | New name, type, and an existing source reference. | The created reference. |
| `POST B/trees/{name}/merge` | Source reference and optional merge modes. The path names the target branch. | The target reference after success. |
| `DELETE B/trees/{name}` | Optional expected `type` in the body. | The deleted reference. |

Database merge includes fast-forward when applicable. There is no database-level `/forward`
endpoint. The existing table-level forward API is separate.

### Create a branch or tag

Create an experiment branch from `main`:

```http
POST /v1/catalog/databases/training/trees
Content-Type: application/json

{
  "name": "experiment",
  "type": "BRANCH",
  "source": {"type": "BRANCH", "name": "main"}
}
```

Freeze the experiment under a database tag:

```json
{
  "name": "train_v1",
  "type": "TAG",
  "source": {"type": "BRANCH", "name": "experiment"}
}
```

Both requests use the same path. The source must exist in the same database. A source can be a
branch or an immutable tag; the new reference can also be either type. Successful singular
operations return `DatabaseReferenceResponse`:

```json
{"reference": {"type": "TAG", "name": "train_v1"}}
```

### Inspect and list

```http
GET /v1/catalog/databases/training/trees/train_v1
GET /v1/catalog/databases/training/trees?type=tag&maxResults=100
```

The list filter uses lowercase `branch` or `tag`; JSON reference types use uppercase enum names.
Omitting `type` includes both. A missing or zero `maxResults` uses the server default. Pass the
returned `nextPageToken` unchanged to request the next page; a missing token ends iteration.

```json
{
  "references": [{"type": "TAG", "name": "train_v1"}],
  "nextPageToken": "next-page"
}
```

Getting a reference returns its name and type, not the table membership, source branch, or table
version map. Pagination discovers references; it does not create a frozen view across pages.

### Merge

```http
POST /v1/catalog/databases/training/trees/main/merge
Content-Type: application/json

{
  "source": {"type": "BRANCH", "name": "experiment"},
  "defaultMergeMode": "NORMAL",
  "tableMergeModes": [
    {"table": "features", "mergeMode": "FORCE"},
    {"table": "scratch", "mergeMode": "DROP"}
  ]
}
```

Only `source` is required. Omitting modes gives `NORMAL` for every table. Per-table modes override
the default, and an omitted or empty override list applies the default everywhere. Table names
are exact names within this database. Duplicate table overrides are a bad request; an override
for a table without source-side changes has no effect.

The target is always a branch. A source tag is allowed and remains immutable. The response remains
`DatabaseReferenceResponse`; it does not include a commit hash or a detailed merge report.

### Delete

```http
DELETE /v1/catalog/databases/training/trees/train_v1
Content-Type: application/json

{"type": "TAG"}
```

The optional type checks the reference before deletion. Omitting the body or sending `{}` omits
that check. An absent reference is an error. The MVP server should protect the default `main`
branch. Logical deletion does not authorize deleting table versions still needed by another
reference.

### Errors

| Situation | HTTP behavior |
| --- | --- |
| Missing database or reference | `404`; merge distinguishes the missing source or target in its error details. |
| Creating an existing reference | `409`. |
| Merge target is a tag, no merge base is available, or unresolved table conflicts remain | `409`; the target stays unchanged. |
| Invalid merge request, such as duplicate per-table modes | `400`. |
| Deleting a protected default branch or supplying the wrong expected type | `409`. |
| Server does not implement an operation | No client fallback; the server error is propagated. |

Errors use `ErrorResponse`. The Java merge client converts `409` to `MergeConflictException` and
preserves the resource type/name, message, request ID, and cause. Resource creation still uses
`AlreadyExistsException`. See the [OpenAPI specification](/rest-catalog-open-api.yaml) for the
individual operations and their documented responses.

## Reference-scoped table API

A database name can include exactly one reference selector:

| Database name | Meaning |
| --- | --- |
| `training` | The ordinary physical database, with its existing main-table behavior. |
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

There are no additional table routes below `/trees/{reference}`. `/trees` remains the reference
management resource and always takes the physical database name, such as `training`.

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
Database listing returns physical database names only; use `/trees` to discover branches and tags.

CREATE, DROP and ALTER DATABASE do not accept reference suffixes. In particular, dropping a
virtual database must never drop its physical database. Create, delete and merge references through
`/databases/training/trees` instead. This does not prevent ordinary create/alter/drop **table**
operations from modifying membership or metadata in a writable branch.

The markers `$branch_` and `$tag_` are case-sensitive reserved syntax. The base database must be
nonblank, and the reference follows the name rules above. Missing names, multiple selectors, or
invalid reference names are rejected rather than interpreted as literal database names. Other
uses of `$`, such as `training$archive`, remain literal. Catalogs adopting this contract must resolve
any pre-existing physical database names containing the reserved markers before enabling it;
lookup must not switch between literal and reference meanings based on which object exists.

Caller-supplied table branch suffixes cannot be combined with a database selector. For example,
`training$branch_a.features$branch_b` is rejected. Storage commits can supply a physical table branch
internally; RESTCatalog removes that internal table suffix while preserving the database selector.

### Branch and tag behavior

A branch resolves to its current membership and table versions. A tag resolves to the membership,
schemas, options and snapshots captured when it was created, even after its source branch advances.
Tag snapshot listing exposes only the pinned snapshot. `LATEST` and `EARLIEST` select that snapshot;
other version selectors must resolve to it or return `404`. Schema reads may access the captured
schema and older schemas retained for reading the captured data, but never later source schemas.
An empty captured table is still returned by `GET table`; snapshot lookup returns `404` with
`resourceType: SNAPSHOT`.

The server rejects content changes through a tag with `409`. Read authorization remains allowed
through `POST .../auth`; HTTP method alone does not determine whether an operation is a write.
Tag credentials must permit reading without allowing mutation of retained metadata or data.

Missing databases, references and tables return `404`. A selector whose type does not match the
reference, such as `$branch_train_v1` for a tag, returns `409`. Malformed selectors return `400`.
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
auth and token requests carry the same database name. Caches keyed by full table identifiers
naturally distinguish the physical database, branches and tags.

SQL clients can pass the selector as a quoted database name, using their ordinary identifier
quoting rules. For example:

```sql
SELECT * FROM `training$branch_experiment`.features;
SELECT * FROM `training$tag_train_v1`.features;
```

A REST server implementing virtual database lookup and table resolution is required. There is no
new engine catalog option or reference-switch operation.

Rename, register, replace, rollback, partition/consumer endpoints, nested table branch/tag
management, views, functions and table policies do not yet accept database reference suffixes in
the Java client. Global table listing and lookup by table ID retain their physical-catalog meaning;
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
this addressing scheme does not require proxying or copying table data. The storage and merge work
remains the server orchestration described below.

## Java management usage

Obtain tree management from an already configured `RESTCatalog`. It shares that catalog's
prefix, authentication, and HTTP configuration:

```java
import org.apache.paimon.PagedList;
import org.apache.paimon.management.TreeManagement;
import org.apache.paimon.rest.DatabaseReference;
import org.apache.paimon.rest.DatabaseReferenceType;
import org.apache.paimon.rest.MergeMode;
import org.apache.paimon.rest.TableMergeMode;

import java.util.Collections;

TreeManagement trees = restCatalog.treeManagement();
DatabaseReference main = new DatabaseReference(DatabaseReferenceType.BRANCH, "main");
DatabaseReference experiment = trees.createReference(
        "training", "experiment", DatabaseReferenceType.BRANCH, main);

// Run batch writes on the corresponding table branches before freezing this tag.
DatabaseReference trainingTag = trees.createReference(
        "training", "train_v1", DatabaseReferenceType.TAG, experiment);

PagedList<DatabaseReference> page = trees.listReferencesPaged(
        "training", DatabaseReferenceType.TAG, 100, null);

// Default three-way merge, failing on conflicting table versions.
trees.mergeBranch("training", "main", trainingTag);
```

When resolving a conflict, use the following call instead of the default merge to accept the
source version of `features`. Changing modes after a successful merge does not reapply that source:

```java
trees.mergeBranch(
        "training", "main", trainingTag, MergeMode.NORMAL,
        Collections.singletonList(new TableMergeMode("features", MergeMode.FORCE)));
```

`RESTApi` exposes equivalent methods: `listDatabaseReferencesPaged`, `getDatabaseReference`,
`createDatabaseReference`, `mergeDatabaseBranch`, and `deleteDatabaseReference`. Listing is paged;
there is no non-paged database-reference helper.

These are management calls. Creating a database branch does not switch the catalog's ordinary
table operations to that branch.

## Reusing table branches and tags on the server

The server coordinates existing table-level operations and keeps database metadata around them.
An illustrative mapping is:

```text
training / main [BRANCH]
  features -> table identity A, table branch main
  labels   -> table identity B, table branch main

training / experiment [BRANCH]
  features -> table identity A, table branch experiment
  labels   -> table identity B, table branch experiment

training / train_v1 [TAG]
  features -> table identity A, experiment branch, pinned table tag train_v1
  labels   -> table identity B, experiment branch, pinned table tag train_v1
```

Names such as `experiment` can also name the corresponding backing table
branches, and `train_v1` can name each table tag in its source table branch. These names are owned
by the service. Reject collisions with unrelated existing table references; do not adopt them just
because the names match. Additional internal baseline tags can use private, service-generated names.

The public database-reference name rules and native table-branch rules are not identical. For
example, the database protocol permits a purely numeric name, while native table branch creation
rejects it. A server supporting the full name contract needs an alias mapping to valid physical
branch names and must resolve the scoped logical table address through that mapping. It must not
silently narrow the database API's name rules. The examples use names valid in both layers.

### Minimal metadata

The server needs:

- A database reference record: name, type, current membership, and internal ancestry/merge history.
- A mapping from logical table name to stable table identity and backing table branch or tag.
- Captured table versions at branch points, tag creation, and merges, including the schema and
  snapshot state needed for comparisons and reads.

A captured version can reuse a snapshot UUID, a pinned schema, and relevant table properties.
Empty tables need an explicit no-snapshot state. Numeric snapshot/schema IDs alone are not enough
to compare independently written branches. Table identity distinguishes a dropped-and-recreated
table from its predecessor. Copying metadata to a new physical branch does not itself constitute a
logical table change.

These records can live in the catalog backend. Their internal identities are not public hashes and
need not introduce a new versioned storage engine. The server must update its recorded table state
when a managed branch accepts a table commit or schema change; names alone cannot support merge.
Use the server's table commit and schema operations for those writes. Uncoordinated filesystem
writes or direct edits of service-owned table references would bypass this bookkeeping.

### Bootstrap main

A version-enabled new database starts with an empty `main` branch. Otherwise every create-reference
request would require a source that does not yet exist. For an existing database, the server can
initialize `main` from its current tables while writers are stopped. Automatic online conversion of
an actively written database is outside the first MVP.

This is a server lifecycle rule, not an additional REST endpoint. Reference creation always keeps
its existing `source` field.

### Create a database branch

1. Capture the source membership and each selected table version while writes are paused.
2. For a populated table, pin the selected source snapshot with a service-owned table tag and create
   the destination table branch from that tag.
3. For an empty table, create a schema-only table branch. Preserve the selected schema and properties.
4. Record the common baseline and publish the database branch after all table branches are ready.

The existing `FileSystemBranchManager.createBranch(name)` creates an empty branch by copying
schemas. It does not clone the source data. `createBranch(name, tagName)` copies the selected
snapshot and its schemas. If the captured current schema is newer than the snapshot's schema,
the server must also preserve that schema-only change; snapshot cloning alone is insufficient.

No data files need to be copied merely to create a branch. In the single-process MVP, table-level
setup can run sequentially; do not expose an incomplete database reference as successfully created.
Failures can leave private work to clean up or resume.

### Create and retain a database tag

Capture the table membership and pin a table tag for each populated table. Persist the source table
branch with each pin: native Paimon tags belong to a table branch, not a database-wide directory.
An empty table has no snapshot to tag, so its frozen entry must retain the schema and empty state;
the server cannot blindly call `createTag` on every table.

A database tag never follows subsequent writes to its source. For an empty tagged table, reads must
remain empty even if the source later receives its first snapshot. The source's later schema
changes must also leave the tagged schema unchanged. A demonstration server that has not implemented
empty-table reads must restrict tagging to populated tables explicitly.

Service-owned pins must not expire through ordinary automatic tag-retention settings or be replaced
through user table-tag operations. Table branch deletion removes its metadata directory, including
the tags in that directory. Keep a backing branch while a database tag or merge baseline still needs
it, or relocate the retained metadata before deleting it.

The first MVP can defer physical deletion and cleanup. Removing a logical database reference need
not immediately drop its underlying table branches or files. Enable physical cleanup only when it
accounts for all retained database references and merge baselines.

## Merge semantics and execution

Merge operates on complete table versions, including schema, properties, and snapshot state. It
also defines how table presence or absence is combined once branch-aware DDL is available.

Let `B`, `S`, and `T` be a table's base, source, and target version, with absence represented as a
state. First determine whether the source changed relative to `B`:

| Condition or mode | Result |
| --- | --- |
| `S = B` | Keep `T`, including target-only changes. |
| Source changed, mode `DROP` | Keep `T`, even when there would be no conflict. |
| Source changed, mode `FORCE` | Use `S`, including source-side deletion. |
| Source changed, mode `NORMAL`, and `T = B` | Use `S`. |
| Source changed, mode `NORMAL`, and `S = T` | Accept the identical result. |
| Source changed, mode `NORMAL`, and both sides changed differently | Fail the merge with `409`. |

Different tables can therefore change independently and merge successfully. Different versions of
the same table conflict under `NORMAL`, even when an application might know how to combine their
rows. `FORCE` selects a complete source version; `DROP` skips all source changes to the selected
table, not just conflicting changes.

### Publication

1. Resolve the source and target and find their merge base, including earlier merges.
2. Compare table versions and compute the complete result using the selected modes.
3. If any unresolved conflict exists, return `409` before changing target tables.
4. Prepare the selected target table versions with table-level snapshot/schema mechanisms and publish
   the database result. Record the source as merged; never modify the source reference.

When the target is an ancestor of the source, fast-forward is possible only if the chosen modes
produce exactly the source state. Already-merged sources and identical reference states succeed
without changing the target. Divergent histories use three-way merge.

The existing table `mergeBranch` implementation merges append-only data-file changes. It is not an
implementation of this whole-table-version algorithm. Existing table `fastForward` also has its own
replacement semantics and can remove target metadata and tags. A server needs an adapter that
checks the database result first and preserves retained references; looping over either operation
without that adapter is insufficient.

Preparing fresh backing branches and publishing a new mapping is one possible server implementation.
The server can update the reference mapping to those prepared versions while retaining any
physical branches needed by tags or merge baselines. The client-visible table address must continue
to resolve correctly. Source and target must remain independently writable: pointing both at the
same mutable table branch would make future source writes modify the target as well.

### Repeated merge

After a successful merge, the server records the integrated source version even if `DROP` preserved
all target table contents. Repeating a merge of that same source state must not bring skipped
changes back. A later source write can participate in a subsequent merge using the updated history.

Do not identify prior merges only by the source branch name; that branch can continue to advance.
Internal ancestry is required even though the public API has no hash. The first MVP can serialize
these operations and pause writers instead of introducing public concurrency tokens or multi-table
transactions. Reads during a multi-table publication need not provide an atomic database view in
this restricted MVP. Partial backend execution still needs a recoverable server operation record;
an HTTP success must mean that the planned result is installed.

## Exercise the fixed-table MVP

The following workflow requires a server that implements the orchestration above. The client tests
exercise suffix-based HTTP addressing and table loaders; they do not implement database reference
storage or the database merge algorithm.

1. Create database `training` and two populated managed tables, `features` and `labels`, on `main`.
   Stop writes and create database branch `experiment` from `main` using tree management.
2. List and load `features` and `labels` from database `training$branch_experiment`, then write
   experiment data with the usual batch write API. Their metadata reads and commits use the
   existing table paths with the complete suffixed database name.
3. Stop experiment writes and create database tag `train_v1` from `experiment`. Use the same
   catalog to access database `training$tag_train_v1`. Load the same logical table names for training;
   the service resolves the pinned table versions without a source-branch hint.
4. Advance the experiment tables, then reload and read them through the tag-suffixed database name.
   The tagged data and schemas must remain unchanged. Verify that writes through the tag are rejected.
5. With main and experiment writers stopped, merge `train_v1` into `main`. This publishes the
   evaluated source version. Merging the live `experiment` branch would instead include its newer
   state. If both sides changed a table, choose a per-table merge mode when appropriate.
6. Reload main tables and verify the published state. Resume writes separately on `main` and
   `experiment` and verify that neither changes the other. Merge the same source state again to
   check no-op behavior. Delete unused database references through tree management.

## Beyond the fixed-table MVP

The database name suffix now identifies the selected database view for database lookup, table listing,
reads, commits and the ordinary create/alter/drop endpoints. A complete server namespace still
needs branch-local membership changes, stable identities across rename, and new identities for
drop-and-recreate. The fixed-table server may return `501` for unsupported scoped DDL.

Global table IDs, global listings, rename and the other deferred endpoints need explicit reference
semantics before they can be extended. Engine integrations must preserve the full database name in
identifiers and perform database existence checks through the catalog. These additions do not
require callers to construct per-table branch names.

## Validation and implementation sequence

The reference tests validate HTTP paths, request bodies, authentication/configuration, pagination,
JSON compatibility, exception propagation and suffix preservation through serialized tables and
catalog loaders. Tests cover virtual database lookup, mutation guards and malformed or mixed
selectors. The OpenAPI validator checks that reference access uses the ordinary table paths. A stateful test fixture also uses real Paimon data files
to exercise batch writes on separate branches, frozen tag reads after source writes, and tag write
rejection. This validates client integration with a resolving server; production reference
lifecycle, snapshot retention and database merge still require server integration tests.

Implement and verify in this order:

1. **Reference records and bootstrap:** create `main`, list/get/create/delete references, and protect
   managed table-reference names.
2. **Table orchestration:** clone populated and empty tables correctly; record baselines; route
   scoped logical table names through existing Paimon readers and writers.
3. **Frozen training inputs:** pin table tags and schemas, validate repeated reads after source
   writes, and retain dependencies after logical reference deletion. Add empty-table coverage when
   that case is enabled.
4. **Merge:** verify automatic fast-forward, independent changes to different tables, same-table
   conflicts leaving the target unchanged, all three modes, repeated merge including `DROP`, and
   continued independent writes after merge.
5. **Complete database views:** implement branch-local DDL storage and verify membership changes,
   then extend the scoped protocol to the deferred operations as needed.

A useful acceptance test uses real Paimon snapshots for two tables and exercises the workflow above
against a stateful server. Passing that test establishes the fixed-table MVP; a full database-view
MVP additionally requires the final namespace step.
