---
title: "REST Management API"
hide_table_of_contents: true
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

The REST Management API is an experimental OpenAPI 3.1 control-plane extension for object
privileges, row filters, and column masks in a Paimon REST Catalog. Its current contract version is
`1.0` and may evolve incompatibly while the design is being validated.

`RESTCatalog` exposes `permissionManagement()` and `policyManagement()` directly. These methods are
intentionally not part of the generic `Catalog` interface. Other catalog implementations do not
expose this management contract.

## Catalog addressing

All management endpoints use the opaque `prefix` returned by the REST Catalog config endpoint. It
is not a catalog name in a payload and is independent of local engine catalog aliases.

```
GET  /v1/{prefix}/permissions
POST /v1/{prefix}/permissions/grant
POST /v1/{prefix}/permissions/revoke

GET    /v1/{prefix}/databases/{database}/tables/{table}/policies
POST   /v1/{prefix}/databases/{database}/tables/{table}/policies
POST   /v1/{prefix}/databases/{database}/tables/{table}/policies/drop
```

Policies are currently attached only to tables. The path is the attachment identity, so policy
request bodies do not repeat a catalog, database, table, or resource type. Catalog- and
database-level matching can be added later with explicit matching semantics instead of implied
path inheritance.

The complete wire contract is available in
[`rest-management-open-api.yaml`](/rest-management-open-api.yaml).

## Privileges and policies are independent

A permission grants one access on one resource to one principal. A data policy restricts rows or
columns visible through an already-authorized read. Creating a policy never grants `SELECT`, and
revoking `SELECT` does not delete policies.

This separation also defines the expected query path:

1. The server evaluates object privileges.
2. The server resolves all row-filter and column-masking policies applicable to the caller.
3. The existing REST Catalog table authorization endpoint returns the stored Paimon predicate and
   column transforms to the engine.
4. The engine applies those restrictions when planning the scan.

Management payloads use the same serialized Paimon `Predicate` and `Transform` representation as
the existing `AuthTableQueryResponse`. Policy conflict detection, schema validation, and principal
resolution are server responsibilities.

## Permission model

Permission resources are structured objects:

| Resource type | Required locator | Example |
| --- | --- | --- |
| `CATALOG` | none | `{"type":"CATALOG"}` |
| `CATALOG_ALL` | none | `{"type":"CATALOG_ALL"}` |
| `DATABASE` | `database` | `{"type":"DATABASE","database":"sales"}` |
| `DATABASE_ALL` | `database` | `{"type":"DATABASE_ALL","database":"sales"}` |
| `TABLE` | `database`, `table` | `{"type":"TABLE","database":"sales","table":"orders"}` |
| `COLUMN` | `database`, `table` | `{"type":"COLUMN","database":"sales","table":"orders"}` |
| `FUNCTION` | `database`, `function` | `{"type":"FUNCTION","database":"sales","function":"calculate_tax"}` |
| `VIEW` | `database`, `view` | `{"type":"VIEW","database":"sales","view":"daily_orders"}` |

Principals are opaque, canonical strings that are globally unique in the server namespace. Their
format is server-defined and may encode a user, group, role, or service identity, for example
`role:analyst` or an external identity-provider ARN. Principal type and membership resolution are
server responsibilities. Access values are limited to 32 characters and principals to 128
characters. An implementation may resolve wire locators and principals to different stable
persistence identifiers; those internal ids are not exposed by this API.

The built-in accesses use a common data-authorization vocabulary. Creation accesses intentionally
use their persisted names without underscores:

| Access | Meaning |
| --- | --- |
| `ALL` | All accesses applicable to the resource. |
| `CREATEDATABASE` | Create a database in a catalog. |
| `DESCRIBE` | Read database metadata or select the current database. |
| `ALTER` | Modify resource metadata. |
| `DROP` | Drop the resource. |
| `CREATETABLE` | Create a table in a database. |
| `CREATEFUNCTION` | Create a function in a database. |
| `CREATEVIEW` | Create a view in a database. |
| `LIST` | List resources in a database. |
| `SELECT` | Read table or view data, or use a function. |
| `UPDATE` | Write table data, including insert, update, and delete operations. |
| `GRANT` | Grant or revoke assignments on the resource. |

Java helpers accept access names case-insensitively and normalize them before sending. The REST
wire format uses upper case. Built-in accesses are resource-specific:

| Resource | Accesses |
| --- | --- |
| `CATALOG` | `ALL`, `ALTER`, `DROP`, `GRANT`, `CREATEDATABASE` |
| `CATALOG_ALL` | `ALL`, `DESCRIBE`, `ALTER`, `DROP`, `GRANT`, `CREATETABLE`, `CREATEVIEW`, `CREATEFUNCTION`, `LIST`, `SELECT`, `UPDATE` |
| `DATABASE` | `ALL`, `DESCRIBE`, `ALTER`, `DROP`, `GRANT`, `CREATETABLE`, `CREATEVIEW`, `CREATEFUNCTION`, `LIST` |
| `DATABASE_ALL` | `ALL`, `SELECT`, `UPDATE`, `ALTER`, `DROP`, `GRANT` |
| `TABLE` | `ALL`, `SELECT`, `UPDATE`, `ALTER`, `DROP`, `GRANT` |
| `COLUMN` | `SELECT` |
| `VIEW` | `ALL`, `SELECT`, `ALTER`, `DROP`, `GRANT` |
| `FUNCTION` | `ALL`, `SELECT`, `ALTER`, `DROP`, `GRANT` |

An assignment identity is `resource`, `access`, and `principal`. Granting the same identity replaces
its expiry, and revocation is idempotent. `CATALOG`, `DATABASE`, `TABLE`, `COLUMN`, `VIEW`, and
`FUNCTION` apply only to the exact referenced resource. `CATALOG_ALL` is an explicit scope over the
configured catalog's database, table, view, and function descendants; `DATABASE_ALL` is an explicit
scope over the named database's table, view, and function descendants. These scope assignments also
apply to descendants created later. They remain direct assignments in listing responses; the server
does not synthesize inherited assignments. Resolving group membership and role inheritance remains a
server responsibility.

### Column permissions

A column permission uses a `COLUMN` resource whose locator is the containing table, `SELECT`
access, and one `columns` object. Exactly one non-empty list is allowed:

- `columnNames` is an allowlist. Only the named top-level columns are readable.
- `excludedColumnNames` is a denylist. Every current top-level column except the named columns is
  readable.

For example, this assignment allows only `order_id` and `region`:

```json
{
  "resource": {
    "type": "COLUMN",
    "database": "sales",
    "table": "orders"
  },
  "access": "SELECT",
  "principal": "role:analyst",
  "columns": {
    "columnNames": ["order_id", "region"]
  }
}
```

The assignment identity remains `(resource, access, principal)`; `columns` is not part of the
identity. Granting the same identity replaces the entire previous allowlist or denylist rather than
merging individual names. Revocation therefore omits `columns` and removes the whole column
assignment.

All named columns must exist when granted, and the table must enforce query authorization. A server
may enable `query-auth.enabled` atomically with the grant; otherwise it must reject the grant. Column
names refer only to top-level fields. For every effective caller principal, applicable column ranges
are intersected. If any applicable range rejects a selected column, the query fails rather than
silently dropping that column.

Schema evolution keeps the assignment attached to the stable table identity. Renaming a referenced
column updates its stored name. Dropping a referenced column removes it from the range. The server
must reject a schema change that would leave an allowlist empty because removing that assignment
would widen access; an empty denylist is equivalent to no column restriction, so that assignment is
removed. An allowlist denies columns added later, while a denylist allows them, so allowlists are
safer when new columns may contain sensitive data.

`expireTime`, when present, is an exclusive upper bound evaluated against the REST server clock.
At `now >= expireTime`, the assignment must not authorize access. Expired direct assignments may
remain visible in listings until server cleanup. Timestamps must not be more precise than
milliseconds; the wire value uses UTC `Z` and contains at most three fractional digits.

Resource objects in this API are wire locators, not persistence identities. Servers must bind direct
assignments to a stable internal resource identity: renaming a database, table, function, or view
retains its assignments and subsequent responses use the new locator; dropping it removes its direct
assignments; recreating the same locator does not restore them.

## Data policy model

A data policy is attached directly to one table and one principal. It applies whenever that
principal is effective for the caller after the server resolves group and role membership. A
principal can have at most one row filter on a table and at most one column mask on each table
column. A row-filter identity is `(table, ROW_FILTER, principal)`; a column-mask identity is
`(table, COLUMN_MASKING, principal, onColumn)`.

Each policy contains exactly one typed definition:

| Definition | Required fields | Result |
| --- | --- | --- |
| `rowFilter` | `predicate` | One serialized Paimon `Predicate`, applied to every scan. |
| `columnMask` | `onColumn`, `transform` | One serialized Paimon `Transform` whose result replaces the protected column. |

The common field is one `principal`. `rowFilter.predicate` maps directly to one entry in
`AuthTableQueryResponse.filter`. `columnMask.onColumn` and `columnMask.transform` map directly to
one key and value in `AuthTableQueryResponse.columnMasking`. Each JSON value is limited to 60 KiB
in UTF-8. This is Paimon's versioned serialization format rather than SQL text or a portable policy
DSL; clients and servers must use compatible Paimon versions.

Policy creation must be rejected unless all of these conditions hold:

1. The target database and table exist.
2. The table has `query-auth.enabled=true`; otherwise a stored policy could be silently bypassed.
3. The referenced principal exists.
4. The predicate or transform is recognized by the server, deserializes to a non-null Paimon
   object, and is canonicalized before storage.
5. Every referenced field and `onColumn` exists in the target table, and a transform's output type
   matches its protected column.

These invariants continue to apply for the whole table lifecycle. Servers must bind policies to a
stable table identity, preserve that binding across table renames, and remove the policies when the
table is dropped. A table with policies must reject changes that disable `query-auth.enabled` or
remove or rename a protected or referenced column, unless the policy update and schema change are
performed atomically. If an implementation persists all masks for one principal in one document,
creating or dropping one column mask must atomically preserve masks for other columns.

At authorization time, all applicable row filters must be combined with logical `AND`. More than
one applicable column mask targeting the same column must fail closed. An invalid, unsupported, or
schema-incompatible predicate or transform must also fail closed rather than omit a restriction.

This experimental contract deliberately does not define governed tags, catalog/database policy
inheritance, or tag-driven matching. Those features need explicit match conditions and conflict
rules before being added.
