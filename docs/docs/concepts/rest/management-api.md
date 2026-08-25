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
intentionally not part of the generic `Catalog` interface. A non-REST catalog therefore reports an
unsupported-operation error when a management procedure is called.

## Catalog addressing

All management endpoints use the opaque `prefix` returned by the REST Catalog config endpoint. It
is not a catalog name in a payload and is independent of the local engine alias such as `paimon` in
`CALL paimon.sys...`.

```
GET  /v1/{prefix}/permissions
POST /v1/{prefix}/permissions/grant
POST /v1/{prefix}/permissions/revoke

GET    /v1/{prefix}/databases/{database}/tables/{table}/policies
POST   /v1/{prefix}/databases/{database}/tables/{table}/policies
PUT    /v1/{prefix}/databases/{database}/tables/{table}/policies
DELETE /v1/{prefix}/databases/{database}/tables/{table}/policies
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
| `DATABASE` | `database` | `{"type":"DATABASE","database":"sales"}` |
| `TABLE` | `database`, `table` | `{"type":"TABLE","database":"sales","table":"orders"}` |
| `FUNCTION` | `database`, `function` | `{"type":"FUNCTION","database":"sales","function":"calculate_tax"}` |
| `VIEW` | `database`, `view` | `{"type":"VIEW","database":"sales","view":"daily_orders"}` |

Principals are opaque, canonical strings that are globally unique in the server namespace. Their
format is server-defined and may encode a user, group, role, or service identity, for example
`role:analyst` or an external identity-provider ARN. Principal type and membership resolution are
server responsibilities. Access values are limited to 32 characters and principals to 128
characters. An implementation may resolve wire locators and principals to different stable
persistence identifiers; those internal ids are not exposed by this API.

Built-in accesses are resource-specific:

| Resource | Accesses |
| --- | --- |
| `CATALOG` | `USE_CATALOG`, `CREATE_DATABASE`, `MANAGE_PERMISSIONS` |
| `DATABASE` | `USE_DATABASE`, `CREATE_TABLE`, `CREATE_VIEW`, `CREATE_FUNCTION`, `ALTER`, `DROP`, `MANAGE_PERMISSIONS` |
| `TABLE` | `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `ALTER`, `DROP`, `MANAGE_PERMISSIONS` |
| `VIEW` | `SELECT`, `ALTER`, `DROP`, `MANAGE_PERMISSIONS` |
| `FUNCTION` | `EXECUTE`, `ALTER`, `DROP`, `MANAGE_PERMISSIONS` |

Java and Spark helpers accept access names case-insensitively and normalize them before sending.
The REST wire format uses upper case. Implementations may add namespaced accesses such as
`VENDOR.EXAMPLE/SOME_ACCESS`; unknown unnamespaced accesses are rejected.

An assignment identity is `resource`, `access`, and `principal`. Granting the same identity replaces
its expiry, and revocation is idempotent. Assignments apply only to the exact referenced resource;
resource inheritance is not part of this experimental contract. Resolving group membership and role
inheritance remains a server responsibility.

`expireTime`, when present, is an exclusive upper bound evaluated against the REST server clock.
At `now >= expireTime`, the assignment must not authorize access. Expired direct assignments may
remain visible in listings until server cleanup. Timestamps must not be more precise than
milliseconds.

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

Policy create and create-or-replace must be rejected unless all of these conditions hold:

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
creating, replacing, or dropping one column mask must atomically preserve masks for other columns.

At authorization time, all applicable row filters must be combined with logical `AND`. More than
one applicable column mask targeting the same column must fail closed. An invalid, unsupported, or
schema-incompatible predicate or transform must also fail closed rather than omit a restriction.

This experimental contract deliberately does not define governed tags, catalog/database policy
inheritance, or tag-driven matching. Those features need explicit match conditions and conflict
rules before being added.

## Spark SQL procedures

The following examples assume a Spark catalog named `paimon`. Replace it with the catalog name in
`spark.sql.catalog.<catalog-name>`.

### Grant permissions

`grant_permission` returns one row with `result = true` when the server accepts the assignment.

Grant catalog access:

```sql
CALL paimon.sys.grant_permission(
  resource_type => 'CATALOG',
  access => 'USE_CATALOG',
  principal => 'role:catalog_user'
);
```

Grant a database privilege with an optional expiration time:

```sql
CALL paimon.sys.grant_permission(
  resource_type => 'DATABASE',
  database => 'sales',
  access => 'CREATE_TABLE',
  principal => 'role:data_engineer',
  expire_time => '2027-01-01T00:00:00Z'
);
```

Grant table, function, and view access with the matching locator:

```sql
CALL paimon.sys.grant_permission(
  resource_type => 'TABLE',
  database => 'sales',
  table => 'orders',
  access => 'SELECT',
  principal => 'user:alice'
);

CALL paimon.sys.grant_permission(
  resource_type => 'FUNCTION',
  database => 'sales',
  function => 'calculate_tax',
  access => 'EXECUTE',
  principal => 'role:analyst'
);

CALL paimon.sys.grant_permission(
  resource_type => 'VIEW',
  database => 'sales',
  view => 'daily_orders',
  access => 'SELECT',
  principal => 'service:reporting_job'
);
```

### List permissions

`list_permissions` always addresses one exact resource. Omit optional filters to list every direct
assignment on it:

```sql
CALL paimon.sys.list_permissions(
  resource_type => 'TABLE',
  database => 'sales',
  table => 'orders'
);
```

Filter by principal or access:

```sql
CALL paimon.sys.list_permissions(
  resource_type => 'TABLE',
  database => 'sales',
  table => 'orders',
  principal => 'role:analyst',
  access => 'SELECT'
);
```

The `next_page_token` output is opaque; pass it back unchanged with the same filters:

```sql
CALL paimon.sys.list_permissions(
  resource_type => 'TABLE',
  database => 'sales',
  table => 'orders',
  max_results => 50,
  page_token => 'opaque-token-from-previous-row'
);
```

### Revoke permissions

Supply the same three identity fields used by the grant. `expire_time` is not part of identity.

```sql
CALL paimon.sys.revoke_permission(
  resource_type => 'TABLE',
  database => 'sales',
  table => 'orders',
  access => 'SELECT',
  principal => 'role:sales_reader'
);
```

Repeating the same call succeeds even when the assignment is already absent.

### Create row-filter policies

Before attaching any policy, enable table query authorization:

```sql
ALTER TABLE paimon.sales.orders
SET TBLPROPERTIES ('query-auth.enabled' = 'true');
```

`create_policy` accepts the canonical `principal` and a serialized Paimon `Predicate`. The JSON
below is the same representation accepted in one `AuthTableQueryResponse.filter` entry. Named
arguments are recommended because row-filter and column-mask definitions use different fields:

```sql
CALL paimon.sys.create_policy(
  database => 'sales',
  table => 'orders',
  policy_type => 'ROW_FILTER',
  principal => 'group:analysts',
  predicate_json => '{"kind":"LEAF","transform":{"name":"FIELD_REF","fieldRef":{"index":1,"name":"region","type":"STRING"}},"function":"EQUAL","literals":["APAC"]}'
);
```

The call fails if that principal already has a row filter on the table. Use
`create_or_replace_policy` when upsert semantics are intended. Create another policy for a second
principal with a separate call.

### Create column-masking policies

For column masking, `on_column` identifies the protected column and `transform_json` is the same
serialized Paimon `Transform` representation used as an
`AuthTableQueryResponse.columnMasking` value. This example replaces every visible phone number
with a fixed string:

```sql
CALL paimon.sys.create_policy(
  database => 'sales',
  table => 'customers',
  policy_type => 'COLUMN_MASKING',
  principal => 'role:support',
  on_column => 'phone_number',
  transform_json => '{"name":"CONCAT","inputs":["****"]}'
);
```

A transform may reference table fields by name. The server remaps their indices to the current
schema, rejects missing fields, and verifies that the result type matches `on_column`:

```sql
CALL paimon.sys.create_policy(
  database => 'sales',
  table => 'customers',
  policy_type => 'COLUMN_MASKING',
  principal => 'group:support',
  on_column => 'email',
  transform_json => '{"name":"CONCAT","inputs":[{"index":1,"name":"region","type":"STRING"},"-masked"]}'
);
```

`predicate_json` is required only for `ROW_FILTER`. `on_column` and `transform_json` are required
only for `COLUMN_MASKING`. JSON containing a single quote must escape it as `''` inside the SQL
string literal.

### Create or fully replace a policy

`create_or_replace_policy` maps to HTTP `PUT`. It creates an absent policy or fully replaces the
policy with the same table, policy type, principal, and, for a mask, protected column. Omitted
optional values are cleared.

```sql
CALL paimon.sys.create_or_replace_policy(
  database => 'sales',
  table => 'orders',
  policy_type => 'ROW_FILTER',
  principal => 'group:analysts',
  predicate_json => '{"kind":"LEAF","transform":{"name":"FIELD_REF","fieldRef":{"index":1,"name":"region","type":"STRING"}},"function":"EQUAL","literals":["EMEA"]}'
);
```

This is deliberately separate from `create_policy`: callers must opt in to replacement instead of
passing a generic `replace` flag.

### List policies

List every policy directly attached to one table:

```sql
CALL paimon.sys.list_policies(
  database => 'sales',
  table => 'orders'
);
```

Filter by policy type or principal. A `column` filter is valid only with
`policy_type => 'COLUMN_MASKING'`:

```sql
CALL paimon.sys.list_policies(
  database => 'sales',
  table => 'orders',
  policy_type => 'ROW_FILTER',
  principal => 'group:analysts'
);
```

The output columns are `database`, `table`, `policy_type`, `principal`, `predicate_json`,
`on_column`, `transform_json`, and `next_page_token`. A row filter has only `predicate_json`; a
column mask has only `on_column` and `transform_json`. Pass an opaque continuation token back
unchanged with the same filters:

```sql
CALL paimon.sys.list_policies(
  database => 'sales',
  table => 'orders',
  max_results => 50,
  page_token => 'opaque-token-from-previous-row'
);
```

Management listing follows the existing Paimon pagination contract: an empty page terminates
pagination and therefore has no continuation token. Each Spark procedure returns exactly the page
selected by `page_token`; pass a non-null `next_page_token` back unchanged to retrieve the next page.

### Drop policies

Drop an existing policy:

```sql
CALL paimon.sys.drop_policy(
  database => 'sales',
  table => 'orders',
  policy_type => 'ROW_FILTER',
  principal => 'group:analysts'
);
```

By default an absent policy is an error. Set `if_exists => true` for an idempotent operation:

```sql
CALL paimon.sys.drop_policy(
  database => 'sales',
  table => 'orders',
  policy_type => 'ROW_FILTER',
  principal => 'group:analysts',
  if_exists => true
);
```

Creating, replacing, dropping, or inspecting permissions and policies requires the server to
authorize the caller for `MANAGE_PERMISSIONS` on the relevant resource. Authentication, principal
membership, policy persistence, schema validation, and audit logging remain REST server concerns.
