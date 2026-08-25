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
`0.4.0` and may evolve incompatibly while the design is being validated.

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
request bodies do not repeat a catalog, database, table, resource type, or scope. Catalog- and
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
3. The existing REST Catalog table authorization endpoint returns the compiled Paimon predicate
   and column transforms to the engine.
4. The engine applies those restrictions when planning the scan.

Management payloads store function references and arguments, not executable SQL text or compiled
Paimon expressions. Function lookup, signature checking, policy conflict detection, and expression
compilation are server responsibilities.

## Permission model

Permission resources are structured objects:

| Resource type | Required locator | Example |
| --- | --- | --- |
| `CATALOG` | none | `{"type":"CATALOG"}` |
| `DATABASE` | `database` | `{"type":"DATABASE","database":"sales"}` |
| `TABLE` | `database`, `table` | `{"type":"TABLE","database":"sales","table":"orders"}` |
| `FUNCTION` | `database`, `function` | `{"type":"FUNCTION","database":"sales","function":"calculate_tax"}` |
| `VIEW` | `database`, `view` | `{"type":"VIEW","database":"sales","view":"daily_orders"}` |

`SELF` applies only to the referenced resource and is the default. `DESCENDANTS` applies below a
`CATALOG` or `DATABASE`; it is invalid on `TABLE`, `FUNCTION`, and `VIEW` resources.

Principals are opaque, canonical strings that are globally unique in the server namespace. Their
format is server-defined and may encode a user, group, role, or service identity, for example
`role:analyst` or an external identity-provider ARN. Principal type and membership resolution are
server responsibilities. Access values are limited to 32 characters and principals to 128
characters. An implementation may resolve wire locators and principals to different stable
persistence identifiers; those internal ids are not exposed by this API.

Built-in accesses are resource-specific:

| Resource | `SELF` accesses |
| --- | --- |
| `CATALOG` | `USE_CATALOG`, `CREATE_DATABASE`, `MANAGE_PERMISSIONS` |
| `DATABASE` | `USE_DATABASE`, `CREATE_TABLE`, `CREATE_VIEW`, `CREATE_FUNCTION`, `ALTER`, `DROP`, `MANAGE_PERMISSIONS` |
| `TABLE` | `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `ALTER`, `DROP`, `MANAGE_PERMISSIONS` |
| `VIEW` | `SELECT`, `ALTER`, `DROP`, `MANAGE_PERMISSIONS` |
| `FUNCTION` | `EXECUTE`, `ALTER`, `DROP`, `MANAGE_PERMISSIONS` |

Java and Spark helpers accept access names case-insensitively and normalize them before sending.
The REST wire format uses upper case. Implementations may add namespaced accesses such as
`VENDOR.EXAMPLE/SOME_ACCESS`; unknown unnamespaced accesses are rejected.

An assignment identity is `resource`, `scope`, `access`, and `principal`. Granting the same identity
replaces its expiry. Revocation is idempotent. `includeInherited` only expands resource ancestry;
resolving group membership and role inheritance remains a server responsibility.

`expireTime`, when present, is an exclusive upper bound evaluated against the REST server clock.
At `now >= expireTime`, the assignment must not authorize access. Expired direct assignments may
remain visible in listings until server cleanup, but `includeInherited=true` must not materialize an
expired assignment as an effective inherited view. Timestamps must not be more precise than
milliseconds.

Resource objects in this API are wire locators, not persistence identities. Servers must bind direct
assignments to a stable internal resource identity: renaming a database, table, function, or view
retains its assignments and subsequent responses use the new locator; dropping it removes its direct
assignments; recreating the same locator does not restore them. A child rename does not change a
`DESCENDANTS` assignment attached to its catalog or database ancestor.

## Data policy model

A data policy is attached directly to one table and one principal. It applies whenever that
principal is effective for the caller after the server resolves group and role membership. A
principal can have at most one row filter on a table and at most one column mask on each table
column. A row-filter identity is `(table, ROW_FILTER, principal)`; a column-mask identity is
`(table, COLUMN_MASKING, principal, onColumn)`.

Each policy contains exactly one typed definition:

| Definition | Required fields | Result |
| --- | --- | --- |
| `rowFilter` | `functionName`, ordered `functionArguments` | A boolean predicate applied to every scan. |
| `columnMask` | `functionName`, `onColumn`, ordered `functionArguments` | A value compatible with the protected column. |

The common field is one `principal`. A function argument is exactly one of
`{"column":"region"}` or `{"constant":"APAC"}`. Constants are strings and may be empty;
column names may not be empty. `functionArguments` is optional and defaults to an empty list.

For a row filter, the arguments are passed positionally and the function must return boolean. For a
column mask, `onColumn` is the protected input and `functionArguments` contains additional
positional arguments. The REST server resolves the named function and compiles it into the existing
Paimon predicate or column transform returned by table query authorization.

Policy create and create-or-replace must be rejected unless all of these conditions hold:

1. The target database and table exist.
2. The table has `query-auth.enabled=true`; otherwise a stored policy could be silently bypassed.
3. The referenced principal exists.
4. The policy function exists, has a compatible signature, and can be compiled by the server.
5. `onColumn` and every column argument exist in the target table.

These invariants continue to apply for the whole table lifecycle. Servers must bind policies to a
stable table identity, preserve that binding across table renames, and remove the policies when the
table is dropped. A table with policies must reject changes that disable `query-auth.enabled` or
remove or rename a protected or argument column, unless the policy update and schema change are
performed atomically. If an implementation persists all masks for one principal in one document,
creating, replacing, or dropping one column mask must atomically preserve masks for other columns.

At authorization time, all applicable row filters must be combined with logical `AND`. More than
one applicable column mask targeting the same column must fail closed. An unresolved function,
invalid signature, or policy compilation failure must also fail closed rather than omit a
restriction.

This experimental contract deliberately does not define governed tags, catalog/database policy
inheritance, or tag-driven matching. Those features need explicit match conditions and conflict
rules before being added.

## Spark SQL procedures

The following examples assume a Spark catalog named `paimon`. Replace it with the catalog name in
`spark.sql.catalog.<catalog-name>`.

### Grant permissions

`grant_permission` returns one row with `result = true` when the server accepts the assignment.

Grant catalog access or a privilege inherited by catalog descendants:

```sql
CALL paimon.sys.grant_permission(
  resource_type => 'CATALOG',
  access => 'USE_CATALOG',
  principal => 'role:catalog_user'
);

CALL paimon.sys.grant_permission(
  resource_type => 'CATALOG',
  scope => 'DESCENDANTS',
  access => 'SELECT',
  principal => 'group:analysts'
);
```

Grant database privileges on the database itself or all objects below it:

```sql
CALL paimon.sys.grant_permission(
  resource_type => 'DATABASE',
  database => 'sales',
  access => 'CREATE_TABLE',
  principal => 'role:data_engineer'
);

CALL paimon.sys.grant_permission(
  resource_type => 'DATABASE',
  database => 'sales',
  scope => 'DESCENDANTS',
  access => 'SELECT',
  principal => 'role:sales_reader',
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

Filter by principal, scope, or access:

```sql
CALL paimon.sys.list_permissions(
  resource_type => 'TABLE',
  database => 'sales',
  table => 'orders',
  principal => 'role:analyst',
  access => 'SELECT'
);
```

Include catalog and database descendant assignments effective on a table:

```sql
CALL paimon.sys.list_permissions(
  resource_type => 'TABLE',
  database => 'sales',
  table => 'orders',
  include_inherited => true
);
```

The `inherited_from_json` output identifies the direct assignment source. `next_page_token` is
opaque; pass it back unchanged with the same filters:

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

Supply the same four identity fields used by the grant. `expire_time` is not part of identity.

```sql
CALL paimon.sys.revoke_permission(
  resource_type => 'DATABASE',
  database => 'sales',
  scope => 'DESCENDANTS',
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

`create_policy` accepts the canonical `principal` directly. Function arguments use `column:value`
or `constant:value`. Only the first colon is a delimiter, so a constant may contain later colons.
`constant:` represents a valid empty string.

This example asks the server to resolve
`security.filter_region(region, 'APAC')` as a boolean predicate:

```sql
CALL paimon.sys.create_policy(
  database => 'sales',
  table => 'orders',
  policy_type => 'ROW_FILTER',
  principal => 'group:analysts',
  function_name => 'security.filter_region',
  function_arguments => array('column:region', 'constant:APAC')
);
```

The call fails if that principal already has a row filter on the table. Use
`create_or_replace_policy` when upsert semantics are intended. Create another policy for a second
principal with a separate call.

### Create column-masking policies

For column masking, `on_column` identifies the protected column. Additional columns or constants
can be supplied positionally:

```sql
CALL paimon.sys.create_policy(
  database => 'sales',
  table => 'customers',
  policy_type => 'COLUMN_MASKING',
  principal => 'role:support',
  function_name => 'security.mask_phone',
  on_column => 'phone_number',
  function_arguments => array('column:region', 'constant:partial')
);
```

A mask with no additional arguments may omit `function_arguments`. An empty-string argument is
written explicitly:

```sql
CALL paimon.sys.create_policy(
  database => 'sales',
  table => 'customers',
  policy_type => 'COLUMN_MASKING',
  principal => 'group:support',
  function_name => 'security.mask_email',
  on_column => 'email',
  function_arguments => array('constant:')
);
```

`on_column` is required for `COLUMN_MASKING` and rejected for `ROW_FILTER`.

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
  function_name => 'security.filter_region_v2',
  function_arguments => array('column:region')
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

The output columns are `database`, `table`, `policy_type`, `principal`, `function_name`,
`on_column`, `function_arguments_json`, and `next_page_token`. Pass an opaque continuation token
back unchanged with the same filters:

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
membership, policy persistence, function resolution, and audit logging remain REST server
concerns.
