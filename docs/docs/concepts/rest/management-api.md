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

The OpenAPI 3.1 document below defines the language-neutral control-plane contract for listing,
granting, and revoking permissions on Paimon catalog resources.

The permission payload uses a flat resource-oriented model. User and role lifecycle,
authentication, policy persistence, and authorization decisions remain responsibilities of the
REST Catalog server implementation.

## Client configuration

Permission management is provided directly by `RESTCatalog`; it is not part of the generic
`Catalog` interface. Configure the remote catalog identifier used in the management endpoint with
`management.catalog`. The value identifies the `{catalog}` path parameter and is independent of an
engine's local catalog alias.

For Spark, this can be supplied by the REST server's config response or configured directly:

```properties
spark.sql.catalog.paimon.management.catalog=my_remote_catalog
```

Non-REST catalog implementations fail these operations with an explicit unsupported error.

## Spark SQL

Spark exposes the management contract through procedures. Custom `GRANT` and `REVOKE` grammar is
not required, so the SQL layer remains a thin adapter over the same Java and REST contracts.

The examples below assume that the Spark catalog is named `paimon`. Replace it with the local
catalog name from `spark.sql.catalog.<catalog-name>`. Procedure arguments are named so optional
resource fields can be omitted. `resource_type` and `access` are case-insensitive and are sent to
the server in upper case; `principal` is an opaque, non-empty identifier understood by the server.

### Resource types

The resource type determines which locator arguments are required and which arguments must be
omitted.

| Resource type | Required locator | Meaning |
| --- | --- | --- |
| `CATALOG` | none | A permission on the catalog itself. |
| `CATALOG_ALL` | none | A catalog permission inherited by descendant resources. |
| `DATABASE` | `database` | A permission on one database. |
| `DATABASE_ALL` | `database` | A database permission inherited by descendant resources. |
| `TABLE` | `database`, `table` | A permission on one table. |
| `FUNCTION` | `database`, `function` | A permission on one function. |
| `VIEW` | `database`, `view` | A permission on one view. |
| `COLUMN` | `database`, `table` | A `SELECT` permission on selected table columns. |
| `ROW_FILTER` | `database`, `table` | A row-filter policy granted separately from table access. |
| `COLUMN_MASKING` | `database`, `table` | Column-masking policies granted separately from table access. |

### Grant permissions

`grant_permission` creates or replaces a grant with the same permission identity and returns one
row with `result = true` when the server accepts the request.

| Argument | Required | Description |
| --- | --- | --- |
| `resource_type` | yes | One of the resource types listed above. |
| `access` | yes | Access name such as `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `ALTER`, or `ALL`. The server defines the supported access names, except for the policy-specific values below. |
| `principal` | yes | Principal identifier, for example `user:alice` or `role:analyst`. |
| `database` | conditional | Required for database-, table-, function-, view-, and policy-scoped resources. |
| `table` | conditional | Required for `TABLE`, `COLUMN`, `ROW_FILTER`, and `COLUMN_MASKING`. |
| `function` | conditional | Required only for `FUNCTION`. |
| `view` | conditional | Required only for `VIEW`. |
| `column_names` | conditional | Non-empty `array<string>` of included columns for `COLUMN`. Mutually exclusive with `excluded_column_names`. |
| `excluded_column_names` | conditional | Non-empty `array<string>` granting all columns except the listed columns for `COLUMN`. |
| `row_filter` | conditional | Required only for `ROW_FILTER`; `access` must also be `ROW_FILTER`. |
| `column_masking` | conditional | Non-empty `map<string, string>` from column name to masking expression for `COLUMN_MASKING`; `access` must also be `COLUMN_MASKING`. |
| `expire_time` | no | ISO-8601 UTC expiration time. It is not supported for `ROW_FILTER` or `COLUMN_MASKING`. |

Grant catalog, database, table, function, and view permissions by selecting the matching locator:

```sql
-- SELECT on all resources below the catalog.
CALL paimon.sys.grant_permission(
  resource_type => 'CATALOG_ALL',
  access => 'SELECT',
  principal => 'role:analyst'
);

-- ALL access on one database until the specified time.
CALL paimon.sys.grant_permission(
  resource_type => 'DATABASE',
  access => 'ALL',
  principal => 'role:data_engineer',
  database => 'sales',
  expire_time => '2027-01-01T00:00:00Z'
);

-- SELECT on one table.
CALL paimon.sys.grant_permission(
  resource_type => 'TABLE',
  access => 'SELECT',
  principal => 'user:alice',
  database => 'sales',
  table => 'orders'
);

-- ALL access on one function.
CALL paimon.sys.grant_permission(
  resource_type => 'FUNCTION',
  access => 'ALL',
  principal => 'role:analyst',
  database => 'sales',
  function => 'calculate_tax'
);

-- SELECT on one view.
CALL paimon.sys.grant_permission(
  resource_type => 'VIEW',
  access => 'SELECT',
  principal => 'role:analyst',
  database => 'sales',
  view => 'daily_orders'
);
```

A `COLUMN` grant must use `SELECT` access and specify exactly one of `column_names` and
`excluded_column_names`:

```sql
-- Grant access only to the selected columns.
CALL paimon.sys.grant_permission(
  resource_type => 'COLUMN',
  access => 'SELECT',
  principal => 'role:analyst',
  database => 'sales',
  table => 'orders',
  column_names => array('id', 'amount'),
  expire_time => '2027-01-01T00:00:00Z'
);

-- Grant access to every column except sensitive columns.
CALL paimon.sys.grant_permission(
  resource_type => 'COLUMN',
  access => 'SELECT',
  principal => 'role:support',
  database => 'sales',
  table => 'customers',
  excluded_column_names => array('id_card_number', 'phone_number')
);
```

Grant row filtering and column masking as independent table-scoped permissions. The REST server
interprets each expression and may return its compiled predicate or transform when the grant is
listed. Policy grants do not accept `expire_time`.

```sql
-- Restrict visible rows.
CALL paimon.sys.grant_permission(
  resource_type => 'ROW_FILTER',
  access => 'ROW_FILTER',
  principal => 'role:analyst',
  database => 'sales',
  table => 'orders',
  row_filter => 'region = ''cn'''
);

-- Apply a different masking expression to each named column.
CALL paimon.sys.grant_permission(
  resource_type => 'COLUMN_MASKING',
  access => 'COLUMN_MASKING',
  principal => 'role:support',
  database => 'sales',
  table => 'customers',
  column_masking => map(
    'email', 'UPPER(email)',
    'phone_number', 'NULL'
  )
);
```

### List permissions

`list_permissions` returns explicitly granted permissions and supports exact-match resource and
principal filters. `resource_type` is required. `database` is required whenever `table`,
`function`, or `view` is specified.

| Argument | Required | Description |
| --- | --- | --- |
| `resource_type` | yes | Resource type to list. The related-type expansion described below also applies. |
| `database` | no | Exact database filter. |
| `table` | no | Exact table filter; requires `database`. |
| `function` | no | Exact function filter; requires `database`. |
| `view` | no | Exact view filter; requires `database`. |
| `principal` | no | Exact principal filter. |
| `max_results` | no | Positive maximum number of results requested from the server. |
| `page_token` | no | Opaque token returned by a previous call. |

For example, list all grants that affect a table for a principal:

```sql
CALL paimon.sys.list_permissions(
  resource_type => 'TABLE',
  database => 'sales',
  table => 'orders',
  principal => 'role:analyst'
);
```

Listing `CATALOG` also includes `CATALOG_ALL`, listing `DATABASE` includes `DATABASE_ALL`, and
listing `TABLE` includes `COLUMN`, `ROW_FILTER`, and `COLUMN_MASKING` rows. This makes a table query
useful for inspecting both ordinary access and policies affecting that table.

Use `max_results` and the returned `next_page_token` to page through a larger result. The token is
server-defined and must be passed back unchanged:

```sql
-- First page.
CALL paimon.sys.list_permissions(
  resource_type => 'CATALOG',
  max_results => 100
);

-- Next page. Copy the token from the first page.
CALL paimon.sys.list_permissions(
  resource_type => 'CATALOG',
  max_results => 100,
  page_token => '<opaque next_page_token>'
);
```

The procedure returns the following columns:

| Column | Description |
| --- | --- |
| `resource_type` | Actual resource type of the grant. |
| `catalog` | Catalog carried in the returned permission payload, if provided by the server. |
| `database` | Database locator. |
| `table` | Table locator. |
| `function` | Function locator. |
| `view` | View locator. |
| `columns_json` | Included or excluded column selection as JSON. |
| `row_filter_json` | Row-filter expression and optional compiled predicate as JSON. |
| `column_masking_json` | Column-to-mask mapping as JSON. |
| `access` | Granted access. |
| `principal` | Granted principal. |
| `expire_time` | Grant expiration time, if any. |
| `next_page_token` | Token for the next page. It is repeated on every row and is null on the final page. |

### Revoke permissions

`revoke_permission` identifies a grant by `resource_type`, its resource locator, `access`, and
`principal`. Column selection, row-filter, column-masking, and expiration payloads are deliberately
omitted. It returns one row with `result = true` when the server accepts the request.

```sql
-- Revoke a table permission.
CALL paimon.sys.revoke_permission(
  resource_type => 'TABLE',
  access => 'SELECT',
  principal => 'user:alice',
  database => 'sales',
  table => 'orders'
);

-- Revoke a column permission, regardless of its included or excluded columns.
CALL paimon.sys.revoke_permission(
  resource_type => 'COLUMN',
  access => 'SELECT',
  principal => 'role:analyst',
  database => 'sales',
  table => 'orders'
);

-- Revoke a function permission.
CALL paimon.sys.revoke_permission(
  resource_type => 'FUNCTION',
  access => 'ALL',
  principal => 'role:analyst',
  database => 'sales',
  function => 'calculate_tax'
);
```

Revoking a permission which does not exist may return HTTP 404. Resource arguments are validated
before a request is sent: conflicting locators are rejected, table-level resources require both
`database` and `table`, and function and view resources require their matching locator.

## OpenAPI contract

<body>
    <iframe src="/docs/master/rest-management-open-api.yaml" width="100%" height="800px" />
</body>
