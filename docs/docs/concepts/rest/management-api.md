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

Grant access to selected columns:

```sql
CALL paimon.sys.grant_permission(
  resource_type => 'COLUMN',
  access => 'SELECT',
  principal => 'role:analyst',
  database => 'sales',
  table => 'orders',
  column_names => array('id', 'amount'),
  expire_time => '2027-01-01T00:00:00Z'
);
```

`resource_type`, `access`, and `principal` are required. `CATALOG_ALL` and `DATABASE_ALL` represent
permissions inherited by descendant resources. `COLUMN`, `ROW_FILTER`, and `COLUMN_MASKING` are
independent table-scoped permissions. Exactly one of `column_names` and `excluded_column_names` may
be set for `COLUMN`.

Grant a row filter separately. The server compiles the expression into its predicate representation:

```sql
CALL paimon.sys.grant_permission(
  resource_type => 'ROW_FILTER',
  access => 'ROW_FILTER',
  principal => 'role:analyst',
  database => 'sales',
  table => 'orders',
  row_filter => 'region = ''cn'''
);
```

List grants using any combination of exact-match filters:

```sql
CALL paimon.sys.list_permissions(
  resource_type => 'TABLE',
  principal => 'role:analyst',
  max_results => 100,
  page_token => '<opaque token from a previous result>'
);
```

Listing `CATALOG` also includes `CATALOG_ALL`, listing `DATABASE` includes `DATABASE_ALL`, and
listing `TABLE` includes `COLUMN`, `ROW_FILTER`, and `COLUMN_MASKING` rows. The result mirrors the
flat permission payload with separate JSON columns for columns, row filters, and column masking.
The next-page token is repeated on each row and is null when no further page exists.

Revoke by stable permission identity. Constraints and expiration are deliberately omitted:

```sql
CALL paimon.sys.revoke_permission(
  resource_type => 'COLUMN',
  access => 'SELECT',
  principal => 'role:analyst',
  database => 'sales',
  table => 'orders'
);
```

Grant uses upsert behavior. Revoke identifies a permission by resource type, resource, access, and
principal; row-filter, column-masking, column-selection, and expiration payloads are not part of
that identity. Revoking a permission which does not exist may return HTTP 404.

## OpenAPI contract

<body>
    <iframe src="/docs/master/rest-management-open-api.yaml" width="100%" height="800px" />
</body>
