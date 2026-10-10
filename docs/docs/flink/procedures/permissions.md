---
title: "Permissions"
sidebar_position: 9
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

# Permissions

Grant, revoke, and list direct permission assignments on a [REST catalog](../../concepts/rest/management-api).
These procedures require Flink 1.19 or later. Named and positional calls both use that API.
Other catalogs fail with `Catalog does not support permission management.`

`resource_type` is one of `CATALOG`, `CATALOG_ALL`, `DATABASE`, `DATABASE_ALL`, `TABLE`, `COLUMN`,
`VIEW`, or `FUNCTION`. Names are case-insensitive. `CATALOG_ALL` and `DATABASE_ALL` grant an access
on every applicable object in that scope; they do not grant catalog or database administration
access such as `CREATEDATABASE`.

Repeating `grant_permission` for the same resource, access, and principal replaces `expire_time`
and the column range. Repeating `revoke_permission` succeeds when the assignment is already absent.
`list_permissions` returns only direct assignments on the requested resource. It does not
synthesize assignments inherited from a parent scope. `next_page_token` is opaque and is repeated
on every row of a page; pass it back unchanged with the same filters.

See [Procedures](../procedures) for Flink version requirements, argument conventions, and catalog
selection. The portable contract is described in [REST Management API](../../concepts/rest/management-api).

## grant_permission

Grant one permission assignment.

**Arguments**

- `resource_type` (`STRING`, required): resource or explicit descendant scope.
- `access` (`STRING`, required): permission to grant, such as `SELECT` or `CREATEDATABASE`.
- `principal` (`STRING`, required): principal that receives the permission.
- `database` (`STRING`, optional): database locator. Required for database, table, column, view, and function resources.
- `table` (`STRING`, optional): table locator. Required for `TABLE` and `COLUMN`.
- `function` (`STRING`, optional): function locator. Required for `FUNCTION`.
- `view` (`STRING`, optional): view locator. Required for `VIEW`.
- `expire_time` (`STRING`, optional): exclusive expiration instant, for example `2027-01-01T00:00:00Z`.
- `column_names` (`STRING`, optional): JSON array allowlist for a `COLUMN` assignment.
- `excluded_column_names` (`STRING`, optional): JSON array denylist for a `COLUMN` assignment.

A `COLUMN` assignment sets exactly one of `column_names` or `excluded_column_names`. The value is a
JSON array of strings, for example `["order_id", "region"]`. Each element is the exact column name,
including leading or trailing spaces and embedded commas. The table must already have
`query-auth.enabled` set to `true`.

**Output**

| Column | Type | Description |
| --- | --- | --- |
| `result` | `BOOLEAN` | `true` when the server accepts the assignment. |

**Example**

```sql
-- Flink 1.19 and later, named arguments
CALL sys.grant_permission(
    resource_type => 'TABLE',
    `database` => 'sales',
    `table` => 'orders',
    access => 'SELECT',
    principal => 'analyst',
    expire_time => '2027-01-01T00:00:00Z'
);

CALL sys.grant_permission(
    resource_type => 'COLUMN',
    `database` => 'sales',
    `table` => 'orders',
    access => 'SELECT',
    principal => 'analyst',
    column_names => '["order_id", "region"]'
);

-- Flink 1.19 and later, positional arguments in parameter order
CALL sys.grant_permission(
    'TABLE', 'SELECT', 'analyst', 'sales', 'orders', '', '', '2027-01-01T00:00:00Z');
```

## list_permissions

List direct assignments on one resource.

**Arguments**

- `resource_type` (`STRING`, required): resource or explicit descendant scope to list.
- `database` (`STRING`, optional): database locator.
- `table` (`STRING`, optional): table locator.
- `function` (`STRING`, optional): function locator.
- `view` (`STRING`, optional): view locator.
- `principal` (`STRING`, optional): return only this principal.
- `access` (`STRING`, optional): return only this access.
- `max_results` (`INT`, optional): maximum number of assignments in the page.
- `page_token` (`STRING`, optional): opaque token from the previous page.

**Output**

| Column | Type | Description |
| --- | --- | --- |
| `resource_type` | `STRING` | Resource type of the assignment. |
| `database` | `STRING` | Database locator, or `NULL`. |
| `table` | `STRING` | Table locator, or `NULL`. |
| `function` | `STRING` | Function locator, or `NULL`. |
| `view` | `STRING` | View locator, or `NULL`. |
| `access` | `STRING` | Granted access. |
| `principal` | `STRING` | Principal that holds the access. |
| `column_names` | `ARRAY<STRING>` | Column allowlist, or `NULL`. |
| `excluded_column_names` | `ARRAY<STRING>` | Column denylist, or `NULL`. |
| `expire_time` | `STRING` | Exclusive expiration instant, or `NULL`. |
| `next_page_token` | `STRING` | Opaque token for the next page, or `NULL` on the last page. |

**Example**

```sql
CALL sys.list_permissions(
    resource_type => 'TABLE',
    `database` => 'sales',
    `table` => 'orders',
    principal => 'analyst',
    max_results => 50,
    page_token => 'opaque-token-from-previous-row'
);
```

## revoke_permission

Remove one permission assignment. `expire_time` and the column range are not part of the identity.
Revoking a `COLUMN` assignment removes the whole range for that resource, access, and principal.

**Arguments**

- `resource_type` (`STRING`, required): resource or explicit descendant scope.
- `access` (`STRING`, required): permission to revoke.
- `principal` (`STRING`, required): principal that holds the permission.
- `database` (`STRING`, optional): database locator.
- `table` (`STRING`, optional): table locator.
- `function` (`STRING`, optional): function locator.
- `view` (`STRING`, optional): view locator.

**Output**

| Column | Type | Description |
| --- | --- | --- |
| `result` | `BOOLEAN` | `true` when the server accepts the revocation. |

**Example**

```sql
CALL sys.revoke_permission(
    resource_type => 'TABLE',
    `database` => 'sales',
    `table` => 'orders',
    access => 'SELECT',
    principal => 'analyst'
);
```
