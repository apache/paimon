---
title: "Manage Privileges"
weight: 10
type: docs
aliases:
- /maintenance/manage-privileges.html
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

# Manage Privileges

Paimon provides a privilege system on catalogs.
Privileges determine which users can perform which operations on which objects,
so that you can manage table access in a fine-grained manner.

Currently, Paimon adopts the identity-based access control (IBAC) privilege model.
That is, privileges are directly assigned to users.

{{< hint warning >}}
This privilege system only prevents unwanted users from accessing tables through catalogs.
It does not block access through temporary table (by specifying table path on filesystem),
nor does it prevent user from directly modifying data files on filesystem.
If you need more serious protection, use a filesystem with access management instead.
{{< /hint >}}

## Basic Concepts

We now introduce the basic concepts of the privilege system.

### Object

An object is an entity to which access can be granted. Unless allowed by a grant, access is denied.

Currently, the privilege system in Paimon has three types of objects: CATALOG, DATABASE and TABLE.
Objects have a logical hierarchy, which is related to the concept they represent.
For example:
* If a user is granted a privilege on the catalog,
he will also have this privilege on all databases and all tables in the catalog.
* If a user is granted a privilege on the database,
he will also have this privilege on all tables in that database.
* If a user is revoked a privilege from the catalog,
he will also lose this privilege on all databases and all tables in the catalog.
* If a user is revoked a privilege from the database, 
he will also lose this privilege on all tables in that database.

### Privilege

A privilege is a defined level of access to an object.
Multiple privileges can be used to control the granularity of access granted on an object.
Privileges are object-specific. Different objects may have different privileges.

Currently, we support the following privileges.

| Privilege   | Description                                                                               | Can be Granted on        |
|-------------|-------------------------------------------------------------------------------------------|--------------------------|
| SELECT      | Queries data in a table.                                                                  | TABLE, DATABASE, CATALOG |
| INSERT      | Inserts, updates or drops data in a table. Creates or drops tags and branches in a table. | TABLE, DATABASE, CATALOG |
| ALTER_TABLE | Alters metadata of a table, including table name, column names, table options, etc.       | TABLE, DATABASE, CATALOG |
| DROP_TABLE | Drops a table. | TABLE, DATABASE, CATALOG |
| CREATE_TABLE | Creates a table in a database. | DATABASE, CATALOG |
| DROP_DATABASE | Drops a database. | DATABASE, CATALOG |
| CREATE_DATABASE | Creates a database in the catalog. | CATALOG |
| ADMIN | Creates or drops privileged users, grants or revokes privileges from users in a catalog. | CATALOG |

### User

The entity to which privileges can be granted. Users are authenticated by their password.

When the privilege system is enabled, two special users will be created automatically.

* The `root` user,
which is identified by the provided root password when enabling the privilege system.
This user always has all privileges in the catalog.
* The `anonymous` user.
This is the default user if no username and password is provided when creating the catalog.

## Enable Privileges

Paimon currently only supports file-based privilege system.
Only catalogs with `'metastore' = 'filesystem'` (the default value) support such privilege system.

To enable the privilege system on a filesystem catalog, do the following steps.

{{< tabs "enable-privileges" >}}

{{< tab "Flink 1.18+" >}}
Run the following Flink SQL.
```sql
-- use the catalog where you want to enable the privilege system
USE CATALOG `my-catalog`;
    
-- initialize privilege system by providing a root password
-- change 'root-password' to the password you want
CALL sys.init_file_based_privilege('root-password');
```
{{< /tab >}}

{{< /tabs >}}

After the privilege system is enabled,
please re-create the catalog and authenticate as `root` to create other users and grant them privileges.

{{< hint info >}}
Privilege system does not affect existing catalogs.
That is, these catalogs can still access and modify the tables freely.
Please drop and re-create all catalogs with the desired warehouse path
if you want to use the privilege system in these catalogs.
{{< /hint >}}

## Accessing Privileged Catalogs

To access a privileged catalog and to be authenticated as a user,
you need to define `user` and `password` catalog options when creating the catalog.
For example, the following SQL creates a catalog while trying to be authenticated as `root`,
whose password is `mypassword`.

{{< tabs "access-catalog" >}}

{{< tab "Flink" >}}
```sql
CREATE CATALOG `my-catalog` WITH (
    'type' = 'paimon',
    -- ...
    'user' = 'root',
    'password' = 'mypassword'
);
```
{{< /tab >}}

{{< /tabs >}}

## Creating Users

You must be authenticated as a user with `ADMIN` privilege (for example, `root`) to perform this operation.

Do the following steps to create a user in the privilege system.

{{< tabs "create-users" >}}

{{< tab "Flink 1.18+" >}}
Run the following Flink SQL.
```sql
-- use the catalog where you want to create a user
-- you must be authenticated as a user with ADMIN privilege in this catalog
USE CATALOG `my-catalog`;

-- create a user authenticated by the specified password
-- change 'user' and 'password' to the username and password you want
CALL sys.create_privileged_user('user', 'password');
```
{{< /tab >}}

{{< /tabs >}}

## Dropping Users

You must be authenticated as a user with `ADMIN` privilege (for example, `root`) to perform this operation.

Do the following steps to drop a user in the privilege system.

{{< tabs "drop-users" >}}

{{< tab "Flink 1.18+" >}}
Run the following Flink SQL.
```sql
-- use the catalog where you want to drop a user
-- you must be authenticated as a user with ADMIN privilege in this catalog
USE CATALOG `my-catalog`;

-- change 'user' to the username you want to drop
CALL sys.drop_privileged_user('user');
```
{{< /tab >}}

{{< /tabs >}}

## Granting Privileges to Users

You must be authenticated as a user with `ADMIN` privilege (for example, `root`) to perform this operation.

Do the following steps to grant a user with privilege in the privilege system.

{{< tabs "grant-to-users" >}}

{{< tab "Flink 1.18+" >}}
Run the following Flink SQL.
```sql
-- use the catalog where you want to drop a user
-- you must be authenticated as a user with ADMIN privilege in this catalog
USE CATALOG `my-catalog`;

-- you can change 'user' to the username you want, and 'SELECT' to other privilege you want
-- grant 'user' with privilege 'SELECT' on the whole catalog
CALL sys.grant_privilege_to_user('user', 'SELECT');
-- grant 'user' with privilege 'SELECT' on database my_db
CALL sys.grant_privilege_to_user('user', 'SELECT', 'my_db');
-- grant 'user' with privilege 'SELECT' on table my_db.my_tbl
CALL sys.grant_privilege_to_user('user', 'SELECT', 'my_db', 'my_tbl');
```
{{< /tab >}}

{{< /tabs >}}

## Revoking Privileges to Users

You must be authenticated as a user with `ADMIN` privilege (for example, `root`) to perform this operation.

Do the following steps to revoke a privilege from user in the privilege system.

{{< tabs "revoke-from-users" >}}

{{< tab "Flink 1.18+" >}}
Run the following Flink SQL.
```sql
-- use the catalog where you want to drop a user
-- you must be authenticated as a user with ADMIN privilege in this catalog
USE CATALOG `my-catalog`;

-- you can change 'user' to the username you want, and 'SELECT' to other privilege you want
-- revoke 'user' with privilege 'SELECT' on the whole catalog
CALL sys.revoke_privilege_from_user('user', 'SELECT');
-- revoke 'user' with privilege 'SELECT' on database my_db
CALL sys.revoke_privilege_from_user('user', 'SELECT', 'my_db');
-- revoke 'user' with privilege 'SELECT' on table my_db.my_tbl
CALL sys.revoke_privilege_from_user('user', 'SELECT', 'my_db', 'my_tbl');
```
{{< /tab >}}

{{< /tabs >}}
