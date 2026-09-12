---
title: "CLI: Catalogs and Databases"
sidebar_label: "Catalogs and Databases"
description: "List databases and tables, inspect database properties, and create or change databases."
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

# CLI: Catalogs and Databases

List databases and tables, inspect database properties, and create or change databases. These commands use the catalog configuration described in [CLI basic usage](./cli#basic-usage).

## Catalog Commands

### Catalog List DBs

List all databases in the catalog.

```shell
paimon catalog list-dbs
```

Output:
```
default
mydb
analytics
```

## Database Commands

### DB Get

Get and display database information in JSON format.

```shell
paimon db get mydb
```

Output:
```json
{
  "name": "mydb",
  "options": {}
}
```

### DB Create

Create a new database.

```shell
# Create a simple database
paimon db create mydb

# Create with properties
paimon db create mydb -p '{"key1": "value1", "key2": "value2"}'

# Create and ignore if already exists
paimon db create mydb -i
```

### DB Drop

Drop an existing database.

```shell
# Drop a database
paimon db drop mydb

# Drop and ignore if not exists
paimon db drop mydb -i

# Drop with all tables (cascade)
paimon db drop mydb --cascade
```

### DB Alter

Alter database properties by setting or removing properties.

```shell
# Set properties
paimon db alter mydb --set '{"key1": "value1", "key2": "value2"}'

# Remove properties
paimon db alter mydb --remove key1 key2

# Set and remove properties in one command
paimon db alter mydb --set '{"key1": "new_value"}' --remove key2
```

### DB List Tables

List all tables in a database.

```shell
paimon db list-tables mydb
```

Output:
```
orders
products
users
```
