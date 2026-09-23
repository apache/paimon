---
title: "Command Line Interface"
description: "Use `paimon` to manage catalogs and tables, inspect data, and run queries from your terminal."
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

# Command Line Interface

Use `paimon` to manage catalogs and tables, inspect data, and run queries from your terminal. Start with the local walkthrough, then use the task references below for the full command options.

## Installation

The `paimon` command is included in the base package:

```shell
python -m pip install pypaimon
paimon --help
```

See [Installation](./installation) for environment setup and optional packages.

## Basic Usage

Commands read catalog options from `paimon.yaml` in the current directory. Pass
`-c` before the command to select a different configuration:

```shell
paimon -c paimon.yaml catalog list-dbs
paimon table read --help
```

For local storage, save the following as `paimon.yaml`. Use a fresh warehouse for
the walkthrough, or choose new database and table names if you run it again.

```yaml
metastore: filesystem
warehouse: /tmp/paimon-cli-warehouse
```

For a REST catalog, replace the configuration with your server and warehouse:

```yaml
metastore: rest
uri: http://localhost:8080
warehouse: catalog_name
```

## Try a local workflow

The following steps use the filesystem configuration above. They create a small
append table and import two rows without a catalog server.

Save this schema as `people-schema.json`:

```json
{
  "fields": [
    {"id": 0, "name": "id", "type": "BIGINT"},
    {"id": 1, "name": "name", "type": "STRING"}
  ],
  "options": {"bucket": "-1"}
}
```

Save the input rows as `people.json`:

```json
[
  {"id": 1, "name": "Alice"},
  {"id": 2, "name": "Bob"}
]
```

Run these commands from the directory containing the three files:

```shell
paimon db create demo --ignore-if-exists
paimon table create demo.people --schema people-schema.json
paimon table import demo.people --input people.json
paimon table read demo.people --select id,name --where "id = 2" --format json
```

The query returns the row with `id` 2 and `name` `Bob`. Inspect what you created:

```shell
paimon table get demo.people
paimon table snapshot demo.people
paimon table explain demo.people --where "id = 2"
```

## Table Commands

<span id="table-read"></span>
<span id="table-explain"></span>
<span id="table-get"></span>
<span id="table-snapshot"></span>
<span id="table-create"></span>
<span id="table-import"></span>
<span id="import-from-csv"></span>
<span id="import-from-json"></span>
<span id="important-notes"></span>
<span id="table-list-partitions"></span>
<span id="table-rename"></span>
<span id="table-full-text-search"></span>
<span id="table-drop"></span>
<span id="table-alter"></span>
<span id="basic-syntax"></span>
<span id="set-option"></span>
<span id="remove-option"></span>
<span id="add-column"></span>
<span id="drop-column"></span>
<span id="rename-column"></span>
<span id="alter-column"></span>
<span id="update-comment"></span>

| Task | Reference |
| --- | --- |
| Read rows, inspect a plan, or search text | [Query and inspect](./cli-query) |
| Show schemas, snapshots, and partitions | [Table metadata](./cli-query#table-get) |
| Create a table or import local files | [Create and change tables](./cli-tables) |
| Alter a schema or table options | [Table alter](./cli-tables#table-alter) |

## Catalog Commands

<span id="catalog-list-dbs"></span>

[List the databases](./cli-catalogs#catalog-list-dbs) available in the configured catalog.

## Database Commands

<span id="db-get"></span>
<span id="db-create"></span>
<span id="db-drop"></span>
<span id="db-alter"></span>
<span id="db-list-tables"></span>

[Create, inspect, change, or drop databases](./cli-catalogs#database-commands), and list their tables.

## Tag Commands

<span id="tag-create"></span>
<span id="tag-list"></span>
<span id="tag-get"></span>
<span id="tag-delete"></span>

[Create and inspect tags](./cli-versions#tag-commands) to retain a snapshot for future reads.

## Branch Commands

<span id="branch-create"></span>
<span id="branch-list"></span>
<span id="branch-delete"></span>
<span id="branch-rename"></span>
<span id="branch-fast-forward"></span>

[Manage branches](./cli-versions#branch-commands) to keep separate table histories.

## SQL Command

<span id="one-shot-query"></span>
<span id="interactive-repl"></span>

[Run a query or start the interactive SQL shell](./sql#sql-command). SQL requires
Python 3.10 or newer and the `sql` extra.
