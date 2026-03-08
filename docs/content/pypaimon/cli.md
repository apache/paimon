---
title: "Command Line Interface"
weight: 99
type: docs
aliases:
- /pypaimon/cli.html
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

PyPaimon provides a command-line interface (CLI) for interacting with Paimon catalogs and tables. 
The CLI allows you to read data from Paimon tables directly from the command line.

## Installation

The CLI is installed automatically when you install PyPaimon:

```shell
pip install pypaimon
```

After installation, the `paimon` command will be available in your terminal.

## Configuration

Before using the CLI, you need to create a catalog configuration file. 
By default, the CLI looks for a `paimon.yaml` file in the current directory.

### Create Configuration File

Create a `paimon.yaml` file with your catalog settings:

**Filesystem Catalog:**

```yaml
metastore: filesystem
warehouse: /path/to/warehouse
```

**REST Catalog:**

```yaml
metastore: rest
uri: http://localhost:8080
warehouse: catalog_name
```

## Usage

### Basic Syntax

```shell
paimon [OPTIONS] COMMAND [ARGS]...
```

### Global Options

- `-c, --config PATH`: Path to catalog configuration file (default: `paimon.yaml`)
- `--help`: Show help message and exit

## Commands

### Table Read

Read data from a Paimon table and display it in a formatted table.

#### Basic Usage

```shell
paimon table read DATABASE.TABLE
```

**Example:**

```shell
paimon table read mydb.users
```

Output:
```
 id    name  age      city
  1   Alice   25   Beijing
  2     Bob   30  Shanghai
  3 Charlie   35 Guangzhou
  4   David   28  Shenzhen
  5     Eve   32  Hangzhou
```

#### Limit Results

Use the `-l` or `--limit` option (default 100) to limit the number of rows displayed:

```shell
paimon table read DATABASE.TABLE -l 10
```

**Example:**

```shell
paimon table read mydb.users -l 2
```

Output:
```
 id  name  age     city
  1 Alice   25  Beijing
  2   Bob   30 Shanghai
```

### Table Get

Get and display table schema information in JSON format. The output format is the same as the schema JSON format used in table create, making it easy to export and reuse table schemas.

```shell
paimon table get DATABASE.TABLE
```

**Example:**

```shell
paimon table get mydb.users
```

Output:
```json
{
  "fields": [
    {"id": 0, "name": "user_id", "type": "BIGINT"},
    {"id": 1, "name": "username", "type": "STRING"},
    {"id": 2, "name": "email", "type": "STRING"},
    {"id": 3, "name": "age", "type": "INT"},
    {"id": 4, "name": "city", "type": "STRING"},
    {"id": 5, "name": "created_at", "type": "TIMESTAMP"},
    {"id": 6, "name": "is_active", "type": "BOOLEAN"}
  ],
  "partitionKeys": ["city"],
  "primaryKeys": ["user_id"],
  "options": {
    "bucket": "4",
    "changelog-producer": "input"
  },
  "comment": "User information table"
}
```

**Note:** The output JSON can be saved to a file and used directly with the `table create` command to recreate the table structure.

### Table Create

Create a new Paimon table with a schema defined in a JSON file. The schema JSON format is the same as the output from `table get`, ensuring consistency and easy schema reuse.

```shell
paimon table create DATABASE.TABLE --schema SCHEMA_FILE
```

**Options:**

- `--schema, -s`: Path to schema JSON file - **Required**
- `--ignore-if-exists, -i`: Do not raise error if table already exists

The schema JSON file follows the same format as output by `table get`:

**Field Properties:**

- `id`: Field ID (integer, typically starts from 0) - **Required**
- `name`: Field name - **Required**
- `type`: Field data type (e.g., `INT`, `BIGINT`, `STRING`, `TIMESTAMP`, `DECIMAL(10,2)`) - **Required**
- `description`: Optional field description

**Schema Properties:**

- `fields`: List of field definitions - **Required**
- `partitionKeys`: List of partition key column names
- `primaryKeys`: List of primary key column names
- `options`: Table options as key-value pairs
- `comment`: Table comment

**Example Workflow:**

1. Export schema from an existing table:
   ```shell
   paimon table get mydb.users > users_schema.json
   ```

2. Create a new table with the same schema:
   ```shell
   paimon table create mydb.users_copy --schema users_schema.json
   ```