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

## Basic Usage

Before using the CLI, you need to create a catalog configuration file. 
By default, the CLI looks for a `paimon.yaml` file in the current directory.

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

**Usage:**

```shell
paimon [OPTIONS] COMMAND [ARGS]...
```

- `-c, --config PATH`: Path to catalog configuration file (default: `paimon.yaml`)
- `--help`: Show help message and exit

## Table Commands

### Table Read

Read data from a Paimon table and display it in a formatted table.

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

Use the `-l` or `--limit` option (default 100) to limit the number of rows displayed:

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

Get and display table schema information in JSON format. The output format is the same as the schema JSON format used
in table create, making it easy to export and reuse table schemas.

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

Create a new Paimon table with a schema defined in a JSON file. The schema JSON format is the same as the output from
`table get`, ensuring consistency and easy schema reuse.

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

### Table Import

Import data from CSV or JSON files into an existing Paimon table. This is useful for bulk loading data from external sources.

**Options:**

- `--input, -i`: Path to input file (CSV or JSON format) - **Required**

**Supported Formats:**

- **CSV** (`.csv`): Comma-separated values file
- **JSON** (`.json`): JSON file with array of objects format

#### Import from CSV

The CSV file should have:
- A header row with column names matching the table schema
- Data types compatible with the table columns

```csv
id,name,age,city
1,Alice,25,Beijing
2,Bob,30,Shanghai
3,Charlie,35,Guangzhou
```

Output:
```
Successfully imported 3 rows into 'mydb.users'.
```

#### Import from JSON

The JSON file should be an array of objects with keys matching the table column names.

```json
[
  {"id": 1, "name": "Alice", "age": 25, "city": "Beijing"},
  {"id": 2, "name": "Bob", "age": 30, "city": "Shanghai"},
  {"id": 3, "name": "Charlie", "age": 35, "city": "Guangzhou"}
]
```

Output:
```
Successfully imported 3 rows into 'mydb.users'.
```

#### Important Notes

- The target table must exist before importing data
- Column names in the file must match the table schema
- Data types should be compatible with the table schema
- The import operation appends data to the existing table

### Table Drop

Drop a table from the catalog. This will permanently delete the table and all its data.

**Options:**

- `--ignore-if-not-exists, -i`: Do not raise error if table does not exist

```shell
paimon table drop mydb.old_table
```

Output:
```
Table 'mydb.old_table' dropped successfully.
```

**Warning:** This operation cannot be undone. All data in the table will be permanently deleted.

### Table Alter

Alter a table's schema or options. This command supports multiple sub-commands for different types of schema changes.

#### Basic Syntax

```shell
paimon table alter DATABASE.TABLE [--ignore-if-not-exists] SUBCOMMAND [OPTIONS]
```

**Global Options:**

- `--ignore-if-not-exists, -i`: Do not raise error if table does not exist

#### Set Option

Set a table option (key-value pair):

```shell
paimon table alter mydb.users set-option -k snapshot.num-retained-max -v 10
```

#### Remove Option

Remove a table option:

```shell
paimon table alter mydb.users remove-option -k snapshot.num-retained-max
```

#### Add Column

Add a new column to the table:

**Example:**

```shell
paimon table alter mydb.users add-column -n email -t STRING -c "User email address"
```

**Example with position (first):**

```shell
paimon table alter mydb.users add-column -n row_id -t BIGINT --first
```

**Example with position (after):**

```shell
paimon table alter mydb.users add-column -n email -t STRING --after name
```

#### Drop Column

Drop a column from the table:

```shell
paimon table alter mydb.users drop-column -n email
```

#### Rename Column

Rename an existing column:

```shell
paimon table alter mydb.users rename-column -n username -m user_name
```

#### Alter Column

Alter an existing column's type, comment, or position. Multiple changes can be specified in a single command.

**Change Column Type:**

```shell
paimon table alter mydb.users alter-column -n age -t BIGINT
```

**Change Column Comment:**

```shell
paimon table alter mydb.users alter-column -n age -c 'User age in years'
```

**Change Column Position:**

```shell
paimon table alter mydb.users alter-column -n age --first

paimon table alter mydb.users alter-column -n age --after name
```

**Multiple changes in one command:**

```shell
paimon table alter mydb.users alter-column -n age -t BIGINT -c 'User age in years'
```

#### Update Comment

```shell
paimon table alter mydb.users update-comment -c "Updated user information table"
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
paimon db drop mydb --ignore-if-not-exists

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
