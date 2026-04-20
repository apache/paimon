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

Read data from a Paimon table and display it in a tabular format.

```shell
paimon table read mydb.users
```

**Options:**

- `--select, -s`: Select specific columns to read (comma-separated)
- `--where, -w`: Filter condition in SQL-like syntax
- `--limit, -l`: Maximum number of results to display (default: 100)
- `--format, -f`: Output format: `table` (default) or `json`

**Examples:**

```shell
# Read with limit
paimon table read mydb.users -l 50

# Read specific columns
paimon table read mydb.users -s id,name,age

# Filter with WHERE clause
paimon table read mydb.users --where "age > 18"

# Combine select, where, and limit
paimon table read mydb.users -s id,name -w "age >= 20 AND city = 'Beijing'" -l 50

# Output as JSON (for programmatic use)
paimon table read mydb.users --format json
```

**WHERE Operators**

The `--where` option supports SQL-like filter expressions:

| Operator | Example |
|---|---|
| `=`, `!=`, `<>` | `name = 'Alice'` |
| `<`, `<=`, `>`, `>=` | `age > 18` |
| `IS NULL`, `IS NOT NULL` | `deleted_at IS NULL` |
| `IN (...)`, `NOT IN (...)` | `status IN ('active', 'pending')` |
| `BETWEEN ... AND ...` | `age BETWEEN 20 AND 30` |
| `LIKE` | `name LIKE 'A%'` |

Multiple conditions can be combined with `AND` and `OR` (AND has higher precedence). Parentheses are supported for grouping:

```shell
# AND condition
paimon table read mydb.users -w "age >= 20 AND age <= 30"

# OR condition
paimon table read mydb.users -w "city = 'Beijing' OR city = 'Shanghai'"

# Parenthesized grouping
paimon table read mydb.users -w "(age > 18 OR name = 'Bob') AND city = 'Beijing'"

# IN list
paimon table read mydb.users -w "city IN ('Beijing', 'Shanghai', 'Hangzhou')"

# BETWEEN
paimon table read mydb.users -w "age BETWEEN 25 AND 35"

# LIKE pattern
paimon table read mydb.users -w "name LIKE 'A%'"

# IS NULL / IS NOT NULL
paimon table read mydb.users -w "email IS NOT NULL"
```

Literal values are automatically cast to the appropriate Python type based on the table schema (e.g., `INT` fields cast to `int`, `DOUBLE` to `float`).

Output:
```
 id    name  age      city
  1   Alice   25   Beijing
  2     Bob   30  Shanghai
  3 Charlie   35 Guangzhou
  4   David   28  Shenzhen
  5     Eve   32  Hangzhou
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

### Table Snapshot

Get and display the latest snapshot information of a Paimon table in JSON format. The snapshot contains metadata about the current state of the table.

```shell
paimon table snapshot mydb.users
```

Output:
```json
{
  "version": 3,
  "id": 5,
  "schemaId": 1,
  "baseManifestList": "manifest-list-5-base-...",
  "deltaManifestList": "manifest-list-5-delta-...",
  "changelogManifestList": null,
  "totalRecordCount": 1000,
  "deltaRecordCount": 100,
  "changelogRecordCount": null,
  "commitUser": "user-123",
  "commitIdentifier": 1709123456789,
  "commitKind": "APPEND",
  "timeMillis": 1709123456789,
  "watermark": null,
  "statistics": null,
  "nextRowId": null
}
```

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

### Table List Partitions

List partitions of a Paimon table. Supports optional pattern filtering to match specific partitions.

```shell
paimon table list-partitions mydb.orders
```

**Options:**

- `--pattern, -p`: Partition name pattern to filter partitions
- `--format, -f`: Output format: `table` (default) or `json`

**Examples:**

```shell
# List all partitions
paimon table list-partitions mydb.orders

# List partitions matching a pattern
paimon table list-partitions mydb.orders --pattern "dt=2024*"

# Output as JSON (for programmatic use)
paimon table list-partitions mydb.orders --format json
```

Output:
```
              Partition  RecordCount  FileSizeInBytes  FileCount  LastFileCreationTime       UpdatedAt  UpdatedBy
dt=2024-01-01,region=us          500          1048576         10         1704067200000  1704153600000      admin
dt=2024-01-02,region=eu          300           524288          5         1704153600000  1704240000000      user1
dt=2024-01-03,region=us          200           262144          3         1704240000000  1704326400000      admin
```

### Table Rename

Rename a table in the catalog. Both source and target must be specified in `database.table` format.

```shell
paimon table rename mydb.old_name mydb.new_name
```

Output:
```
Table 'mydb.old_name' renamed to 'mydb.new_name' successfully.
```

**Note:** Both filesystem and REST catalogs support table rename. For filesystem catalogs, the rename is performed by renaming the underlying table directory.

### Table Full-Text Search

Perform full-text search on a Paimon table with a Tantivy full-text index and display matching rows.

```shell
paimon table full-text-search mydb.articles --column content --query "paimon lake"
```

**Options:**

- `--column, -c`: Text column to search on - **Required**
- `--query, -q`: Query text to search for - **Required**
- `--limit, -l`: Maximum number of results to return (default: 10)
- `--select, -s`: Select specific columns to display (comma-separated)
- `--format, -f`: Output format: `table` (default) or `json`

**Examples:**

```shell
# Basic full-text search
paimon table full-text-search mydb.articles -c content -q "paimon lake"

# Search with limit
paimon table full-text-search mydb.articles -c content -q "streaming data" -l 20

# Search with column projection
paimon table full-text-search mydb.articles -c content -q "paimon" -s "id,title,content"

# Output as JSON
paimon table full-text-search mydb.articles -c content -q "paimon" -f json
```

Output:
```
 id                                            content
  0  Apache Paimon is a streaming data lake platform
  2  Paimon supports real-time data ingestion and...
  4  Data lake platforms like Paimon handle large-...
```

**Note:** The table must have a Tantivy full-text index built on the target column. See [Global Index]({{< ref "append-table/global-index" >}}) for how to create full-text indexes.

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

## SQL Command

Execute SQL queries on Paimon tables directly from the command line. This feature is powered by pypaimon-rust and DataFusion.

**Prerequisites:**

```shell
pip install pypaimon[sql]
```

### One-Shot Query

Execute a single SQL query and display the result:

```shell
paimon sql "SELECT * FROM users LIMIT 10"
```

Output:
```
 id    name  age      city
  1   Alice   25   Beijing
  2     Bob   30  Shanghai
  3 Charlie   35 Guangzhou
```

**Options:**

- `--format, -f`: Output format: `table` (default) or `json`

**Examples:**

```shell
# Direct table name (uses default catalog and database)
paimon sql "SELECT * FROM users"

# Two-part: database.table
paimon sql "SELECT * FROM mydb.users"

# Query with filter and aggregation
paimon sql "SELECT city, COUNT(*) AS cnt FROM users GROUP BY city ORDER BY cnt DESC"

# Output as JSON
paimon sql "SELECT * FROM users LIMIT 5" --format json
```

### Interactive REPL

Start an interactive SQL session by running `paimon sql` without a query argument. The REPL supports arrow keys for line editing, and command history is persisted across sessions in `~/.paimon_history`.

```shell
paimon sql
```

Output:
```
    ____        _
   / __ \____ _(_)___ ___  ____  ____
  / /_/ / __ `/ / __ `__ \/ __ \/ __ \
 / ____/ /_/ / / / / / / / /_/ / / / /
/_/    \__,_/_/_/ /_/ /_/\____/_/ /_/

  Powered by pypaimon-rust + DataFusion
  Type 'help' for usage, 'exit' to quit.

paimon> SHOW DATABASES;
default
mydb

paimon> USE mydb;
Using database 'mydb'.

paimon> SHOW TABLES;
orders
users

paimon> SELECT count(*) AS cnt
     > FROM users
     > WHERE age > 18;
 cnt
  42
(1 row in 0.05s)

paimon> exit
Bye!
```

SQL statements end with `;` and can span multiple lines. The continuation prompt `     >` indicates that more input is expected.

**REPL Commands:**

| Command | Description |
|---|---|
| `USE <database>;` | Switch the default database |
| `SHOW DATABASES;` | List all databases |
| `SHOW TABLES;` | List tables in the current database |
| `SELECT ...;` | Execute a SQL query |
| `help` | Show usage information |
| `exit` / `quit` | Exit the REPL |

For more details on SQL syntax and the Python API, see [SQL Query]({{< ref "pypaimon/sql" >}}).
