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

Get and display detailed schema information about a Paimon table.

```shell
paimon table get DATABASE.TABLE
```

**Example:**

```shell
paimon table get mydb.users
```

Output:
```
================================================================================
Table: mydb.users
================================================================================

Schema ID: 0

Comment: User information table

================================================================================
Fields:
================================================================================
ID    Name              Type                 Nullable   Description
----- ------------------ -------------------- --------- --------------------------
0     user_id           BIGINT               YES        
1     username          STRING               YES        
2     email             STRING               YES        
3     age               INT                  YES        
4     city              STRING               YES        
5     created_at        TIMESTAMP(6)         YES        
6     is_active         BOOLEAN              YES        

================================================================================
Partition Keys: city

================================================================================
Primary Keys: user_id

================================================================================
Table Options:
================================================================================
  bucket                                   = 4
  changelog-producer                       = input
```

### Table Create

Create a new Paimon table with a schema defined in a JSON or YAML file.

```shell
paimon table create DATABASE.TABLE --schema-file SCHEMA_FILE
```

**Options:**

- `--schema-file, -s`: Path to schema definition file (JSON or YAML) - **Required**
- `--ignore-if-exists, -i`: Do not raise error if table already exists

The schema file should be a JSON or YAML file with the following structure:

**JSON Example (`schema.json`):**

```json
{
  "fields": [
    {"name": "user_id", "type": "BIGINT"},
    {"name": "username", "type": "STRING"},
    {"name": "email", "type": "STRING"},
    {"name": "age", "type": "INT"},
    {"name": "city", "type": "STRING"},
    {"name": "created_at", "type": "TIMESTAMP"},
    {"name": "is_active", "type": "BOOLEAN"}
  ],
  "partition_keys": ["city"],
  "primary_keys": ["user_id"],
  "options": {
    "bucket": "4",
    "changelog-producer": "input"
  },
  "comment": "User information table"
}
```

**YAML Example (`schema.yaml`):**

```yaml
fields:
  - name: user_id
    type: BIGINT
  - name: username
    type: STRING
  - name: email
    type: STRING
  - name: age
    type: INT
  - name: city
    type: STRING
  - name: created_at
    type: TIMESTAMP
  - name: is_active
    type: BOOLEAN

partition_keys:
  - city

primary_keys:
  - user_id

options:
  bucket: "4"
  changelog-producer: input

comment: User information table
```
