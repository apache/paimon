---
title: "Java CLI"
weight: 4
type: docs
aliases:
- /program-api/java-cli.html
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

# Java CLI

Paimon provides a Java-based command-line interface (CLI) for interacting with Paimon catalogs and tables.
The CLI supports reading, writing, and managing tables directly from the command line without requiring a compute engine like Flink or Spark.

## Installation

### Quick Install (from source)

```shell
cd paimon-cli
./bin/install.sh
```

This builds the fat jar and installs the `paimon` command to `/usr/local/bin`. You can specify a custom prefix:

```shell
./bin/install.sh --prefix ~/.local
```

### Development Mode

After building, use the bundled launcher directly without installing:

```shell
cd paimon-cli
mvn clean package -DskipTests
./bin/paimon --help
```

## Basic Usage

Before using the CLI, create a catalog configuration file. By default, the CLI looks for a `paimon.yaml` file in the current directory.

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

**Hive Catalog:**

```yaml
metastore: hive
warehouse: /path/to/warehouse
uri: thrift://localhost:9083
```

Run a command:

```shell
paimon <command> [options]
```

**Global Options:**

- `-c, --config PATH`: Path to catalog configuration file (default: `paimon.yaml`)
- `-h, --help`: Show help message and exit
- `-v, --version`: Show version and exit

## Catalog Commands

### list-databases

List all databases in the catalog.

```shell
paimon list-databases
```

Output:
```
default
mydb
analytics
```

### list-tables

List all tables in a database.

```shell
paimon list-tables mydb
```

Output:
```
orders
products
users
```

## Table Commands

### schema

Display table schema information including columns, types, primary keys, and partition keys.

```shell
paimon schema mydb.users
```

**Options:**

- `-f, --format`: Output format: `table` (default) or `json`

Output (table format):
```
Table: mydb.users

  Column                         Type                                Nullable
  ------------------------------  -----------------------------------  --------
  id                              INT NOT NULL                         NO
  name                            STRING                               YES
  age                             INT                                  YES
  city                            STRING                               YES

Primary keys: id
Partition keys: city

Options:
  bucket = 4
  changelog-producer = input
```

Output (json format):
```shell
paimon schema mydb.users --format json
```

```json
{"fields":[{"id":0,"name":"id","type":"INT NOT NULL"},{"id":1,"name":"name","type":"STRING"},{"id":2,"name":"age","type":"INT"},{"id":3,"name":"city","type":"STRING"}],"partitionKeys":["city"],"primaryKeys":["id"],"options":{"bucket":"4","changelog-producer":"input"},"comment":"User information table"}
```

The JSON output matches the `create-table --schema` input format, enabling round-trip `schema → create-table` workflows.

### read

Read data from a Paimon table and display it in tabular format.

```shell
paimon read mydb.users
```

**Options:**

- `-s, --select`: Select specific columns to read (comma-separated)
- `-w, --where`: Filter condition (SQL-like syntax)
- `-l, --limit`: Maximum number of rows to display (default: 100)
- `-f, --format`: Output format: `tsv` (default), `csv`, or `json`

**Examples:**

```shell
# Read with limit
paimon read mydb.users --limit 50

# Read specific columns
paimon read mydb.users --select id,name,age

# Combine select and limit
paimon read mydb.users -s id,name -l 10

# Output as JSON (one JSON object per line)
paimon read mydb.users --format json

# Output as CSV
paimon read mydb.users --format csv

# Filter with WHERE clause
paimon read mydb.users --where "age > 18 AND city = 'Beijing'"

# Combined filtering and projection
paimon read mydb.users -s name,age -w "age BETWEEN 20 AND 30" -l 50
```

**WHERE Syntax:**

The `--where` option supports SQL-like filter expressions:

| Operator | Example |
|---|---|
| `=`, `!=`, `<>` | `age = 25`, `city != 'Beijing'` |
| `<`, `>`, `<=`, `>=` | `age > 18`, `price <= 100.0` |
| `IS NULL`, `IS NOT NULL` | `email IS NOT NULL` |
| `IN (...)` | `city IN ('Beijing', 'Shanghai')` |
| `NOT IN (...)` | `status NOT IN (0, -1)` |
| `BETWEEN ... AND ...` | `age BETWEEN 20 AND 30` |
| `LIKE` | `name LIKE 'A%'` |
| `AND`, `OR` | `age > 18 AND city = 'Beijing'` |
| Parentheses | `(a = 1 OR b = 2) AND c > 0` |

{{< hint info >}}
The WHERE filter leverages Paimon's scan-level predicate pushdown for partition pruning and file skipping. This provides significant performance benefits for partitioned tables.
{{< /hint >}}

Output (default tsv format):
```
id	name	age	city
1	Alice	25	Beijing
2	Bob	30	Shanghai
3	Charlie	35	Guangzhou
```

Output (json format):
```json
{"id":1,"name":"Alice","age":25,"city":"Beijing"}
{"id":2,"name":"Bob","age":30,"city":"Shanghai"}
{"id":3,"name":"Charlie","age":35,"city":"Guangzhou"}
```

### write

Write data from a local file into a Paimon table.

```shell
paimon write data.csv mydb.users
```

**Options:**

- `-f, --format`: Input format: `csv` (default) or `json`
- `-d, --delimiter`: CSV delimiter (default: `,`)
- `-e, --encoding`: File encoding (default: `utf-8`)

**Examples:**

```shell
# Write from CSV
paimon write users.csv mydb.users

# Write from JSON
paimon write users.json mydb.users --format json

# Write from TSV (tab-separated)
paimon write data.tsv mydb.users --delimiter "\t"

# Specify encoding
paimon write data.csv mydb.users --encoding gbk
```

**CSV Format:**

The CSV file may optionally include a header row matching the table's column names. If a header is detected, it is skipped automatically.

```csv
id,name,age,city
1,Alice,25,Beijing
2,Bob,30,Shanghai
3,Charlie,35,Guangzhou
```

**JSON Format:**

The JSON file must be an array of objects with keys matching the table column names.

```json
[
  {"id": 1, "name": "Alice", "age": 25, "city": "Beijing"},
  {"id": 2, "name": "Bob", "age": 30, "city": "Shanghai"}
]
```

Output:
```
Successfully wrote 3 rows into 'mydb.users'.
```

### create-table

Create a new table from a schema JSON file.

```shell
paimon create-table mydb.users --schema schema.json
```

**Options:**

- `-s, --schema`: Path to schema JSON file — **Required**
- `-i, --ignore-if-exists`: Do not raise error if table already exists

**Schema JSON Format:**

```json
{
  "fields": [
    {"name": "id", "type": "BIGINT"},
    {"name": "name", "type": "STRING"},
    {"name": "age", "type": "INT"},
    {"name": "city", "type": "STRING"},
    {"name": "created_at", "type": "TIMESTAMP"}
  ],
  "partitionKeys": ["city"],
  "primaryKeys": ["id"],
  "options": {
    "bucket": "4",
    "changelog-producer": "input"
  },
  "comment": "User information table"
}
```

**Supported Data Types:**

| Type | Description |
|---|---|
| `BOOLEAN` | Boolean value |
| `TINYINT`, `SMALLINT`, `INT`, `BIGINT` | Integer types |
| `FLOAT`, `DOUBLE` | Floating point types |
| `DECIMAL(p, s)` | Fixed-point decimal |
| `CHAR(n)`, `VARCHAR(n)`, `STRING` | String types |
| `DATE` | Date without time |
| `TIMESTAMP`, `TIMESTAMP(p)` | Timestamp with precision |
| `BINARY`, `VARBINARY`, `BYTES` | Binary types |

### drop-table

Drop a table from the catalog.

```shell
paimon drop-table mydb.old_table
```

**Options:**

- `-i, --ignore-if-not-exists`: Do not raise error if table does not exist

Output:
```
Table 'mydb.old_table' dropped successfully.
```

### rename-table

Rename a table in the catalog.

```shell
paimon rename-table mydb.old_name mydb.new_name
```

Output:
```
Table 'mydb.old_name' renamed to 'mydb.new_name' successfully.
```

### alter-table

Alter a table's schema or options. Supports multiple sub-actions.

**Set/Remove Options:**

```shell
# Set a table option
paimon alter-table mydb.users set-option --key bucket --value 8

# Remove a table option
paimon alter-table mydb.users remove-option --key changelog-producer
```

**Column Operations:**

```shell
# Add a new column
paimon alter-table mydb.users add-column --name email --type STRING

# Add column with comment and position
paimon alter-table mydb.users add-column --name score --type DOUBLE --comment "User score" --after age

# Add column at the beginning
paimon alter-table mydb.users add-column --name row_id --type BIGINT --first

# Drop a column
paimon alter-table mydb.users drop-column --name old_field

# Rename a column
paimon alter-table mydb.users rename-column --name city --new-name region

# Alter column type or comment
paimon alter-table mydb.users alter-column --name age --type BIGINT
paimon alter-table mydb.users alter-column --name age --comment "User age in years"
paimon alter-table mydb.users alter-column --name age --after name
```

**Update Table Comment:**

```shell
paimon alter-table mydb.users update-comment --comment "Updated user table"
```

**Options:**

- `-i, --ignore-if-not-exists`: Do not raise error if table does not exist

### snapshot

Display the latest snapshot information of a table as JSON.

```shell
paimon snapshot mydb.users
```

Output:
```json
{"version":3,"id":5,"schemaId":0,"baseManifestList":"manifest-list-...","deltaManifestList":"manifest-list-...","changelogManifestList":null,"commitUser":"...","commitIdentifier":4,"commitKind":"APPEND","timeMillis":1700000000000,"logOffsets":{},"totalRecordCount":1000,"deltaRecordCount":200,"changelogRecordCount":0,"watermark":-9223372036854775808}
```

### list-partitions

List all partitions of a table with summary statistics.

```shell
paimon list-partitions mydb.events
```

Output:
```
Partition            RecordCount    FileSizeInBytes    FileCount    LastFileCreationTime
dt=2024-01-01       15000          4521983            3            2024-01-02T00:05:00
dt=2024-01-02       22000          6812045            5            2024-01-03T00:05:00
dt=2024-01-03       18500          5604211            4            2024-01-04T00:05:00
```

### expire-snapshots

Expire old snapshots to reclaim storage.

```shell
paimon expire-snapshots mydb.users
```

**Options:**

- `--retain-max`: Maximum number of snapshots to retain (default: 50)
- `--retain-min`: Minimum number of snapshots to retain (default: 10)

```shell
paimon expire-snapshots mydb.users --retain-max 20 --retain-min 5
```

### tag

Manage table tags (named snapshots).

```shell
# Create a tag from the latest snapshot
paimon tag mydb.users create --name release-v1

# Create a tag from a specific snapshot
paimon tag mydb.users create --name release-v1 --snapshot 5

# Delete a tag
paimon tag mydb.users delete --name release-v1

# List all tags
paimon tag mydb.users list
```

### rollback

Roll back a table to a previous snapshot or tag.

```shell
# Rollback to a snapshot
paimon rollback mydb.users --snapshot 5

# Rollback to a tag
paimon rollback mydb.users --tag release-v1
```

### explain

Show the scan plan of a query without reading any data files. Useful for previewing the pruning effect of a predicate.

```shell
paimon explain mydb.events
```

**Options:**

- `-s, --select`: Columns to project (comma-separated)
- `-w, --where`: Filter condition (SQL-like syntax)
- `-l, --limit`: Row limit to push down

**Examples:**

```shell
# Full table scan plan
paimon explain mydb.events

# Push filter and projection
paimon explain mydb.events --where "dt = '2024-01-01' AND id > 100" -s dt,id,val
```

Output:
```
== Paimon Scan Plan ==
Table:              mydb.events (PK, HASH_FIXED)
Snapshot:           5  (schema 0)
Predicate:          dt = '2024-01-01' AND id > 100
Projection:         [dt, id, val]
Limit:              <none>

Splits:             3
  raw-convertible:  3 / 3
  with DV:          0 / 3
  files/split:      min=1  max=2  avg=1.33
  size/split:       min=2.6 KiB  p50=4.1 KiB  p95=5.2 KiB  max=5.2 KiB
Files:              4
Total size:         15.1 KiB
Estimated rows:     150   (merged: 148)
Level histogram:    L0=2  L1=2
Deletion files:     0
Partitions hit:     1
Buckets hit:        3
```

### full-text-search

Perform full-text search on a table column using a Tantivy index.

```shell
paimon full-text-search mydb.articles --column content --query "paimon lake"
```

**Options:**

- `-c, --column`: Text column to search on (required)
- `-q, --query`: Query text to search for (required)
- `-l, --limit`: Maximum results (default: 10)
- `-s, --select`: Columns to display (comma-separated)
- `-f, --format`: Output format: `tsv` (default) / `csv` / `json`

{{< hint info >}}
Requires `paimon-tantivy-index` on the classpath. Place the jar in the `plugins/` directory.
{{< /hint >}}

### sql

Execute SQL queries on Paimon tables. Supports one-shot mode and interactive REPL.

**One-shot mode:**

```shell
paimon sql "SELECT * FROM mydb.users WHERE age > 18 LIMIT 10"
paimon sql "SELECT id, name FROM mydb.users ORDER BY age DESC LIMIT 5"
paimon sql "SHOW DATABASES"
```

**Interactive REPL:**

```shell
paimon sql
```

```
Paimon SQL (type 'help' for usage, 'exit' to quit)

paimon> USE mydb;
Using database 'mydb'.

paimon> SHOW TABLES;
orders
users

paimon> SELECT id, name, age FROM users
     > WHERE age > 20
     > ORDER BY age DESC
     > LIMIT 5;
id	name	age
3	Charlie	35
2	Bob	30
5	Eve	28
(3 rows)

paimon> exit
Bye!
```

**Supported SQL:**

```sql
SELECT [col1, col2, ... | *] FROM [db.]table [WHERE ...] [ORDER BY col [ASC|DESC], ...] [LIMIT n]
SHOW DATABASES
SHOW TABLES [IN database]
USE database
```

## Database Commands

### create-database

Create a new database.

```shell
paimon create-database mydb
```

**Options:**

- `-i, --ignore-if-exists`: Do not raise error if database already exists

Output:
```
Database 'mydb' created successfully.
```

### drop-database

Drop an existing database.

```shell
paimon drop-database mydb
```

**Options:**

- `-i, --ignore-if-not-exists`: Do not raise error if database does not exist
- `--cascade`: Drop all tables in the database before dropping it

**Examples:**

```shell
# Drop empty database
paimon drop-database mydb

# Drop database with all tables
paimon drop-database mydb --cascade

# Ignore if not exists
paimon drop-database mydb -i
```

Output:
```
Database 'mydb' dropped successfully.
```

### get-database

Show database details including options and comment.

```shell
paimon get-database mydb
```

Output:
```json
{"name":"mydb","options":{"owner":"alice"},"comment":"My analytics database"}
```

### alter-database

Alter database properties.

```shell
# Set properties
paimon alter-database mydb --set owner=alice,team=data

# Remove properties
paimon alter-database mydb --remove old_key1,old_key2

# Combine set and remove
paimon alter-database mydb --set owner=bob --remove deprecated_key
```

**Options:**

- `--set`: Properties to set (comma-separated key=value pairs)
- `--remove`: Properties to remove (comma-separated keys)
- `-i, --ignore-if-not-exists`: Do not raise error if database does not exist

## SPI Extension

The Java CLI uses Java SPI (ServiceLoader) for command discovery, making it easy for downstream projects to extend with custom commands.

### Implementing a Custom Command

```java
import org.apache.paimon.cli.Command;
import org.apache.paimon.cli.CommandContext;

public class MyCommand implements Command {

    @Override
    public String name() {
        return "my-command";
    }

    @Override
    public String description() {
        return "My custom command";
    }

    @Override
    public void execute(CommandContext ctx, String[] args) throws Exception {
        // Your command logic here
        System.out.println("Hello from my custom command!");
    }

    @Override
    public String usage() {
        return "Usage: paimon my-command [options]\n\nDescription of my command.";
    }
}
```

### Registering the Command

Create a file at `META-INF/services/org.apache.paimon.cli.Command` in your project's resources:

```
com.mycompany.MyCommand
```

### Using the Extension

Place your jar on the classpath alongside paimon-cli:

```shell
java -cp "paimon-cli.jar:my-extension.jar" org.apache.paimon.cli.PaimonCli my-command
```

### Customizing the Entry Point

For a fully branded CLI, subclass `PaimonCli`:

```java
import org.apache.paimon.cli.PaimonCli;

public class MyCli extends PaimonCli {

    @Override
    protected String programName() {
        return "mycli";
    }

    @Override
    protected String banner() {
        return "MyCLI v1.0 - powered by Apache Paimon";
    }

    public static void main(String[] args) throws Exception {
        new MyCli().run(args);
    }
}
```

## Filesystem Support

The CLI bundles OSS and S3 filesystem support out of the box — no extra downloads needed. Just configure `paimon.yaml` and use it.

### Aliyun OSS

```yaml
metastore: filesystem
warehouse: oss://my-bucket/warehouse

fs.oss.endpoint: oss-cn-hangzhou.aliyuncs.com
fs.oss.accessKeyId: your-access-key-id
fs.oss.accessKeySecret: your-access-key-secret
```

Or nested YAML style:

```yaml
metastore: filesystem
warehouse: oss://my-bucket/warehouse

fs:
  oss:
    endpoint: oss-cn-hangzhou.aliyuncs.com
    accessKeyId: your-access-key-id
    accessKeySecret: your-access-key-secret
```

Then use commands as usual:

```shell
paimon list-databases
paimon read mydb.users --limit 10
```

### Amazon S3

```yaml
metastore: filesystem
warehouse: s3://my-bucket/warehouse

s3.endpoint: s3.us-east-1.amazonaws.com
s3.access-key: your-access-key
s3.secret-key: your-secret-key
```

### HDFS

HDFS is supported natively. Set `HADOOP_HOME` and the CLI will automatically include the Hadoop classpath:

```shell
export HADOOP_HOME=/opt/hadoop
paimon list-databases
```

Or configure Hadoop options in `paimon.yaml`:

```yaml
metastore: filesystem
warehouse: hdfs://namenode:8020/warehouse
hadoop-conf-dir: /etc/hadoop/conf
```

### Additional Filesystems

For filesystems not bundled (Azure, GCS, etc.), place the corresponding `paimon-*` jar into the `plugins/` directory or set `PAIMON_PLUGINS_DIR`:

```shell
cp paimon-azure-*.jar plugins/
```

## Configuration Reference

The `paimon.yaml` file supports all Paimon catalog options as flat key-value pairs or nested YAML:

```yaml
# Flat style
metastore: filesystem
warehouse: /data/paimon

# Nested style (automatically flattened to dotted keys)
fs:
  oss:
    endpoint: oss-cn-hangzhou.aliyuncs.com
    accessKeyId: your-key
    accessKeySecret: your-secret
```

Both styles above produce the same result. Nested keys are flattened with `.` separator (e.g., `fs.oss.endpoint`).

### Common Options

| Option | Description |
|---|---|
| `metastore` | Catalog type: `filesystem`, `hive`, or `rest` |
| `warehouse` | Warehouse path (local, `oss://`, `s3://`, `hdfs://`) |
| `fs.oss.endpoint` | Aliyun OSS endpoint |
| `fs.oss.accessKeyId` | Aliyun OSS access key ID |
| `fs.oss.accessKeySecret` | Aliyun OSS access key secret |
| `s3.endpoint` | S3 endpoint |
| `s3.access-key` | S3 access key |
| `s3.secret-key` | S3 secret key |
| `hadoop-conf-dir` | Path to Hadoop configuration directory |
| `uri` | Hive metastore URI (for Hive catalog) |
