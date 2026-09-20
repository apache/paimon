---
title: "Catalog API"
sidebar_position: 2
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

# Catalog API

Use `Catalog` to manage databases and tables, and to load a `Table` for reading or writing.
Catalog configuration and table options have different scopes: configure the warehouse and metastore
on the catalog; put storage and read/write behavior in the table schema's options.

## Set up the examples

Add the [Java dependency](java-api#dependency) and save the
[`CreateCatalog` helper](java-api#create-catalog). The snippets below are method-body fragments.
Place the operations you want to run inside this catalog scope:

```java
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.PropertyChange;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import java.util.Arrays;
import java.util.List;

public class CatalogExample {

    public static void main(String[] args) throws Exception {
        try (Catalog catalog = CreateCatalog.createFilesystemCatalog()) {
            // Insert the relevant snippets here.
        }
    }
}
```

The `ignoreIfExists` and `ignoreIfNotExists` flags handle an existing or missing object, respectively.
They do not suppress other validation errors. Use `false` when an unexpected catalog state should
fail the operation. The examples propagate exceptions; applications can handle the corresponding
`Catalog.*Exception` at their error-handling boundary.

## Manage databases

### Create Database

```java
catalog.createDatabase("my_db", false);
```

### Determine Whether Database Exists

```java
boolean exists = catalog.databaseExists("my_db");
```

### List Databases

```java
List<String> databases = catalog.listDatabases();
```

### Alter Database

Use `PropertyChange` for database properties. Hive, JDBC, and REST catalogs support this operation;
the filesystem catalog does not. Run this fragment with a suitable catalog, such as one created
with `CreateCatalog.createHiveCatalog()`, and an existing database.

```java
List<PropertyChange> changes = Arrays.asList(
        PropertyChange.setProperty("owner", "analytics"),
        PropertyChange.removeProperty("obsolete-property"));
catalog.alterDatabase("my_db", changes, false);
```

## Manage tables

### Create Table

This is the same schema used by the Java reading and writing examples. Create `my_db` first.
For partitioned tables with fixed buckets, include partition columns in the primary key.
Tables that update a key across partitions require a different layout; see
[cross-partition upserts](../primary-key-table/data-distribution#cross-partitions-upsert).

```java
Identifier identifier = Identifier.create("my_db", "my_table");
Schema schema = Schema.newBuilder()
        .column("f0", DataTypes.STRING().notNull())
        .column("f1", DataTypes.INT())
        .primaryKey("f0")
        .option("bucket", "2")
        .build();
catalog.createTable(identifier, schema, false);
```

### Get Table

```java
Table table = catalog.getTable(Identifier.create("my_db", "my_table"));
```

Use the returned table with [Java Reads](java-reading) or [Java Writes](java-writing).

### Determine Whether Table Exists

```java
boolean exists = catalog.tableExists(Identifier.create("my_db", "my_table"));
```

### List Tables

```java
List<String> tables = catalog.listTables("my_db");
```

### Rename Table

```java
catalog.renameTable(
        Identifier.create("my_db", "my_table"),
        Identifier.create("my_db", "renamed_table"),
        false);
```

Subsequent operations must use the new identifier.

## Alter Table

Pass one `SchemaChange` or an ordered list of changes to `catalog.alterTable`. The following example
uses a separate table so that schema changes do not invalidate the Java read/write walkthrough.

### Create a table for schema changes

```java
Identifier alterIdentifier = Identifier.create("my_db", "schema_example");
Schema alterSchema = Schema.newBuilder()
        .column("id", DataTypes.STRING().notNull())
        .column("region", DataTypes.STRING().notNull())
        .column("amount", DataTypes.INT().notNull())
        .column("description", DataTypes.STRING())
        .column("obsolete", DataTypes.STRING())
        .column("details", DataTypes.ROW(
                new DataField(0, "city", DataTypes.STRING().notNull())))
        .primaryKey("id", "region")
        .partitionKeys("region")
        .option("bucket", "2")
        .option("snapshot.num-retained.max", "20")
        .build();
catalog.createTable(alterIdentifier, alterSchema, false);
```

### Apply schema and option changes

```java
List<SchemaChange> changes = Arrays.asList(
        SchemaChange.setOption("snapshot.time-retained", "2h"),
        SchemaChange.removeOption("snapshot.num-retained.max"),
        SchemaChange.addColumn("note", DataTypes.STRING(), "Optional note",
                SchemaChange.Move.after("note", "description")),
        SchemaChange.renameColumn("description", "description_text"),
        SchemaChange.dropColumn("obsolete"),
        SchemaChange.updateColumnComment(new String[] {"amount"}, "Order amount"),
        SchemaChange.updateColumnComment(new String[] {"details", "city"}, "City name"),
        SchemaChange.updateColumnType("amount", DataTypes.BIGINT()),
        SchemaChange.updateColumnPosition(SchemaChange.Move.first("amount")),
        SchemaChange.updateColumnNullability(new String[] {"amount"}, true),
        SchemaChange.updateColumnNullability(new String[] {"details", "city"}, true));
catalog.alterTable(alterIdentifier, changes, false);

// Load the updated schema before building new readers or writers.
Table updatedTable = catalog.getTable(alterIdentifier);
```

### Schema constraints

- Newly added columns must be nullable.
- Primary-key and partition-column types cannot be changed. Primary-key nullability cannot be changed.
- The example widens `INT` to `BIGINT` and relaxes `NOT NULL`. Tightening a nullable column to
  `NOT NULL` is disabled by default through `alter-column-null-to-not-null.disabled`.
- Nested field operations use a field path, for example `new String[] {"details", "city"}`.
  Supported type changes depend on the source and target types; replacing an entire row type is
  different from changing one nested field.

See [schema evolution](../flink/sql-alter) for supported changes and their constraints.

## Remove objects

Drop operations remove catalog objects and can remove their stored data. Run these separately from
the read/write walkthrough, using the identifier that currently exists.

### Drop Table

```java
catalog.dropTable(Identifier.create("my_db", "renamed_table"), false);
```

### Drop Database

Use `cascade=false` to reject dropping a nonempty database:

```java
catalog.dropDatabase("my_db", false, false);
```

Use `cascade=true` only when you intend to remove the database and its tables:

```java
catalog.dropDatabase("my_db", false, true);
```
