---
title: "Catalog API"
weight: 3
type: docs
aliases:
- /api/catalog-api.html
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

## Create Database

You can use the catalog to create databases. The created databases are persistence in the file system.

```java
import org.apache.paimon.catalog.Catalog;

public class CreateDatabase {

    public static void main(String[] args) {
        try {
            Catalog catalog = CreateCatalog.createFilesystemCatalog();
            catalog.createDatabase("my_db", false);
        } catch (Catalog.DatabaseAlreadyExistException e) {
            // do something
        }
    }
}
```

## Determine Whether Database Exists

You can use the catalog to determine whether the database exists

```java
import org.apache.paimon.catalog.Catalog;

public class DatabaseExists {

    public static void main(String[] args) {
        Catalog catalog = CreateCatalog.createFilesystemCatalog();
        boolean exists = catalog.databaseExists("my_db");
    }
}
```

## List Databases

You can use the catalog to list databases.

```java
import org.apache.paimon.catalog.Catalog;

import java.util.List;

public class ListDatabases {

    public static void main(String[] args) {
        Catalog catalog = CreateCatalog.createFilesystemCatalog();
        List<String> databases = catalog.listDatabases();
    }
}
```

## Drop Database

You can use the catalog to drop databases.

```java
import org.apache.paimon.catalog.Catalog;

public class DropDatabase {

    public static void main(String[] args) {
        try {
            Catalog catalog = CreateCatalog.createFilesystemCatalog();
            catalog.dropDatabase("my_db", false, true);
        } catch (Catalog.DatabaseNotEmptyException e) {
            // do something
        } catch (Catalog.DatabaseNotExistException e) {
            // do something
        }
    }
}
```

## Alter Database

You can use the catalog to alter databases.(ps: only support hive and jdbc catalog)

```java
import java.util.ArrayList;
import org.apache.paimon.catalog.Catalog;

public class AlterDatabase {

    public static void main(String[] args) {
        try {
            Catalog catalog = CreateCatalog.createHiveCatalog();
            List<DatabaseChange> changes = new ArrayList<>();
            changes.add(DatabaseChange.setProperty("k1", "v1"));
            changes.add(DatabaseChange.removeProperty("k2"));
            catalog.alterDatabase("my_db", changes, true);
        } catch (Catalog.DatabaseNotExistException e) {
            // do something
        }
    }
}
```

## Determine Whether Table Exists

You can use the catalog to determine whether the table exists

```java
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;

public class TableExists {

    public static void main(String[] args) {
        Identifier identifier = Identifier.create("my_db", "my_table");
        Catalog catalog = CreateCatalog.createFilesystemCatalog();
        boolean exists = catalog.tableExists(identifier);
    }
}
```

## List Tables

You can use the catalog to list tables.

```java
import org.apache.paimon.catalog.Catalog;

import java.util.List;

public class ListTables {

    public static void main(String[] args) {
        try {
            Catalog catalog = CreateCatalog.createFilesystemCatalog();
            List<String> tables = catalog.listTables("my_db");
        } catch (Catalog.DatabaseNotExistException e) {
            // do something
        }
    }
}
```

## Drop Table

You can use the catalog to drop table.

```java
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;

public class DropTable {

    public static void main(String[] args) {
        Identifier identifier = Identifier.create("my_db", "my_table");
        try {
            Catalog catalog = CreateCatalog.createFilesystemCatalog();
            catalog.dropTable(identifier, false);
        } catch (Catalog.TableNotExistException e) {
            // do something
        }
    }
}
```

## Rename Table

You can use the catalog to rename a table.

```java
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;

public class RenameTable {

    public static void main(String[] args) {
        Identifier fromTableIdentifier = Identifier.create("my_db", "my_table");
        Identifier toTableIdentifier = Identifier.create("my_db", "test_table");
        try {
            Catalog catalog = CreateCatalog.createFilesystemCatalog();
            catalog.renameTable(fromTableIdentifier, toTableIdentifier, false);
        } catch (Catalog.TableAlreadyExistException e) {
            // do something
        } catch (Catalog.TableNotExistException e) {
            // do something
        }
    }
}
```

## Alter Table

You can use the catalog to alter a table, but you need to pay attention to the following points.

- Column %s cannot specify NOT NULL in the %s table.
- Cannot update partition column type in the table.
- Cannot change nullability of primary key.
- If the type of the column is nested row type, update the column type is not supported.
- Update column to nested row type is not supported.

```java
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class AlterTable {

    public static void main(String[] args) {
        Identifier identifier = Identifier.create("my_db", "my_table");

        Map<String, String> options = new HashMap<>();
        options.put("bucket", "4");
        options.put("compaction.max.file-num", "40");

        Catalog catalog = CreateCatalog.createFilesystemCatalog();
        catalog.createDatabase("my_db", false);

        try {
            catalog.createTable(
                identifier,
                new Schema(
                    Lists.newArrayList(
                        new DataField(0, "col1", DataTypes.STRING(), "field1"),
                        new DataField(1, "col2", DataTypes.STRING(), "field2"),
                        new DataField(2, "col3", DataTypes.STRING(), "field3"),
                        new DataField(3, "col4", DataTypes.BIGINT(), "field4"),
                        new DataField(
                            4,
                            "col5",
                            DataTypes.ROW(
                                new DataField(
                                    5, "f1", DataTypes.STRING(), "f1"),
                                new DataField(
                                    6, "f2", DataTypes.STRING(), "f2"),
                                new DataField(
                                    7, "f3", DataTypes.STRING(), "f3")),
                            "field5"),
                        new DataField(8, "col6", DataTypes.STRING(), "field6")),
                    Lists.newArrayList("col1"), // partition keys
                    Lists.newArrayList("col1", "col2"), // primary key
                    options,
                    "table comment"),
                false);
        } catch (Catalog.TableAlreadyExistException e) {
            // do something
        } catch (Catalog.DatabaseNotExistException e) {
            // do something
        }

        // add option
        SchemaChange addOption = SchemaChange.setOption("snapshot.time-retained", "2h");
        // remove option
        SchemaChange removeOption = SchemaChange.removeOption("compaction.max.file-num");
        // add column
        SchemaChange addColumn = SchemaChange.addColumn("col1_after", DataTypes.STRING());
        // add a column after col1
        SchemaChange.Move after = SchemaChange.Move.after("col1_after", "col1");
        SchemaChange addColumnAfterField =
            SchemaChange.addColumn("col7", DataTypes.STRING(), "", after);
        // rename column
        SchemaChange renameColumn = SchemaChange.renameColumn("col3", "col3_new_name");
        // drop column
        SchemaChange dropColumn = SchemaChange.dropColumn("col6");
        // update column comment
        SchemaChange updateColumnComment =
            SchemaChange.updateColumnComment(new String[] {"col4"}, "col4 field");
        // update nested column comment
        SchemaChange updateNestedColumnComment =
            SchemaChange.updateColumnComment(new String[] {"col5", "f1"}, "col5 f1 field");
        // update column type
        SchemaChange updateColumnType = SchemaChange.updateColumnType("col4", DataTypes.DOUBLE());
        // update column position, you need to pass in a parameter of type Move
        SchemaChange updateColumnPosition =
            SchemaChange.updateColumnPosition(SchemaChange.Move.first("col4"));
        // update column nullability
        SchemaChange updateColumnNullability =
            SchemaChange.updateColumnNullability(new String[] {"col4"}, false);
        // update nested column nullability
        SchemaChange updateNestedColumnNullability =
            SchemaChange.updateColumnNullability(new String[] {"col5", "f2"}, false);

        SchemaChange[] schemaChanges =
            new SchemaChange[] {
                addOption,
                removeOption,
                addColumn,
                addColumnAfterField,
                renameColumn,
                dropColumn,
                updateColumnComment,
                updateNestedColumnComment,
                updateColumnType,
                updateColumnPosition,
                updateColumnNullability,
                updateNestedColumnNullability
            };
        try {
            catalog.alterTable(identifier, Arrays.asList(schemaChanges), false);
        } catch (Catalog.TableNotExistException e) {
            // do something
        } catch (Catalog.ColumnAlreadyExistException e) {
            // do something
        } catch (Catalog.ColumnNotExistException e) {
            // do something
        }
    }
}
```
