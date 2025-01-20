/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.catalog;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.TableType;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.object.ObjectTable;
import org.apache.paimon.table.system.AllTableOptionsTable;
import org.apache.paimon.table.system.CatalogOptionsTable;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.CoreOptions.PARTITION_DEFAULT_NAME;
import static org.apache.paimon.CoreOptions.PARTITION_GENERATE_LEGCY_NAME;
import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.catalog.Catalog.SYSTEM_DATABASE_NAME;
import static org.apache.paimon.catalog.Catalog.TABLE_DEFAULT_OPTION_PREFIX;
import static org.apache.paimon.options.OptionsUtils.convertToPropertiesPrefixKey;
import static org.apache.paimon.table.system.AllTableOptionsTable.ALL_TABLE_OPTIONS;
import static org.apache.paimon.table.system.CatalogOptionsTable.CATALOG_OPTIONS;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Utils for {@link Catalog}. */
public class CatalogUtils {

    public static Path path(String warehouse, String database, String table) {
        return new Path(String.format("%s/%s.db/%s", warehouse, database, table));
    }

    public static String stringifyPath(String warehouse, String database, String table) {
        return String.format("%s/%s.db/%s", warehouse, database, table);
    }

    public static String warehouse(String path) {
        return new Path(path).getParent().getParent().toString();
    }

    public static String database(Path path) {
        return SchemaManager.identifierFromPath(path.toString(), false).getDatabaseName();
    }

    public static String database(String path) {
        return SchemaManager.identifierFromPath(path, false).getDatabaseName();
    }

    public static String table(Path path) {
        return SchemaManager.identifierFromPath(path.toString(), false).getObjectName();
    }

    public static String table(String path) {
        return SchemaManager.identifierFromPath(path, false).getObjectName();
    }

    public static Map<String, String> tableDefaultOptions(Map<String, String> options) {
        return convertToPropertiesPrefixKey(options, TABLE_DEFAULT_OPTION_PREFIX);
    }

    public static boolean isSystemDatabase(String database) {
        return SYSTEM_DATABASE_NAME.equals(database);
    }

    /** Validate database cannot be a system database. */
    public static void checkNotSystemDatabase(String database) {
        if (isSystemDatabase(database)) {
            throw new Catalog.ProcessSystemDatabaseException();
        }
    }

    public static boolean isTableInSystemDatabase(Identifier identifier) {
        return isSystemDatabase(identifier.getDatabaseName()) || identifier.isSystemTable();
    }

    public static void checkNotSystemTable(Identifier identifier, String method) {
        if (isTableInSystemDatabase(identifier)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot '%s' for system table '%s', please use data table.",
                            method, identifier));
        }
    }

    public static void checkNotBranch(Identifier identifier, String method) {
        if (identifier.getBranchName() != null) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot '%s' for branch table '%s', "
                                    + "please modify the table with the default branch.",
                            method, identifier));
        }
    }

    public static void validateAutoCreateClose(Map<String, String> options) {
        checkArgument(
                !Boolean.parseBoolean(
                        options.getOrDefault(
                                CoreOptions.AUTO_CREATE.key(),
                                CoreOptions.AUTO_CREATE.defaultValue().toString())),
                String.format(
                        "The value of %s property should be %s.",
                        CoreOptions.AUTO_CREATE.key(), Boolean.FALSE));
    }

    public static List<Partition> listPartitionsFromFileSystem(Table table) {
        Options options = Options.fromMap(table.options());
        InternalRowPartitionComputer computer =
                new InternalRowPartitionComputer(
                        options.get(PARTITION_DEFAULT_NAME),
                        table.rowType().project(table.partitionKeys()),
                        table.partitionKeys().toArray(new String[0]),
                        options.get(PARTITION_GENERATE_LEGCY_NAME));
        List<PartitionEntry> partitionEntries =
                table.newReadBuilder().newScan().listPartitionEntries();
        List<Partition> partitions = new ArrayList<>(partitionEntries.size());
        for (PartitionEntry entry : partitionEntries) {
            partitions.add(
                    new Partition(
                            computer.generatePartValues(entry.partition()),
                            entry.recordCount(),
                            entry.fileSizeInBytes(),
                            entry.fileCount(),
                            entry.lastFileCreationTime()));
        }
        return partitions;
    }

    /**
     * Load table from {@link Catalog}, this table can be:
     *
     * <ul>
     *   <li>1. Global System table: contains the statistical information of all the tables exists.
     *   <li>2. Format table: refers to a directory that contains multiple files of the same format.
     *   <li>3. Data table: Normal {@link FileStoreTable}, primary key table or append table.
     *   <li>4. Object table: provides metadata indexes for unstructured data in the location.
     *   <li>5. System table: wraps Data table or Object table, such as the snapshots created.
     * </ul>
     */
    public static Table loadTable(
            Catalog catalog,
            Identifier identifier,
            FileIO fileIO,
            TableMetadata.Loader metadataLoader,
            SnapshotCommit.Factory commitFactory)
            throws Catalog.TableNotExistException {
        if (SYSTEM_DATABASE_NAME.equals(identifier.getDatabaseName())) {
            return CatalogUtils.createGlobalSystemTable(identifier.getTableName(), catalog);
        }

        TableMetadata metadata = metadataLoader.load(identifier);
        TableSchema schema = metadata.schema();
        CoreOptions options = CoreOptions.fromMap(schema.options());
        if (options.type() == TableType.FORMAT_TABLE) {
            return toFormatTable(identifier, schema);
        }

        CatalogEnvironment catalogEnv =
                new CatalogEnvironment(
                        identifier, metadata.uuid(), catalog.catalogLoader(), commitFactory);
        Path path = new Path(schema.options().get(PATH.key()));
        FileStoreTable table = FileStoreTableFactory.create(fileIO, path, schema, catalogEnv);

        if (options.type() == TableType.OBJECT_TABLE) {
            table = toObjectTable(catalog, table);
        }

        if (identifier.isSystemTable()) {
            return CatalogUtils.createSystemTable(identifier, table);
        }

        return table;
    }

    private static Table createGlobalSystemTable(String tableName, Catalog catalog)
            throws Catalog.TableNotExistException {
        switch (tableName.toLowerCase()) {
            case ALL_TABLE_OPTIONS:
                try {
                    Map<Identifier, Map<String, String>> allOptions = new HashMap<>();
                    for (String database : catalog.listDatabases()) {
                        for (String name : catalog.listTables(database)) {
                            Identifier identifier = Identifier.create(database, name);
                            Table table = catalog.getTable(identifier);
                            allOptions.put(identifier, table.options());
                        }
                    }
                    return new AllTableOptionsTable(allOptions);
                } catch (Catalog.DatabaseNotExistException | Catalog.TableNotExistException e) {
                    throw new RuntimeException("Database is deleted while listing", e);
                }
            case CATALOG_OPTIONS:
                return new CatalogOptionsTable(Options.fromMap(catalog.options()));
            default:
                throw new Catalog.TableNotExistException(
                        Identifier.create(SYSTEM_DATABASE_NAME, tableName));
        }
    }

    private static Table createSystemTable(Identifier identifier, Table originTable)
            throws Catalog.TableNotExistException {
        if (!(originTable instanceof FileStoreTable)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Only data table support system tables, but this table %s is %s.",
                            identifier, originTable.getClass()));
        }
        Table table =
                SystemTableLoader.load(
                        Preconditions.checkNotNull(identifier.getSystemTableName()),
                        (FileStoreTable) originTable);
        if (table == null) {
            throw new Catalog.TableNotExistException(identifier);
        }
        return table;
    }

    private static FormatTable toFormatTable(Identifier identifier, TableSchema schema) {
        Map<String, String> options = schema.options();
        FormatTable.Format format =
                FormatTable.parseFormat(
                        options.getOrDefault(
                                CoreOptions.FILE_FORMAT.key(),
                                CoreOptions.FILE_FORMAT.defaultValue()));
        String location = options.get(CoreOptions.PATH.key());
        return FormatTable.builder()
                .identifier(identifier)
                .rowType(schema.logicalRowType())
                .partitionKeys(schema.partitionKeys())
                .location(location)
                .format(format)
                .options(options)
                .comment(schema.comment())
                .build();
    }

    private static ObjectTable toObjectTable(Catalog catalog, FileStoreTable underlyingTable) {
        CoreOptions options = underlyingTable.coreOptions();
        String objectLocation = options.objectLocation();
        FileIO objectFileIO = catalog.fileIO(new Path(objectLocation));
        return ObjectTable.builder()
                .underlyingTable(underlyingTable)
                .objectLocation(objectLocation)
                .objectFileIO(objectFileIO)
                .build();
    }
}
