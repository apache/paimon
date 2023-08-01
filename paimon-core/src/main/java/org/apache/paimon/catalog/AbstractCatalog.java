/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.catalog;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.utils.StringUtils;

import java.util.HashMap;
import java.util.Map;

/** Common implementation of {@link Catalog}. */
public abstract class AbstractCatalog implements Catalog {

    public static final String DB_SUFFIX = ".db";

    protected static final String TABLE_DEFAULT_OPTION_PREFIX = "table-default.";

    protected final FileIO fileIO;

    protected final Map<String, String> tableDefaultOptions;

    protected AbstractCatalog(FileIO fileIO) {
        this.fileIO = fileIO;
        this.tableDefaultOptions = new HashMap<>();
    }

    protected AbstractCatalog(FileIO fileIO, Map<String, String> options) {
        this.fileIO = fileIO;
        this.tableDefaultOptions = new HashMap<>();

        options.keySet().stream()
                .filter(key -> key.startsWith(TABLE_DEFAULT_OPTION_PREFIX))
                .forEach(
                        key ->
                                this.tableDefaultOptions.put(
                                        key.substring(TABLE_DEFAULT_OPTION_PREFIX.length()),
                                        options.get(key)));
    }

    @Override
    public Table getTable(Identifier identifier) throws TableNotExistException {
        if (isSystemTable(identifier)) {
            String[] splits = tableAndSystemName(identifier);
            String tableName = splits[0];
            String type = splits[1];
            FileStoreTable originTable =
                    getDataTable(new Identifier(identifier.getDatabaseName(), tableName));
            Table table = SystemTableLoader.load(type, fileIO, originTable);
            if (table == null) {
                throw new TableNotExistException(identifier);
            }
            return table;
        } else {
            return getDataTable(identifier);
        }
    }

    private FileStoreTable getDataTable(Identifier identifier) throws TableNotExistException {
        TableSchema tableSchema = getDataTableSchema(identifier);
        return FileStoreTableFactory.create(
                fileIO,
                getDataTableLocation(identifier),
                tableSchema,
                Lock.factory(lockFactory().orElse(null), identifier),
                metastoreClientFactory(identifier).orElse(null));
    }

    @VisibleForTesting
    public Path databasePath(String database) {
        return databasePath(warehouse(), database);
    }

    protected abstract String warehouse();

    protected abstract TableSchema getDataTableSchema(Identifier identifier)
            throws TableNotExistException;

    @VisibleForTesting
    public Path getDataTableLocation(Identifier identifier) {
        return dataTableLocation(warehouse(), identifier);
    }

    private static boolean isSystemTable(Identifier identifier) {
        return identifier.getObjectName().contains(SYSTEM_TABLE_SPLITTER);
    }

    protected void checkNotSystemTable(Identifier identifier, String method) {
        if (isSystemTable(identifier)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot '%s' for system table '%s', please use data table.",
                            method, identifier));
        }
    }

    protected void copyTableDefaultOptions(Map<String, String> options) {
        tableDefaultOptions.forEach(options::putIfAbsent);
    }

    private String[] tableAndSystemName(Identifier identifier) {
        String[] splits = StringUtils.split(identifier.getObjectName(), SYSTEM_TABLE_SPLITTER);
        if (splits.length != 2) {
            throw new IllegalArgumentException(
                    "System table can only contain one '$' separator, but this is: "
                            + identifier.getObjectName());
        }
        return splits;
    }

    public static Path dataTableLocation(String warehouse, Identifier identifier) {
        if (isSystemTable(identifier)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Table name[%s] cannot contain '%s' separator",
                            identifier.getObjectName(), SYSTEM_TABLE_SPLITTER));
        }
        return new Path(
                databasePath(warehouse, identifier.getDatabaseName()), identifier.getObjectName());
    }

    public static Path databasePath(String warehouse, String database) {
        return new Path(warehouse, database + DB_SUFFIX);
    }

    @Override
    public void truncateTable(Identifier identifier) throws TableNotExistException {
        getDataTable(identifier).truncate();
    }
}
