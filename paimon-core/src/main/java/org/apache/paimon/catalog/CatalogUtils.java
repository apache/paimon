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

import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.utils.Preconditions;

import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.catalog.Catalog.DB_SUFFIX;
import static org.apache.paimon.catalog.Catalog.SYSTEM_DATABASE_NAME;
import static org.apache.paimon.catalog.Catalog.TABLE_DEFAULT_OPTION_PREFIX;
import static org.apache.paimon.options.CatalogOptions.LOCK_ENABLED;
import static org.apache.paimon.options.CatalogOptions.LOCK_TYPE;
import static org.apache.paimon.options.OptionsUtils.convertToPropertiesPrefixKey;

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

    public static Path newDatabasePath(String warehouse, String database) {
        return new Path(warehouse, database + DB_SUFFIX);
    }

    public static Path newTableLocation(String warehouse, Identifier identifier) {
        checkNotBranch(identifier, "newTableLocation");
        checkNotSystemTable(identifier, "newTableLocation");
        return new Path(
                newDatabasePath(warehouse, identifier.getDatabaseName()),
                identifier.getTableName());
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

    public static Optional<CatalogLockFactory> lockFactory(
            Options options, FileIO fileIO, Optional<CatalogLockFactory> defaultLockFactoryOpt) {
        boolean lockEnabled = lockEnabled(options, fileIO);
        if (!lockEnabled) {
            return Optional.empty();
        }

        String lock = options.get(LOCK_TYPE);
        if (lock == null) {
            return defaultLockFactoryOpt;
        }

        return Optional.of(
                FactoryUtil.discoverFactory(
                        AbstractCatalog.class.getClassLoader(), CatalogLockFactory.class, lock));
    }

    public static Optional<CatalogLockContext> lockContext(Options options) {
        return Optional.of(CatalogLockContext.fromOptions(options));
    }

    public static boolean lockEnabled(Options options, FileIO fileIO) {
        return options.getOptional(LOCK_ENABLED).orElse(fileIO != null && fileIO.isObjectStore());
    }

    public static Table getSystemTable(Identifier identifier, Table originTable)
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
}
