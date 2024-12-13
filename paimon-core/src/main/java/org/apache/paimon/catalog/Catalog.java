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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Table;
import org.apache.paimon.view.View;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.options.OptionsUtils.convertToPropertiesPrefixKey;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * This interface is responsible for reading and writing metadata such as database/table from a
 * paimon catalog.
 *
 * @see CatalogFactory
 * @since 0.4.0
 */
@Public
public interface Catalog extends AutoCloseable {

    String DEFAULT_DATABASE = "default";

    String SYSTEM_TABLE_SPLITTER = "$";
    String SYSTEM_DATABASE_NAME = "sys";
    String SYSTEM_BRANCH_PREFIX = "branch_";
    String TABLE_DEFAULT_OPTION_PREFIX = "table-default.";
    String DB_SUFFIX = ".db";

    String COMMENT_PROP = "comment";
    String OWNER_PROP = "owner";
    String DB_LOCATION_PROP = "location";
    String NUM_ROWS_PROP = "numRows";
    String NUM_FILES_PROP = "numFiles";
    String TOTAL_SIZE_PROP = "totalSize";
    String LAST_UPDATE_TIME_PROP = "lastUpdateTime";
    String HIVE_LAST_UPDATE_TIME_PROP = "transient_lastDdlTime";

    /** Warehouse root path containing all database directories in this catalog. */
    String warehouse();

    /** Catalog options. */
    Map<String, String> options();

    FileIO fileIO();

    /**
     * Get the names of all databases in this catalog.
     *
     * @return a list of the names of all databases
     */
    List<String> listDatabases();

    /**
     * Create a database, see {@link Catalog#createDatabase(String name, boolean ignoreIfExists, Map
     * properties)}.
     */
    default void createDatabase(String name, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException {
        createDatabase(name, ignoreIfExists, Collections.emptyMap());
    }

    /**
     * Create a database with properties.
     *
     * @param name Name of the database to be created
     * @param ignoreIfExists Flag to specify behavior when a database with the given name already
     *     exists: if set to false, throw a DatabaseAlreadyExistException, if set to true, do
     *     nothing.
     * @param properties properties to be associated with the database
     * @throws DatabaseAlreadyExistException if the given database already exists and ignoreIfExists
     *     is false
     */
    void createDatabase(String name, boolean ignoreIfExists, Map<String, String> properties)
            throws DatabaseAlreadyExistException;

    /**
     * Return a {@link Database} identified by the given name.
     *
     * @param name Database name
     * @return The requested {@link Database}
     * @throws DatabaseNotExistException if the requested database does not exist
     */
    Database getDatabase(String name) throws DatabaseNotExistException;

    /**
     * Drop a database.
     *
     * @param name Name of the database to be dropped.
     * @param ignoreIfNotExists Flag to specify behavior when the database does not exist: if set to
     *     false, throw an exception, if set to true, do nothing.
     * @param cascade Flag to specify behavior when the database contains table or function: if set
     *     to true, delete all tables and functions in the database and then delete the database, if
     *     set to false, throw an exception.
     * @throws DatabaseNotEmptyException if the given database is not empty and isRestrict is true
     */
    void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException;

    /**
     * Alter a database.
     *
     * @param name Name of the database to alter.
     * @param changes a collection of changes to apply to the database.
     * @param ignoreIfNotExists Flag to specify behavior when the database does not exist: if set to
     *     false, throw an exception, if set to true, do nothing.
     * @throws DatabaseNotExistException if the given database is not exist and ignoreIfNotExists is
     *     false
     */
    void alterDatabase(String name, List<DatabaseChange> changes, boolean ignoreIfNotExists)
            throws DatabaseNotExistException;

    /**
     * Return a {@link Table} identified by the given {@link Identifier}.
     *
     * <p>System tables can be got by '$' splitter.
     *
     * @param identifier Path of the table
     * @return The requested table
     * @throws TableNotExistException if the target does not exist
     */
    Table getTable(Identifier identifier) throws TableNotExistException;

    /**
     * Get the table location in this catalog. If the table exists, return the location of the
     * table; If the table does not exist, construct the location for table.
     *
     * @return the table location
     */
    Path getTableLocation(Identifier identifier);

    /**
     * Get names of all tables under this database. An empty list is returned if none exists.
     *
     * <p>NOTE: System tables will not be listed.
     *
     * @return a list of the names of all tables in this database
     * @throws DatabaseNotExistException if the database does not exist
     */
    List<String> listTables(String databaseName) throws DatabaseNotExistException;

    /**
     * Drop a table.
     *
     * <p>NOTE: System tables can not be dropped.
     *
     * @param identifier Path of the table to be dropped
     * @param ignoreIfNotExists Flag to specify behavior when the table does not exist: if set to
     *     false, throw an exception, if set to true, do nothing.
     * @throws TableNotExistException if the table does not exist
     */
    void dropTable(Identifier identifier, boolean ignoreIfNotExists) throws TableNotExistException;

    /**
     * Create a new table.
     *
     * <p>NOTE: System tables can not be created.
     *
     * @param identifier path of the table to be created
     * @param schema the table definition
     * @param ignoreIfExists flag to specify behavior when a table already exists at the given path:
     *     if set to false, it throws a TableAlreadyExistException, if set to true, do nothing.
     * @throws TableAlreadyExistException if table already exists and ignoreIfExists is false
     * @throws DatabaseNotExistException if the database in identifier doesn't exist
     */
    void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException;

    /**
     * Rename a table.
     *
     * <p>NOTE: If you use object storage, such as S3 or OSS, please use this syntax carefully,
     * because the renaming of object storage is not atomic, and only partial files may be moved in
     * case of failure.
     *
     * <p>NOTE: System tables can not be renamed.
     *
     * @param fromTable the name of the table which need to rename
     * @param toTable the new table
     * @param ignoreIfNotExists Flag to specify behavior when the table does not exist: if set to
     *     false, throw an exception, if set to true, do nothing.
     * @throws TableNotExistException if the fromTable does not exist
     * @throws TableAlreadyExistException if the toTable already exists
     */
    void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException;

    /**
     * Modify an existing table from {@link SchemaChange}s.
     *
     * <p>NOTE: System tables can not be altered.
     *
     * @param identifier path of the table to be modified
     * @param changes the schema changes
     * @param ignoreIfNotExists flag to specify behavior when the table does not exist: if set to
     *     false, throw an exception, if set to true, do nothing.
     * @throws TableNotExistException if the table does not exist
     */
    void alterTable(Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException;

    /**
     * Invalidate cached table metadata for an {@link Identifier identifier}.
     *
     * <p>If the table is already loaded or cached, drop cached data. If the table does not exist or
     * is not cached, do nothing. Calling this method should not query remote services.
     *
     * @param identifier a table identifier
     */
    default void invalidateTable(Identifier identifier) {}

    /**
     * Create the partition of the specify table.
     *
     * <p>Only catalog with metastore can support this method, and only table with
     * 'metastore.partitioned-table' can support this method.
     *
     * @param identifier path of the table to drop partition
     * @param partitionSpec the partition to be created
     * @throws TableNotExistException if the table does not exist
     */
    void createPartition(Identifier identifier, Map<String, String> partitionSpec)
            throws TableNotExistException;

    /**
     * Drop the partition of the specify table.
     *
     * @param identifier path of the table to drop partition
     * @param partitions the partition to be deleted
     * @throws TableNotExistException if the table does not exist
     * @throws PartitionNotExistException if the partition does not exist
     */
    void dropPartition(Identifier identifier, Map<String, String> partitions)
            throws TableNotExistException, PartitionNotExistException;

    /**
     * Get PartitionEntry of all partitions of the table.
     *
     * @param identifier path of the table to list partitions
     * @throws TableNotExistException if the table does not exist
     */
    List<PartitionEntry> listPartitions(Identifier identifier) throws TableNotExistException;

    /**
     * Modify an existing table from a {@link SchemaChange}.
     *
     * <p>NOTE: System tables can not be altered.
     *
     * @param identifier path of the table to be modified
     * @param change the schema change
     * @param ignoreIfNotExists flag to specify behavior when the table does not exist: if set to
     *     false, throw an exception, if set to true, do nothing.
     * @throws TableNotExistException if the table does not exist
     */
    default void alterTable(Identifier identifier, SchemaChange change, boolean ignoreIfNotExists)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        alterTable(identifier, Collections.singletonList(change), ignoreIfNotExists);
    }

    /**
     * Return a {@link View} identified by the given {@link Identifier}.
     *
     * @param identifier Path of the view
     * @return The requested view
     * @throws ViewNotExistException if the target does not exist
     */
    default View getView(Identifier identifier) throws ViewNotExistException {
        throw new ViewNotExistException(identifier);
    }

    /**
     * Drop a view.
     *
     * @param identifier Path of the view to be dropped
     * @param ignoreIfNotExists Flag to specify behavior when the view does not exist: if set to
     *     false, throw an exception, if set to true, do nothing.
     * @throws ViewNotExistException if the view does not exist
     */
    default void dropView(Identifier identifier, boolean ignoreIfNotExists)
            throws ViewNotExistException {
        throw new UnsupportedOperationException();
    }

    /**
     * Create a new view.
     *
     * @param identifier path of the view to be created
     * @param view the view definition
     * @param ignoreIfExists flag to specify behavior when a view already exists at the given path:
     *     if set to false, it throws a ViewAlreadyExistException, if set to true, do nothing.
     * @throws ViewAlreadyExistException if view already exists and ignoreIfExists is false
     * @throws DatabaseNotExistException if the database in identifier doesn't exist
     */
    default void createView(Identifier identifier, View view, boolean ignoreIfExists)
            throws ViewAlreadyExistException, DatabaseNotExistException {
        throw new UnsupportedOperationException();
    }

    /**
     * Get names of all views under this database. An empty list is returned if none exists.
     *
     * @return a list of the names of all views in this database
     * @throws DatabaseNotExistException if the database does not exist
     */
    default List<String> listViews(String databaseName) throws DatabaseNotExistException {
        return Collections.emptyList();
    }

    /**
     * Rename a view.
     *
     * @param fromView identifier of the view to rename
     * @param toView new view identifier
     * @throws ViewNotExistException if the fromView does not exist
     * @throws ViewAlreadyExistException if the toView already exists
     */
    default void renameView(Identifier fromView, Identifier toView, boolean ignoreIfNotExists)
            throws ViewNotExistException, ViewAlreadyExistException {
        throw new UnsupportedOperationException();
    }

    /** Return a boolean that indicates whether this catalog allow upper case. */
    boolean allowUpperCase();

    default void repairCatalog() {
        throw new UnsupportedOperationException();
    }

    default void repairDatabase(String databaseName) {
        throw new UnsupportedOperationException();
    }

    default void repairTable(Identifier identifier) throws TableNotExistException {
        throw new UnsupportedOperationException();
    }

    static Map<String, String> tableDefaultOptions(Map<String, String> options) {
        return convertToPropertiesPrefixKey(options, TABLE_DEFAULT_OPTION_PREFIX);
    }

    /** Validate database, table and field names must be lowercase when not case-sensitive. */
    static void validateCaseInsensitive(boolean caseSensitive, String type, String... names) {
        validateCaseInsensitive(caseSensitive, type, Arrays.asList(names));
    }

    /** Validate database, table and field names must be lowercase when not case-sensitive. */
    static void validateCaseInsensitive(boolean caseSensitive, String type, List<String> names) {
        if (caseSensitive) {
            return;
        }
        List<String> illegalNames =
                names.stream().filter(f -> !f.equals(f.toLowerCase())).collect(Collectors.toList());
        checkArgument(
                illegalNames.isEmpty(),
                String.format(
                        "%s name %s cannot contain upper case in the catalog.",
                        type, illegalNames));
    }

    /** Exception for trying to drop on a database that is not empty. */
    class DatabaseNotEmptyException extends Exception {
        private static final String MSG = "Database %s is not empty.";

        private final String database;

        public DatabaseNotEmptyException(String database, Throwable cause) {
            super(String.format(MSG, database), cause);
            this.database = database;
        }

        public DatabaseNotEmptyException(String database) {
            this(database, null);
        }

        public String database() {
            return database;
        }
    }

    /** Exception for trying to create a database that already exists. */
    class DatabaseAlreadyExistException extends Exception {
        private static final String MSG = "Database %s already exists.";

        private final String database;

        public DatabaseAlreadyExistException(String database, Throwable cause) {
            super(String.format(MSG, database), cause);
            this.database = database;
        }

        public DatabaseAlreadyExistException(String database) {
            this(database, null);
        }

        public String database() {
            return database;
        }
    }

    /** Exception for trying to operate on a database that doesn't exist. */
    class DatabaseNotExistException extends Exception {
        private static final String MSG = "Database %s does not exist.";

        private final String database;

        public DatabaseNotExistException(String database, Throwable cause) {
            super(String.format(MSG, database), cause);
            this.database = database;
        }

        public DatabaseNotExistException(String database) {
            this(database, null);
        }

        public String database() {
            return database;
        }
    }

    /** Exception for trying to operate on a system database. */
    class ProcessSystemDatabaseException extends IllegalArgumentException {
        private static final String MSG = "Can't do operation on system database.";

        public ProcessSystemDatabaseException() {
            super(MSG);
        }
    }

    /** Exception for trying to create a table that already exists. */
    class TableAlreadyExistException extends Exception {

        private static final String MSG = "Table %s already exists.";

        private final Identifier identifier;

        public TableAlreadyExistException(Identifier identifier) {
            this(identifier, null);
        }

        public TableAlreadyExistException(Identifier identifier, Throwable cause) {
            super(String.format(MSG, identifier.getFullName()), cause);
            this.identifier = identifier;
        }

        public Identifier identifier() {
            return identifier;
        }
    }

    /** Exception for trying to operate on a table that doesn't exist. */
    class TableNotExistException extends Exception {

        private static final String MSG = "Table %s does not exist.";

        private final Identifier identifier;

        public TableNotExistException(Identifier identifier) {
            this(identifier, null);
        }

        public TableNotExistException(Identifier identifier, Throwable cause) {
            super(String.format(MSG, identifier.getFullName()), cause);
            this.identifier = identifier;
        }

        public Identifier identifier() {
            return identifier;
        }
    }

    /** Exception for trying to operate on a partition that doesn't exist. */
    class PartitionNotExistException extends Exception {

        private static final String MSG = "Partition %s do not exist in the table %s.";

        private final Identifier identifier;

        private final Map<String, String> partitionSpec;

        public PartitionNotExistException(
                Identifier identifier, Map<String, String> partitionSpec) {
            this(identifier, partitionSpec, null);
        }

        public PartitionNotExistException(
                Identifier identifier, Map<String, String> partitionSpec, Throwable cause) {
            super(String.format(MSG, partitionSpec, identifier.getFullName()), cause);
            this.identifier = identifier;
            this.partitionSpec = partitionSpec;
        }

        public Identifier identifier() {
            return identifier;
        }

        public Map<String, String> partitionSpec() {
            return partitionSpec;
        }
    }

    /** Exception for trying to alter a column that already exists. */
    class ColumnAlreadyExistException extends Exception {

        private static final String MSG = "Column %s already exists in the %s table.";

        private final Identifier identifier;
        private final String column;

        public ColumnAlreadyExistException(Identifier identifier, String column) {
            this(identifier, column, null);
        }

        public ColumnAlreadyExistException(Identifier identifier, String column, Throwable cause) {
            super(String.format(MSG, column, identifier.getFullName()), cause);
            this.identifier = identifier;
            this.column = column;
        }

        public Identifier identifier() {
            return identifier;
        }

        public String column() {
            return column;
        }
    }

    /** Exception for trying to operate on a column that doesn't exist. */
    class ColumnNotExistException extends Exception {

        private static final String MSG = "Column %s does not exist in the %s table.";

        private final Identifier identifier;
        private final String column;

        public ColumnNotExistException(Identifier identifier, String column) {
            this(identifier, column, null);
        }

        public ColumnNotExistException(Identifier identifier, String column, Throwable cause) {
            super(String.format(MSG, column, identifier.getFullName()), cause);
            this.identifier = identifier;
            this.column = column;
        }

        public Identifier identifier() {
            return identifier;
        }

        public String column() {
            return column;
        }
    }

    /** Exception for trying to create a view that already exists. */
    class ViewAlreadyExistException extends Exception {

        private static final String MSG = "View %s already exists.";

        private final Identifier identifier;

        public ViewAlreadyExistException(Identifier identifier) {
            this(identifier, null);
        }

        public ViewAlreadyExistException(Identifier identifier, Throwable cause) {
            super(String.format(MSG, identifier.getFullName()), cause);
            this.identifier = identifier;
        }

        public Identifier identifier() {
            return identifier;
        }
    }

    /** Exception for trying to operate on a view that doesn't exist. */
    class ViewNotExistException extends Exception {

        private static final String MSG = "View %s does not exist.";

        private final Identifier identifier;

        public ViewNotExistException(Identifier identifier) {
            this(identifier, null);
        }

        public ViewNotExistException(Identifier identifier, Throwable cause) {
            super(String.format(MSG, identifier.getFullName()), cause);
            this.identifier = identifier;
        }

        public Identifier identifier() {
            return identifier;
        }
    }

    /** Loader of {@link Catalog}. */
    @FunctionalInterface
    interface Loader extends Serializable {
        Catalog load();
    }
}
