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

package org.apache.flink.table.store.file.catalog;

import org.apache.flink.table.store.file.schema.Schema;
import org.apache.flink.table.store.file.schema.SchemaChange;
import org.apache.flink.table.store.table.Table;

import org.apache.paimon.annotation.Experimental;

import java.util.List;
import java.util.Optional;

/**
 * This interface is responsible for reading and writing metadata such as database/table from a
 * table store catalog.
 *
 * @see CatalogFactory
 * @since 0.4.0
 */
@Experimental
public interface Catalog extends AutoCloseable {

    String DEFAULT_DATABASE = "default";

    String SYSTEM_TABLE_SPLITTER = "$";

    /**
     * Get lock factory from catalog. Lock is used to support multiple concurrent writes on the
     * object store.
     */
    Optional<CatalogLock.Factory> lockFactory();

    /**
     * Get the names of all databases in this catalog.
     *
     * @return a list of the names of all databases
     */
    List<String> listDatabases();

    /**
     * Check if a database exists in this catalog.
     *
     * @param databaseName Name of the database
     * @return true if the given database exists in the catalog false otherwise
     */
    boolean databaseExists(String databaseName);

    /**
     * Create a database.
     *
     * @param name Name of the database to be created
     * @param ignoreIfExists Flag to specify behavior when a database with the given name already
     *     exists: if set to false, throw a DatabaseAlreadyExistException, if set to true, do
     *     nothing.
     * @throws DatabaseAlreadyExistException if the given database already exists and ignoreIfExists
     *     is false
     */
    void createDatabase(String name, boolean ignoreIfExists) throws DatabaseAlreadyExistException;

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
            throws TableNotExistException;

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
}
