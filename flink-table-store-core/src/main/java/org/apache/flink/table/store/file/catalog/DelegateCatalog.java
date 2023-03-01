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

import org.apache.flink.table.store.annotation.Experimental;
import org.apache.flink.table.store.file.schema.Schema;
import org.apache.flink.table.store.file.schema.SchemaChange;
import org.apache.flink.table.store.table.Table;

import java.util.List;
import java.util.Optional;

/**
 * A {@link Catalog} to delegate another catalog, and implementations can overwrite some methods to
 * add some callbacks.
 *
 * @see CatalogFactory
 * @since 0.4.0
 */
@Experimental
public abstract class DelegateCatalog implements Catalog {

    private final Catalog catalog;

    public DelegateCatalog(Catalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public Optional<CatalogLock.Factory> lockFactory() {
        return catalog.lockFactory();
    }

    @Override
    public List<String> listDatabases() {
        return catalog.listDatabases();
    }

    @Override
    public boolean databaseExists(String databaseName) {
        return catalog.databaseExists(databaseName);
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException {
        catalog.createDatabase(name, ignoreIfExists);
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        catalog.dropDatabase(name, ignoreIfNotExists, cascade);
    }

    @Override
    public Table getTable(Identifier identifier) throws TableNotExistException {
        return catalog.getTable(identifier);
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException {
        return catalog.listTables(databaseName);
    }

    @Override
    public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            throws TableNotExistException {
        catalog.dropTable(identifier, ignoreIfNotExists);
    }

    @Override
    public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        catalog.createTable(identifier, schema, ignoreIfExists);
    }

    @Override
    public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException {
        catalog.renameTable(fromTable, toTable, ignoreIfNotExists);
    }

    @Override
    public void alterTable(
            Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
            throws TableNotExistException {
        catalog.alterTable(identifier, changes, ignoreIfNotExists);
    }

    @Override
    public void close() throws Exception {
        catalog.close();
    }
}
