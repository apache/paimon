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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metastore.MetastoreClient;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Table;
import org.apache.paimon.view.View;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** A {@link Catalog} to delegate all operations to another {@link Catalog}. */
public class DelegateCatalog implements Catalog {

    protected final Catalog wrapped;

    public DelegateCatalog(Catalog wrapped) {
        this.wrapped = wrapped;
    }

    public Catalog wrapped() {
        return wrapped;
    }

    @Override
    public boolean allowUpperCase() {
        return wrapped.allowUpperCase();
    }

    @Override
    public String warehouse() {
        return wrapped.warehouse();
    }

    @Override
    public Map<String, String> options() {
        return wrapped.options();
    }

    @Override
    public FileIO fileIO() {
        return wrapped.fileIO();
    }

    @Override
    public Optional<CatalogLockFactory> lockFactory() {
        return wrapped.lockFactory();
    }

    @Override
    public Optional<CatalogLockContext> lockContext() {
        return wrapped.lockContext();
    }

    @Override
    public Optional<MetastoreClient.Factory> metastoreClientFactory(Identifier identifier)
            throws TableNotExistException {
        return wrapped.metastoreClientFactory(identifier);
    }

    @Override
    public List<String> listDatabases() {
        return wrapped.listDatabases();
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists, Map<String, String> properties)
            throws DatabaseAlreadyExistException {
        wrapped.createDatabase(name, ignoreIfExists, properties);
    }

    @Override
    public Map<String, String> loadDatabaseProperties(String name)
            throws DatabaseNotExistException {
        return wrapped.loadDatabaseProperties(name);
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        wrapped.dropDatabase(name, ignoreIfNotExists, cascade);
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException {
        return wrapped.listTables(databaseName);
    }

    @Override
    public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            throws TableNotExistException {
        wrapped.dropTable(identifier, ignoreIfNotExists);
    }

    @Override
    public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        wrapped.createTable(identifier, schema, ignoreIfExists);
    }

    @Override
    public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException {
        wrapped.renameTable(fromTable, toTable, ignoreIfNotExists);
    }

    @Override
    public void alterTable(
            Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        wrapped.alterTable(identifier, changes, ignoreIfNotExists);
    }

    @Override
    public Table getTable(Identifier identifier) throws TableNotExistException {
        return wrapped.getTable(identifier);
    }

    @Override
    public boolean tableExists(Identifier identifier) {
        return wrapped.tableExists(identifier);
    }

    @Override
    public boolean viewExists(Identifier identifier) {
        return wrapped.viewExists(identifier);
    }

    @Override
    public View getView(Identifier identifier) throws ViewNotExistException {
        return wrapped.getView(identifier);
    }

    @Override
    public void dropView(Identifier identifier, boolean ignoreIfNotExists)
            throws ViewNotExistException {
        wrapped.dropView(identifier, ignoreIfNotExists);
    }

    @Override
    public void createView(Identifier identifier, View view, boolean ignoreIfExists)
            throws ViewAlreadyExistException, DatabaseNotExistException {
        wrapped.createView(identifier, view, ignoreIfExists);
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException {
        return wrapped.listViews(databaseName);
    }

    @Override
    public void renameView(Identifier fromView, Identifier toView, boolean ignoreIfNotExists)
            throws ViewNotExistException, ViewAlreadyExistException {
        wrapped.renameView(fromView, toView, ignoreIfNotExists);
    }

    @Override
    public Path getTableLocation(Identifier identifier) {
        return wrapped.getTableLocation(identifier);
    }

    @Override
    public void createPartition(Identifier identifier, Map<String, String> partitions)
            throws TableNotExistException {
        wrapped.createPartition(identifier, partitions);
    }

    @Override
    public void dropPartition(Identifier identifier, Map<String, String> partitions)
            throws TableNotExistException, PartitionNotExistException {
        wrapped.dropPartition(identifier, partitions);
    }

    @Override
    public List<PartitionEntry> listPartitions(Identifier identifier)
            throws TableNotExistException {
        return wrapped.listPartitions(identifier);
    }

    @Override
    public void repairCatalog() {
        wrapped.repairCatalog();
    }

    @Override
    public void repairDatabase(String databaseName) {
        wrapped.repairDatabase(databaseName);
    }

    @Override
    public void repairTable(Identifier identifier) throws TableNotExistException {
        wrapped.repairTable(identifier);
    }

    @Override
    public void close() throws Exception {
        wrapped.close();
    }
}
