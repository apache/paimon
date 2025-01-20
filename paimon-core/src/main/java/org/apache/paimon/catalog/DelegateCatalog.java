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
import org.apache.paimon.partition.Partition;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Table;
import org.apache.paimon.view.View;

import java.util.List;
import java.util.Map;

/** A {@link Catalog} to delegate all operations to another {@link Catalog}. */
public abstract class DelegateCatalog implements Catalog {

    protected final Catalog wrapped;

    public DelegateCatalog(Catalog wrapped) {
        this.wrapped = wrapped;
    }

    public Catalog wrapped() {
        return wrapped;
    }

    @Override
    public boolean caseSensitive() {
        return wrapped.caseSensitive();
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
    public FileIO fileIO(Path path) {
        return wrapped.fileIO(path);
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
    public Database getDatabase(String name) throws DatabaseNotExistException {
        return wrapped.getDatabase(name);
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        wrapped.dropDatabase(name, ignoreIfNotExists, cascade);
    }

    @Override
    public void alterDatabase(String name, List<PropertyChange> changes, boolean ignoreIfNotExists)
            throws DatabaseNotExistException {
        wrapped.alterDatabase(name, changes, ignoreIfNotExists);
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
    public void createPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {
        wrapped.createPartitions(identifier, partitions);
    }

    @Override
    public void dropPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {
        wrapped.dropPartitions(identifier, partitions);
    }

    @Override
    public void alterPartitions(Identifier identifier, List<Partition> partitions)
            throws TableNotExistException {
        wrapped.alterPartitions(identifier, partitions);
    }

    @Override
    public void markDonePartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {
        wrapped.markDonePartitions(identifier, partitions);
    }

    @Override
    public Table getTable(Identifier identifier) throws TableNotExistException {
        return wrapped.getTable(identifier);
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
    public List<Partition> listPartitions(Identifier identifier) throws TableNotExistException {
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
