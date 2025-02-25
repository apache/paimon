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

package org.apache.paimon.rest;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.TableType;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.PropertyChange;
import org.apache.paimon.catalog.SupportsSnapshots;
import org.apache.paimon.catalog.TableMetadata;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.view.View;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** A catalog for testing RESTCatalog. */
public class InMemoryCatalog extends FileSystemCatalog implements SupportsSnapshots {

    public final Map<String, Database> databaseStore;
    public final Map<String, TableSchema> tableSchemaStore;
    public final Map<String, List<Partition>> tablePartitionsStore;
    public final Map<String, View> viewStore;
    public final Map<String, Snapshot> tableSnapshotStore;

    public InMemoryCatalog(
            FileIO fileIO,
            Path warehouse,
            Options options,
            Map<String, Database> databaseStore,
            Map<String, TableSchema> tableSchemaStore,
            Map<String, Snapshot> tableSnapshotStore,
            Map<String, List<Partition>> tablePartitionsStore,
            Map<String, View> viewStore) {
        super(fileIO, warehouse, options);
        this.databaseStore = databaseStore;
        this.tableSchemaStore = tableSchemaStore;
        this.tablePartitionsStore = tablePartitionsStore;
        this.tableSnapshotStore = tableSnapshotStore;
        this.viewStore = viewStore;
    }

    public static InMemoryCatalog create(
            CatalogContext context,
            Map<String, Database> databaseStore,
            Map<String, TableSchema> tableSchemaStore,
            Map<String, Snapshot> tableSnapshotStore,
            Map<String, List<Partition>> tablePartitionsStore,
            Map<String, View> viewStore) {
        String warehouse = CatalogFactory.warehouse(context).toUri().toString();

        Path warehousePath = new Path(warehouse);
        FileIO fileIO;

        try {
            fileIO = FileIO.get(warehousePath, context);
            fileIO.checkOrMkdirs(warehousePath);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return new InMemoryCatalog(
                fileIO,
                warehousePath,
                context.options(),
                databaseStore,
                tableSchemaStore,
                tableSnapshotStore,
                tablePartitionsStore,
                viewStore);
    }

    // todo: overview
    @Override
    public FileIO fileIO() {
        return fileIO;
    }

    @Override
    public List<String> listDatabases() {
        return new ArrayList<>(databaseStore.keySet());
    }

    @Override
    protected void createDatabaseImpl(String name, Map<String, String> properties) {
        super.createDatabaseImpl(name, properties);
        databaseStore.put(name, Database.of(name, properties, null));
    }

    @Override
    public Database getDatabaseImpl(String name) throws DatabaseNotExistException {
        return databaseStore.get(name);
    }

    @Override
    protected void dropDatabaseImpl(String name) {
        super.dropDatabaseImpl(name);
        databaseStore.remove(name);
    }

    @Override
    protected void alterDatabaseImpl(String name, List<PropertyChange> changes) {
        Pair<Map<String, String>, Set<String>> setPropertiesToRemoveKeys =
                PropertyChange.getSetPropertiesToRemoveKeys(changes);
        Map<String, String> setProperties = setPropertiesToRemoveKeys.getLeft();
        Set<String> removeKeys = setPropertiesToRemoveKeys.getRight();
        Database database = databaseStore.get(name);
        Map<String, String> parameter = database.options();
        if (!setProperties.isEmpty()) {
            parameter.putAll(setProperties);
        }
        if (!removeKeys.isEmpty()) {
            parameter.keySet().removeAll(removeKeys);
        }
        Database alterDatabase = Database.of(name, parameter, null);
        databaseStore.put(name, alterDatabase);
    }

    @Override
    protected List<String> listTablesImpl(String databaseName) {
        List<String> tables = new ArrayList<>();
        for (Map.Entry<String, TableSchema> entry : tableSchemaStore.entrySet()) {
            Identifier identifier = Identifier.fromString(entry.getKey());
            if (databaseName.equals(identifier.getDatabaseName())) {
                tables.add(identifier.getTableName());
            }
        }
        return tables;
    }

    @Override
    protected void dropTableImpl(Identifier identifier) {
        if (tableSchemaStore.containsKey(identifier.getFullName())) {
            tableSchemaStore.remove(identifier.getFullName());
        } else {
            super.dropTableImpl(identifier);
        }
    }

    @Override
    public void renameTableImpl(Identifier fromTable, Identifier toTable) {
        if (tableSchemaStore.containsKey(fromTable.getFullName())) {
            TableSchema tableSchema = tableSchemaStore.get(fromTable.getFullName());
            tableSchemaStore.remove(fromTable.getFullName());
            tableSchemaStore.put(toTable.getFullName(), tableSchema);
        } else {
            super.renameTableImpl(fromTable, toTable);
        }
    }

    @Override
    protected void alterTableImpl(Identifier identifier, List<SchemaChange> changes)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        if (tableSchemaStore.containsKey(identifier.getFullName())) {
            TableSchema schema = tableSchemaStore.get(identifier.getFullName());
            Options options = Options.fromMap(schema.options());
            if (options.get(CoreOptions.TYPE) == TableType.FORMAT_TABLE) {
                throw new UnsupportedOperationException("Only data table support alter table.");
            }
        } else {
            super.alterTableImpl(identifier, changes);
        }
    }

    @Override
    public void createFormatTable(Identifier identifier, Schema schema) {
        Map<String, String> options = new HashMap<>(schema.options());
        // todo: whether need fix
        options.put("path", "/tmp/format_table");
        TableSchema tableSchema =
                new TableSchema(
                        1L,
                        schema.fields(),
                        1,
                        schema.partitionKeys(),
                        schema.primaryKeys(),
                        options,
                        schema.comment());
        tableSchemaStore.put(identifier.getFullName(), tableSchema);
    }

    @Override
    protected TableMetadata loadTableMetadata(Identifier identifier) throws TableNotExistException {
        if (tableSchemaStore.containsKey(identifier.getFullName())) {
            TableSchema tableSchema = tableSchemaStore.get(identifier.getFullName());
            return new TableMetadata(tableSchema, false, "uuid");
        }
        return super.loadTableMetadata(identifier);
    }

    @Override
    public void createPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {
        getTable(identifier);
        tablePartitionsStore.put(
                identifier.getFullName(),
                partitions.stream()
                        .map(partition -> spec2Partition(partition))
                        .collect(Collectors.toList()));
    }

    @Override
    public void dropPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {
        getTable(identifier);
        List<Partition> existPartitions = tablePartitionsStore.get(identifier.getFullName());
        partitions.forEach(
                partition -> {
                    for (Map.Entry<String, String> entry : partition.entrySet()) {
                        existPartitions.stream()
                                .filter(
                                        p ->
                                                p.spec().containsKey(entry.getKey())
                                                        && p.spec()
                                                                .get(entry.getKey())
                                                                .equals(entry.getValue()))
                                .findFirst()
                                .ifPresent(
                                        existPartition -> existPartitions.remove(existPartition));
                    }
                });
    }

    @Override
    public void alterPartitions(Identifier identifier, List<Partition> partitions)
            throws TableNotExistException {
        getTable(identifier);
        List<Partition> existPartitions = tablePartitionsStore.get(identifier.getFullName());
        partitions.forEach(
                partition -> {
                    for (Map.Entry<String, String> entry : partition.spec().entrySet()) {
                        existPartitions.stream()
                                .filter(
                                        p ->
                                                p.spec().containsKey(entry.getKey())
                                                        && p.spec()
                                                                .get(entry.getKey())
                                                                .equals(entry.getValue()))
                                .findFirst()
                                .ifPresent(
                                        existPartition -> existPartitions.remove(existPartition));
                    }
                });
        existPartitions.addAll(partitions);
        tablePartitionsStore.put(identifier.getFullName(), existPartitions);
    }

    @Override
    public List<Partition> listPartitions(Identifier identifier) throws TableNotExistException {
        getTable(identifier);
        return tablePartitionsStore.get(identifier.getFullName());
    }

    @Override
    public View getView(Identifier identifier) throws ViewNotExistException {
        if (viewStore.containsKey(identifier.getFullName())) {
            return viewStore.get(identifier.getFullName());
        }
        throw new ViewNotExistException(identifier);
    }

    @Override
    public void dropView(Identifier identifier, boolean ignoreIfNotExists)
            throws ViewNotExistException {
        if (viewStore.containsKey(identifier.getFullName())) {
            viewStore.remove(identifier.getFullName());
        }
        if (!ignoreIfNotExists) {
            throw new ViewNotExistException(identifier);
        }
    }

    @Override
    public void createView(Identifier identifier, View view, boolean ignoreIfExists)
            throws ViewAlreadyExistException, DatabaseNotExistException {
        getDatabase(identifier.getDatabaseName());
        if (viewStore.containsKey(identifier.getFullName()) && !ignoreIfExists) {
            throw new ViewAlreadyExistException(identifier);
        }
        viewStore.put(identifier.getFullName(), view);
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException {
        getDatabase(databaseName);
        return viewStore.keySet().stream()
                .map(v -> Identifier.fromString(v))
                .filter(identifier -> identifier.getDatabaseName().equals(databaseName))
                .map(identifier -> identifier.getTableName())
                .collect(Collectors.toList());
    }

    @Override
    public void renameView(Identifier fromView, Identifier toView, boolean ignoreIfNotExists)
            throws ViewNotExistException, ViewAlreadyExistException {
        if (!viewStore.containsKey(fromView.getFullName()) && !ignoreIfNotExists) {
            throw new ViewNotExistException(fromView);
        }
        if (viewStore.containsKey(toView.getFullName())) {
            throw new ViewAlreadyExistException(toView);
        }
        if (viewStore.containsKey(fromView.getFullName())) {
            View view = viewStore.get(fromView.getFullName());
            viewStore.remove(fromView.getFullName());
            viewStore.put(toView.getFullName(), view);
        }
    }

    private Partition spec2Partition(Map<String, String> spec) {
        //todo: need update
        return new Partition(spec, 123, 456, 789, 123);
    }

    @Override
    public Optional<Snapshot> loadSnapshot(Identifier identifier) throws TableNotExistException {
        return Optional.ofNullable(tableSnapshotStore.get(identifier.getFullName()));
    }
}
