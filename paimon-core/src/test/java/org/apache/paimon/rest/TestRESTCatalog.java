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
import org.apache.paimon.TableType;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.TableMetadata;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.view.View;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** A catalog for testing RESTCatalog. */
public class TestRESTCatalog extends FileSystemCatalog {

    public Map<String, TableSchema> tableFullName2Schema = new HashMap<String, TableSchema>();
    public Map<String, List<Partition>> tableFullName2Partitions =
            new HashMap<String, List<Partition>>();
    public final Map<String, View> viewFullName2View = new HashMap<String, View>();

    public TestRESTCatalog(FileIO fileIO, Path warehouse, Options options) {
        super(fileIO, warehouse, options);
    }

    public static TestRESTCatalog create(CatalogContext context) {
        String warehouse = CatalogFactory.warehouse(context).toUri().toString();

        Path warehousePath = new Path(warehouse);
        FileIO fileIO;

        try {
            fileIO = FileIO.get(warehousePath, context);
            fileIO.checkOrMkdirs(warehousePath);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return new TestRESTCatalog(fileIO, warehousePath, context.options());
    }

    @Override
    public void createPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {
        getTable(identifier);
        tableFullName2Partitions.put(
                identifier.getFullName(),
                partitions.stream()
                        .map(partition -> spec2Partition(partition))
                        .collect(Collectors.toList()));
    }

    @Override
    public void dropPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {
        getTable(identifier);
        List<Partition> existPartitions = tableFullName2Partitions.get(identifier.getFullName());
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
        List<Partition> existPartitions = tableFullName2Partitions.get(identifier.getFullName());
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
        tableFullName2Partitions.put(identifier.getFullName(), existPartitions);
    }

    @Override
    public List<Partition> listPartitions(Identifier identifier) throws TableNotExistException {
        getTable(identifier);
        return tableFullName2Partitions.get(identifier.getFullName());
    }

    @Override
    public View getView(Identifier identifier) throws ViewNotExistException {
        if (viewFullName2View.containsKey(identifier.getFullName())) {
            return viewFullName2View.get(identifier.getFullName());
        }
        throw new ViewNotExistException(identifier);
    }

    @Override
    public void dropView(Identifier identifier, boolean ignoreIfNotExists)
            throws ViewNotExistException {
        if (viewFullName2View.containsKey(identifier.getFullName())) {
            viewFullName2View.remove(identifier.getFullName());
        }
        if (!ignoreIfNotExists) {
            throw new ViewNotExistException(identifier);
        }
    }

    @Override
    public void createView(Identifier identifier, View view, boolean ignoreIfExists)
            throws ViewAlreadyExistException, DatabaseNotExistException {
        getDatabase(identifier.getDatabaseName());
        if (viewFullName2View.containsKey(identifier.getFullName()) && !ignoreIfExists) {
            throw new ViewAlreadyExistException(identifier);
        }
        viewFullName2View.put(identifier.getFullName(), view);
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException {
        getDatabase(databaseName);
        return viewFullName2View.keySet().stream()
                .map(v -> Identifier.fromString(v))
                .filter(identifier -> identifier.getDatabaseName().equals(databaseName))
                .map(identifier -> identifier.getTableName())
                .collect(Collectors.toList());
    }

    @Override
    public void renameView(Identifier fromView, Identifier toView, boolean ignoreIfNotExists)
            throws ViewNotExistException, ViewAlreadyExistException {
        if (!viewFullName2View.containsKey(fromView.getFullName()) && !ignoreIfNotExists) {
            throw new ViewNotExistException(fromView);
        }
        if (viewFullName2View.containsKey(toView.getFullName())) {
            throw new ViewAlreadyExistException(toView);
        }
        if (viewFullName2View.containsKey(fromView.getFullName())) {
            View view = viewFullName2View.get(fromView.getFullName());
            viewFullName2View.remove(fromView.getFullName());
            viewFullName2View.put(toView.getFullName(), view);
        }
    }

    @Override
    protected List<String> listTablesImpl(String databaseName) {
        List<String> tables = super.listTablesImpl(databaseName);
        for (Map.Entry<String, TableSchema> entry : tableFullName2Schema.entrySet()) {
            Identifier identifier = Identifier.fromString(entry.getKey());
            if (databaseName.equals(identifier.getDatabaseName())) {
                tables.add(identifier.getTableName());
            }
        }
        return tables;
    }

    @Override
    protected void dropTableImpl(Identifier identifier) {
        if (tableFullName2Schema.containsKey(identifier.getFullName())) {
            tableFullName2Schema.remove(identifier.getFullName());
        } else {
            super.dropTableImpl(identifier);
        }
    }

    @Override
    public void renameTableImpl(Identifier fromTable, Identifier toTable) {
        if (tableFullName2Schema.containsKey(fromTable.getFullName())) {
            TableSchema tableSchema = tableFullName2Schema.get(fromTable.getFullName());
            tableFullName2Schema.remove(fromTable.getFullName());
            tableFullName2Schema.put(toTable.getFullName(), tableSchema);
        } else {
            super.renameTableImpl(fromTable, toTable);
        }
    }

    @Override
    protected void alterTableImpl(Identifier identifier, List<SchemaChange> changes)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        if (tableFullName2Schema.containsKey(identifier.getFullName())) {
            TableSchema schema = tableFullName2Schema.get(identifier.getFullName());
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
        TableSchema tableSchema =
                new TableSchema(
                        1L,
                        schema.fields(),
                        1,
                        schema.partitionKeys(),
                        schema.primaryKeys(),
                        schema.options(),
                        schema.comment());
        tableFullName2Schema.put(identifier.getFullName(), tableSchema);
    }

    @Override
    protected TableMetadata loadTableMetadata(Identifier identifier) throws TableNotExistException {
        if (tableFullName2Schema.containsKey(identifier.getFullName())) {
            TableSchema tableSchema = tableFullName2Schema.get(identifier.getFullName());
            return new TableMetadata(tableSchema, "uuid");
        }
        return super.loadTableMetadata(identifier);
    }

    private Partition spec2Partition(Map<String, String> spec) {
        return new Partition(spec, 123, 456, 789, 123);
    }
}
