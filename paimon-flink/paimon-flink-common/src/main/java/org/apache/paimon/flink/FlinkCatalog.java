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

package org.apache.paimon.flink;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;

import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;
import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.flink.LogicalTypeConversion.toDataType;
import static org.apache.paimon.flink.LogicalTypeConversion.toLogicalType;

/** Catalog for paimon. */
public class FlinkCatalog extends AbstractCatalog {

    private final Catalog catalog;

    public FlinkCatalog(Catalog catalog, String name, String defaultDatabase) {
        super(name, defaultDatabase);
        this.catalog = catalog;
        try {
            this.catalog.createDatabase(defaultDatabase, true);
        } catch (Catalog.DatabaseAlreadyExistException ignore) {
        }
    }

    @VisibleForTesting
    public Catalog catalog() {
        return catalog;
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new FlinkTableFactory(catalog.lockFactory().orElse(null)));
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return catalog.listDatabases();
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return catalog.databaseExists(databaseName);
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        if (databaseExists(databaseName)) {
            return new CatalogDatabaseImpl(Collections.emptyMap(), null);
        }
        throw new DatabaseNotExistException(getName(), databaseName);
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        if (database != null) {
            if (database.getProperties().size() > 0) {
                throw new UnsupportedOperationException(
                        "Create database with properties is unsupported.");
            }

            if (database.getDescription().isPresent()
                    && !database.getDescription().get().equals("")) {
                throw new UnsupportedOperationException(
                        "Create database with description is unsupported.");
            }
        }

        try {
            catalog.createDatabase(name, ignoreIfExists);
        } catch (Catalog.DatabaseAlreadyExistException e) {
            throw new DatabaseAlreadyExistException(getName(), e.database());
        }
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotEmptyException, DatabaseNotExistException, CatalogException {
        try {
            catalog.dropDatabase(name, ignoreIfNotExists, cascade);
        } catch (Catalog.DatabaseNotEmptyException e) {
            throw new DatabaseNotEmptyException(getName(), e.database());
        } catch (Catalog.DatabaseNotExistException e) {
            throw new DatabaseNotExistException(getName(), e.database());
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        try {
            return catalog.listTables(databaseName);
        } catch (Catalog.DatabaseNotExistException e) {
            throw new DatabaseNotExistException(getName(), e.database());
        }
    }

    @Override
    public CatalogTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        Table table;
        try {
            table = catalog.getTable(toIdentifier(tablePath));
        } catch (Catalog.TableNotExistException e) {
            throw new TableNotExistException(getName(), tablePath);
        }

        if (table instanceof FileStoreTable) {
            return toCatalogTable(table);
        } else {
            return new SystemCatalogTable(table);
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        return catalog.tableExists(toIdentifier(tablePath));
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        try {
            catalog.dropTable(toIdentifier(tablePath), ignoreIfNotExists);
        } catch (Catalog.TableNotExistException e) {
            throw new TableNotExistException(getName(), tablePath);
        }
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        if (!(table instanceof CatalogTable)) {
            throw new UnsupportedOperationException(
                    "Only support CatalogTable, but is: " + table.getClass());
        }
        CatalogTable catalogTable = (CatalogTable) table;
        Map<String, String> options = table.getOptions();
        if (options.containsKey(CONNECTOR.key())) {
            throw new CatalogException(
                    String.format(
                            "Paimon Catalog only supports paimon tables ,"
                                    + " and you don't need to specify  'connector'= '"
                                    + FlinkCatalogFactory.IDENTIFIER
                                    + "' when using Paimon Catalog\n"
                                    + " You can create TEMPORARY table instead if you want to create the table of other connector.",
                            options.get(CONNECTOR.key())));
        }

        // remove table path
        String specific = options.remove(PATH.key());
        if (specific != null) {
            catalogTable = catalogTable.copy(options);
        }

        try {
            catalog.createTable(
                    toIdentifier(tablePath),
                    FlinkCatalog.fromCatalogTable(catalogTable),
                    ignoreIfExists);
        } catch (Catalog.TableAlreadyExistException e) {
            throw new TableAlreadyExistException(getName(), tablePath);
        } catch (Catalog.DatabaseNotExistException e) {
            throw new DatabaseNotExistException(getName(), e.database());
        }
    }

    @Override
    public void alterTable(
            ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (ignoreIfNotExists && !tableExists(tablePath)) {
            return;
        }

        CatalogTable table = getTable(tablePath);

        // Currently, Flink SQL only support altering table properties.
        validateAlterTable(table, (CatalogTable) newTable);

        List<SchemaChange> changes = new ArrayList<>();
        Map<String, String> oldProperties = table.getOptions();
        for (Map.Entry<String, String> entry : newTable.getOptions().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            if (Objects.equals(value, oldProperties.get(key))) {
                continue;
            }

            if (PATH.key().equalsIgnoreCase(key)) {
                throw new IllegalArgumentException("Illegal table path in table options: " + value);
            }

            changes.add(SchemaChange.setOption(key, value));
        }

        oldProperties
                .keySet()
                .forEach(
                        k -> {
                            if (!newTable.getOptions().containsKey(k)) {
                                changes.add(SchemaChange.removeOption(k));
                            }
                        });

        try {
            catalog.alterTable(toIdentifier(tablePath), changes, ignoreIfNotExists);
        } catch (Catalog.TableNotExistException e) {
            throw new TableNotExistException(getName(), tablePath);
        }
    }

    private static void validateAlterTable(CatalogTable ct1, CatalogTable ct2) {
        org.apache.flink.table.api.TableSchema ts1 = ct1.getSchema();
        org.apache.flink.table.api.TableSchema ts2 = ct2.getSchema();
        boolean pkEquality = false;

        if (ts1.getPrimaryKey().isPresent() && ts2.getPrimaryKey().isPresent()) {
            pkEquality =
                    Objects.equals(
                                    ts1.getPrimaryKey().get().getType(),
                                    ts2.getPrimaryKey().get().getType())
                            && Objects.equals(
                                    ts1.getPrimaryKey().get().getColumns(),
                                    ts2.getPrimaryKey().get().getColumns());
        } else if (!ts1.getPrimaryKey().isPresent() && !ts2.getPrimaryKey().isPresent()) {
            pkEquality = true;
        }

        if (!(Objects.equals(ts1.getTableColumns(), ts2.getTableColumns())
                && Objects.equals(ts1.getWatermarkSpecs(), ts2.getWatermarkSpecs())
                && pkEquality)) {
            throw new UnsupportedOperationException("Altering schema is not supported yet.");
        }

        if (!ct1.getPartitionKeys().equals(ct2.getPartitionKeys())) {
            throw new UnsupportedOperationException(
                    "Altering partition keys is not supported yet.");
        }
    }

    @Override
    public final void open() throws CatalogException {}

    @Override
    public final void close() throws CatalogException {
        try {
            catalog.close();
        } catch (Exception e) {
            throw new CatalogException("Failed to close catalog " + catalog.toString(), e);
        }
    }

    private CatalogTableImpl toCatalogTable(Table table) {
        TableSchema schema;
        Map<String, String> newOptions = new HashMap<>(table.options());

        // try to read schema from options
        // in the case of virtual columns and watermark
        DescriptorProperties tableSchemaProps = new DescriptorProperties(true);
        tableSchemaProps.putProperties(newOptions);
        Optional<TableSchema> optional =
                tableSchemaProps.getOptionalTableSchema(
                        org.apache.flink.table.descriptors.Schema.SCHEMA);
        if (optional.isPresent()) {
            schema = optional.get();

            // remove schema from options
            DescriptorProperties removeProperties = new DescriptorProperties(false);
            removeProperties.putTableSchema(SCHEMA, schema);
            removeProperties.asMap().keySet().forEach(newOptions::remove);
        } else {
            TableSchema.Builder builder = TableSchema.builder();
            for (RowType.RowField field : toLogicalType(table.rowType()).getFields()) {
                builder.field(field.getName(), fromLogicalToDataType(field.getType()));
            }
            if (table.primaryKeys().size() > 0) {
                builder.primaryKey(table.primaryKeys().toArray(new String[0]));
            }

            schema = builder.build();
        }

        return new DataCatalogTable(
                table, schema, table.partitionKeys(), newOptions, table.comment().orElse(""));
    }

    public static Schema fromCatalogTable(CatalogTable catalogTable) {
        TableSchema schema = catalogTable.getSchema();
        RowType rowType = (RowType) schema.toPhysicalRowDataType().getLogicalType();
        List<String> primaryKeys = new ArrayList<>();
        if (schema.getPrimaryKey().isPresent()) {
            primaryKeys = schema.getPrimaryKey().get().getColumns();
        }

        Map<String, String> options = new HashMap<>(catalogTable.getOptions());

        // Serialize virtual columns and watermark to the options
        // This is what Flink SQL needs, the storage itself does not need them
        if (schema.getTableColumns().stream().anyMatch(c -> !c.isPhysical())
                || schema.getWatermarkSpecs().size() > 0) {
            DescriptorProperties tableSchemaProps = new DescriptorProperties(true);
            tableSchemaProps.putTableSchema(
                    org.apache.flink.table.descriptors.Schema.SCHEMA, schema);
            options.putAll(tableSchemaProps.asMap());
        }

        return new Schema(
                addColumnComments(toDataType(rowType).getFields(), getColumnComments(catalogTable)),
                catalogTable.getPartitionKeys(),
                primaryKeys,
                options,
                catalogTable.getComment());
    }

    private static Map<String, String> getColumnComments(CatalogTable catalogTable) {
        return catalogTable.getUnresolvedSchema().getColumns().stream()
                .filter(c -> c.getComment().isPresent())
                .collect(Collectors.toMap(UnresolvedColumn::getName, c -> c.getComment().get()));
    }

    private static List<DataField> addColumnComments(
            List<DataField> fields, Map<String, String> columnComments) {
        return fields.stream()
                .map(
                        field -> {
                            String comment = columnComments.get(field.name());
                            return comment == null ? field : field.newDescription(comment);
                        })
                .collect(Collectors.toList());
    }

    public static Identifier toIdentifier(ObjectPath path) {
        return new Identifier(path.getDatabaseName(), path.getObjectName());
    }

    // --------------------- unsupported methods ----------------------------

    @Override
    public final void alterDatabase(
            String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void renameTable(
            ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws CatalogException, TableNotExistException, TableAlreadyExistException {
        ObjectPath toTable = new ObjectPath(tablePath.getDatabaseName(), newTableName);
        try {
            catalog.renameTable(toIdentifier(tablePath), toIdentifier(toTable), ignoreIfNotExists);
        } catch (Catalog.TableNotExistException e) {
            throw new TableNotExistException(getName(), tablePath);
        } catch (Catalog.TableAlreadyExistException e) {
            throw new TableAlreadyExistException(getName(), toTable);
        }
    }

    @Override
    public final List<String> listViews(String databaseName) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public final List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public final List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public final List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath, List<Expression> filters) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public final CatalogPartition getPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
    }

    @Override
    public final boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        return false;
    }

    @Override
    public final void createPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfExists)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void dropPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void alterPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition,
            boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public final List<String> listFunctions(String dbName) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public final CatalogFunction getFunction(ObjectPath functionPath)
            throws FunctionNotExistException, CatalogException {
        throw new FunctionNotExistException(getName(), functionPath);
    }

    @Override
    public final boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

    @Override
    public final void createFunction(
            ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
            throws CatalogException {
        throw new UnsupportedOperationException(
                "Create function is not supported,"
                        + " maybe you can use 'CREATE TEMPORARY FUNCTION' instead.");
    }

    @Override
    public final void alterFunction(
            ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public final CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
            throws CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public final CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public final CatalogTableStatistics getPartitionStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public final CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public final void alterTableStatistics(
            ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void alterTableColumnStatistics(
            ObjectPath tablePath,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void alterPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics,
            boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void alterPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }
}
