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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.flink.log.LogStoreRegister;
import org.apache.paimon.flink.log.LogStoreRegisterFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.WatermarkSpec;
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
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.TableChange.AddColumn;
import org.apache.flink.table.catalog.TableChange.AddWatermark;
import org.apache.flink.table.catalog.TableChange.After;
import org.apache.flink.table.catalog.TableChange.ColumnPosition;
import org.apache.flink.table.catalog.TableChange.DropColumn;
import org.apache.flink.table.catalog.TableChange.DropWatermark;
import org.apache.flink.table.catalog.TableChange.First;
import org.apache.flink.table.catalog.TableChange.ModifyColumnComment;
import org.apache.flink.table.catalog.TableChange.ModifyColumnName;
import org.apache.flink.table.catalog.TableChange.ModifyColumnPosition;
import org.apache.flink.table.catalog.TableChange.ModifyPhysicalColumnType;
import org.apache.flink.table.catalog.TableChange.ModifyWatermark;
import org.apache.flink.table.catalog.TableChange.ResetOption;
import org.apache.flink.table.catalog.TableChange.SetOption;
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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.descriptors.DescriptorProperties.NAME;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_ROWTIME;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_DATA_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_EXPR;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;
import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOG_SYSTEM;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOG_SYSTEM_REGISTER;
import static org.apache.paimon.flink.FlinkConnectorOptions.NONE;
import static org.apache.paimon.flink.LogicalTypeConversion.toDataType;
import static org.apache.paimon.flink.LogicalTypeConversion.toLogicalType;
import static org.apache.paimon.flink.utils.FlinkCatalogPropertiesUtil.compoundKey;
import static org.apache.paimon.flink.utils.FlinkCatalogPropertiesUtil.deserializeNonPhysicalColumn;
import static org.apache.paimon.flink.utils.FlinkCatalogPropertiesUtil.deserializeWatermarkSpec;
import static org.apache.paimon.flink.utils.FlinkCatalogPropertiesUtil.nonPhysicalColumnsCount;
import static org.apache.paimon.flink.utils.FlinkCatalogPropertiesUtil.serializeNonPhysicalColumns;
import static org.apache.paimon.flink.utils.FlinkCatalogPropertiesUtil.serializeWatermarkSpec;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Catalog for paimon. */
public class FlinkCatalog extends AbstractCatalog {
    private final ClassLoader classLoader;

    private final Catalog catalog;

    public FlinkCatalog(
            Catalog catalog, String name, String defaultDatabase, ClassLoader classLoader) {
        super(name, defaultDatabase);
        this.catalog = catalog;
        this.classLoader = classLoader;
        try {
            this.catalog.createDatabase(defaultDatabase, true);
        } catch (Catalog.DatabaseAlreadyExistException ignore) {
        }
    }

    public Catalog catalog() {
        return catalog;
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new FlinkTableFactory());
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
        Identifier identifier = toIdentifier(tablePath);
        Table table = null;
        try {
            if (catalog.tableExists(identifier)) {
                table = catalog.getTable(identifier);
            }
            catalog.dropTable(toIdentifier(tablePath), ignoreIfNotExists);
            if (table != null) {
                unRegisterLogSystem(identifier, table.options());
            }
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
                    "Paimon Catalog only supports paimon tables ,"
                            + " and you don't need to specify  'connector'= '"
                            + FlinkCatalogFactory.IDENTIFIER
                            + "' when using Paimon Catalog\n"
                            + " You can create TEMPORARY table instead if you want to create the table of other connector.");
        }

        Identifier identifier = toIdentifier(tablePath);
        Map<String, String> logSystemOptions = registerLogSystem(identifier, options);
        // remove table path
        String specific = options.remove(PATH.key());
        if (specific != null || !logSystemOptions.isEmpty()) {
            options.putAll(logSystemOptions);
            catalogTable = catalogTable.copy(options);
        }

        boolean unRegisterLogSystem = false;
        try {
            catalog.createTable(
                    identifier, FlinkCatalog.fromCatalogTable(catalogTable), ignoreIfExists);
        } catch (Catalog.TableAlreadyExistException e) {
            unRegisterLogSystem = true;
            throw new TableAlreadyExistException(getName(), tablePath);
        } catch (Catalog.DatabaseNotExistException e) {
            unRegisterLogSystem = true;
            throw new DatabaseNotExistException(getName(), e.database());
        } finally {
            if (unRegisterLogSystem && !logSystemOptions.isEmpty()) {
                unRegisterLogSystem(identifier, options);
            }
        }
    }

    private Map<String, String> registerLogSystem(
            Identifier identifier, Map<String, String> options) {
        Options tableOptions = Options.fromMap(options);
        Optional<String> register = tableOptions.getOptional(LOG_SYSTEM_REGISTER);
        if (register.isPresent()) {
            checkArgument(
                    !tableOptions.get(LOG_SYSTEM).equalsIgnoreCase(NONE),
                    String.format(
                            "%s must be configured when you use log system register.",
                            LOG_SYSTEM.key()));
            if (catalog.tableExists(identifier)) {
                return Collections.emptyMap();
            }
            LogStoreRegisterFactory registerFactory =
                    FactoryUtil.discoverFactory(
                            classLoader, LogStoreRegisterFactory.class, register.get());
            LogStoreRegister logStoreRegister =
                    registerFactory.createLogStoreRegister(tableOptions);
            return logStoreRegister.registerTopic(identifier, options);
        }
        return Collections.emptyMap();
    }

    private void unRegisterLogSystem(Identifier identifier, Map<String, String> options) {
        Options tableOptions = Options.fromMap(options);
        tableOptions
                .getOptional(LOG_SYSTEM_REGISTER)
                .ifPresent(
                        register -> {
                            checkArgument(
                                    !tableOptions.get(LOG_SYSTEM).equalsIgnoreCase(NONE),
                                    String.format(
                                            "%s must be configured when you use log system register.",
                                            LOG_SYSTEM.key()));
                            LogStoreRegisterFactory registerFactory =
                                    FactoryUtil.discoverFactory(
                                            classLoader, LogStoreRegisterFactory.class, register);
                            LogStoreRegister logStoreRegister =
                                    registerFactory.createLogStoreRegister(tableOptions);
                            logStoreRegister.unRegisterTopic(identifier, options);
                        });
    }

    private List<SchemaChange> toSchemaChange(TableChange change) {
        List<SchemaChange> schemaChanges = new ArrayList<>();
        if (change instanceof AddColumn) {
            AddColumn add = (AddColumn) change;
            String comment = add.getColumn().getComment().orElse(null);
            SchemaChange.Move move = getMove(add.getPosition(), add.getColumn().getName());
            schemaChanges.add(
                    SchemaChange.addColumn(
                            add.getColumn().getName(),
                            LogicalTypeConversion.toDataType(
                                    add.getColumn().getDataType().getLogicalType()),
                            comment,
                            move));
            return schemaChanges;
        } else if (change instanceof AddWatermark) {
            AddWatermark add = (AddWatermark) change;
            setWatermarkOptions(add.getWatermark(), schemaChanges);
            return schemaChanges;
        } else if (change instanceof DropColumn) {
            DropColumn drop = (DropColumn) change;
            schemaChanges.add(SchemaChange.dropColumn(drop.getColumnName()));
            return schemaChanges;
        } else if (change instanceof DropWatermark) {
            String watermarkPrefix = compoundKey(SCHEMA, WATERMARK, 0);
            schemaChanges.add(
                    SchemaChange.removeOption(compoundKey(watermarkPrefix, WATERMARK_ROWTIME)));
            schemaChanges.add(
                    SchemaChange.removeOption(
                            compoundKey(watermarkPrefix, WATERMARK_STRATEGY_EXPR)));
            schemaChanges.add(
                    SchemaChange.removeOption(
                            compoundKey(watermarkPrefix, WATERMARK_STRATEGY_DATA_TYPE)));
            return schemaChanges;
        } else if (change instanceof ModifyColumnName) {
            ModifyColumnName modify = (ModifyColumnName) change;
            schemaChanges.add(
                    SchemaChange.renameColumn(
                            modify.getOldColumnName(), modify.getNewColumnName()));
            return schemaChanges;
        } else if (change instanceof ModifyPhysicalColumnType) {
            ModifyPhysicalColumnType modify = (ModifyPhysicalColumnType) change;
            LogicalType newColumnType = modify.getNewType().getLogicalType();
            LogicalType oldColumnType = modify.getOldColumn().getDataType().getLogicalType();
            if (newColumnType.isNullable() != oldColumnType.isNullable()) {
                schemaChanges.add(
                        SchemaChange.updateColumnNullability(
                                modify.getNewColumn().getName(), newColumnType.isNullable()));
            }
            schemaChanges.add(
                    SchemaChange.updateColumnType(
                            modify.getOldColumn().getName(),
                            LogicalTypeConversion.toDataType(newColumnType)));
            return schemaChanges;
        } else if (change instanceof ModifyColumnPosition) {
            ModifyColumnPosition modify = (ModifyColumnPosition) change;
            SchemaChange.Move move =
                    getMove(modify.getNewPosition(), modify.getNewColumn().getName());
            schemaChanges.add(SchemaChange.updateColumnPosition(move));
            return schemaChanges;
        } else if (change instanceof TableChange.ModifyColumnComment) {
            ModifyColumnComment modify = (ModifyColumnComment) change;
            schemaChanges.add(
                    SchemaChange.updateColumnComment(
                            modify.getNewColumn().getName(), modify.getNewComment()));
            return schemaChanges;
        } else if (change instanceof ModifyWatermark) {
            ModifyWatermark modify = (ModifyWatermark) change;
            setWatermarkOptions(modify.getNewWatermark(), schemaChanges);
            return schemaChanges;
        } else if (change instanceof SetOption) {
            SetOption setOption = (SetOption) change;
            String key = setOption.getKey();
            String value = setOption.getValue();

            SchemaManager.checkAlterTablePath(key);

            schemaChanges.add(SchemaChange.setOption(key, value));
            return schemaChanges;
        } else if (change instanceof ResetOption) {
            ResetOption resetOption = (ResetOption) change;
            schemaChanges.add(SchemaChange.removeOption(resetOption.getKey()));
            return schemaChanges;
        } else {
            throw new UnsupportedOperationException(
                    "Change is not supported: " + change.getClass());
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
        } catch (Catalog.ColumnAlreadyExistException | Catalog.ColumnNotExistException e) {
            throw new CatalogException(e);
        }
    }

    @Override
    public void alterTable(
            ObjectPath tablePath,
            CatalogBaseTable newTable,
            List<TableChange> tableChanges,
            boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (ignoreIfNotExists && !tableExists(tablePath)) {
            return;
        }

        CatalogTable table = getTable(tablePath);

        validateAlterTable(table, (CatalogTable) newTable);

        List<SchemaChange> changes = new ArrayList<>();
        if (null != tableChanges) {
            List<SchemaChange> schemaChanges =
                    tableChanges.stream()
                            .flatMap(tableChange -> toSchemaChange(tableChange).stream())
                            .collect(Collectors.toList());
            changes.addAll(schemaChanges);
        }

        try {
            catalog.alterTable(toIdentifier(tablePath), changes, ignoreIfNotExists);
        } catch (Catalog.TableNotExistException
                | Catalog.ColumnAlreadyExistException
                | Catalog.ColumnNotExistException e) {
            throw new TableNotExistException(getName(), tablePath);
        }
    }

    private SchemaChange.Move getMove(ColumnPosition columnPosition, String fieldName) {
        SchemaChange.Move move = null;
        if (columnPosition instanceof First) {
            move = SchemaChange.Move.first(fieldName);
        } else if (columnPosition instanceof After) {
            move = SchemaChange.Move.after(fieldName, ((After) columnPosition).column());
        }
        return move;
    }

    private String getWatermarkKeyPrefix() {
        return compoundKey(SCHEMA, WATERMARK, 0);
    }

    private String getWatermarkRowTimeKey(String watermarkPrefix) {
        return compoundKey(watermarkPrefix, WATERMARK_ROWTIME);
    }

    private String getWatermarkExprKey(String watermarkPrefix) {
        return compoundKey(watermarkPrefix, WATERMARK_STRATEGY_EXPR);
    }

    private String getWatermarkExprDataTypeKey(String watermarkPrefix) {
        return compoundKey(watermarkPrefix, WATERMARK_STRATEGY_DATA_TYPE);
    }

    private void setWatermarkOptions(
            org.apache.flink.table.catalog.WatermarkSpec wms, List<SchemaChange> schemaChanges) {
        String watermarkPrefix = getWatermarkKeyPrefix();
        schemaChanges.add(
                SchemaChange.setOption(
                        getWatermarkRowTimeKey(watermarkPrefix), wms.getRowtimeAttribute()));
        schemaChanges.add(
                SchemaChange.setOption(
                        getWatermarkExprKey(watermarkPrefix),
                        wms.getWatermarkExpression().asSerializableString()));
        schemaChanges.add(
                SchemaChange.setOption(
                        getWatermarkExprDataTypeKey(watermarkPrefix),
                        wms.getWatermarkExpression()
                                .getOutputDataType()
                                .getLogicalType()
                                .asSerializableString()));
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

        if (!pkEquality) {
            throw new UnsupportedOperationException("Altering primary key is not supported yet.");
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
        Map<String, String> newOptions = new HashMap<>(table.options());

        TableSchema.Builder builder = TableSchema.builder();

        // add columns
        List<RowType.RowField> physicalRowFields = toLogicalType(table.rowType()).getFields();
        List<String> physicalColumns = table.rowType().getFieldNames();
        int columnCount =
                physicalRowFields.size() + nonPhysicalColumnsCount(newOptions, physicalColumns);
        int physicalColumnIndex = 0;
        for (int i = 0; i < columnCount; i++) {
            String optionalName = newOptions.get(compoundKey(SCHEMA, i, NAME));
            // to check old style physical column option
            if (optionalName == null || physicalColumns.contains(optionalName)) {
                // build physical column from table row field
                RowType.RowField field = physicalRowFields.get(physicalColumnIndex++);
                builder.field(field.getName(), fromLogicalToDataType(field.getType()));
            } else {
                // build non-physical column from options
                builder.add(deserializeNonPhysicalColumn(newOptions, i));
            }
        }

        // extract watermark information
        if (newOptions.keySet().stream()
                .anyMatch(key -> key.startsWith(compoundKey(SCHEMA, WATERMARK)))) {
            builder.watermark(deserializeWatermarkSpec(newOptions));
        }

        // add primary keys
        if (table.primaryKeys().size() > 0) {
            builder.primaryKey(table.primaryKeys().toArray(new String[0]));
        }

        TableSchema schema = builder.build();

        // remove schema from options
        DescriptorProperties removeProperties = new DescriptorProperties(false);
        removeProperties.putTableSchema(SCHEMA, schema);
        removeProperties.asMap().keySet().forEach(newOptions::remove);

        return new DataCatalogTable(
                table, schema, table.partitionKeys(), newOptions, table.comment().orElse(""));
    }

    public static Schema fromCatalogTable(CatalogTable catalogTable) {
        TableSchema schema = catalogTable.getSchema();
        RowType rowType = (RowType) schema.toPhysicalRowDataType().getLogicalType();

        Map<String, String> options = new HashMap<>(catalogTable.getOptions());
        // Serialize virtual columns and watermark to the options
        // This is what Flink SQL needs, the storage itself does not need them
        options.putAll(columnOptions(schema));

        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .comment(catalogTable.getComment())
                        .options(options)
                        .primaryKey(
                                schema.getPrimaryKey()
                                        .map(pk -> pk.getColumns())
                                        .orElse(Collections.emptyList()))
                        .partitionKeys(catalogTable.getPartitionKeys());
        Map<String, String> columnComments = getColumnComments(catalogTable);
        rowType.getFields()
                .forEach(
                        field ->
                                schemaBuilder.column(
                                        field.getName(),
                                        toDataType(field.getType()),
                                        columnComments.get(field.getName())));

        return schemaBuilder.build();
    }

    private static Map<String, String> getColumnComments(CatalogTable catalogTable) {
        return catalogTable.getUnresolvedSchema().getColumns().stream()
                .filter(c -> c.getComment().isPresent())
                .collect(
                        Collectors.toMap(
                                org.apache.flink.table.api.Schema.UnresolvedColumn::getName,
                                c -> c.getComment().get()));
    }

    /** Only reserve necessary options. */
    private static Map<String, String> columnOptions(TableSchema schema) {
        Map<String, String> columnOptions = new HashMap<>();
        // field name -> index
        final Map<String, Integer> indexMap = new HashMap<>();
        List<TableColumn> tableColumns = schema.getTableColumns();
        for (int i = 0; i < tableColumns.size(); i++) {
            indexMap.put(tableColumns.get(i).getName(), i);
        }

        // non-physical columns
        List<TableColumn> nonPhysicalColumns =
                tableColumns.stream().filter(c -> !c.isPhysical()).collect(Collectors.toList());
        if (!nonPhysicalColumns.isEmpty()) {
            columnOptions.putAll(serializeNonPhysicalColumns(indexMap, nonPhysicalColumns));
        }

        // watermark
        List<WatermarkSpec> watermarkSpecs = schema.getWatermarkSpecs();
        if (!watermarkSpecs.isEmpty()) {
            checkArgument(watermarkSpecs.size() == 1);
            columnOptions.putAll(serializeWatermarkSpec(watermarkSpecs.get(0)));
        }

        return columnOptions;
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
