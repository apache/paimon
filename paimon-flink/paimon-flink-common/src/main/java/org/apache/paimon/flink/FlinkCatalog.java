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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.TableType;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.procedure.ProcedureUtil;
import org.apache.paimon.flink.utils.FlinkCatalogPropertiesUtil;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StringUtils;
import org.apache.paimon.view.View;
import org.apache.paimon.view.ViewImpl;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionImpl;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogView;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.TableChange.AddColumn;
import org.apache.flink.table.catalog.TableChange.AddWatermark;
import org.apache.flink.table.catalog.TableChange.After;
import org.apache.flink.table.catalog.TableChange.ColumnPosition;
import org.apache.flink.table.catalog.TableChange.DropColumn;
import org.apache.flink.table.catalog.TableChange.DropWatermark;
import org.apache.flink.table.catalog.TableChange.First;
import org.apache.flink.table.catalog.TableChange.MaterializedTableChange;
import org.apache.flink.table.catalog.TableChange.ModifyColumnComment;
import org.apache.flink.table.catalog.TableChange.ModifyColumnName;
import org.apache.flink.table.catalog.TableChange.ModifyColumnPosition;
import org.apache.flink.table.catalog.TableChange.ModifyPhysicalColumnType;
import org.apache.flink.table.catalog.TableChange.ModifyRefreshHandler;
import org.apache.flink.table.catalog.TableChange.ModifyRefreshStatus;
import org.apache.flink.table.catalog.TableChange.ModifyWatermark;
import org.apache.flink.table.catalog.TableChange.ResetOption;
import org.apache.flink.table.catalog.TableChange.SetOption;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.ProcedureNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.procedures.Procedure;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.table.descriptors.DescriptorProperties.COMMENT;
import static org.apache.flink.table.descriptors.DescriptorProperties.NAME;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_ROWTIME;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_DATA_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_EXPR;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;
import static org.apache.flink.table.utils.EncodingUtils.decodeBase64ToBytes;
import static org.apache.flink.table.utils.EncodingUtils.encodeBytesToBase64;
import static org.apache.paimon.CoreOptions.MATERIALIZED_TABLE_DEFINITION_QUERY;
import static org.apache.paimon.CoreOptions.MATERIALIZED_TABLE_INTERVAL_FRESHNESS;
import static org.apache.paimon.CoreOptions.MATERIALIZED_TABLE_INTERVAL_FRESHNESS_TIME_UNIT;
import static org.apache.paimon.CoreOptions.MATERIALIZED_TABLE_LOGICAL_REFRESH_MODE;
import static org.apache.paimon.CoreOptions.MATERIALIZED_TABLE_REFRESH_HANDLER_BYTES;
import static org.apache.paimon.CoreOptions.MATERIALIZED_TABLE_REFRESH_HANDLER_DESCRIPTION;
import static org.apache.paimon.CoreOptions.MATERIALIZED_TABLE_REFRESH_MODE;
import static org.apache.paimon.CoreOptions.MATERIALIZED_TABLE_REFRESH_STATUS;
import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.catalog.Catalog.LAST_UPDATE_TIME_PROP;
import static org.apache.paimon.catalog.Catalog.NUM_FILES_PROP;
import static org.apache.paimon.catalog.Catalog.NUM_ROWS_PROP;
import static org.apache.paimon.catalog.Catalog.TOTAL_SIZE_PROP;
import static org.apache.paimon.flink.FlinkCatalogOptions.DISABLE_CREATE_TABLE_IN_DEFAULT_DB;
import static org.apache.paimon.flink.FlinkCatalogOptions.LOG_SYSTEM_AUTO_REGISTER;
import static org.apache.paimon.flink.FlinkCatalogOptions.REGISTER_TIMEOUT;
import static org.apache.paimon.flink.LogicalTypeConversion.toDataType;
import static org.apache.paimon.flink.LogicalTypeConversion.toLogicalType;
import static org.apache.paimon.flink.log.LogStoreRegister.registerLogSystem;
import static org.apache.paimon.flink.log.LogStoreRegister.unRegisterLogSystem;
import static org.apache.paimon.flink.utils.FlinkCatalogPropertiesUtil.compoundKey;
import static org.apache.paimon.flink.utils.FlinkCatalogPropertiesUtil.deserializeNonPhysicalColumn;
import static org.apache.paimon.flink.utils.FlinkCatalogPropertiesUtil.deserializeWatermarkSpec;
import static org.apache.paimon.flink.utils.FlinkCatalogPropertiesUtil.nonPhysicalColumnsCount;
import static org.apache.paimon.flink.utils.FlinkCatalogPropertiesUtil.serializeNewWatermarkSpec;
import static org.apache.paimon.flink.utils.TableStatsUtil.createTableColumnStats;
import static org.apache.paimon.flink.utils.TableStatsUtil.createTableStats;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Catalog for paimon. */
public class FlinkCatalog extends AbstractCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkCatalog.class);

    private final ClassLoader classLoader;
    private final Catalog catalog;
    private final String name;
    private final boolean logStoreAutoRegister;

    private final Duration logStoreAutoRegisterTimeout;

    private final boolean disableCreateTableInDefaultDatabase;

    public FlinkCatalog(
            Catalog catalog,
            String name,
            String defaultDatabase,
            ClassLoader classLoader,
            Options options) {
        super(name, defaultDatabase);
        this.catalog = catalog;
        this.name = name;
        this.classLoader = classLoader;
        this.logStoreAutoRegister = options.get(LOG_SYSTEM_AUTO_REGISTER);
        this.logStoreAutoRegisterTimeout = options.get(REGISTER_TIMEOUT);
        this.disableCreateTableInDefaultDatabase = options.get(DISABLE_CREATE_TABLE_IN_DEFAULT_DB);
        if (!disableCreateTableInDefaultDatabase) {
            if (!catalog.databaseExists(defaultDatabase)) {
                try {
                    catalog.createDatabase(defaultDatabase, true);
                } catch (Catalog.DatabaseAlreadyExistException ignore) {
                }
            }
        }
    }

    public Catalog catalog() {
        return catalog;
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new FlinkTableFactory(this));
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
            if (database.getDescription().isPresent()
                    && !database.getDescription().get().equals("")) {
                throw new UnsupportedOperationException(
                        "Create database with description is unsupported.");
            }
        }

        try {
            catalog.createDatabase(
                    name,
                    ignoreIfExists,
                    database == null ? Collections.emptyMap() : database.getProperties());
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
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return getTable(tablePath, null);
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 1.17-.
     */
    public CatalogBaseTable getTable(ObjectPath tablePath, long timestamp)
            throws TableNotExistException, CatalogException {
        return getTable(tablePath, Long.valueOf(timestamp));
    }

    private CatalogBaseTable getTable(ObjectPath tablePath, @Nullable Long timestamp)
            throws TableNotExistException {
        Table table;
        try {
            table = catalog.getTable(toIdentifier(tablePath));
        } catch (Catalog.TableNotExistException e) {
            Optional<CatalogBaseTable> view = getView(tablePath, timestamp);
            if (view.isPresent()) {
                return view.get();
            }

            throw new TableNotExistException(getName(), tablePath);
        }

        if (table instanceof FormatTable) {
            if (timestamp != null) {
                throw new UnsupportedOperationException(
                        String.format(
                                "Format table %s cannot support as of timestamp.", tablePath));
            }
            return new FormatCatalogTable((FormatTable) table);
        }

        if (timestamp != null) {
            Options options = new Options();
            options.set(CoreOptions.SCAN_TIMESTAMP_MILLIS, timestamp);
            table = table.copy(options.toMap());
        }

        if (table instanceof FileStoreTable) {
            return toCatalogTable(table);
        } else {
            return new SystemCatalogTable(table);
        }
    }

    private Optional<CatalogBaseTable> getView(ObjectPath tablePath, @Nullable Long timestamp) {
        View view;
        try {
            view = catalog.getView(toIdentifier(tablePath));
        } catch (Catalog.ViewNotExistException e) {
            return Optional.empty();
        }

        if (timestamp != null) {
            throw new UnsupportedOperationException(
                    String.format("View %s does not support time travel.", tablePath));
        }

        org.apache.flink.table.api.Schema schema =
                org.apache.flink.table.api.Schema.newBuilder()
                        .fromRowDataType(fromLogicalToDataType(toLogicalType(view.rowType())))
                        .build();
        return Optional.of(
                CatalogView.of(
                        schema,
                        view.comment().orElse(null),
                        view.query(),
                        view.query(),
                        view.options()));
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        Identifier identifier = toIdentifier(tablePath);
        return catalog.tableExists(identifier) || catalog.viewExists(identifier);
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        Identifier identifier = toIdentifier(tablePath);
        if (catalog.viewExists(identifier)) {
            try {
                catalog.dropView(identifier, ignoreIfNotExists);
                return;
            } catch (Catalog.ViewNotExistException e) {
                throw new RuntimeException("Unexpected exception.", e);
            }
        }

        try {
            Table table = null;
            if (logStoreAutoRegister && catalog.tableExists(identifier)) {
                table = catalog.getTable(identifier);
            }
            catalog.dropTable(toIdentifier(tablePath), ignoreIfNotExists);
            if (logStoreAutoRegister && table != null) {
                unRegisterLogSystem(identifier, table.options(), classLoader);
            }
        } catch (Catalog.TableNotExistException e) {
            throw new TableNotExistException(getName(), tablePath);
        }
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        if (Objects.equals(getDefaultDatabase(), tablePath.getDatabaseName())
                && disableCreateTableInDefaultDatabase) {
            throw new UnsupportedOperationException(
                    "Creating table in default database is disabled, please specify a database name.");
        }

        if (table instanceof CatalogView) {
            createView(tablePath, (ResolvedCatalogView) table, ignoreIfExists);
            return;
        }

        Identifier identifier = toIdentifier(tablePath);
        // the returned value of "table.getOptions" may be unmodifiable (for example from
        // TableDescriptor)
        Map<String, String> options = new HashMap<>(table.getOptions());
        if (table instanceof CatalogMaterializedTable) {
            fillOptionsForMaterializedTable((CatalogMaterializedTable) table, options);
        }
        Schema paimonSchema = buildPaimonSchema(identifier, table, options);

        boolean unRegisterLogSystem = false;
        try {
            catalog.createTable(identifier, paimonSchema, ignoreIfExists);
        } catch (Catalog.TableAlreadyExistException e) {
            unRegisterLogSystem = true;
            throw new TableAlreadyExistException(getName(), tablePath);
        } catch (Catalog.DatabaseNotExistException e) {
            unRegisterLogSystem = true;
            throw new DatabaseNotExistException(getName(), e.database());
        } finally {
            if (logStoreAutoRegister && unRegisterLogSystem) {
                unRegisterLogSystem(identifier, options, classLoader);
            }
        }
    }

    private void createView(ObjectPath tablePath, ResolvedCatalogView table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        Identifier identifier = toIdentifier(tablePath);
        org.apache.paimon.types.RowType.Builder builder = org.apache.paimon.types.RowType.builder();
        table.getResolvedSchema()
                .getColumns()
                .forEach(
                        column ->
                                builder.field(
                                        column.getName(),
                                        toDataType(column.getDataType().getLogicalType()),
                                        column.getComment().orElse(null)));
        View view =
                new ViewImpl(
                        identifier,
                        builder.build(),
                        table.getOriginalQuery(),
                        table.getComment(),
                        table.getOptions());
        try {
            catalog.createView(identifier, view, ignoreIfExists);
        } catch (Catalog.ViewAlreadyExistException e) {
            throw new TableAlreadyExistException(getName(), tablePath);
        } catch (Catalog.DatabaseNotExistException e) {
            throw new DatabaseNotExistException(getName(), tablePath.getDatabaseName());
        }
    }

    private static void fillOptionsForMaterializedTable(
            CatalogMaterializedTable mt, Map<String, String> options) {
        Options mtOptions = new Options();
        mtOptions.set(CoreOptions.TYPE, TableType.MATERIALIZED_TABLE);
        mtOptions.set(MATERIALIZED_TABLE_DEFINITION_QUERY, mt.getDefinitionQuery());
        mtOptions.set(
                MATERIALIZED_TABLE_INTERVAL_FRESHNESS, mt.getDefinitionFreshness().getInterval());
        mtOptions.set(
                MATERIALIZED_TABLE_INTERVAL_FRESHNESS_TIME_UNIT,
                CoreOptions.MaterializedTableIntervalFreshnessTimeUnit.valueOf(
                        mt.getDefinitionFreshness().getTimeUnit().name()));
        mtOptions.set(
                MATERIALIZED_TABLE_LOGICAL_REFRESH_MODE,
                CoreOptions.MaterializedTableRefreshMode.valueOf(
                        mt.getLogicalRefreshMode().name()));
        mtOptions.set(
                MATERIALIZED_TABLE_REFRESH_MODE,
                CoreOptions.MaterializedTableRefreshMode.valueOf(mt.getRefreshMode().name()));
        mtOptions.set(
                MATERIALIZED_TABLE_REFRESH_STATUS,
                CoreOptions.MaterializedTableRefreshStatus.valueOf(mt.getRefreshStatus().name()));
        mt.getRefreshHandlerDescription()
                .ifPresent(
                        desc ->
                                mtOptions.set(
                                        MATERIALIZED_TABLE_REFRESH_HANDLER_DESCRIPTION, desc));
        byte[] serializedRefreshHandler = mt.getSerializedRefreshHandler();
        if (serializedRefreshHandler != null) {
            mtOptions.set(
                    MATERIALIZED_TABLE_REFRESH_HANDLER_BYTES,
                    encodeBytesToBase64(serializedRefreshHandler));
        }
        options.putAll(mtOptions.toMap());
    }

    protected Schema buildPaimonSchema(
            Identifier identifier, CatalogBaseTable catalogTable, Map<String, String> options) {
        String connector = options.get(CONNECTOR.key());
        options.remove(CONNECTOR.key());
        if (!StringUtils.isNullOrWhitespaceOnly(connector)
                && !FlinkCatalogFactory.IDENTIFIER.equals(connector)) {
            throw new CatalogException(
                    "Paimon Catalog only supports paimon tables,"
                            + " but you specify  'connector'= '"
                            + connector
                            + "' when using Paimon Catalog\n"
                            + " You can create TEMPORARY table instead if you want to create the table of other connector.");
        }

        if (logStoreAutoRegister) {
            // Although catalog.createTable will copy the default options, but we need this info
            // here before create table, such as table-default.kafka.bootstrap.servers defined in
            // catalog options. Temporarily, we copy the default options here.
            Catalog.tableDefaultOptions(catalog.options()).forEach(options::putIfAbsent);
            options.put(REGISTER_TIMEOUT.key(), logStoreAutoRegisterTimeout.toString());
            registerLogSystem(catalog, identifier, options, classLoader);
        }

        // remove table path
        String path = options.remove(PATH.key());
        if (path != null) {
            Path expectedPath = catalog.getTableLocation(identifier);
            if (!new Path(path).equals(expectedPath)) {
                throw new CatalogException(
                        String.format(
                                "You specified the Path when creating the table, "
                                        + "but the Path '%s' is different from where it should be '%s'. "
                                        + "Please remove the Path.",
                                path, expectedPath));
            }
        }

        if (catalogTable instanceof CatalogTable) {
            return fromCatalogTable(((CatalogTable) catalogTable).copy(options));
        }
        return fromCatalogTable(((CatalogMaterializedTable) catalogTable).copy(options));
    }

    private List<SchemaChange> toSchemaChange(
            TableChange change, Map<String, Integer> oldTableNonPhysicalColumnIndex) {
        List<SchemaChange> schemaChanges = new ArrayList<>();
        if (change instanceof AddColumn) {
            if (((AddColumn) change).getColumn().isPhysical()) {
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
            }
            return schemaChanges;
        } else if (change instanceof AddWatermark) {
            AddWatermark add = (AddWatermark) change;
            setWatermarkOptions(add.getWatermark(), schemaChanges);
            return schemaChanges;
        } else if (change instanceof DropColumn) {
            if (!oldTableNonPhysicalColumnIndex.containsKey(
                    ((DropColumn) change).getColumnName())) {
                DropColumn drop = (DropColumn) change;
                schemaChanges.add(SchemaChange.dropColumn(drop.getColumnName()));
            }
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
            if (!oldTableNonPhysicalColumnIndex.containsKey(
                    ((ModifyColumnName) change).getOldColumnName())) {
                ModifyColumnName modify = (ModifyColumnName) change;
                schemaChanges.add(
                        SchemaChange.renameColumn(
                                modify.getOldColumnName(), modify.getNewColumnName()));
            }
            return schemaChanges;
        } else if (change instanceof ModifyPhysicalColumnType) {
            if (!oldTableNonPhysicalColumnIndex.containsKey(
                    ((ModifyPhysicalColumnType) change).getOldColumn().getName())) {
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
            }
            return schemaChanges;
        } else if (change instanceof ModifyColumnPosition) {
            if (!oldTableNonPhysicalColumnIndex.containsKey(
                    ((ModifyColumnPosition) change).getOldColumn().getName())) {
                ModifyColumnPosition modify = (ModifyColumnPosition) change;
                SchemaChange.Move move =
                        getMove(modify.getNewPosition(), modify.getNewColumn().getName());
                schemaChanges.add(SchemaChange.updateColumnPosition(move));
            }
            return schemaChanges;
        } else if (change instanceof TableChange.ModifyColumnComment) {
            if (!oldTableNonPhysicalColumnIndex.containsKey(
                    ((ModifyColumnComment) change).getOldColumn().getName())) {
                ModifyColumnComment modify = (ModifyColumnComment) change;
                schemaChanges.add(
                        SchemaChange.updateColumnComment(
                                modify.getNewColumn().getName(), modify.getNewComment()));
            }
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

            if (Catalog.COMMENT_PROP.equals(key)) {
                schemaChanges.add(SchemaChange.updateComment(value));
            } else {
                schemaChanges.add(SchemaChange.setOption(key, value));
            }
            return schemaChanges;
        } else if (change instanceof ResetOption) {
            ResetOption resetOption = (ResetOption) change;
            String key = resetOption.getKey();
            if (Catalog.COMMENT_PROP.equals(key)) {
                schemaChanges.add(SchemaChange.updateComment(null));
            } else {
                schemaChanges.add(SchemaChange.removeOption(resetOption.getKey()));
            }
            return schemaChanges;
        } else if (change instanceof TableChange.ModifyColumn) {
            // let non-physical column handle by option
            if (oldTableNonPhysicalColumnIndex.containsKey(
                            ((TableChange.ModifyColumn) change).getOldColumn().getName())
                    && !(((TableChange.ModifyColumn) change).getNewColumn()
                            instanceof Column.PhysicalColumn)) {
                return schemaChanges;
            } else {
                throw new UnsupportedOperationException(
                        "Change is not supported: " + change.getClass());
            }
        } else if (change instanceof MaterializedTableChange
                && handleMaterializedTableChange(change, schemaChanges)) {
            return schemaChanges;
        }
        throw new UnsupportedOperationException("Change is not supported: " + change.getClass());
    }

    /**
     * Try handle change related to materialized table.
     *
     * @return true, if change can be identified as {@link MaterializedTableChange} and is handled
     *     properly. Otherwise, false.
     */
    protected boolean handleMaterializedTableChange(
            TableChange change, List<SchemaChange> schemaChanges) {
        if (change instanceof ModifyRefreshStatus) {
            ModifyRefreshStatus modifyRefreshStatus = (ModifyRefreshStatus) change;
            CatalogMaterializedTable.RefreshStatus newRefreshStatus =
                    modifyRefreshStatus.getRefreshStatus();
            schemaChanges.add(
                    SchemaChange.setOption(
                            MATERIALIZED_TABLE_REFRESH_STATUS.key(), newRefreshStatus.name()));
            return true;
        } else if (change instanceof ModifyRefreshHandler) {
            ModifyRefreshHandler modifyRefreshHandler = (ModifyRefreshHandler) change;
            String newHandlerDesc = modifyRefreshHandler.getRefreshHandlerDesc();
            byte[] newHandlerBytes = modifyRefreshHandler.getRefreshHandlerBytes();
            schemaChanges.add(
                    SchemaChange.setOption(
                            MATERIALIZED_TABLE_REFRESH_HANDLER_DESCRIPTION.key(), newHandlerDesc));
            schemaChanges.add(
                    SchemaChange.setOption(
                            MATERIALIZED_TABLE_REFRESH_HANDLER_BYTES.key(),
                            encodeBytesToBase64(newHandlerBytes)));
            return true;
        }
        return false;
    }

    @Override
    public void alterTable(
            ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (ignoreIfNotExists && !tableExists(tablePath)) {
            return;
        }

        CatalogBaseTable table = getTable(tablePath);

        // Currently, Flink SQL only support altering table properties.
        validateAlterTable(table, newTable);

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

        Table table;
        try {
            table = catalog.getTable(toIdentifier(tablePath));
        } catch (Catalog.TableNotExistException e) {
            throw new TableNotExistException(getName(), tablePath);
        }

        Preconditions.checkArgument(table instanceof FileStoreTable, "Can't alter system table.");
        validateAlterTable(toCatalogTable(table), newTable);
        Map<String, Integer> oldTableNonPhysicalColumnIndex =
                FlinkCatalogPropertiesUtil.nonPhysicalColumns(
                        table.options(), table.rowType().getFieldNames());

        List<SchemaChange> changes = new ArrayList<>();

        Map<String, String> schemaOptions =
                FlinkCatalogPropertiesUtil.serializeNonPhysicalNewColumns(
                        ((ResolvedCatalogBaseTable<?>) newTable).getResolvedSchema());
        table.options()
                .forEach(
                        (k, v) -> {
                            if (FlinkCatalogPropertiesUtil.isNonPhysicalColumnKey(k)) {
                                // drop non-physical column
                                if (!schemaOptions.containsKey(k)) {
                                    changes.add(SchemaChange.removeOption(k));
                                }
                            }
                        });
        schemaOptions.forEach(
                (k, v) -> {
                    // add/modify non-physical column
                    if (!table.options().containsKey(k) || !table.options().get(k).equals(v)) {
                        changes.add(SchemaChange.setOption(k, v));
                    }
                });
        if (null != tableChanges) {
            List<SchemaChange> schemaChanges =
                    tableChanges.stream()
                            .flatMap(
                                    tableChange ->
                                            toSchemaChange(
                                                    tableChange, oldTableNonPhysicalColumnIndex)
                                                    .stream())
                            .collect(Collectors.toList());
            changes.addAll(schemaChanges);
        }

        try {
            catalog.alterTable(toIdentifier(tablePath), changes, ignoreIfNotExists);
        } catch (Catalog.TableNotExistException
                | Catalog.ColumnAlreadyExistException
                | Catalog.ColumnNotExistException e) {
            throw new CatalogException(e);
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

    private static void validateAlterTable(CatalogBaseTable ct1, CatalogBaseTable ct2) {
        if (ct1 instanceof SystemCatalogTable) {
            throw new UnsupportedOperationException("Can't alter system table.");
        }
        boolean table1IsMaterialized = ct1 instanceof CatalogMaterializedTable;
        boolean table2IsMaterialized = ct2 instanceof CatalogMaterializedTable;
        if ((table1IsMaterialized || table2IsMaterialized)
                && !(table1IsMaterialized && table2IsMaterialized)) {
            throw new UnsupportedOperationException(
                    "Convert a non-materialized table to materialized table or vice versa is not allowed.");
        }
        // materialized table is not resolved at this time.
        if (!table1IsMaterialized) {
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
                throw new UnsupportedOperationException(
                        "Altering primary key is not supported yet.");
            }
        }

        if (!getPartitionKeys(ct1).equals(getPartitionKeys(ct2))) {
            throw new UnsupportedOperationException(
                    "Altering partition keys is not supported yet.");
        }
    }

    private static List<String> getPartitionKeys(CatalogBaseTable table) {
        if (table instanceof CatalogTable) {
            return ((CatalogTable) table).getPartitionKeys();
        } else if (table instanceof CatalogMaterializedTable) {
            return ((CatalogMaterializedTable) table).getPartitionKeys();
        } else {
            throw new UnsupportedOperationException(
                    "Only support CatalogTable and CatalogMaterializedTable, but is: "
                            + table.getClass());
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

    private CatalogBaseTable toCatalogTable(Table table) {
        Map<String, String> newOptions = new HashMap<>(table.options());

        TableSchema.Builder builder = TableSchema.builder();
        Map<String, String> nonPhysicalColumnComments = new HashMap<>();

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
                if (newOptions.containsKey(compoundKey(SCHEMA, i, COMMENT))) {
                    nonPhysicalColumnComments.put(
                            optionalName, newOptions.get(compoundKey(SCHEMA, i, COMMENT)));
                    newOptions.remove(compoundKey(SCHEMA, i, COMMENT));
                }
            }
        }

        // extract watermark information
        if (newOptions.keySet().stream()
                .anyMatch(key -> key.startsWith(compoundKey(SCHEMA, WATERMARK)))) {
            builder.watermark(deserializeWatermarkSpec(newOptions));
        }

        // add primary keys
        if (table.primaryKeys().size() > 0) {
            builder.primaryKey(
                    table.primaryKeys().stream().collect(Collectors.joining("_", "PK_", "")),
                    table.primaryKeys().toArray(new String[0]));
        }

        TableSchema schema = builder.build();

        // remove schema from options
        DescriptorProperties removeProperties = new DescriptorProperties(false);
        removeProperties.putTableSchema(SCHEMA, schema);
        removeProperties.asMap().keySet().forEach(newOptions::remove);

        Options options = Options.fromMap(newOptions);
        if (TableType.MATERIALIZED_TABLE == options.get(CoreOptions.TYPE)) {
            return buildMaterializedTable(table, newOptions, schema, options);
        }
        return new DataCatalogTable(
                table,
                schema,
                table.partitionKeys(),
                newOptions,
                table.comment().orElse(""),
                nonPhysicalColumnComments);
    }

    private CatalogMaterializedTable buildMaterializedTable(
            Table table, Map<String, String> newOptions, TableSchema schema, Options options) {
        String definitionQuery = options.get(MATERIALIZED_TABLE_DEFINITION_QUERY);
        IntervalFreshness freshness =
                IntervalFreshness.of(
                        options.get(MATERIALIZED_TABLE_INTERVAL_FRESHNESS),
                        IntervalFreshness.TimeUnit.valueOf(
                                options.get(MATERIALIZED_TABLE_INTERVAL_FRESHNESS_TIME_UNIT)
                                        .name()));
        CatalogMaterializedTable.LogicalRefreshMode logicalRefreshMode =
                CatalogMaterializedTable.LogicalRefreshMode.valueOf(
                        options.get(MATERIALIZED_TABLE_LOGICAL_REFRESH_MODE).name());
        CatalogMaterializedTable.RefreshMode refreshMode =
                CatalogMaterializedTable.RefreshMode.valueOf(
                        options.get(MATERIALIZED_TABLE_REFRESH_MODE).name());
        CatalogMaterializedTable.RefreshStatus refreshStatus =
                CatalogMaterializedTable.RefreshStatus.valueOf(
                        options.get(MATERIALIZED_TABLE_REFRESH_STATUS).name());
        String refreshHandlerDescription =
                options.get(MATERIALIZED_TABLE_REFRESH_HANDLER_DESCRIPTION);
        byte[] serializedRefreshHandler =
                decodeRefreshHandlerBytes(options.get(MATERIALIZED_TABLE_REFRESH_HANDLER_BYTES));
        // remove materialized table related options
        allMaterializedTableAttributes().forEach(newOptions::remove);
        return CatalogMaterializedTable.newBuilder()
                .schema(schema.toSchema())
                .comment(table.comment().orElse(""))
                .partitionKeys(table.partitionKeys())
                .options(newOptions)
                .definitionQuery(definitionQuery)
                .freshness(freshness)
                .logicalRefreshMode(logicalRefreshMode)
                .refreshMode(refreshMode)
                .refreshStatus(refreshStatus)
                .refreshHandlerDescription(refreshHandlerDescription)
                .serializedRefreshHandler(serializedRefreshHandler)
                .build();
    }

    private byte[] decodeRefreshHandlerBytes(String refreshHandlerBytes) {
        return org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly(refreshHandlerBytes)
                ? null
                : decodeBase64ToBytes(refreshHandlerBytes);
    }

    public static Schema fromCatalogTable(CatalogBaseTable catalogTable) {
        ResolvedSchema schema =
                ((ResolvedCatalogBaseTable<CatalogBaseTable>) catalogTable).getResolvedSchema();
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
                        .partitionKeys(getPartitionKeys(catalogTable));
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

    private static Map<String, String> getColumnComments(CatalogBaseTable catalogTable) {
        return catalogTable.getUnresolvedSchema().getColumns().stream()
                .filter(c -> c.getComment().isPresent())
                .collect(
                        Collectors.toMap(
                                org.apache.flink.table.api.Schema.UnresolvedColumn::getName,
                                c -> c.getComment().get()));
    }

    /** Only reserve necessary options. */
    private static Map<String, String> columnOptions(ResolvedSchema schema) {
        Map<String, String> columnOptions =
                new HashMap<>(FlinkCatalogPropertiesUtil.serializeNonPhysicalNewColumns(schema));
        List<WatermarkSpec> watermarkSpecs = schema.getWatermarkSpecs();
        if (!watermarkSpecs.isEmpty()) {
            checkArgument(watermarkSpecs.size() == 1);
            columnOptions.putAll(serializeNewWatermarkSpec(watermarkSpecs.get(0)));
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
    public final List<String> listViews(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        try {
            return catalog.listViews(databaseName);
        } catch (Catalog.DatabaseNotExistException e) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    @Override
    public final List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        // The method is skipped if it is being called as part of FlinkRecomputeStatisticsProgram,
        // since this program scans the entire table and all its partitions, which is a
        // time-consuming operation. By returning an empty result, we can prompt the FlinkPlanner to
        // use the FlinkTableSource#reportStatistics method to gather the necessary statistics.
        if (isCalledFromFlinkRecomputeStatisticsProgram()) {
            LOG.info(
                    "Skipping listPartitions method due to detection of FlinkRecomputeStatisticsProgram call.");
            return Collections.emptyList();
        }
        return getPartitionSpecs(tablePath, null);
    }

    private Table getPaimonTable(ObjectPath tablePath) throws TableNotExistException {
        try {
            Identifier identifier = toIdentifier(tablePath);
            return catalog.getTable(identifier);
        } catch (Catalog.TableNotExistException e) {
            throw new TableNotExistException(getName(), tablePath);
        }
    }

    private List<PartitionEntry> getPartitionEntries(
            Table table, ObjectPath tablePath, @Nullable CatalogPartitionSpec partitionSpec)
            throws TableNotPartitionedException {
        if (table.partitionKeys() == null || table.partitionKeys().size() == 0) {
            throw new TableNotPartitionedException(getName(), tablePath);
        }

        ReadBuilder readBuilder = table.newReadBuilder();
        if (partitionSpec != null && partitionSpec.getPartitionSpec() != null) {
            readBuilder.withPartitionFilter(partitionSpec.getPartitionSpec());
        }
        return readBuilder.newScan().listPartitionEntries();
    }

    private List<CatalogPartitionSpec> getPartitionSpecs(
            ObjectPath tablePath, @Nullable CatalogPartitionSpec partitionSpec)
            throws CatalogException, TableNotPartitionedException, TableNotExistException {
        FileStoreTable table = (FileStoreTable) getPaimonTable(tablePath);
        List<PartitionEntry> partitionEntries =
                getPartitionEntries(table, tablePath, partitionSpec);
        org.apache.paimon.types.RowType partitionRowType = table.schema().logicalPartitionType();

        CoreOptions options = new CoreOptions(table.options());
        InternalRowPartitionComputer partitionComputer =
                FileStorePathFactory.getPartitionComputer(
                        partitionRowType,
                        options.partitionDefaultName(),
                        options.legacyPartitionName());

        return partitionEntries.stream()
                .map(
                        e -> {
                            LinkedHashMap<String, String> partValues =
                                    partitionComputer.generatePartValues(
                                            Preconditions.checkNotNull(
                                                    e.partition(),
                                                    "Partition row data is null. This is unexpected."));
                            return new CatalogPartitionSpec(partValues);
                        })
                .collect(Collectors.toList());
    }

    @Override
    public final List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return getPartitionSpecs(tablePath, partitionSpec);
    }

    @Override
    public final List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath, List<Expression> filters) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        checkNotNull(partitionSpec, "partition spec shouldn't be null");
        try {
            List<PartitionEntry> partitionEntries =
                    getPartitionEntries(getPaimonTable(tablePath), tablePath, partitionSpec);
            if (partitionEntries.isEmpty()) {
                throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
            }
            // This was already filtered by the expected partition.
            PartitionEntry partitionEntry = partitionEntries.get(0);
            Map<String, String> properties = new HashMap<>();
            properties.put(NUM_ROWS_PROP, String.valueOf(partitionEntry.recordCount()));
            properties.put(
                    LAST_UPDATE_TIME_PROP, String.valueOf(partitionEntry.lastFileCreationTime()));
            properties.put(NUM_FILES_PROP, String.valueOf(partitionEntry.fileCount()));
            properties.put(TOTAL_SIZE_PROP, String.valueOf(partitionEntry.fileSizeInBytes()));
            return new CatalogPartitionImpl(properties, "");
        } catch (TableNotPartitionedException | TableNotExistException e) {
            throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
        }
    }

    @Override
    public final boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        try {
            List<CatalogPartitionSpec> partitionSpecs = getPartitionSpecs(tablePath, partitionSpec);
            return partitionSpecs.size() > 0;
        } catch (TableNotPartitionedException | TableNotExistException e) {
            throw new CatalogException(e);
        }
    }

    @Override
    public final void createPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfExists)
            throws CatalogException, PartitionAlreadyExistsException {
        if (partitionExists(tablePath, partitionSpec)) {
            if (!ignoreIfExists) {
                throw new PartitionAlreadyExistsException(getName(), tablePath, partitionSpec);
            }
        }

        try {
            Identifier identifier = toIdentifier(tablePath);
            catalog.createPartition(identifier, partitionSpec.getPartitionSpec());
        } catch (Catalog.TableNotExistException e) {
            throw new CatalogException(e);
        }
    }

    @Override
    public final void dropPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {

        if (!partitionExists(tablePath, partitionSpec)) {
            if (!ignoreIfNotExists) {
                throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
            }
        }

        try {
            Identifier identifier = toIdentifier(tablePath);
            catalog.dropPartition(identifier, partitionSpec.getPartitionSpec());
        } catch (Catalog.TableNotExistException e) {
            throw new CatalogException(e);
        } catch (Catalog.PartitionNotExistException e) {
            throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
        }
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
            throws CatalogException, TableNotExistException {
        alterTableStatisticsInternal(
                tablePath, t -> createTableStats(t, tableStatistics), ignoreIfNotExists);
    }

    @Override
    public final void alterTableColumnStatistics(
            ObjectPath tablePath,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws CatalogException, TableNotExistException {
        alterTableStatisticsInternal(
                tablePath, t -> createTableColumnStats(t, columnStatistics), ignoreIfNotExists);
    }

    private void alterTableStatisticsInternal(
            ObjectPath tablePath,
            Function<FileStoreTable, Statistics> statistics,
            boolean ignoreIfNotExists)
            throws TableNotExistException {
        try {
            Table table = catalog.getTable(toIdentifier(tablePath));
            checkArgument(
                    table instanceof FileStoreTable, "Now only support analyze FileStoreTable.");
            if (!table.latestSnapshotId().isPresent()) {
                LOG.info("Skipping analyze table because the snapshot is null.");
                return;
            }

            FileStoreTable storeTable = (FileStoreTable) table;
            Statistics tableStats = statistics.apply(storeTable);
            if (tableStats != null) {
                String commitUser = storeTable.coreOptions().createCommitUser();
                try (FileStoreCommit commit = storeTable.store().newCommit(commitUser)) {
                    commit.commitStatistics(tableStats, BatchWriteBuilder.COMMIT_IDENTIFIER);
                }
            }
        } catch (Catalog.TableNotExistException e) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(getName(), tablePath);
            }
        }
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

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 1.17-.
     */
    public List<String> listProcedures(String dbName)
            throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(dbName)) {
            throw new DatabaseNotExistException(name, dbName);
        }

        return ProcedureUtil.listProcedures();
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 1.17-.
     */
    public Procedure getProcedure(ObjectPath procedurePath)
            throws ProcedureNotExistException, CatalogException {
        return ProcedureUtil.getProcedure(catalog, procedurePath)
                .orElseThrow(() -> new ProcedureNotExistException(name, procedurePath));
    }

    private boolean isCalledFromFlinkRecomputeStatisticsProgram() {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        for (StackTraceElement stackTraceElement : stackTrace) {
            if (stackTraceElement.getClassName().contains("FlinkRecomputeStatisticsProgram")) {
                return true;
            }
        }
        return false;
    }

    private List<String> allMaterializedTableAttributes() {
        return Arrays.asList(
                MATERIALIZED_TABLE_DEFINITION_QUERY.key(),
                MATERIALIZED_TABLE_INTERVAL_FRESHNESS.key(),
                MATERIALIZED_TABLE_INTERVAL_FRESHNESS_TIME_UNIT.key(),
                MATERIALIZED_TABLE_REFRESH_MODE.key(),
                MATERIALIZED_TABLE_LOGICAL_REFRESH_MODE.key(),
                MATERIALIZED_TABLE_REFRESH_STATUS.key(),
                MATERIALIZED_TABLE_REFRESH_HANDLER_DESCRIPTION.key(),
                MATERIALIZED_TABLE_REFRESH_HANDLER_BYTES.key());
    }
}
