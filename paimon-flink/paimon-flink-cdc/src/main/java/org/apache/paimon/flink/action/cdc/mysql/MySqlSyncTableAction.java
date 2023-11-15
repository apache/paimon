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

package org.apache.paimon.flink.action.cdc.mysql;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionBase;
import org.apache.paimon.flink.action.cdc.CdcActionCommonUtils;
import org.apache.paimon.flink.action.cdc.CdcMetadataConverter;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.mysql.schema.MySqlSchemasInfo;
import org.apache.paimon.flink.action.cdc.mysql.schema.MySqlTableInfo;
import org.apache.paimon.flink.sink.cdc.CdcSinkBuilder;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecordEventParser;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.flink.action.cdc.ComputedColumnUtils.buildComputedColumns;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * An {@link Action} which synchronize one or multiple MySQL tables into one Paimon table.
 *
 * <p>You should specify MySQL source table in {@code mySqlConfig}. See <a
 * href="https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mysql-cdc.html#connector-options">document
 * of flink-cdc-connectors</a> for detailed keys and values.
 *
 * <p>If the specified Paimon table does not exist, this action will automatically create the table.
 * Its schema will be derived from all specified MySQL tables. If the Paimon table already exists,
 * its schema will be compared against the schema of all specified MySQL tables.
 *
 * <p>This action supports a limited number of schema changes. Currently, the framework can not drop
 * columns, so the behaviors of `DROP` will be ignored, `RENAME` will add a new column. Currently
 * supported schema changes includes:
 *
 * <ul>
 *   <li>Adding columns.
 *   <li>Altering column types. More specifically,
 *       <ul>
 *         <li>altering from a string type (char, varchar, text) to another string type with longer
 *             length,
 *         <li>altering from a binary type (binary, varbinary, blob) to another binary type with
 *             longer length,
 *         <li>altering from an integer type (tinyint, smallint, int, bigint) to another integer
 *             type with wider range,
 *         <li>altering from a floating-point type (float, double) to another floating-point type
 *             with wider range,
 *       </ul>
 *       are supported.
 * </ul>
 */
public class MySqlSyncTableAction extends ActionBase {

    private final String database;
    private final String table;
    private final Configuration mySqlConfig;
    private FileStoreTable fileStoreTable;

    private List<String> partitionKeys = new ArrayList<>();
    private List<String> primaryKeys = new ArrayList<>();

    private Map<String, String> tableConfig = new HashMap<>();
    private List<String> computedColumnArgs = new ArrayList<>();
    private List<String> metadataColumn = new ArrayList<>();
    private TypeMapping typeMapping = TypeMapping.defaultMapping();

    public MySqlSyncTableAction(
            String warehouse,
            String database,
            String table,
            Map<String, String> catalogConfig,
            Map<String, String> mySqlConfig) {
        super(warehouse, catalogConfig);
        this.database = database;
        this.table = table;
        this.mySqlConfig = Configuration.fromMap(mySqlConfig);

        MySqlActionUtils.registerJdbcDriver();
    }

    public MySqlSyncTableAction withPartitionKeys(String... partitionKeys) {
        return withPartitionKeys(Arrays.asList(partitionKeys));
    }

    public MySqlSyncTableAction withPartitionKeys(List<String> partitionKeys) {
        this.partitionKeys = partitionKeys;
        return this;
    }

    public MySqlSyncTableAction withPrimaryKeys(String... primaryKeys) {
        return withPrimaryKeys(Arrays.asList(primaryKeys));
    }

    public MySqlSyncTableAction withPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
        return this;
    }

    public MySqlSyncTableAction withTableConfig(Map<String, String> tableConfig) {
        this.tableConfig = tableConfig;
        return this;
    }

    public MySqlSyncTableAction withComputedColumnArgs(List<String> computedColumnArgs) {
        this.computedColumnArgs = computedColumnArgs;
        return this;
    }

    public MySqlSyncTableAction withTypeMapping(TypeMapping typeMapping) {
        this.typeMapping = typeMapping;
        return this;
    }

    public MySqlSyncTableAction withMetadataKeys(List<String> metadataKeys) {
        this.metadataColumn = metadataKeys;
        return this;
    }

    @Override
    public void build() throws Exception {
        checkArgument(
                mySqlConfig.contains(MySqlSourceOptions.TABLE_NAME),
                String.format(
                        "mysql-conf [%s] must be specified.", MySqlSourceOptions.TABLE_NAME.key()));

        boolean caseSensitive = catalog.caseSensitive();

        if (!caseSensitive) {
            validateCaseInsensitive();
        }

        MySqlSchemasInfo mySqlSchemasInfo =
                MySqlActionUtils.getMySqlTableInfos(
                        mySqlConfig,
                        monitorTablePredication(),
                        new ArrayList<>(),
                        typeMapping,
                        caseSensitive);
        validateMySqlTableInfos(mySqlSchemasInfo);

        catalog.createDatabase(database, true);

        MySqlTableInfo tableInfo = mySqlSchemasInfo.mergeAll();
        Identifier identifier = new Identifier(database, table);
        List<ComputedColumn> computedColumns =
                buildComputedColumns(computedColumnArgs, tableInfo.schema().fields());

        CdcMetadataConverter[] metadataConverters =
                metadataColumn.stream()
                        .map(
                                key ->
                                        Stream.of(MySqlMetadataProcessor.values())
                                                .filter(m -> m.getKey().equals(key))
                                                .findFirst()
                                                .orElseThrow(IllegalStateException::new))
                        .map(MySqlMetadataProcessor::getConverter)
                        .toArray(CdcMetadataConverter[]::new);

        Schema fromMySql =
                CdcActionCommonUtils.buildPaimonSchema(
                        partitionKeys,
                        primaryKeys,
                        computedColumns,
                        tableConfig,
                        tableInfo.schema(),
                        metadataConverters,
                        true);
        try {
            fileStoreTable = (FileStoreTable) catalog.getTable(identifier);
            fileStoreTable = fileStoreTable.copy(tableConfig);
            if (!computedColumns.isEmpty()) {
                List<String> computedFields =
                        computedColumns.stream()
                                .map(ComputedColumn::columnName)
                                .collect(Collectors.toList());
                List<String> fieldNames = fileStoreTable.schema().fieldNames();
                checkArgument(
                        new HashSet<>(fieldNames).containsAll(computedFields),
                        " Exists Table should contain all computed columns %s, but are %s.",
                        computedFields,
                        fieldNames);
            }
            CdcActionCommonUtils.assertSchemaCompatible(
                    fileStoreTable.schema(), fromMySql.fields());
        } catch (Catalog.TableNotExistException e) {
            catalog.createTable(identifier, fromMySql, false);
            fileStoreTable = (FileStoreTable) catalog.getTable(identifier);
        }

        String tableList =
                mySqlSchemasInfo.pkTables().stream()
                        .map(i -> i.getDatabaseName() + "\\." + i.getObjectName())
                        .collect(Collectors.joining("|"));
        MySqlSource<String> source = MySqlActionUtils.buildMySqlSource(mySqlConfig, tableList);

        TypeMapping typeMapping = this.typeMapping;
        MySqlRecordParser recordParser =
                new MySqlRecordParser(
                        mySqlConfig,
                        caseSensitive,
                        computedColumns,
                        typeMapping,
                        metadataConverters);

        EventParser.Factory<RichCdcMultiplexRecord> parserFactory =
                () -> new RichCdcMultiplexRecordEventParser(caseSensitive);

        CdcSinkBuilder<RichCdcMultiplexRecord> sinkBuilder =
                new CdcSinkBuilder<RichCdcMultiplexRecord>()
                        .withInput(
                                env.fromSource(
                                                source,
                                                WatermarkStrategy.noWatermarks(),
                                                "MySQL Source")
                                        .flatMap(recordParser)
                                        .name("Parse"))
                        .withParserFactory(parserFactory)
                        .withTable(fileStoreTable)
                        .withIdentifier(identifier)
                        .withCatalogLoader(catalogLoader());
        String sinkParallelism = tableConfig.get(FlinkConnectorOptions.SINK_PARALLELISM.key());
        if (sinkParallelism != null) {
            sinkBuilder.withParallelism(Integer.parseInt(sinkParallelism));
        }
        sinkBuilder.build();
    }

    private void validateCaseInsensitive() {
        checkArgument(
                database.equals(database.toLowerCase()),
                String.format(
                        "Database name [%s] cannot contain upper case in case-insensitive catalog.",
                        database));
        checkArgument(
                table.equals(table.toLowerCase()),
                String.format(
                        "Table name [%s] cannot contain upper case in case-insensitive catalog.",
                        table));
        for (String part : partitionKeys) {
            checkArgument(
                    part.equals(part.toLowerCase()),
                    String.format(
                            "Partition keys [%s] cannot contain upper case in case-insensitive catalog.",
                            partitionKeys));
        }
        for (String pk : primaryKeys) {
            checkArgument(
                    pk.equals(pk.toLowerCase()),
                    String.format(
                            "Primary keys [%s] cannot contain upper case in case-insensitive catalog.",
                            primaryKeys));
        }
    }

    private void validateMySqlTableInfos(MySqlSchemasInfo mySqlSchemasInfo) {
        List<Identifier> nonPkTables = mySqlSchemasInfo.nonPkTables();
        checkArgument(
                nonPkTables.isEmpty(),
                "Source tables of MySQL table synchronization job cannot contain table "
                        + "which doesn't have primary keys.\n"
                        + "They are: %s",
                nonPkTables.stream().map(Identifier::getFullName).collect(Collectors.joining(",")));

        checkArgument(
                !mySqlSchemasInfo.pkTables().isEmpty(),
                "No table satisfies the given database name and table name.");
    }

    private Predicate<String> monitorTablePredication() {
        return tableName -> {
            Pattern tableNamePattern =
                    Pattern.compile(mySqlConfig.get(MySqlSourceOptions.TABLE_NAME));
            return tableNamePattern.matcher(tableName).matches();
        };
    }

    @VisibleForTesting
    public Map<String, String> tableConfig() {
        return tableConfig;
    }

    @VisibleForTesting
    public FileStoreTable fileStoreTable() {
        return fileStoreTable;
    }

    // ------------------------------------------------------------------------
    //  Flink run methods
    // ------------------------------------------------------------------------

    @Override
    public void run() throws Exception {
        build();
        execute(String.format("MySQL-Paimon Table Sync: %s.%s", database, table));
    }
}
