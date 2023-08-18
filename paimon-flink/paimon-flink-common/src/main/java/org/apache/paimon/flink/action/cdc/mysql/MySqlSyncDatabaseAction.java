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
import org.apache.paimon.flink.action.cdc.DatabaseSyncMode;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.action.cdc.mysql.schema.MySqlSchemasInfo;
import org.apache.paimon.flink.action.cdc.mysql.schema.MySqlTableInfo;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.flink.sink.cdc.FlinkCdcSyncDatabaseSinkBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.cdc.DatabaseSyncMode.COMBINED;
import static org.apache.paimon.flink.action.cdc.DatabaseSyncMode.DIVIDED;
import static org.apache.paimon.flink.action.cdc.mysql.MySqlActionUtils.MYSQL_CONVERTER_TINYINT1_BOOL;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * An {@link Action} which synchronize the whole MySQL database into one Paimon database.
 *
 * <p>You should specify MySQL source database in {@code mySqlConfig}. See <a
 * href="https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mysql-cdc.html#connector-options">document
 * of flink-cdc-connectors</a> for detailed keys and values.
 *
 * <p>For each MySQL table to be synchronized, if the corresponding Paimon table does not exist,
 * this action will automatically create the table. Its schema will be derived from all specified
 * MySQL tables. If the Paimon table already exists, its schema will be compared against the schema
 * of all specified MySQL tables.
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
 *
 * <p>This action creates a Paimon table sink for each Paimon table to be written, so this action is
 * not very efficient in resource saving. We may optimize this action by merging all sinks into one
 * instance in the future.
 */
public class MySqlSyncDatabaseAction extends ActionBase {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlSyncDatabaseAction.class);

    private final String database;
    private final Configuration mySqlConfig;

    private Map<String, String> tableConfig = new HashMap<>();
    private boolean ignoreIncompatible = false;
    private boolean mergeShards = true;
    private String tablePrefix = "";
    private String tableSuffix = "";
    private String includingTables = ".*";
    @Nullable String excludingTables;
    private DatabaseSyncMode mode = DIVIDED;

    // for test purpose
    private final List<Identifier> monitoredTables = new ArrayList<>();
    private final List<Identifier> excludedTables = new ArrayList<>();

    public MySqlSyncDatabaseAction(
            String warehouse, String database, Map<String, String> mySqlConfig) {
        this(warehouse, database, Collections.emptyMap(), mySqlConfig);
    }

    public MySqlSyncDatabaseAction(
            String warehouse,
            String database,
            Map<String, String> catalogConfig,
            Map<String, String> mySqlConfig) {
        super(warehouse, catalogConfig);
        this.database = database;
        this.mySqlConfig = Configuration.fromMap(mySqlConfig);
    }

    public MySqlSyncDatabaseAction withTableConfig(Map<String, String> tableConfig) {
        this.tableConfig = tableConfig;
        return this;
    }

    public MySqlSyncDatabaseAction ignoreIncompatible(boolean ignoreIncompatible) {
        this.ignoreIncompatible = ignoreIncompatible;
        return this;
    }

    public MySqlSyncDatabaseAction mergeShards(boolean mergeShards) {
        this.mergeShards = mergeShards;
        return this;
    }

    public MySqlSyncDatabaseAction withTablePrefix(@Nullable String tablePrefix) {
        if (tablePrefix != null) {
            this.tablePrefix = tablePrefix;
        }
        return this;
    }

    public MySqlSyncDatabaseAction withTableSuffix(@Nullable String tableSuffix) {
        if (tableSuffix != null) {
            this.tableSuffix = tableSuffix;
        }
        return this;
    }

    public MySqlSyncDatabaseAction includingTables(@Nullable String includingTables) {
        if (includingTables != null) {
            this.includingTables = includingTables;
        }
        return this;
    }

    public MySqlSyncDatabaseAction excludingTables(@Nullable String excludingTables) {
        this.excludingTables = excludingTables;
        return this;
    }

    public MySqlSyncDatabaseAction withMode(DatabaseSyncMode mode) {
        this.mode = mode;
        return this;
    }

    public void build(StreamExecutionEnvironment env) throws Exception {
        checkArgument(
                !mySqlConfig.contains(MySqlSourceOptions.TABLE_NAME),
                MySqlSourceOptions.TABLE_NAME.key()
                        + " cannot be set for mysql-sync-database. "
                        + "If you want to sync several MySQL tables into one Paimon table, "
                        + "use mysql-sync-table instead.");
        boolean caseSensitive = catalog.caseSensitive();

        if (!caseSensitive) {
            validateCaseInsensitive();
        }

        Pattern includingPattern = Pattern.compile(includingTables);
        Pattern excludingPattern =
                excludingTables == null ? null : Pattern.compile(excludingTables);
        MySqlSchemasInfo mySqlSchemasInfo =
                MySqlActionUtils.getMySqlTableInfos(
                        mySqlConfig,
                        tableName ->
                                shouldMonitorTable(tableName, includingPattern, excludingPattern),
                        excludedTables);

        logNonPkTables(mySqlSchemasInfo.nonPkTables());
        List<MySqlTableInfo> mySqlTableInfos = mySqlSchemasInfo.toMySqlTableInfos(mergeShards);

        checkArgument(
                mySqlTableInfos.size() > 0,
                "No tables found in MySQL database "
                        + mySqlConfig.get(MySqlSourceOptions.DATABASE_NAME)
                        + ", or MySQL database does not exist.");

        catalog.createDatabase(database, true);
        TableNameConverter tableNameConverter =
                new TableNameConverter(caseSensitive, mergeShards, tablePrefix, tableSuffix);

        List<FileStoreTable> fileStoreTables = new ArrayList<>();
        for (MySqlTableInfo tableInfo : mySqlTableInfos) {
            Identifier identifier =
                    Identifier.create(
                            database, tableNameConverter.convert(tableInfo.toPaimonTableName()));
            FileStoreTable table;
            Schema fromMySql =
                    MySqlActionUtils.buildPaimonSchema(
                            tableInfo,
                            Collections.emptyList(),
                            Collections.emptyList(),
                            Collections.emptyList(),
                            tableConfig,
                            caseSensitive);
            try {
                table = (FileStoreTable) catalog.getTable(identifier);
                Supplier<String> errMsg =
                        incompatibleMessage(table.schema(), tableInfo, identifier);
                if (shouldMonitorTable(table.schema(), fromMySql, errMsg)) {
                    fileStoreTables.add(table);
                    monitoredTables.addAll(tableInfo.identifiers());
                } else {
                    excludedTables.addAll(tableInfo.identifiers());
                }
            } catch (Catalog.TableNotExistException e) {
                catalog.createTable(identifier, fromMySql, false);
                table = (FileStoreTable) catalog.getTable(identifier);
                fileStoreTables.add(table);
                monitoredTables.addAll(tableInfo.identifiers());
            }
        }

        Preconditions.checkState(
                !monitoredTables.isEmpty(),
                "No tables to be synchronized. Possible cause is the schemas of all tables in specified "
                        + "MySQL database are not compatible with those of existed Paimon tables. Please check the log.");

        MySqlSource<String> source =
                MySqlActionUtils.buildMySqlSource(mySqlConfig, buildTableList());

        String serverTimeZone = mySqlConfig.get(MySqlSourceOptions.SERVER_TIME_ZONE);
        ZoneId zoneId = serverTimeZone == null ? ZoneId.systemDefault() : ZoneId.of(serverTimeZone);
        Boolean convertTinyint1ToBool = mySqlConfig.get(MYSQL_CONVERTER_TINYINT1_BOOL);
        MySqlTableSchemaBuilder schemaBuilder =
                new MySqlTableSchemaBuilder(tableConfig, caseSensitive, convertTinyint1ToBool);

        EventParser.Factory<String> parserFactory =
                () ->
                        new MySqlDebeziumJsonEventParser(
                                zoneId,
                                caseSensitive,
                                tableNameConverter,
                                schemaBuilder,
                                includingPattern,
                                excludingPattern,
                                convertTinyint1ToBool);

        String database = this.database;
        DatabaseSyncMode mode = this.mode;
        FlinkCdcSyncDatabaseSinkBuilder<String> sinkBuilder =
                new FlinkCdcSyncDatabaseSinkBuilder<String>()
                        .withInput(
                                env.fromSource(
                                        source, WatermarkStrategy.noWatermarks(), "MySQL Source"))
                        .withParserFactory(parserFactory)
                        .withDatabase(database)
                        .withCatalogLoader(catalogLoader())
                        .withTables(fileStoreTables)
                        .withMode(mode);

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
                tablePrefix.equals(tablePrefix.toLowerCase()),
                String.format(
                        "Table prefix [%s] cannot contain upper case in case-insensitive catalog.",
                        tablePrefix));
        checkArgument(
                tableSuffix.equals(tableSuffix.toLowerCase()),
                String.format(
                        "Table suffix [%s] cannot contain upper case in case-insensitive catalog.",
                        tableSuffix));
    }

    private void logNonPkTables(List<Identifier> nonPkTables) {
        if (!nonPkTables.isEmpty()) {
            LOG.debug(
                    "Didn't find primary keys for tables '{}'. "
                            + "These tables won't be synchronized.",
                    nonPkTables.stream()
                            .map(Identifier::getFullName)
                            .collect(Collectors.joining(",")));
            excludedTables.addAll(nonPkTables);
        }
    }

    private boolean shouldMonitorTable(
            String mySqlTableName, Pattern includingPattern, @Nullable Pattern excludingPattern) {
        boolean shouldMonitor = includingPattern.matcher(mySqlTableName).matches();
        if (excludingPattern != null) {
            shouldMonitor = shouldMonitor && !excludingPattern.matcher(mySqlTableName).matches();
        }
        if (!shouldMonitor) {
            LOG.debug("Source table '{}' is excluded.", mySqlTableName);
        }
        return shouldMonitor;
    }

    private boolean shouldMonitorTable(
            TableSchema tableSchema, Schema mySqlSchema, Supplier<String> errMsg) {
        if (MySqlActionUtils.schemaCompatible(tableSchema, mySqlSchema)) {
            return true;
        } else if (ignoreIncompatible) {
            LOG.warn(errMsg.get() + "This table will be ignored.");
            return false;
        } else {
            throw new IllegalArgumentException(
                    errMsg.get()
                            + "If you want to ignore the incompatible tables, please specify --ignore-incompatible to true.");
        }
    }

    private Supplier<String> incompatibleMessage(
            TableSchema paimonSchema, MySqlTableInfo mySqlTableInfo, Identifier identifier) {
        return () ->
                String.format(
                        "Incompatible schema found.\n"
                                + "Paimon table is: %s, fields are: %s.\n"
                                + "MySQL table is: %s, fields are: %s.\n",
                        identifier.getFullName(),
                        paimonSchema.fields(),
                        mySqlTableInfo.location(),
                        mySqlTableInfo.schema().fields());
    }

    @VisibleForTesting
    public List<Identifier> monitoredTables() {
        return monitoredTables;
    }

    @VisibleForTesting
    public List<Identifier> excludedTables() {
        return excludedTables;
    }

    /**
     * See {@link com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils#discoverCapturedTables}
     * and {@code MySqlSyncDatabaseTableListITCase}.
     */
    private String buildTableList() {
        String separatorRex = "\\.";
        if (mode == DIVIDED) {
            // In DIVIDED mode, we only concern about existed tables
            return monitoredTables.stream()
                    .map(t -> t.getDatabaseName() + separatorRex + t.getObjectName())
                    .collect(Collectors.joining("|"));
        } else if (mode == COMBINED) {
            // In COMBINED mode, we should consider both existed tables and possible newly created
            // tables, so we should use regular expression to monitor all valid tables and exclude
            // certain invalid tables

            // The table list is built by template:
            // (?!(^db\\.tbl$)|(^...$))((databasePattern)\\.(including_pattern1|...))

            // The excluding pattern ?!(^db\\.tbl$)|(^...$) can exclude tables whose qualified name
            // is exactly equal to 'db.tbl'
            // The including pattern (databasePattern)\\.(including_pattern1|...) can include tables
            // whose qualified name matches one of the patterns

            // a table can be monitored only when its name meets the including pattern and doesn't
            // be excluded by excluding pattern at the same time
            String includingPattern =
                    String.format(
                            "(%s)%s(%s)",
                            mySqlConfig.get(MySqlSourceOptions.DATABASE_NAME),
                            separatorRex,
                            includingTables);
            if (excludedTables.isEmpty()) {
                return includingPattern;
            }

            String excludingPattern =
                    excludedTables.stream()
                            .map(
                                    t ->
                                            String.format(
                                                    "(^%s$)",
                                                    t.getDatabaseName()
                                                            + separatorRex
                                                            + t.getObjectName()))
                            .collect(Collectors.joining("|"));
            excludingPattern = "?!" + excludingPattern;
            return String.format("(%s)(%s)", excludingPattern, includingPattern);
        }

        throw new UnsupportedOperationException("Unknown DatabaseSyncMode: " + mode);
    }

    // ------------------------------------------------------------------------
    //  Flink run methods
    // ------------------------------------------------------------------------

    @Override
    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        build(env);
        execute(env, String.format("MySQL-Paimon Database Sync: %s", database));
    }
}
