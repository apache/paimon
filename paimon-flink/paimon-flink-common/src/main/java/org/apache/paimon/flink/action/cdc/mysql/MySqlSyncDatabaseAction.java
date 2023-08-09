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
import java.util.function.Predicate;
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

    private final Configuration mySqlConfig;
    private final String database;
    private final boolean ignoreIncompatible;
    private final boolean mergeShards;
    private final String tablePrefix;
    private final String tableSuffix;
    private final Pattern includingPattern;
    @Nullable private final Pattern excludingPattern;
    private final Map<String, String> tableConfig;
    private final String includingTables;
    private final DatabaseSyncMode mode;

    private List<Identifier> monitoredTables;

    public MySqlSyncDatabaseAction(
            Map<String, String> mySqlConfig,
            String warehouse,
            String database,
            boolean ignoreIncompatible,
            Map<String, String> catalogConfig,
            Map<String, String> tableConfig) {
        this(
                mySqlConfig,
                warehouse,
                database,
                ignoreIncompatible,
                true,
                null,
                null,
                null,
                null,
                catalogConfig,
                tableConfig,
                DIVIDED);
    }

    public MySqlSyncDatabaseAction(
            Map<String, String> mySqlConfig,
            String warehouse,
            String database,
            boolean ignoreIncompatible,
            boolean mergeShards,
            @Nullable String tablePrefix,
            @Nullable String tableSuffix,
            @Nullable String includingTables,
            @Nullable String excludingTables,
            Map<String, String> catalogConfig,
            Map<String, String> tableConfig,
            DatabaseSyncMode mode) {
        super(warehouse, catalogConfig);
        this.mySqlConfig = Configuration.fromMap(mySqlConfig);
        this.database = database;
        this.ignoreIncompatible = ignoreIncompatible;
        this.mergeShards = mergeShards;
        this.tablePrefix = tablePrefix == null ? "" : tablePrefix;
        this.tableSuffix = tableSuffix == null ? "" : tableSuffix;
        this.includingTables = includingTables == null ? ".*" : includingTables;
        this.includingPattern = Pattern.compile(this.includingTables);
        this.excludingPattern = excludingTables == null ? null : Pattern.compile(excludingTables);
        this.tableConfig = tableConfig;
        this.mode = mode;
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

        List<Identifier> excludedTables = new ArrayList<>();
        List<MySqlSchema> beforeMerging =
                MySqlActionUtils.getMySqlSchemaList(
                        mySqlConfig, monitorTablePredication(), excludedTables);
        monitoredTables =
                beforeMerging.stream().map(MySqlSchema::identifier).collect(Collectors.toList());
        List<MySqlSchema> mySqlSchemas = mergeShards ? mergeShards(beforeMerging) : beforeMerging;

        checkArgument(
                mySqlSchemas.size() > 0,
                "No tables found in MySQL database "
                        + mySqlConfig.get(MySqlSourceOptions.DATABASE_NAME)
                        + ", or MySQL database does not exist.");

        catalog.createDatabase(database, true);
        TableNameConverter tableNameConverter =
                new TableNameConverter(caseSensitive, mergeShards, tablePrefix, tableSuffix);

        List<FileStoreTable> fileStoreTables = new ArrayList<>();
        for (MySqlSchema mySqlSchema : mySqlSchemas) {
            Identifier identifier = buildPaimonIdentifier(tableNameConverter, mySqlSchema);
            FileStoreTable table;
            Schema fromMySql =
                    MySqlActionUtils.buildPaimonSchema(
                            mySqlSchema,
                            Collections.emptyList(),
                            Collections.emptyList(),
                            Collections.emptyList(),
                            tableConfig,
                            caseSensitive);
            try {
                table = (FileStoreTable) catalog.getTable(identifier);
                Supplier<String> errMsg =
                        incompatibleMessage(table.schema(), mySqlSchema, identifier);
                if (shouldMonitorTable(table.schema(), fromMySql, errMsg)) {
                    fileStoreTables.add(table);
                } else {
                    unmonitor(mySqlSchema);
                }
            } catch (Catalog.TableNotExistException e) {
                catalog.createTable(identifier, fromMySql, false);
                table = (FileStoreTable) catalog.getTable(identifier);
                fileStoreTables.add(table);
            }
        }

        Preconditions.checkState(
                !monitoredTables.isEmpty(),
                "No tables to be synchronized. Possible cause is the schemas of all tables in specified "
                        + "MySQL database are not compatible with those of existed Paimon tables. Please check the log.");

        MySqlSource<String> source =
                MySqlActionUtils.buildMySqlSource(mySqlConfig, buildTableList(excludedTables));

        String serverTimeZone = mySqlConfig.get(MySqlSourceOptions.SERVER_TIME_ZONE);
        ZoneId zoneId = serverTimeZone == null ? ZoneId.systemDefault() : ZoneId.of(serverTimeZone);
        MySqlTableSchemaBuilder schemaBuilder =
                new MySqlTableSchemaBuilder(tableConfig, caseSensitive);
        Pattern includingPattern = this.includingPattern;
        Pattern excludingPattern = this.excludingPattern;
        Boolean convertTinyint1ToBool = mySqlConfig.get(MYSQL_CONVERTER_TINYINT1_BOOL);
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

    private Predicate<MySqlSchema> monitorTablePredication() {
        return schema -> {
            if (schema.primaryKeys().isEmpty()) {
                LOG.debug(
                        "Didn't find primary keys from table '{}'. "
                                + "This table won't be synchronized.",
                        schema.identifier());
                return false;
            }
            return shouldMonitorTable(schema.tableName());
        };
    }

    private boolean shouldMonitorTable(String mySqlTableName) {
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
            TableSchema paimonSchema, MySqlSchema mySqlSchema, Identifier identifier) {
        return () ->
                String.format(
                        "Incompatible schema found.\n"
                                + "Paimon table is: %s, fields are: %s.\n"
                                + "MySQL table is: %s, fields are: %s.\n",
                        identifier.getFullName(),
                        paimonSchema.fields(),
                        mySqlSchema.tableName(),
                        mySqlSchema.fields());
    }

    /** Merge schemas for tables that have the same table name. */
    private List<MySqlSchema> mergeShards(List<MySqlSchema> rawMySqlSchemas) {
        Map<String, MySqlSchema> schemaMap = new HashMap<>();
        for (MySqlSchema rawSchema : rawMySqlSchemas) {
            String tableName = rawSchema.tableName();
            MySqlSchema schema = schemaMap.get(tableName);
            if (schema == null) {
                schemaMap.put(tableName, rawSchema);
            } else {
                schemaMap.put(tableName, schema.merge(rawSchema));
            }
        }
        return new ArrayList<>(schemaMap.values());
    }

    private Identifier buildPaimonIdentifier(
            TableNameConverter tableNameConverter, MySqlSchema mySqlSchema) {
        String tableName;
        if (mergeShards) {
            tableName = tableNameConverter.convert(mySqlSchema.tableName());
        } else {
            // the Paimon table name should be compound of origin database name and table name
            // together to avoid name conflict
            tableName = tableNameConverter.convert(mySqlSchema.identifier());
        }

        return Identifier.create(database, tableName);
    }

    private void unmonitor(MySqlSchema mySqlSchema) {
        if (mergeShards) {
            // if schema has been merged, all shards with the same table name should be removed
            monitoredTables =
                    monitoredTables.stream()
                            .filter(id -> !id.getObjectName().equals(mySqlSchema.tableName()))
                            .collect(Collectors.toList());
        } else {
            monitoredTables.remove(mySqlSchema.identifier());
        }
    }

    @VisibleForTesting
    public List<Identifier> monitoredTables() {
        return monitoredTables;
    }

    /**
     * See {@link com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils#discoverCapturedTables}
     * and {@code MySqlSyncDatabaseTableListITCase}.
     */
    private String buildTableList(List<Identifier> excludedTables) {
        String separatorRex = "\\.";
        if (mode == DIVIDED) {
            // In DIVIDED mode, we only concern about existed tables
            return monitoredTables.stream()
                    .map(t -> t.getDatabaseName() + separatorRex + t.getObjectName())
                    .collect(Collectors.joining("|"));
        } else if (mode == COMBINED) {
            // In COMBINED mode, we should consider both existed tables and possible newly added
            // tables, so we should use regular expression to monitor all valid tables and exclude
            // certain invalid tables

            // The table list is built by template:
            // (?!(^db\\.tbl$)|(^...$))(databasePattern\\.(including_pattern1|...))

            // The excluding pattern ?!(^db\\.tbl$)|(^...$) can exclude tables whose qualified name
            // is exactly equal to 'db.tbl'
            // The including pattern databasePattern\\.(including_pattern1|...) can include tables
            // whose qualified name matches one of the patterns

            // a table can be monitored only when its name meets the including pattern and doesn't
            // be excluded by excluding pattern at the same time
            String includingPattern =
                    String.format(
                            "%s%s(%s)",
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
