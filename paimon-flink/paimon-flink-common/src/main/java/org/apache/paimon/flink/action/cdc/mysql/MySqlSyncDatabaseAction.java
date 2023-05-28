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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Matcher;
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
    private final boolean syncToMultipleDB;
    private final boolean ignoreIncompatible;
    private final String tablePrefix;
    private final String tableSuffix;
    private final Pattern includingPattern;
    @Nullable private final Pattern excludingPattern;
    private final Map<String, String> tableConfig;
    private final String includingTables;
    private final DatabaseSyncMode mode;

    public MySqlSyncDatabaseAction(
            Map<String, String> mySqlConfig,
            String warehouse,
            String database,
            boolean syncToMultipleDB,
            boolean ignoreIncompatible,
            Map<String, String> catalogConfig,
            Map<String, String> tableConfig) {
        this(
                mySqlConfig,
                warehouse,
                database,
                syncToMultipleDB,
                ignoreIncompatible,
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
            boolean syncToMultipleDB,
            boolean ignoreIncompatible,
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
        this.syncToMultipleDB = syncToMultipleDB;
        this.ignoreIncompatible = ignoreIncompatible;
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
        List<String> excludedTables = new LinkedList<>();
        List<MySqlSchema> mySqlSchemas = getMySqlSchemaList(excludedTables);
        Set<String> databases =
                mySqlSchemas.stream().map(MySqlSchema::databaseName).collect(Collectors.toSet());

        if (!caseSensitive) {
            validateCaseInsensitive(databases);
        }

        String mySqlDatabase = mySqlConfig.get(MySqlSourceOptions.DATABASE_NAME);
        checkArgument(
                mySqlSchemas.size() > 0,
                "No tables found in MySQL database "
                        + mySqlDatabase
                        + ", or MySQL database does not exist.");

        if (syncToMultipleDB) {
            for (String database : databases) {
                catalog.createDatabase(database, true);
            }
        } else {
            catalog.createDatabase(database, true);
        }

        TableNameConverter tableNameConverter =
                new TableNameConverter(caseSensitive, tablePrefix, tableSuffix);

        List<FileStoreTable> fileStoreTables = new ArrayList<>();
        List<String> monitoredTables = new ArrayList<>();
        for (MySqlSchema mySqlSchema : mySqlSchemas) {
            String paimonTableName = tableNameConverter.convert(mySqlSchema.tableName());
            Identifier identifier;
            if (syncToMultipleDB) {
                identifier = new Identifier(mySqlSchema.databaseName(), paimonTableName);
            } else {
                identifier = new Identifier(database, paimonTableName);
            }

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
                    monitoredTables.add(mySqlSchema.databaseName() + "." + mySqlSchema.tableName());
                    fileStoreTables.add(table);
                }
            } catch (Catalog.TableNotExistException e) {
                catalog.createTable(identifier, fromMySql, false);
                table = (FileStoreTable) catalog.getTable(identifier);
                monitoredTables.add(mySqlSchema.databaseName() + "." + mySqlSchema.tableName());
                fileStoreTables.add(table);
            }
        }

        Preconditions.checkState(
                !monitoredTables.isEmpty(),
                "No tables to be synchronized. Possible cause is the schemas of all tables in specified "
                        + "MySQL database are not compatible with those of existed Paimon tables. Please check the log.");
        String tableList;

        if (mode == COMBINED) {
            // First excluding all tables that failed the excludingPattern and don't have primary
            // keys. Then including other tables using regex so that newly added table DDLs and DMLs
            // during job runtime can be captured
            tableList =
                    excludedTables.stream()
                                    .map(t -> String.format("(?!(%s))", t))
                                    .collect(Collectors.joining(""))
                            + includingTables;
        } else {
            tableList = "(" + String.join("|", monitoredTables) + ")";
        }
        mySqlConfig.set(MySqlSourceOptions.TABLE_NAME, tableList);
        MySqlSource<String> source = MySqlActionUtils.buildMySqlSource(mySqlConfig);

        String serverTimeZone = mySqlConfig.get(MySqlSourceOptions.SERVER_TIME_ZONE);
        ZoneId zoneId = serverTimeZone == null ? ZoneId.systemDefault() : ZoneId.of(serverTimeZone);
        MySqlTableSchemaBuilder schemaBuilder =
                new MySqlTableSchemaBuilder(tableConfig, caseSensitive);
        Pattern includingPattern = this.includingPattern;
        Pattern excludingPattern = this.excludingPattern;
        Boolean convertTinyint1ToBool = mySqlConfig.get(MYSQL_CONVERTER_TINYINT1_BOOL);

        // if we use syncToMultipleDB variable directly, it will throw an NotSerializableException.
        boolean enableMultipleDB = syncToMultipleDB;
        EventParser.Factory<String> parserFactory =
                () ->
                        new MySqlDebeziumJsonEventParser(
                                zoneId,
                                caseSensitive,
                                tableNameConverter,
                                schemaBuilder,
                                includingPattern,
                                excludingPattern,
                                convertTinyint1ToBool,
                                enableMultipleDB);

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
                        .withMode(mode)
                        .withSyncMultipleDB(syncToMultipleDB)
                        .withTables(fileStoreTables);
        String sinkParallelism = tableConfig.get(FlinkConnectorOptions.SINK_PARALLELISM.key());
        if (sinkParallelism != null) {
            sinkBuilder.withParallelism(Integer.parseInt(sinkParallelism));
        }

        sinkBuilder.build();
    }

    private void validateCaseInsensitive(Set<String> databases) {
        if (syncToMultipleDB) {
            for (String database : databases) {
                checkArgument(
                        database.equals(database.toLowerCase()),
                        String.format(
                                "Database name [%s] cannot contain upper case in case-insensitive catalog.",
                                database));
            }
        } else {
            checkArgument(
                    database.equals(database.toLowerCase()),
                    String.format(
                            "Database name [%s] cannot contain upper case in case-insensitive catalog.",
                            database));
        }

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

    private List<MySqlSchema> getMySqlSchemaList(List<String> excludedTables) throws Exception {
        Pattern databasePattern =
                Pattern.compile(mySqlConfig.get(MySqlSourceOptions.DATABASE_NAME));
        List<MySqlSchema> mySqlSchemaList = new ArrayList<>();
        try (Connection conn = MySqlActionUtils.getConnection(mySqlConfig)) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet schemas = metaData.getCatalogs()) {
                while (schemas.next()) {
                    String databaseName = schemas.getString("TABLE_CAT");
                    Matcher databaseMatcher = databasePattern.matcher(databaseName);
                    if (databaseMatcher.matches()) {
                        try (ResultSet tables = metaData.getTables(databaseName, null, "%", null)) {
                            while (tables.next()) {
                                String tableName = tables.getString("TABLE_NAME");
                                String fullMysqlTableName = databaseName + "." + tableName;
                                if (!shouldMonitorTable(fullMysqlTableName)) {
                                    excludedTables.add(fullMysqlTableName);
                                    continue;
                                }

                                MySqlSchema mySqlSchema =
                                        new MySqlSchema(
                                                metaData,
                                                databaseName,
                                                tableName,
                                                mySqlConfig.get(MYSQL_CONVERTER_TINYINT1_BOOL));
                                if (mySqlSchema.primaryKeys().size() > 0) {
                                    // only tables with primary keys will be considered
                                    mySqlSchemaList.add(mySqlSchema);
                                } else {

                                    excludedTables.add(fullMysqlTableName);
                                }
                            }
                        }
                    }
                }
            }
        }
        return mySqlSchemaList;
    }

    private boolean shouldMonitorTable(String mySqlTableName) {
        boolean shouldMonitor = includingPattern.matcher(mySqlTableName).matches();
        if (excludingPattern != null) {
            shouldMonitor = shouldMonitor && !excludingPattern.matcher(mySqlTableName).matches();
        }
        LOG.debug("Source table {} is monitored? {}", mySqlTableName, shouldMonitor);
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
                                + "MySQL table is: %s.%s, fields are: %s.\n",
                        identifier.getFullName(),
                        paimonSchema.fields(),
                        mySqlSchema.databaseName(),
                        mySqlSchema.tableName(),
                        mySqlSchema.fields());
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
