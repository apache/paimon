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

package org.apache.paimon.flink.action.cdc.postgresql;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionBase;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.action.cdc.mysql.MySqlActionUtils;
import org.apache.paimon.flink.action.cdc.mysql.MySqlDebeziumJsonEventParser;
import org.apache.paimon.flink.action.cdc.mysql.MySqlSchema;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.flink.sink.cdc.FlinkCdcSyncDatabaseSinkBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * An {@link Action} which synchronize the whole PostgreSQL database into one Paimon database.
 */
public class PostgreSqlSyncDatabaseAction extends ActionBase {

    private static final Logger LOG = LoggerFactory.getLogger(PostgreSqlSyncDatabaseAction.class);

    private final Configuration postgresqlConfig;
    private final String warehouse;
    private final String database;
    private final boolean ignoreIncompatible;
    private final String tablePrefix;
    private final String tableSuffix;
    @Nullable
    private final Pattern includingPattern;
    @Nullable private final Pattern excludingPattern;
    private final Map<String, String> catalogConfig;
    private final Map<String, String> tableConfig;

    PostgreSqlSyncDatabaseAction(
            Map<String, String> postgresqlConfig,
            String warehouse,
            String database,
            boolean ignoreIncompatible,
            Map<String, String> catalogConfig,
            Map<String, String> tableConfig) {
        this(
                postgresqlConfig,
                warehouse,
                database,
                ignoreIncompatible,
                null,
                null,
                null,
                null,
                catalogConfig,
                tableConfig);
    }

    public PostgreSqlSyncDatabaseAction(
            Map<String, String> postgresqlConfig,
            String warehouse,
            String database,
            boolean ignoreIncompatible,
            @Nullable String tablePrefix,
            @Nullable String tableSuffix,
            @Nullable String includingTables,
            @Nullable String excludingTables,
            Map<String, String> catalogConfig,
            Map<String, String> tableConfig) {
        super(warehouse, catalogConfig);
        this.postgresqlConfig = Configuration.fromMap(postgresqlConfig);
        this.warehouse = warehouse;
        this.database = database;
        this.ignoreIncompatible = ignoreIncompatible;
        this.tablePrefix = tablePrefix == null ? "" : tablePrefix;
        this.tableSuffix = tableSuffix == null ? "" : tableSuffix;
        this.includingPattern = includingTables == null ? null : Pattern.compile(includingTables);
        this.excludingPattern = excludingTables == null ? null : Pattern.compile(excludingTables);
        this.catalogConfig = catalogConfig;
        this.tableConfig = tableConfig;
    }

    public void build(StreamExecutionEnvironment env) throws Exception {
        checkArgument(
                !postgresqlConfig.contains(PostgreSqlSourceOptions.TABLE_NAME),
                PostgreSqlSourceOptions.TABLE_NAME.key()
                        + " cannot be set for postgresql-sync-database. "
                        + "If you want to sync several PostgreSQL tables into one Paimon table, "
                        + "use postgresql-sync-table instead.");

        boolean caseSensitive = catalog.caseSensitive();

        if (!caseSensitive) {
            validateCaseInsensitive();
        }

        List<PostgreSqlSchema> postgreSqlSchemas = getPostgreSqlSchemaList();
        checkArgument(
                postgreSqlSchemas.size() > 0,
                "No tables found in PostgreSQL database "
                        + postgresqlConfig.get(MySqlSourceOptions.DATABASE_NAME)
                        + ", or PostgreSQL database does not exist.");

        catalog.createDatabase(database, true);
        TableNameConverter tableNameConverter =
                new TableNameConverter(caseSensitive, tablePrefix, tableSuffix);

        List<FileStoreTable> fileStoreTables = new ArrayList<>();
        List<String> monitoredTables = new ArrayList<>();
        for (PostgreSqlSchema postgreSqlSchema : postgreSqlSchemas) {
            String paimonTableName = tableNameConverter.convert(postgreSqlSchema.tableName());
            Identifier identifier = new Identifier(database, paimonTableName);
            FileStoreTable table;
            Schema fromMySql =
                    PostgreSqlActionUtils.buildPaimonSchema(
                            postgreSqlSchema,
                            Collections.emptyList(),
                            Collections.emptyList(),
                            Collections.emptyList(),
                            tableConfig,
                            caseSensitive);
            try {
                table = (FileStoreTable) catalog.getTable(identifier);
                Supplier<String> errMsg =
                        incompatibleMessage(table.schema(), postgreSqlSchema, identifier);
                if (shouldMonitorTable(table.schema(), fromMySql, errMsg)) {
                    monitoredTables.add(postgreSqlSchema.tableName());
                }
            } catch (Catalog.TableNotExistException e) {
                catalog.createTable(identifier, fromMySql, false);
                table = (FileStoreTable) catalog.getTable(identifier);
                monitoredTables.add(postgreSqlSchema.tableName());
            }
            fileStoreTables.add(table);
        }

        Preconditions.checkState(
                !monitoredTables.isEmpty(),
                "No tables to be synchronized. Possible cause is the schemas of all tables in specified "
                        + "PostgreSQL database are not compatible with those of existed Paimon tables. Please check the log.");

        postgresqlConfig.set(
                MySqlSourceOptions.TABLE_NAME, "(" + String.join("|", monitoredTables) + ")");
        SourceFunction<String> source = PostgreSqlActionUtils.buildPostgreSqlSource(postgresqlConfig);

        String serverTimeZone = postgresqlConfig.get(PostgreSqlSourceOptions.SERVER_TIME_ZONE);
        ZoneId zoneId = serverTimeZone == null ? ZoneId.systemDefault() : ZoneId.of(serverTimeZone);
        EventParser.Factory<String> parserFactory =
                () -> new MySqlDebeziumJsonEventParser(zoneId, caseSensitive, tableNameConverter);

        FlinkCdcSyncDatabaseSinkBuilder<String> sinkBuilder =
                new FlinkCdcSyncDatabaseSinkBuilder<String>()
                        .withInput(
                                env.fromSource(
                                        source, WatermarkStrategy.noWatermarks(), "MySQL Source"))
                        .withParserFactory(parserFactory)
                        .withTables(fileStoreTables);
        String sinkParallelism = tableConfig.get(FlinkConnectorOptions.SINK_PARALLELISM.key());
        if (sinkParallelism != null) {
            sinkBuilder.withParallelism(Integer.parseInt(sinkParallelism));
        }
        sinkBuilder.build();
    }

    private Supplier<String> incompatibleMessage(
            TableSchema paimonSchema, PostgreSqlSchema postgreSqlSchema, Identifier identifier) {
        return () ->
                String.format(
                        "Incompatible schema found.\n"
                                + "Paimon table is: %s, fields are: %s.\n"
                                + "MySQL table is: %s.%s.%s, fields are: %s.\n",
                        identifier.getFullName(),
                        paimonSchema.fields(),
                        postgreSqlSchema.databaseName(),
                        postgreSqlSchema.schemaName(),
                        postgreSqlSchema.tableName(),
                        postgreSqlSchema.fields());
    }

    private List<PostgreSqlSchema> getPostgreSqlSchemaList() throws Exception {
        String databaseName = postgresqlConfig.get(PostgreSqlSourceOptions.DATABASE_NAME);
        String schemaListStr = postgresqlConfig.get(PostgreSqlSourceOptions.SCHEMA_NAME);
        String[] SchemeList = schemaListStr.split(",");
        List<PostgreSqlSchema> postgreSqlSchemaList = new ArrayList<>();
        try (Connection conn = PostgreSqlActionUtils.getConnection(postgresqlConfig)) {
            DatabaseMetaData metaData = conn.getMetaData();
            for (String schemaName : SchemeList) {
                try (ResultSet tables =
                             metaData.getTables(databaseName, schemaName, "%", new String[] {"TABLE"})) {
                    while (tables.next()) {
                        String tableName = tables.getString("TABLE_NAME");
                        if (!shouldMonitorTable(tableName)) {
                            continue;
                        }
                        PostgreSqlSchema postgreSqlSchema = new PostgreSqlSchema(metaData, databaseName, schemaName, tableName);
                        if (postgreSqlSchema.primaryKeys().size() > 0) {
                            // only tables with primary keys will be considered
                            postgreSqlSchemaList.add(postgreSqlSchema);
                        }
                    }
                }
            }
        }
        return postgreSqlSchemaList;
    }

    private boolean shouldMonitorTable(String postgreSqlTableName) {
        boolean shouldMonitor = true;
        if (includingPattern != null) {
            shouldMonitor = includingPattern.matcher(postgreSqlTableName).matches();
        }
        if (excludingPattern != null) {
            shouldMonitor = shouldMonitor && !excludingPattern.matcher(postgreSqlTableName).matches();
        }
        LOG.debug("Source table {} is monitored? {}", postgreSqlTableName, shouldMonitor);
        return shouldMonitor;
    }

    private boolean shouldMonitorTable(
            TableSchema tableSchema, Schema postgreSqlSchema, Supplier<String> errMsg) {
        if (PostgreSqlActionUtils.schemaCompatible(tableSchema, postgreSqlSchema)) {
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

    @Override
    public void run() throws Exception {

    }
}
