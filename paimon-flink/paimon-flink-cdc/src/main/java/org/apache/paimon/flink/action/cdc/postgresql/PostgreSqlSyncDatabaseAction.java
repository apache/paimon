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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionBase;
import org.apache.paimon.flink.action.cdc.CdcActionCommonUtils;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.flink.sink.cdc.FlinkCdcSyncDatabaseSinkBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StringUtils;

import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** An {@link Action} which synchronize the whole PostgreSQL database into one Paimon database. */
public class PostgreSqlSyncDatabaseAction extends ActionBase {

    private static final Logger LOG = LoggerFactory.getLogger(PostgreSqlSyncDatabaseAction.class);

    private final Configuration postgresqlConfig;
    private final String database;
    private boolean ignoreIncompatible = false;
    private String tablePrefix = "";
    private String tableSuffix = "";
    private String includingTables = ".*";
    @Nullable private String excludingTables;
    private Map<String, String> tableConfig;

    public PostgreSqlSyncDatabaseAction(
            String warehouse,
            String database,
            Map<String, String> catalogConfig,
            Map<String, String> postgresqlConfig) {
        super(warehouse, catalogConfig);
        this.postgresqlConfig = Configuration.fromMap(postgresqlConfig);
        this.database = database;
    }

    public PostgreSqlSyncDatabaseAction ignoreIncompatible(boolean ignoreIncompatible) {
        this.ignoreIncompatible = ignoreIncompatible;
        return this;
    }

    public PostgreSqlSyncDatabaseAction withTablePrefix(@Nullable String tablePrefix) {
        if (!StringUtils.isBlank(tablePrefix)) {
            this.tablePrefix = tablePrefix;
        }
        return this;
    }

    public PostgreSqlSyncDatabaseAction withTableSuffix(@Nullable String tableSuffix) {
        if (!StringUtils.isBlank(tableSuffix)) {
            this.tableSuffix = tableSuffix;
        }
        return this;
    }

    public PostgreSqlSyncDatabaseAction includingTables(@Nullable String includingTables) {
        if (!StringUtils.isBlank(includingTables)) {
            this.includingTables = includingTables;
        }
        return this;
    }

    public PostgreSqlSyncDatabaseAction excludingTables(@Nullable String excludingTables) {
        if (!StringUtils.isBlank(excludingTables)) {
            this.excludingTables = excludingTables;
        }
        return this;
    }

    public PostgreSqlSyncDatabaseAction withTableConfig(Map<String, String> tableConfig) {
        this.tableConfig = tableConfig;
        return this;
    }

    public void build() throws Exception {
        checkArgument(
                !postgresqlConfig.contains(PostgresSourceOptions.TABLE_NAME),
                PostgresSourceOptions.TABLE_NAME.key()
                        + " cannot be set for postgresql-sync-database. "
                        + "If you want to sync several PostgreSQL tables into one Paimon table, "
                        + "use postgresql-sync-table instead.");

        boolean caseSensitive = catalog.caseSensitive();

        if (!caseSensitive) {
            validateCaseInsensitive();
        }

        List<PostgreSqlSchema> postgreSqlSchemas = getPostgreSqlSchemaList();
        checkArgument(
                !postgreSqlSchemas.isEmpty(),
                "No tables found in PostgreSQL schema "
                        + postgresqlConfig.get(PostgresSourceOptions.SCHEMA_NAME)
                        + ", or PostgreSQL schema does not exist.");

        catalog.createDatabase(database, true);
        TableNameConverter tableNameConverter =
                new TableNameConverter(caseSensitive, true, tablePrefix, tableSuffix);

        List<FileStoreTable> fileStoreTables = new ArrayList<>();
        List<String> monitoredTables = new ArrayList<>();
        for (PostgreSqlSchema postgreSqlSchema : postgreSqlSchemas) {
            Identifier identifier =
                    new Identifier(
                            database, tableNameConverter.convert(postgreSqlSchema.tableName()));
            Schema fromPostgreSql =
                    CdcActionCommonUtils.buildPaimonSchema(
                            Collections.emptyList(),
                            Collections.emptyList(),
                            Collections.emptyList(),
                            tableConfig,
                            postgreSqlSchema.schema());
            try {
                FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
                Supplier<String> errMsg =
                        incompatibleMessage(table.schema(), postgreSqlSchema, identifier);
                if (shouldMonitorTable(table.schema(), fromPostgreSql, errMsg)) {
                    fileStoreTables.add(table);
                    monitoredTables.add(postgreSqlSchema.tableName());
                }
            } catch (Catalog.TableNotExistException e) {
                catalog.createTable(identifier, fromPostgreSql, false);
                FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
                fileStoreTables.add(table);
                monitoredTables.add(postgreSqlSchema.tableName());
            }
        }

        Preconditions.checkState(
                !monitoredTables.isEmpty(),
                "No tables to be synchronized. Possible cause is the schemas of all tables in specified "
                        + "PostgreSQL database are not compatible with those of existed Paimon tables. Please check the log.");

        postgresqlConfig.set(
                PostgresSourceOptions.TABLE_NAME, "(" + String.join("|", monitoredTables) + ")");
        SourceFunction<String> source =
                PostgreSqlActionUtils.buildPostgreSqlSource(postgresqlConfig);

        String serverTimeZone = postgresqlConfig.get(PostgresSourceOptions.SERVER_TIME_ZONE);
        ZoneId zoneId = serverTimeZone == null ? ZoneId.systemDefault() : ZoneId.of(serverTimeZone);
        EventParser.Factory<String> parserFactory =
                () ->
                        new PostgreSqlDebeziumJsonEventParser(
                                zoneId, caseSensitive, tableNameConverter);

        new FlinkCdcSyncDatabaseSinkBuilder<String>()
                .withInput(env.addSource(source, "PostgreSQL Source"))
                .withParserFactory(parserFactory)
                .withDatabase(database)
                .withCatalogLoader(catalogLoader())
                .withTables(fileStoreTables)
                .build();
    }

    private List<PostgreSqlSchema> getPostgreSqlSchemaList() throws Exception {
        String databaseName = postgresqlConfig.get(PostgresSourceOptions.DATABASE_NAME);
        String schemaName = postgresqlConfig.get(PostgresSourceOptions.SCHEMA_NAME);
        List<PostgreSqlSchema> postgreSqlSchemaList = new ArrayList<>();
        try (Connection conn = PostgreSqlActionUtils.getConnection(postgresqlConfig)) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet tables =
                    metaData.getTables(null, schemaName, "%", new String[] {"TABLE"})) {
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    String tableComment = tables.getString("REMARKS");
                    if (!shouldMonitorTable(tableName)) {
                        continue;
                    }
                    PostgreSqlSchema postgreSqlSchema =
                            new PostgreSqlSchema(
                                    metaData, databaseName, schemaName, tableName, tableComment);
                    if (!postgreSqlSchema.schema().primaryKeys().isEmpty()) {
                        // only tables with primary keys will be considered
                        postgreSqlSchemaList.add(postgreSqlSchema);
                    }
                }
            }
        }
        return postgreSqlSchemaList;
    }

    private boolean shouldMonitorTable(String postgreSqlTableName) {
        Matcher includingMatcher = Pattern.compile(includingTables).matcher(postgreSqlTableName);
        Matcher excludingMatcher =
                excludingTables == null
                        ? null
                        : Pattern.compile(excludingTables).matcher(postgreSqlTableName);

        boolean shouldMonitor =
                includingMatcher.matches()
                        && (excludingMatcher == null || !excludingMatcher.matches());
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

    private Supplier<String> incompatibleMessage(
            TableSchema paimonSchema, PostgreSqlSchema postgreSqlSchema, Identifier identifier) {
        return () ->
                String.format(
                        "Incompatible schema found.\n"
                                + "Paimon table is: %s, fields are: %s.\n"
                                + "PostgreSQL table is: %s.%s.%s, fields are: %s.\n",
                        identifier.getFullName(),
                        paimonSchema.fields(),
                        postgreSqlSchema.databaseName(),
                        postgreSqlSchema.schemaName(),
                        postgreSqlSchema.tableName(),
                        postgreSqlSchema.schema().fields());
    }

    // ------------------------------------------------------------------------
    //  Flink run methods
    // ------------------------------------------------------------------------
    @Override
    public void run() throws Exception {
        build();
        execute(String.format("PostgreSQL-Paimon Database Sync: %s", database));
    }
}
