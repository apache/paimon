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
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionBase;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.flink.sink.cdc.FlinkCdcSyncDatabaseSinkBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static org.apache.paimon.flink.action.Action.getConfigMap;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** An {@link Action} which synchronize the whole PostgreSQL database into one Paimon database. */
public class PostgreSqlSyncDatabaseAction extends ActionBase {

    private static final Logger LOG = LoggerFactory.getLogger(PostgreSqlSyncDatabaseAction.class);

    private final Configuration postgresqlConfig;
    private final String database;
    private final boolean ignoreIncompatible;
    private final String tablePrefix;
    private final String tableSuffix;
    @Nullable private final Pattern includingPattern;
    @Nullable private final Pattern excludingPattern;
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
        this.database = database;
        this.ignoreIncompatible = ignoreIncompatible;
        this.tablePrefix = tablePrefix == null ? "" : tablePrefix;
        this.tableSuffix = tableSuffix == null ? "" : tableSuffix;
        this.includingPattern = includingTables == null ? null : Pattern.compile(includingTables);
        this.excludingPattern = excludingTables == null ? null : Pattern.compile(excludingTables);
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
                "No tables found in PostgreSQL schema "
                        + postgresqlConfig.get(PostgreSqlSourceOptions.SCHEMA_NAME)
                        + ", or PostgreSQL schema does not exist.");

        catalog.createDatabase(database, true);
        TableNameConverter tableNameConverter =
                new TableNameConverter(caseSensitive, tablePrefix, tableSuffix);

        List<FileStoreTable> fileStoreTables = new ArrayList<>();
        List<String> monitoredTables = new ArrayList<>();
        for (PostgreSqlSchema postgreSqlSchema : postgreSqlSchemas) {
            String paimonTableName = tableNameConverter.convert(postgreSqlSchema.tableName());
            Identifier identifier = new Identifier(database, paimonTableName);
            FileStoreTable table;
            Schema fromPostgreSql =
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
                if (shouldMonitorTable(table.schema(), fromPostgreSql, errMsg)) {
                    monitoredTables.add(postgreSqlSchema.tableName());
                }
            } catch (Catalog.TableNotExistException e) {
                catalog.createTable(identifier, fromPostgreSql, false);
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
                PostgreSqlSourceOptions.TABLE_NAME, "(" + String.join("|", monitoredTables) + ")");
        SourceFunction<String> source =
                PostgreSqlActionUtils.buildPostgreSqlSource(postgresqlConfig);

        String serverTimeZone = postgresqlConfig.get(PostgreSqlSourceOptions.SERVER_TIME_ZONE);
        ZoneId zoneId = serverTimeZone == null ? ZoneId.systemDefault() : ZoneId.of(serverTimeZone);
        EventParser.Factory<String> parserFactory =
                () ->
                        new PostgreSqlDebeziumJsonEventParser(
                                zoneId, caseSensitive, tableNameConverter);

        FlinkCdcSyncDatabaseSinkBuilder<String> sinkBuilder =
                new FlinkCdcSyncDatabaseSinkBuilder<String>()
                        .withInput(env.addSource(source, "PostgreSQL Source"))
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
                                + "PostgreSQL table is: %s.%s.%s, fields are: %s.\n",
                        identifier.getFullName(),
                        paimonSchema.fields(),
                        postgreSqlSchema.databaseName(),
                        postgreSqlSchema.schemaName(),
                        postgreSqlSchema.tableName(),
                        postgreSqlSchema.fields());
    }

    private List<PostgreSqlSchema> getPostgreSqlSchemaList() throws Exception {
        String databaseName = postgresqlConfig.get(PostgreSqlSourceOptions.DATABASE_NAME);
        String schemaName = postgresqlConfig.get(PostgreSqlSourceOptions.SCHEMA_NAME);
        List<PostgreSqlSchema> postgreSqlSchemaList = new ArrayList<>();
        try (Connection conn = PostgreSqlActionUtils.getConnection(postgresqlConfig)) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet tables =
                    metaData.getTables(null, schemaName, "%", new String[] {"TABLE"})) {
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    if (!shouldMonitorTable(tableName)) {
                        continue;
                    }
                    PostgreSqlSchema postgreSqlSchema =
                            new PostgreSqlSchema(metaData, databaseName, schemaName, tableName);
                    if (postgreSqlSchema.primaryKeys().size() > 0) {
                        // only tables with primary keys will be considered
                        postgreSqlSchemaList.add(postgreSqlSchema);
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
            shouldMonitor =
                    shouldMonitor && !excludingPattern.matcher(postgreSqlTableName).matches();
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

    // ------------------------------------------------------------------------
    //  Flink run methods
    // ------------------------------------------------------------------------

    public static Optional<Action> create(String[] args) {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        if (params.has("help")) {
            printHelp();
            return Optional.empty();
        }

        String warehouse = params.get("warehouse");
        String database = params.get("database");
        boolean ignoreIncompatible = Boolean.parseBoolean(params.get("ignore-incompatible"));
        String tablePrefix = params.get("table-prefix");
        String tableSuffix = params.get("table-suffix");
        String includingTables = params.get("including-tables");
        String excludingTables = params.get("excluding-tables");

        Optional<Map<String, String>> postgresqlConfigOption =
                getConfigMap(params, "postgresql-conf");
        Optional<Map<String, String>> catalogConfigOption = getConfigMap(params, "catalog-conf");
        Optional<Map<String, String>> tableConfigOption = getConfigMap(params, "table-conf");
        return postgresqlConfigOption.map(
                postgreSqlConfig ->
                        new PostgreSqlSyncDatabaseAction(
                                postgreSqlConfig,
                                warehouse,
                                database,
                                ignoreIncompatible,
                                tablePrefix,
                                tableSuffix,
                                includingTables,
                                excludingTables,
                                catalogConfigOption.orElse(Collections.emptyMap()),
                                tableConfigOption.orElse(Collections.emptyMap())));
    }

    private static void printHelp() {
        System.out.println(
                "Action \"postgresql-sync-database\" creates a streaming job "
                        + "with a Flink PostgreSQL CDC source and multiple Paimon table sinks "
                        + "to synchronize a whole PostgreSQL database into one Paimon database.\n"
                        + "Only PostgreSQL tables with primary keys will be considered. "
                        + "Newly created PostgreSQL tables after the job starts will not be included.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  postgresql-sync-database --warehouse <warehouse-path> --database <database-name> "
                        + "[--ignore-incompatible <true/false>] "
                        + "[--table-prefix <paimon-table-prefix>] "
                        + "[--table-suffix <paimon-table-suffix>] "
                        + "[--including-tables <postgresql-table-name|name-regular-expr>] "
                        + "[--excluding-tables <postgresql-table-name|name-regular-expr>] "
                        + "[--postgresql-conf <postgresql-cdc-source-conf> [--postgresql-conf <postgresql-cdc-source-conf> ...]] "
                        + "[--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]] "
                        + "[--table-conf <paimon-table-sink-conf> [--table-conf <paimon-table-sink-conf> ...]]");
        System.out.println();

        System.out.println(
                "--ignore-incompatible is default false, in this case, if PostgreSQL table name exists in Paimon "
                        + "and their schema is incompatible, an exception will be thrown. "
                        + "You can specify it to true explicitly to ignore the incompatible tables and exception.");
        System.out.println();

        System.out.println(
                "--table-prefix is the prefix of all Paimon tables to be synchronized. For example, if you want all "
                        + "synchronized tables to have \"ods_\" as prefix, you can specify `--table-prefix ods_`.");
        System.out.println("The usage of --table-suffix is same as `--table-prefix`");
        System.out.println();

        System.out.println(
                "--including-tables is used to specify which source tables are to be synchronized. "
                        + "You must use '|' to separate multiple tables. Regular expression is supported.");
        System.out.println(
                "--excluding-tables is used to specify which source tables are not to be synchronized. "
                        + "The usage is same as --including-tables.");
        System.out.println(
                "--excluding-tables has higher priority than --including-tables if you specified both.");
        System.out.println();

        System.out.println("PostgreSQL CDC source conf syntax:");
        System.out.println("  key=value");
        System.out.println(
                "'hostname', 'username', 'password' , 'database-name' and 'schema-name' "
                        + "are required configurations, others are optional. "
                        + "Note that 'database-name' should be the exact name "
                        + "of the PostgreSQL databse you want to synchronize. "
                        + "It can't be a regular expression.");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://ververica.github.io/flink-cdc-connectors/master/content/connectors/postgres-cdc.html#connector-options");
        System.out.println();

        System.out.println("Paimon catalog and table sink conf syntax:");
        System.out.println("  key=value");
        System.out.println("All Paimon sink table will be applied the same set of configurations.");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://paimon.apache.org/docs/master/maintenance/configurations/");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  postgresql-sync-database \\\n"
                        + "    --warehouse hdfs:///path/to/warehouse \\\n"
                        + "    --database test_db \\\n"
                        + "    --postgresql-conf hostname=127.0.0.1 \\\n"
                        + "    --postgresql-conf username=postgres \\\n"
                        + "    --postgresql-conf password=123456 \\\n"
                        + "    --postgresql-conf database-name=source_db \\\n"
                        + "    --postgresql-conf schema-name=schema \\\n"
                        + "    --postgresql-conf slot.name=my_replication_slot \\\n"
                        + "    --postgresql-conf decoding.plugin.name=pgoutput \\\n"
                        + "    --catalog-conf metastore=hive \\\n"
                        + "    --catalog-conf uri=thrift://hive-metastore:9083 \\\n"
                        + "    --table-conf bucket=4 \\\n"
                        + "    --table-conf changelog-producer=input \\\n"
                        + "    --table-conf sink.parallelism=4");
    }

    @Override
    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        build(env);
        execute(env, String.format("PostgreSQL-Paimon Database Sync: %s", database));
    }
}
