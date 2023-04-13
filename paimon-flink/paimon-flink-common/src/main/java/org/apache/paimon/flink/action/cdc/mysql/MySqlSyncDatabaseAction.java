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
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.flink.sink.cdc.FlinkCdcSyncDatabaseSinkBuilder;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

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
 * <p>This action supports a limited number of schema changes. Unsupported schema changes will be
 * ignored. Currently supported schema changes includes:
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
 *       are supported. Other type changes will cause exceptions.
 * </ul>
 *
 * <p>This action creates a Paimon table sink for each Paimon table to be written, so this action is
 * not very efficient in resource saving. We may optimize this action by merging all sinks into one
 * instance in the future.
 */
public class MySqlSyncDatabaseAction implements Action {

    private final Configuration mySqlConfig;
    private final String warehouse;
    private final String database;
    private final Map<String, String> paimonConfig;

    MySqlSyncDatabaseAction(
            Map<String, String> mySqlConfig,
            String warehouse,
            String database,
            Map<String, String> paimonConfig) {
        this.mySqlConfig = Configuration.fromMap(mySqlConfig);
        this.warehouse = warehouse;
        this.database = database;
        this.paimonConfig = paimonConfig;
    }

    public void build(StreamExecutionEnvironment env) throws Exception {
        Preconditions.checkArgument(
                !mySqlConfig.contains(MySqlSourceOptions.TABLE_NAME),
                MySqlSourceOptions.TABLE_NAME.key()
                        + " cannot be set for mysql-sync-database. "
                        + "If you want to sync several MySQL tables into one Paimon table, "
                        + "use mysql-sync-table instead.");
        List<MySqlSchema> mySqlSchemas = getMySqlSchemaList();
        Preconditions.checkArgument(
                mySqlSchemas.size() > 0,
                "No tables found in MySQL database "
                        + mySqlConfig.get(MySqlSourceOptions.DATABASE_NAME)
                        + ", or MySQL database does not exist.");

        mySqlConfig.set(
                MySqlSourceOptions.TABLE_NAME,
                "("
                        + mySqlSchemas.stream()
                                .map(MySqlSchema::tableName)
                                .collect(Collectors.joining("|"))
                        + ")");
        MySqlSource<String> source = MySqlActionUtils.buildMySqlSource(mySqlConfig);

        Catalog catalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(
                                new Options().set(CatalogOptions.WAREHOUSE, warehouse)));
        catalog.createDatabase(database, true);

        List<FileStoreTable> fileStoreTables = new ArrayList<>();
        for (MySqlSchema mySqlSchema : mySqlSchemas) {
            Identifier identifier = new Identifier(database, mySqlSchema.tableName());
            FileStoreTable table;
            try {
                table = (FileStoreTable) catalog.getTable(identifier);
                MySqlActionUtils.assertSchemaCompatible(table.schema(), mySqlSchema);
            } catch (Catalog.TableNotExistException e) {
                Schema schema =
                        MySqlActionUtils.buildPaimonSchema(
                                mySqlSchema,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                paimonConfig);
                catalog.createTable(identifier, schema, false);
                table = (FileStoreTable) catalog.getTable(identifier);
            }
            fileStoreTables.add(table);
        }

        EventParser.Factory<String> parserFactory;
        String serverTimeZone = mySqlConfig.get(MySqlSourceOptions.SERVER_TIME_ZONE);
        if (serverTimeZone != null) {
            parserFactory = () -> new MySqlDebeziumJsonEventParser(ZoneId.of(serverTimeZone));
        } else {
            parserFactory = MySqlDebeziumJsonEventParser::new;
        }

        FlinkCdcSyncDatabaseSinkBuilder<String> sinkBuilder =
                new FlinkCdcSyncDatabaseSinkBuilder<String>()
                        .withInput(
                                env.fromSource(
                                        source, WatermarkStrategy.noWatermarks(), "MySQL Source"))
                        .withParserFactory(parserFactory)
                        .withTables(fileStoreTables);
        String sinkParallelism = paimonConfig.get(FlinkConnectorOptions.SINK_PARALLELISM.key());
        if (sinkParallelism != null) {
            sinkBuilder.withParallelism(Integer.parseInt(sinkParallelism));
        }
        sinkBuilder.build();
    }

    private List<MySqlSchema> getMySqlSchemaList() throws Exception {
        String databaseName = mySqlConfig.get(MySqlSourceOptions.DATABASE_NAME);
        List<MySqlSchema> mySqlSchemaList = new ArrayList<>();
        try (Connection conn = MySqlActionUtils.getConnection(mySqlConfig)) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet tables =
                    metaData.getTables(databaseName, null, "%", new String[] {"TABLE"})) {
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    MySqlSchema mySqlSchema = new MySqlSchema(metaData, databaseName, tableName);
                    if (mySqlSchema.primaryKeys().size() > 0) {
                        // only tables with primary keys will be considered
                        mySqlSchemaList.add(mySqlSchema);
                    }
                }
            }
        }
        return mySqlSchemaList;
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

        Map<String, String> mySqlConfig = getConfigMap(params, "mysql-conf");
        Map<String, String> paimonConfig = getConfigMap(params, "paimon-conf");
        if (mySqlConfig == null || paimonConfig == null) {
            return Optional.empty();
        }

        return Optional.of(
                new MySqlSyncDatabaseAction(mySqlConfig, warehouse, database, paimonConfig));
    }

    private static Map<String, String> getConfigMap(MultipleParameterTool params, String key) {
        Map<String, String> map = new HashMap<>();

        for (String param : params.getMultiParameter(key)) {
            String[] kv = param.split("=");
            if (kv.length == 2) {
                map.put(kv[0], kv[1]);
                continue;
            }

            System.err.println(
                    "Invalid " + key + " " + param + ".\nRun mysql-sync-database --help for help.");
            return null;
        }
        return map;
    }

    private static void printHelp() {
        System.out.println(
                "Action \"mysql-sync-database\" creates a streaming job "
                        + "with a Flink MySQL CDC source and multiple Paimon table sinks "
                        + "to synchronize a whole MySQL database into one Paimon database.\n"
                        + "Only MySQL tables with primary keys will be considered. "
                        + "Newly created MySQL tables after the job starts will not be included.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  mysql-sync-database --warehouse <warehouse-path> --database <database-name> "
                        + "[--mysql-conf <mysql-cdc-source-conf> [--mysql-conf <mysql-cdc-source-conf> ...]] "
                        + "[--paimon-conf <paimon-table-sink-conf> [--paimon-conf <paimon-table-sink-conf> ...]]");
        System.out.println();

        System.out.println("MySQL CDC source conf syntax:");
        System.out.println("  key=value");
        System.out.println(
                "'hostname', 'username', 'password' and 'database-name' "
                        + "are required configurations, others are optional. "
                        + "Note that 'database-name' should be the exact name "
                        + "of the MySQL databse you want to synchronize. "
                        + "It can't be a regular expression.");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mysql-cdc.html#connector-options");
        System.out.println();

        System.out.println("Paimon table sink conf syntax:");
        System.out.println("  key=value");
        System.out.println("All Paimon sink table will be applied the same set of configurations.");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://paimon.apache.org/docs/master/maintenance/configurations/");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  mysql-sync-database \\\n"
                        + "    --warehouse hdfs:///path/to/warehouse \\\n"
                        + "    --database test_db \\\n"
                        + "    --mysql-conf hostname=127.0.0.1 \\\n"
                        + "    --mysql-conf username=root \\\n"
                        + "    --mysql-conf password=123456 \\\n"
                        + "    --mysql-conf database-name=source_db \\\n"
                        + "    --paimon-conf bucket=4 \\\n"
                        + "    --paimon-conf changelog-producer=input \\\n"
                        + "    --paimon-conf sink.parallelism=4");
    }

    @Override
    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        build(env);
        env.execute(String.format("MySQL-Paimon Database Sync: %s", database));
    }
}
