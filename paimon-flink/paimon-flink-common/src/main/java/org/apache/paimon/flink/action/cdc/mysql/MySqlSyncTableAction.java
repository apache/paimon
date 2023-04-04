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
import org.apache.paimon.flink.sink.cdc.FlinkCdcSinkBuilder;
import org.apache.paimon.flink.sink.cdc.SchemaChangeProcessFunction;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffsetBuilder;
import com.ververica.cdc.connectors.mysql.table.JdbcUrlUtils;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.DebeziumOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
 */
public class MySqlSyncTableAction implements Action {

    private final Configuration mySqlConfig;
    private final String warehouse;
    private final String database;
    private final String table;
    private final List<String> partitionKeys;
    private final List<String> primaryKeys;
    private final Map<String, String> paimonConfig;

    MySqlSyncTableAction(
            Map<String, String> mySqlConfig,
            String warehouse,
            String database,
            String table,
            List<String> partitionKeys,
            List<String> primaryKeys,
            Map<String, String> paimonConfig) {
        this.mySqlConfig = Configuration.fromMap(mySqlConfig);
        this.warehouse = warehouse;
        this.database = database;
        this.table = table;
        this.partitionKeys = partitionKeys;
        this.primaryKeys = primaryKeys;
        this.paimonConfig = paimonConfig;
    }

    public void build(StreamExecutionEnvironment env) throws Exception {
        MySqlSource<String> source = buildSource();
        MySqlSchema mySqlSchema =
                getMySqlSchemaList().stream()
                        .reduce(MySqlSchema::merge)
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "No table satisfies the given database name and table name"));

        Catalog catalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(
                                new Options().set(CatalogOptions.WAREHOUSE, warehouse)));
        catalog.createDatabase(database, true);

        Identifier identifier = new Identifier(database, table);
        FileStoreTable table;
        try {
            table = (FileStoreTable) catalog.getTable(identifier);
            if (!schemaCompatible(table.schema(), mySqlSchema)) {
                throw new IllegalArgumentException(
                        "Paimon schema and MySQL schema are not compatible.\n"
                                + "Paimon fields are: "
                                + table.schema().fields()
                                + ".\nMySQL fields are: "
                                + mySqlSchema.fields);
            }
        } catch (Catalog.TableNotExistException e) {
            Schema schema = buildSchema(mySqlSchema);
            catalog.createTable(identifier, schema, false);
            table = (FileStoreTable) catalog.getTable(identifier);
        }

        EventParser.Factory<String> parserFactory;
        String serverTimeZone = mySqlConfig.get(MySqlSourceOptions.SERVER_TIME_ZONE);
        if (serverTimeZone != null) {
            parserFactory = () -> new MySqlDebeziumJsonEventParser(ZoneId.of(serverTimeZone));
        } else {
            parserFactory = MySqlDebeziumJsonEventParser::new;
        }

        FlinkCdcSinkBuilder<String> sinkBuilder =
                new FlinkCdcSinkBuilder<String>()
                        .withInput(
                                env.fromSource(
                                        source, WatermarkStrategy.noWatermarks(), "MySQL Source"))
                        .withParserFactory(parserFactory)
                        .withTable(table);
        String sinkParallelism = paimonConfig.get(FlinkConnectorOptions.SINK_PARALLELISM.key());
        if (sinkParallelism != null) {
            sinkBuilder.withParallelism(Integer.parseInt(sinkParallelism));
        }
        sinkBuilder.build();
    }

    private MySqlSource<String> buildSource() {
        MySqlSourceBuilder<String> sourceBuilder = MySqlSource.builder();

        String databaseName = mySqlConfig.get(MySqlSourceOptions.DATABASE_NAME);
        String tableName = mySqlConfig.get(MySqlSourceOptions.TABLE_NAME);
        sourceBuilder
                .hostname(mySqlConfig.get(MySqlSourceOptions.HOSTNAME))
                .port(mySqlConfig.get(MySqlSourceOptions.PORT))
                .username(mySqlConfig.get(MySqlSourceOptions.USERNAME))
                .password(mySqlConfig.get(MySqlSourceOptions.PASSWORD))
                .databaseList(databaseName)
                .tableList(databaseName + "." + tableName);

        mySqlConfig.getOptional(MySqlSourceOptions.SERVER_ID).ifPresent(sourceBuilder::serverId);
        mySqlConfig
                .getOptional(MySqlSourceOptions.SERVER_TIME_ZONE)
                .ifPresent(sourceBuilder::serverTimeZone);
        mySqlConfig
                .getOptional(MySqlSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE)
                .ifPresent(sourceBuilder::fetchSize);
        mySqlConfig
                .getOptional(MySqlSourceOptions.CONNECT_TIMEOUT)
                .ifPresent(sourceBuilder::connectTimeout);
        mySqlConfig
                .getOptional(MySqlSourceOptions.CONNECT_MAX_RETRIES)
                .ifPresent(sourceBuilder::connectMaxRetries);
        mySqlConfig
                .getOptional(MySqlSourceOptions.CONNECTION_POOL_SIZE)
                .ifPresent(sourceBuilder::connectionPoolSize);
        mySqlConfig
                .getOptional(MySqlSourceOptions.HEARTBEAT_INTERVAL)
                .ifPresent(sourceBuilder::heartbeatInterval);

        String startupMode = mySqlConfig.get(MySqlSourceOptions.SCAN_STARTUP_MODE);
        // see
        // https://github.com/ververica/flink-cdc-connectors/blob/master/flink-connector-mysql-cdc/src/main/java/com/ververica/cdc/connectors/mysql/table/MySqlTableSourceFactory.java#L196
        if ("initial".equalsIgnoreCase(startupMode)) {
            sourceBuilder.startupOptions(StartupOptions.initial());
        } else if ("earliest-offset".equalsIgnoreCase(startupMode)) {
            sourceBuilder.startupOptions(StartupOptions.earliest());
        } else if ("latest-offset".equalsIgnoreCase(startupMode)) {
            sourceBuilder.startupOptions(StartupOptions.latest());
        } else if ("specific-offset".equalsIgnoreCase(startupMode)) {
            BinlogOffsetBuilder offsetBuilder = BinlogOffset.builder();
            String file = mySqlConfig.get(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_FILE);
            Long pos = mySqlConfig.get(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_POS);
            if (file != null && pos != null) {
                offsetBuilder.setBinlogFilePosition(file, pos);
            }
            mySqlConfig
                    .getOptional(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_GTID_SET)
                    .ifPresent(offsetBuilder::setGtidSet);
            mySqlConfig
                    .getOptional(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS)
                    .ifPresent(offsetBuilder::setSkipEvents);
            mySqlConfig
                    .getOptional(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_ROWS)
                    .ifPresent(offsetBuilder::setSkipRows);
            sourceBuilder.startupOptions(StartupOptions.specificOffset(offsetBuilder.build()));
        } else if ("timestamp".equalsIgnoreCase(startupMode)) {
            sourceBuilder.startupOptions(
                    StartupOptions.timestamp(
                            mySqlConfig.get(MySqlSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS)));
        }

        Properties jdbcProperties = new Properties();
        Properties debeziumProperties = new Properties();
        for (Map.Entry<String, String> entry : mySqlConfig.toMap().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.startsWith(JdbcUrlUtils.PROPERTIES_PREFIX)) {
                jdbcProperties.put(key.substring(JdbcUrlUtils.PROPERTIES_PREFIX.length()), value);
            } else if (key.startsWith(DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX)) {
                debeziumProperties.put(
                        key.substring(DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX.length()), value);
            }
        }
        sourceBuilder.jdbcProperties(jdbcProperties);
        sourceBuilder.debeziumProperties(debeziumProperties);

        Map<String, Object> customConverterConfigs = new HashMap<>();
        customConverterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");
        JsonDebeziumDeserializationSchema schema =
                new JsonDebeziumDeserializationSchema(true, customConverterConfigs);
        return sourceBuilder.deserializer(schema).includeSchemaChanges(true).build();
    }

    private List<MySqlSchema> getMySqlSchemaList() throws Exception {
        Pattern databasePattern =
                Pattern.compile(mySqlConfig.get(MySqlSourceOptions.DATABASE_NAME));
        Pattern tablePattern = Pattern.compile(mySqlConfig.get(MySqlSourceOptions.TABLE_NAME));
        List<MySqlSchema> mySqlSchemaList = new ArrayList<>();
        try (Connection conn =
                DriverManager.getConnection(
                        String.format(
                                "jdbc:mysql://%s:%d/",
                                mySqlConfig.get(MySqlSourceOptions.HOSTNAME),
                                mySqlConfig.get(MySqlSourceOptions.PORT)),
                        mySqlConfig.get(MySqlSourceOptions.USERNAME),
                        mySqlConfig.get(MySqlSourceOptions.PASSWORD))) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet schemas = metaData.getCatalogs()) {
                while (schemas.next()) {
                    String databaseName = schemas.getString("TABLE_CAT");
                    Matcher databaseMatcher = databasePattern.matcher(databaseName);
                    if (databaseMatcher.matches()) {
                        try (ResultSet tables = metaData.getTables(databaseName, null, "%", null)) {
                            while (tables.next()) {
                                String tableName = tables.getString("TABLE_NAME");
                                Matcher tableMatcher = tablePattern.matcher(tableName);
                                if (tableMatcher.matches()) {
                                    mySqlSchemaList.add(
                                            new MySqlSchema(metaData, databaseName, tableName));
                                }
                            }
                        }
                    }
                }
            }
        }
        return mySqlSchemaList;
    }

    private boolean schemaCompatible(TableSchema tableSchema, MySqlSchema mySqlSchema) {
        for (Map.Entry<String, DataType> entry : mySqlSchema.fields.entrySet()) {
            int idx = tableSchema.fieldNames().indexOf(entry.getKey());
            if (idx < 0) {
                return false;
            }
            DataType type = tableSchema.fields().get(idx).type();
            if (!SchemaChangeProcessFunction.canConvert(entry.getValue(), type)) {
                return false;
            }
        }
        return true;
    }

    private Schema buildSchema(MySqlSchema mySqlSchema) {
        Schema.Builder builder = Schema.newBuilder();
        builder.options(paimonConfig);

        for (Map.Entry<String, DataType> entry : mySqlSchema.fields.entrySet()) {
            builder.column(entry.getKey(), entry.getValue());
        }

        if (primaryKeys.size() > 0) {
            for (String key : primaryKeys) {
                if (!mySqlSchema.fields.containsKey(key)) {
                    throw new IllegalArgumentException(
                            "Specified primary key " + key + " does not exist in MySQL tables");
                }
            }
            builder.primaryKey(primaryKeys);
        } else if (mySqlSchema.primaryKeys.size() > 0) {
            builder.primaryKey(mySqlSchema.primaryKeys);
        } else {
            throw new IllegalArgumentException(
                    "Primary keys are not specified. "
                            + "Also, can't infer primary keys from MySQL table schemas because "
                            + "MySQL tables have no primary keys or have different primary keys.");
        }

        if (partitionKeys.size() > 0) {
            builder.partitionKeys(partitionKeys);
        }

        return builder.build();
    }

    private static class MySqlSchema {

        private final String databaseName;
        private final String tableName;

        private final Map<String, DataType> fields;
        private final List<String> primaryKeys;

        private MySqlSchema(DatabaseMetaData metaData, String databaseName, String tableName)
                throws Exception {
            this.databaseName = databaseName;
            this.tableName = tableName;

            fields = new LinkedHashMap<>();
            try (ResultSet rs = metaData.getColumns(null, databaseName, tableName, null)) {
                while (rs.next()) {
                    String fieldName = rs.getString("COLUMN_NAME");
                    String fieldType = rs.getString("TYPE_NAME");
                    Integer precision = rs.getInt("COLUMN_SIZE");
                    if (rs.wasNull()) {
                        precision = null;
                    }
                    Integer scale = rs.getInt("DECIMAL_DIGITS");
                    if (rs.wasNull()) {
                        scale = null;
                    }
                    fields.put(fieldName, MySqlTypeUtils.toDataType(fieldType, precision, scale));
                }
            }

            primaryKeys = new ArrayList<>();
            try (ResultSet rs = metaData.getPrimaryKeys(null, databaseName, tableName)) {
                while (rs.next()) {
                    String fieldName = rs.getString("COLUMN_NAME");
                    primaryKeys.add(fieldName);
                }
            }
        }

        private MySqlSchema merge(MySqlSchema other) {
            for (Map.Entry<String, DataType> entry : other.fields.entrySet()) {
                String fieldName = entry.getKey();
                DataType newType = entry.getValue();
                if (fields.containsKey(fieldName)) {
                    DataType oldType = fields.get(fieldName);
                    if (SchemaChangeProcessFunction.canConvert(oldType, newType)) {
                        fields.put(fieldName, newType);
                    } else if (SchemaChangeProcessFunction.canConvert(newType, oldType)) {
                        // nothing to do
                    } else {
                        throw new IllegalArgumentException(
                                String.format(
                                        "Column %s have different types in table %s.%s and table %s.%s",
                                        fieldName,
                                        databaseName,
                                        tableName,
                                        other.databaseName,
                                        other.tableName));
                    }
                } else {
                    fields.put(fieldName, newType);
                }
            }
            if (!primaryKeys.equals(other.primaryKeys)) {
                primaryKeys.clear();
            }
            return this;
        }
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

        Tuple3<String, String, String> tablePath = Action.getTablePath(params);
        if (tablePath == null) {
            return Optional.empty();
        }

        List<String> partitionKeys = Collections.emptyList();
        if (params.has("partition-keys")) {
            partitionKeys =
                    Arrays.stream(params.get("partition-keys").split(","))
                            .collect(Collectors.toList());
        }

        List<String> primaryKeys = Collections.emptyList();
        if (params.has("primary-keys")) {
            primaryKeys =
                    Arrays.stream(params.get("primary-keys").split(","))
                            .collect(Collectors.toList());
        }

        Map<String, String> mySqlConfig = getConfigMap(params, "mysql-conf");
        Map<String, String> paimonConfig = getConfigMap(params, "paimon-conf");
        if (mySqlConfig == null || paimonConfig == null) {
            return Optional.empty();
        }

        return Optional.of(
                new MySqlSyncTableAction(
                        mySqlConfig,
                        tablePath.f0,
                        tablePath.f1,
                        tablePath.f2,
                        partitionKeys,
                        primaryKeys,
                        paimonConfig));
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
                    "Invalid " + key + " " + param + ".\nRun mysql-sync-table --help for help.");
            return null;
        }
        return map;
    }

    private static void printHelp() {
        System.out.println(
                "Action \"mysql-sync-table\" creates a streaming job "
                        + "with a Flink MySQL CDC source and a Paimon table sink to consume CDC events.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  mysql-sync-table --warehouse <warehouse-path> --database <database-name> "
                        + "--table <table-name> "
                        + "[--partition-keys <partition-keys>] "
                        + "[--primary-keys <primary-keys>] "
                        + "[--mysql-conf <mysql-cdc-source-conf> [--mysql-conf <mysql-cdc-source-conf> ...]] "
                        + "[--paimon-conf <paimon-table-sink-conf> [--paimon-conf <paimon-table-sink-conf> ...]]");
        System.out.println();

        System.out.println("Partition keys syntax:");
        System.out.println("  key1,key2,...");
        System.out.println(
                "If partition key is not defined and the specified Paimon table does not exist, "
                        + "this action will automatically create an unpartitioned Paimon table.");
        System.out.println();

        System.out.println("Primary keys syntax:");
        System.out.println("  key1,key2,...");
        System.out.println("Primary keys will be derived from MySQL tables if not specified.");
        System.out.println();

        System.out.println("MySQL CDC source conf syntax:");
        System.out.println("  key=value");
        System.out.println(
                "'hostname', 'username', 'password', 'database-name' and 'table-name' "
                        + "are required configurations, others are optional.");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mysql-cdc.html#connector-options");
        System.out.println();

        System.out.println("Paimon table sink conf syntax:");
        System.out.println("  key=value");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://paimon.apache.org/docs/master/maintenance/configurations/");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  mysql-sync-table \\"
                        + "    --warehouse hdfs:///path/to/warehouse \\"
                        + "    --database test_db \\"
                        + "    --table test_table \\"
                        + "    --partition-keys pt \\"
                        + "    --primary-keys pt,uid \\"
                        + "    --mysql-conf hostname=127.0.0.1 \\"
                        + "    --mysql-conf username=root \\"
                        + "    --mysql-conf password=123456 \\"
                        + "    --mysql-conf database-name=source_db \\"
                        + "    --mysql-conf table-name='source_table_.*' \\"
                        + "    --paimon-conf bucket=4 \\"
                        + "    --paimon-conf changelog-producer=input \\"
                        + "    --paimon-conf sink.parallelism=4 \\");
    }

    @Override
    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        build(env);
        env.execute(String.format("MySQL-Paimon Table Sync: %s.%s", database, table));
    }
}
