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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.SpecialCastRules;
import org.apache.paimon.flink.action.cdc.mysql.schema.MySqlSchema;
import org.apache.paimon.flink.action.cdc.mysql.schema.MySqlSchemasInfo;
import org.apache.paimon.flink.action.cdc.mysql.schema.MySqlTableInfo;
import org.apache.paimon.flink.sink.cdc.UpdatedDataFieldsProcessFunction;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.Pair;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffsetBuilder;
import com.ververica.cdc.connectors.mysql.table.JdbcUrlUtils;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.DebeziumOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Utils for MySQL Action. * */
public class MySqlActionUtils {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlActionUtils.class);
    public static final ConfigOption<Boolean> SCAN_NEWLY_ADDED_TABLE_ENABLED =
            ConfigOptions.key("scan.newly-added-table.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether capture the scan the newly added tables or not, by default is true.");

    public static final ConfigOption<Boolean> MYSQL_CONVERTER_TINYINT1_BOOL =
            ConfigOptions.key("mysql.converter.tinyint1-to-bool")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Mysql tinyint type will be converted to boolean type by default, if you want to convert to tinyint type, "
                                    + "you can set this option to false.");

    static Connection getConnection(Configuration mySqlConfig) throws Exception {
        String url =
                String.format(
                        "jdbc:mysql://%s:%d",
                        mySqlConfig.get(MySqlSourceOptions.HOSTNAME),
                        mySqlConfig.get(MySqlSourceOptions.PORT));

        // we need to add the `tinyInt1isBit` parameter to the connection url to make sure the
        // tinyint(1) in MySQL is converted to bits or not. Refer to
        // https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-result-sets.html#cj-conn-prop_tinyInt1isBit
        if (mySqlConfig.contains(MYSQL_CONVERTER_TINYINT1_BOOL)) {
            url =
                    String.format(
                            "%s?tinyInt1isBit=%s",
                            url, mySqlConfig.get(MYSQL_CONVERTER_TINYINT1_BOOL));
        }

        return DriverManager.getConnection(
                url,
                mySqlConfig.get(MySqlSourceOptions.USERNAME),
                mySqlConfig.get(MySqlSourceOptions.PASSWORD));
    }

    static MySqlSchemasInfo getMySqlTableInfos(
            Configuration mySqlConfig,
            Predicate<String> monitorTablePredication,
            List<Identifier> excludedTables,
            SpecialCastRules specialCastRules)
            throws Exception {
        Pattern databasePattern =
                Pattern.compile(mySqlConfig.get(MySqlSourceOptions.DATABASE_NAME));
        MySqlSchemasInfo mySqlSchemasInfo = new MySqlSchemasInfo();
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
                                MySqlSchema mySqlSchema =
                                        MySqlSchema.buildSchema(
                                                metaData,
                                                databaseName,
                                                tableName,
                                                specialCastRules);
                                Identifier identifier = Identifier.create(databaseName, tableName);
                                if (monitorTablePredication.test(tableName)) {
                                    mySqlSchemasInfo.addSchema(identifier, mySqlSchema);
                                } else {
                                    excludedTables.add(identifier);
                                }
                            }
                        }
                    }
                }
            }
        }
        return mySqlSchemasInfo;
    }

    static void assertSchemaCompatible(TableSchema paimonSchema, Schema mySqlSchema) {
        if (!schemaCompatible(paimonSchema, mySqlSchema)) {
            throw new IllegalArgumentException(
                    "Paimon schema and MySQL schema are not compatible.\n"
                            + "Paimon fields are: "
                            + paimonSchema.fields()
                            + ".\nMySQL fields are: "
                            + mySqlSchema.fields());
        }
    }

    static boolean schemaCompatible(TableSchema paimonSchema, Schema mySqlSchema) {
        for (DataField field : mySqlSchema.fields()) {
            int idx = paimonSchema.fieldNames().indexOf(field.name());
            if (idx < 0) {
                LOG.info("Cannot find field '{}' in Paimon table.", field.name());
                return false;
            }
            DataType type = paimonSchema.fields().get(idx).type();
            if (UpdatedDataFieldsProcessFunction.canConvert(field.type(), type)
                    != UpdatedDataFieldsProcessFunction.ConvertAction.CONVERT) {
                LOG.info(
                        "Cannot convert field '{}' from MySQL type '{}' to Paimon type '{}'.",
                        field.name(),
                        field.type(),
                        type);
                return false;
            }
        }
        return true;
    }

    static Schema buildPaimonSchema(
            MySqlTableInfo mySqlTableInfo,
            List<String> specifiedPartitionKeys,
            List<String> specifiedPrimaryKeys,
            List<ComputedColumn> computedColumns,
            Map<String, String> paimonConfig,
            boolean caseSensitive) {
        Schema.Builder builder = Schema.newBuilder();
        builder.options(paimonConfig);
        MySqlSchema mySqlSchema = mySqlTableInfo.schema();

        // build columns and primary keys from mySqlSchema
        LinkedHashMap<String, Pair<DataType, String>> mySqlFields;
        List<String> mySqlPrimaryKeys;
        if (caseSensitive) {
            mySqlFields = mySqlSchema.fields();
            mySqlPrimaryKeys = mySqlSchema.primaryKeys();
        } else {
            mySqlFields = new LinkedHashMap<>();
            for (Map.Entry<String, Pair<DataType, String>> entry :
                    mySqlSchema.fields().entrySet()) {
                String fieldName = entry.getKey();
                checkArgument(
                        !mySqlFields.containsKey(fieldName.toLowerCase()),
                        String.format(
                                "Duplicate key '%s' in table '%s' appears when converting fields map keys to case-insensitive form.",
                                fieldName, mySqlTableInfo.location()));
                mySqlFields.put(fieldName.toLowerCase(), entry.getValue());
            }
            mySqlPrimaryKeys =
                    mySqlSchema.primaryKeys().stream()
                            .map(String::toLowerCase)
                            .collect(Collectors.toList());
        }

        for (Map.Entry<String, Pair<DataType, String>> entry : mySqlFields.entrySet()) {
            builder.column(entry.getKey(), entry.getValue().getLeft(), entry.getValue().getRight());
        }

        for (ComputedColumn computedColumn : computedColumns) {
            builder.column(computedColumn.columnName(), computedColumn.columnType());
        }

        if (specifiedPrimaryKeys.size() > 0) {
            for (String key : specifiedPrimaryKeys) {
                if (!mySqlFields.containsKey(key)
                        && computedColumns.stream().noneMatch(c -> c.columnName().equals(key))) {
                    throw new IllegalArgumentException(
                            "Specified primary key "
                                    + key
                                    + " does not exist in MySQL tables or computed columns.");
                }
            }
            builder.primaryKey(specifiedPrimaryKeys);
        } else if (mySqlPrimaryKeys.size() > 0) {
            builder.primaryKey(mySqlPrimaryKeys);
        } else {
            throw new IllegalArgumentException(
                    "Primary keys are not specified. "
                            + "Also, can't infer primary keys from MySQL table schemas because "
                            + "MySQL tables have no primary keys or have different primary keys.");
        }

        if (specifiedPartitionKeys.size() > 0) {
            builder.partitionKeys(specifiedPartitionKeys);
        }

        return builder.build();
    }

    static MySqlSource<String> buildMySqlSource(Configuration mySqlConfig, String tableList) {
        validateMySqlConfig(mySqlConfig);
        MySqlSourceBuilder<String> sourceBuilder = MySqlSource.builder();

        sourceBuilder
                .hostname(mySqlConfig.get(MySqlSourceOptions.HOSTNAME))
                .port(mySqlConfig.get(MySqlSourceOptions.PORT))
                .username(mySqlConfig.get(MySqlSourceOptions.USERNAME))
                .password(mySqlConfig.get(MySqlSourceOptions.PASSWORD))
                .databaseList(mySqlConfig.get(MySqlSourceOptions.DATABASE_NAME))
                .tableList(tableList);

        mySqlConfig.getOptional(MySqlSourceOptions.SERVER_ID).ifPresent(sourceBuilder::serverId);
        mySqlConfig
                .getOptional(MySqlSourceOptions.SERVER_TIME_ZONE)
                .ifPresent(sourceBuilder::serverTimeZone);
        // MySQL CDC using increment snapshot, splitSize is used instead of fetchSize (as in JDBC
        // connector). splitSize is the number of records in each snapshot split.
        mySqlConfig
                .getOptional(MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE)
                .ifPresent(sourceBuilder::splitSize);
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

        boolean scanNewlyAddedTables = mySqlConfig.get(SCAN_NEWLY_ADDED_TABLE_ENABLED);

        return sourceBuilder
                .deserializer(schema)
                .includeSchemaChanges(true)
                .scanNewlyAddedTableEnabled(scanNewlyAddedTables)
                .build();
    }

    private static void validateMySqlConfig(Configuration mySqlConfig) {
        checkArgument(
                mySqlConfig.get(MySqlSourceOptions.HOSTNAME) != null,
                String.format(
                        "mysql-conf [%s] must be specified.", MySqlSourceOptions.HOSTNAME.key()));

        checkArgument(
                mySqlConfig.get(MySqlSourceOptions.USERNAME) != null,
                String.format(
                        "mysql-conf [%s] must be specified.", MySqlSourceOptions.USERNAME.key()));

        checkArgument(
                mySqlConfig.get(MySqlSourceOptions.PASSWORD) != null,
                String.format(
                        "mysql-conf [%s] must be specified.", MySqlSourceOptions.PASSWORD.key()));

        checkArgument(
                mySqlConfig.get(MySqlSourceOptions.DATABASE_NAME) != null,
                String.format(
                        "mysql-conf [%s] must be specified.",
                        MySqlSourceOptions.DATABASE_NAME.key()));
    }
}
