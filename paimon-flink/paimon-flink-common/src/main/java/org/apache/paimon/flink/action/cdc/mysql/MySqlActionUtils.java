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
import org.apache.paimon.flink.action.cdc.CdcActionCommonUtils;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.mysql.schema.MySqlSchema;
import org.apache.paimon.flink.action.cdc.mysql.schema.MySqlSchemasInfo;
import org.apache.paimon.flink.action.cdc.mysql.schema.MySqlTableInfo;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.StringUtils;

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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.columnDuplicateErrMsg;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.listCaseConvert;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.mapKeyCaseConvert;
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TINYINT1_NOT_BOOL;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Utils for MySQL Action. */
public class MySqlActionUtils {

    public static final ConfigOption<Boolean> SCAN_NEWLY_ADDED_TABLE_ENABLED =
            ConfigOptions.key("scan.newly-added-table.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether capture the scan the newly added tables or not, by default is true.");

    static Connection getConnection(
            Configuration mySqlConfig, boolean tinyint1NotBool, boolean syncTableComment)
            throws Exception {
        String url =
                String.format(
                        "jdbc:mysql://%s:%d",
                        mySqlConfig.get(MySqlSourceOptions.HOSTNAME),
                        mySqlConfig.get(MySqlSourceOptions.PORT));

        List<String> params = new ArrayList<>();
        // we need to add the `tinyInt1isBit` parameter to the connection url to
        // make sure the tinyint(1) in MySQL is converted to bits or not. Refer to
        // https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-result-sets.html#cj-conn-prop_tinyInt1isBit
        if (tinyint1NotBool) {
            params.add("tinyInt1isBit=false");
        }
        if (syncTableComment) {
            params.add("useInformationSchema=true");
        }

        String param = StringUtils.join(params, "&");
        if (!StringUtils.isBlank(param)) {
            url = url + "?" + param;
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
            TypeMapping typeMapping,
            boolean syncTableComment)
            throws Exception {
        Pattern databasePattern =
                Pattern.compile(mySqlConfig.get(MySqlSourceOptions.DATABASE_NAME));
        MySqlSchemasInfo mySqlSchemasInfo = new MySqlSchemasInfo();
        try (Connection conn =
                MySqlActionUtils.getConnection(
                        mySqlConfig,
                        typeMapping.containsMode(TINYINT1_NOT_BOOL),
                        syncTableComment)) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet schemas = metaData.getCatalogs()) {
                while (schemas.next()) {
                    String databaseName = schemas.getString("TABLE_CAT");
                    Matcher databaseMatcher = databasePattern.matcher(databaseName);
                    if (databaseMatcher.matches()) {
                        try (ResultSet tables = metaData.getTables(databaseName, null, "%", null)) {
                            while (tables.next()) {
                                String tableName = tables.getString("TABLE_NAME");
                                String tableComment = null;
                                if (syncTableComment) {
                                    tableComment = tables.getString("REMARKS");
                                }
                                MySqlSchema mySqlSchema =
                                        MySqlSchema.buildSchema(
                                                metaData,
                                                databaseName,
                                                tableName,
                                                typeMapping,
                                                tableComment);
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

    static Schema buildPaimonSchema(
            MySqlTableInfo mySqlTableInfo,
            List<String> specifiedPartitionKeys,
            List<String> specifiedPrimaryKeys,
            List<ComputedColumn> computedColumns,
            Map<String, String> tableConfig,
            boolean caseSensitive) {
        MySqlSchema mySqlSchema = mySqlTableInfo.schema();
        LinkedHashMap<String, DataType> sourceColumns =
                mapKeyCaseConvert(
                        mySqlSchema.typeMapping(),
                        caseSensitive,
                        columnDuplicateErrMsg(mySqlTableInfo.location()));
        List<String> primaryKeys = listCaseConvert(mySqlSchema.primaryKeys(), caseSensitive);

        return CdcActionCommonUtils.buildPaimonSchema(
                specifiedPartitionKeys,
                specifiedPrimaryKeys,
                computedColumns,
                tableConfig,
                sourceColumns,
                mySqlSchema.comments(),
                primaryKeys,
                mySqlSchema.tableComment());
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
