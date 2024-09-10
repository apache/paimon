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
import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.schema.JdbcSchemaUtils;
import org.apache.paimon.flink.action.cdc.schema.JdbcSchemasInfo;
import org.apache.paimon.flink.action.cdc.serialization.CdcDebeziumDeserializationSchema;
import org.apache.paimon.schema.Schema;

import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSourceBuilder;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffsetBuilder;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.table.DebeziumOptions;
import org.apache.flink.cdc.debezium.utils.JdbcUrlUtils;
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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TINYINT1_NOT_BOOL;
import static org.apache.paimon.flink.action.cdc.mysql.MySqlTypeUtils.toPaimonTypeVisitor;
import static org.apache.paimon.options.OptionsUtils.convertToPropertiesPrefixKey;

/** Utils for MySQL Action. */
public class MySqlActionUtils {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlActionUtils.class);

    public static final ConfigOption<Boolean> SCAN_NEWLY_ADDED_TABLE_ENABLED =
            ConfigOptions.key("scan.newly-added-table.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether capture the scan the newly added tables or not, by default is true.");

    static Connection getConnection(Configuration mySqlConfig, Map<String, String> jdbcProperties)
            throws Exception {
        String paramString = "";
        if (!jdbcProperties.isEmpty()) {
            paramString =
                    "?"
                            + jdbcProperties.entrySet().stream()
                                    .map(e -> e.getKey() + "=" + e.getValue())
                                    .collect(Collectors.joining("&"));
        }

        String url =
                String.format(
                        "jdbc:mysql://%s:%d%s",
                        mySqlConfig.get(MySqlSourceOptions.HOSTNAME),
                        mySqlConfig.get(MySqlSourceOptions.PORT),
                        paramString);

        LOG.info("Connect to MySQL server using url: {}", url);

        return DriverManager.getConnection(
                url,
                mySqlConfig.get(MySqlSourceOptions.USERNAME),
                mySqlConfig.get(MySqlSourceOptions.PASSWORD));
    }

    public static JdbcSchemasInfo getMySqlTableInfos(
            Configuration mySqlConfig,
            Predicate<String> monitorTablePredication,
            List<Identifier> excludedTables,
            TypeMapping typeMapping)
            throws Exception {
        Pattern databasePattern =
                Pattern.compile(mySqlConfig.get(MySqlSourceOptions.DATABASE_NAME));
        JdbcSchemasInfo mySqlSchemasInfo = new JdbcSchemasInfo();
        Map<String, String> jdbcProperties = getJdbcProperties(typeMapping, mySqlConfig);

        try (Connection conn = MySqlActionUtils.getConnection(mySqlConfig, jdbcProperties)) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet schemas = metaData.getCatalogs()) {
                while (schemas.next()) {
                    String databaseName = schemas.getString("TABLE_CAT");
                    Matcher databaseMatcher = databasePattern.matcher(databaseName);
                    if (!databaseMatcher.matches()) {
                        continue;
                    }
                    try (ResultSet tables =
                            metaData.getTables(databaseName, null, "%", new String[] {"TABLE"})) {
                        while (tables.next()) {
                            String tableName = tables.getString("TABLE_NAME");
                            String tableComment = tables.getString("REMARKS");
                            Identifier identifier = Identifier.create(databaseName, tableName);
                            if (monitorTablePredication.test(tableName)) {
                                Schema schema =
                                        JdbcSchemaUtils.buildSchema(
                                                metaData,
                                                databaseName,
                                                tableName,
                                                tableComment,
                                                typeMapping,
                                                toPaimonTypeVisitor());
                                mySqlSchemasInfo.addSchema(identifier, schema);
                            } else {
                                excludedTables.add(identifier);
                            }
                        }
                    }
                }
            }
        }
        return mySqlSchemasInfo;
    }

    public static MySqlSource<CdcSourceRecord> buildMySqlSource(
            Configuration mySqlConfig, String tableList, TypeMapping typeMapping) {
        MySqlSourceBuilder<CdcSourceRecord> sourceBuilder = MySqlSource.builder();

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
        mySqlConfig
                .getOptional(MySqlSourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED)
                .ifPresent(sourceBuilder::closeIdleReaders);
        mySqlConfig
                .getOptional(MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP)
                .ifPresent(sourceBuilder::skipSnapshotBackfill);

        String startupMode = mySqlConfig.get(MySqlSourceOptions.SCAN_STARTUP_MODE);
        // see
        // https://github.com/apache/flink-cdc/blob/master/flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc/src/main/java/org/apache/flink/cdc/connectors/mysql/table/MySqlTableSourceFactory.java#L197
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
        } else if ("snapshot".equalsIgnoreCase(startupMode)) {
            sourceBuilder.startupOptions(StartupOptions.snapshot());
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Unknown scan.startup.mode='%s'. Valid scan.startup.mode for MySQL CDC are [initial, earliest-offset, latest-offset, specific-offset, timestamp, snapshot]",
                            startupMode));
        }

        Properties jdbcProperties = new Properties();
        jdbcProperties.putAll(getJdbcProperties(typeMapping, mySqlConfig));
        sourceBuilder.jdbcProperties(jdbcProperties);

        Properties debeziumProperties = new Properties();
        debeziumProperties.putAll(
                convertToPropertiesPrefixKey(
                        mySqlConfig.toMap(), DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX));
        sourceBuilder.debeziumProperties(debeziumProperties);

        Map<String, Object> customConverterConfigs = new HashMap<>();
        customConverterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");
        CdcDebeziumDeserializationSchema schema =
                new CdcDebeziumDeserializationSchema(true, customConverterConfigs);

        boolean scanNewlyAddedTables = mySqlConfig.get(SCAN_NEWLY_ADDED_TABLE_ENABLED);

        return sourceBuilder
                .deserializer(schema)
                .includeSchemaChanges(true)
                .scanNewlyAddedTableEnabled(scanNewlyAddedTables)
                .build();
    }

    // see
    // https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/mysql-cdc/#connector-options
    // https://dev.mysql.com/doc/connectors/en/connector-j-reference-configuration-properties.html
    private static Map<String, String> getJdbcProperties(
            TypeMapping typeMapping, Configuration mySqlConfig) {
        Map<String, String> jdbcProperties =
                convertToPropertiesPrefixKey(mySqlConfig.toMap(), JdbcUrlUtils.PROPERTIES_PREFIX);

        if (typeMapping.containsMode(TINYINT1_NOT_BOOL)) {
            String tinyInt1isBit = jdbcProperties.get("tinyInt1isBit");
            if (tinyInt1isBit == null) {
                jdbcProperties.put("tinyInt1isBit", "false");
            } else if ("true".equals(jdbcProperties.get("tinyInt1isBit"))) {
                throw new IllegalArgumentException(
                        "Type mapping option 'tinyint1-not-bool' conflicts with jdbc properties 'jdbc.properties.tinyInt1isBit=true'. "
                                + "Option 'tinyint1-not-bool' is equal to 'jdbc.properties.tinyInt1isBit=false'.");
            }
        }

        return jdbcProperties;
    }

    public static void registerJdbcDriver() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException ex) {
            LOG.warn(
                    "Cannot find class com.mysql.cj.jdbc.Driver. Try to load class com.mysql.jdbc.Driver.");
            try {
                Class.forName("com.mysql.jdbc.Driver");
            } catch (Exception e) {
                throw new RuntimeException(
                        "No suitable driver found. Cannot find class com.mysql.cj.jdbc.Driver and com.mysql.jdbc.Driver.");
            }
        }
    }
}
