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

package org.apache.paimon.flink.action.cdc.sqlserver;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.MultiTablesSinkMode;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.schema.JdbcSchemaUtils;
import org.apache.paimon.flink.action.cdc.schema.JdbcSchemasInfo;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.utils.Pair;

import com.ververica.cdc.connectors.base.options.JdbcSourceOptions;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.sqlserver.source.SqlServerSourceBuilder;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.ververica.cdc.debezium.table.DebeziumOptions.getDebeziumProperties;
import static org.apache.paimon.flink.action.MultiTablesSinkMode.COMBINED;
import static org.apache.paimon.flink.action.MultiTablesSinkMode.DIVIDED;
import static org.apache.paimon.flink.action.cdc.sqlserver.SqlServerSourceOptions.SCAN_STARTUP_MODE;
import static org.apache.paimon.flink.action.cdc.sqlserver.SqlServerTypeUtils.toPaimonTypeVisitor;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Utility class for SqlServer action. */
public class SqlServerActionUtils {
    private static final Logger LOG = LoggerFactory.getLogger(SqlServerActionUtils.class);
    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";

    public static Connection getConnection(Configuration sqlServerConfig) throws Exception {
        String url =
                String.format(
                        "jdbc:sqlserver://%s:%s",
                        sqlServerConfig.get(SqlServerSourceOptions.HOSTNAME),
                        sqlServerConfig.get(SqlServerSourceOptions.PORT));

        return DriverManager.getConnection(
                url,
                sqlServerConfig.get(SqlServerSourceOptions.USERNAME),
                sqlServerConfig.get(SqlServerSourceOptions.PASSWORD));
    }

    public static JdbcSchemasInfo getSqlServerTableInfos(
            Configuration sqlServerConfig,
            Predicate<String> monitorTablePredication,
            List<Pair<Identifier, String>> excludedTables,
            TypeMapping typeMapping)
            throws Exception {
        String databaseName = sqlServerConfig.get(SqlServerSourceOptions.DATABASE_NAME);
        Pattern schemaPattern =
                Pattern.compile(sqlServerConfig.get(SqlServerSourceOptions.SCHEMA_NAME));

        JdbcSchemasInfo jdbcSchemasInfo = new JdbcSchemasInfo();
        try (Connection conn = getConnection(sqlServerConfig)) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet schemas = metaData.getSchemas(databaseName, null)) {
                while (schemas.next()) {
                    String schemaName = schemas.getString("TABLE_SCHEM");
                    Matcher schemaMatcher = schemaPattern.matcher(schemaName);
                    if (!schemaMatcher.matches()) {
                        continue;
                    }
                    try (ResultSet tables =
                            metaData.getTables(databaseName, schemaName, "%", null)) {
                        while (tables.next()) {
                            String tableName = tables.getString("TABLE_NAME");
                            if (tableName.startsWith("sys")) {
                                LOG.warn(
                                        String.format(
                                                "Filter out system or not cdc table [%s]",
                                                tableName));
                                continue;
                            }

                            Identifier identifier = Identifier.create(databaseName, tableName);
                            if (monitorTablePredication.test(tableName)) {
                                String tableComment = tables.getString("REMARKS");
                                Schema tableSchema =
                                        JdbcSchemaUtils.buildSchema(
                                                metaData,
                                                databaseName,
                                                schemaName,
                                                tableName,
                                                tableComment,
                                                typeMapping,
                                                toPaimonTypeVisitor());
                                jdbcSchemasInfo.addSchema(identifier, schemaName, tableSchema);
                            } else {
                                excludedTables.add(Pair.of(identifier, schemaName));
                            }
                        }
                    }
                }
            }
        }
        return jdbcSchemasInfo;
    }

    public static List<String> databaseList(List<JdbcSchemasInfo.JdbcSchemaInfo> identifiers) {
        return new ArrayList<>(
                identifiers.stream()
                        .map(jdbcSchemaInfo -> jdbcSchemaInfo.identifier().getDatabaseName())
                        .collect(Collectors.toSet()));
    }

    public static String tableList(
            MultiTablesSinkMode mode,
            String schemaPattern,
            String includingTablePattern,
            List<Pair<Identifier, String>> monitoredTables,
            List<Pair<Identifier, String>> excludedTables) {
        if (mode == DIVIDED) {
            return dividedModeTableList(monitoredTables);
        } else if (mode == COMBINED) {
            return combinedModeTableList(schemaPattern, includingTablePattern, excludedTables);
        }
        throw new UnsupportedOperationException("Unknown MultiTablesSinkMode: " + mode);
    }

    private static String dividedModeTableList(List<Pair<Identifier, String>> monitoredTables) {
        // In DIVIDED mode, we only concern about existed tables
        return monitoredTables.stream()
                .map(t -> t.getRight() + "\\." + t.getLeft().getObjectName())
                .collect(Collectors.joining("|"));
    }

    public static String combinedModeTableList(
            String schemaPattern,
            String includingTablePattern,
            List<Pair<Identifier, String>> excludedTables) {
        String includingPattern =
                String.format("(%s)\\.(%s)", schemaPattern, includingTablePattern);
        if (excludedTables.isEmpty()) {
            return includingPattern;
        }

        String excludingPattern =
                excludedTables.stream()
                        .map(
                                t ->
                                        String.format(
                                                "(^%s$)",
                                                t.getRight() + "\\." + t.getLeft().getObjectName()))
                        .collect(Collectors.joining("|"));
        excludingPattern = "?!" + excludingPattern;
        return String.format("(%s)(%s)", excludingPattern, includingPattern);
    }

    public static SqlServerSourceBuilder.SqlServerIncrementalSource<String> buildSqlServerSource(
            Configuration sqlServerSourceConfig, String tableList) {
        validateSqlServerConfig(sqlServerSourceConfig);

        Map<String, Object> converterConfigs = new HashMap<>();
        converterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");
        JsonDebeziumDeserializationSchema debeziumDeserializationSchema =
                new JsonDebeziumDeserializationSchema(true, converterConfigs);

        sqlServerSourceConfig.setBoolean("debezium.include.schema.changes", false);
        SqlServerSourceBuilder sqlServerSourceBuilder =
                new SqlServerSourceBuilder()
                        .hostname(sqlServerSourceConfig.get(SqlServerSourceOptions.HOSTNAME))
                        .port(sqlServerSourceConfig.get(SqlServerSourceOptions.PORT))
                        .username(sqlServerSourceConfig.get(SqlServerSourceOptions.USERNAME))
                        .password(sqlServerSourceConfig.get(SqlServerSourceOptions.PASSWORD))
                        .databaseList(
                                sqlServerSourceConfig.getString(
                                        SqlServerSourceOptions.DATABASE_NAME))
                        .tableList(tableList)
                        .debeziumProperties(getDebeziumProperties(sqlServerSourceConfig.toMap()))
                        .startupOptions(getStartupOptions(sqlServerSourceConfig))
                        .includeSchemaChanges(false)
                        .deserializer(debeziumDeserializationSchema);

        sqlServerSourceConfig
                .getOptional(SqlServerSourceOptions.SERVER_TIME_ZONE)
                .ifPresent(sqlServerSourceBuilder::serverTimeZone);

        sqlServerSourceConfig
                .getOptional(JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE)
                .ifPresent(sqlServerSourceBuilder::splitSize);
        sqlServerSourceConfig
                .getOptional(JdbcSourceOptions.CHUNK_META_GROUP_SIZE)
                .ifPresent(sqlServerSourceBuilder::splitMetaGroupSize);
        sqlServerSourceConfig
                .getOptional(JdbcSourceOptions.CONNECT_TIMEOUT)
                .ifPresent(sqlServerSourceBuilder::connectTimeout);
        sqlServerSourceConfig
                .getOptional(JdbcSourceOptions.CONNECT_MAX_RETRIES)
                .ifPresent(sqlServerSourceBuilder::connectMaxRetries);
        sqlServerSourceConfig
                .getOptional(JdbcSourceOptions.CONNECTION_POOL_SIZE)
                .ifPresent(sqlServerSourceBuilder::connectionPoolSize);
        sqlServerSourceConfig
                .getOptional(JdbcSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND)
                .ifPresent(sqlServerSourceBuilder::distributionFactorUpper);
        sqlServerSourceConfig
                .getOptional(JdbcSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND)
                .ifPresent(sqlServerSourceBuilder::distributionFactorLower);
        sqlServerSourceConfig
                .getOptional(JdbcSourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED)
                .ifPresent(sqlServerSourceBuilder::closeIdleReaders);
        return sqlServerSourceBuilder.includeSchemaChanges(true).build();
    }

    private static StartupOptions getStartupOptions(Configuration config) {
        String modeString = config.get(SCAN_STARTUP_MODE);

        switch (modeString.toLowerCase()) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                return com.ververica.cdc.connectors.base.options.StartupOptions.initial();

            case SCAN_STARTUP_MODE_VALUE_LATEST:
                return com.ververica.cdc.connectors.base.options.StartupOptions.latest();

            default:
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s], but was: %s",
                                SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
                                SCAN_STARTUP_MODE_VALUE_LATEST,
                                modeString));
        }
    }

    public static void registerJdbcDriver() {
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(
                    "No suitable driver found. Cannot find class com.microsoft.sqlserver.jdbc.SQLServerDriver.");
        }
    }

    private static void validateSqlServerConfig(Configuration sqlServerSourceConfig) {
        checkArgument(
                sqlServerSourceConfig.get(SqlServerSourceOptions.HOSTNAME) != null,
                String.format(
                        "sqlserver-conf [%s] must be specified.",
                        SqlServerSourceOptions.HOSTNAME.key()));

        checkArgument(
                sqlServerSourceConfig.get(SqlServerSourceOptions.PORT) != null,
                String.format(
                        "sqlserver-conf [%s] must be specified.",
                        SqlServerSourceOptions.PORT.key()));

        checkArgument(
                sqlServerSourceConfig.get(SqlServerSourceOptions.PASSWORD) != null,
                String.format(
                        "sqlserver-conf [%s] must be specified.",
                        SqlServerSourceOptions.PASSWORD.key()));

        checkArgument(
                sqlServerSourceConfig.get(SqlServerSourceOptions.DATABASE_NAME) != null,
                String.format(
                        "sqlserver-conf [%s] must be specified.",
                        SqlServerSourceOptions.DATABASE_NAME.key()));

        checkArgument(
                sqlServerSourceConfig.get(SqlServerSourceOptions.SCHEMA_NAME) != null,
                String.format(
                        "sqlserver-conf [%s] must be specified.",
                        SqlServerSourceOptions.SCHEMA_NAME.key()));
    }
}
