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

package org.apache.paimon.flink.action.cdc.postgres;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.schema.JdbcSchemaUtils;
import org.apache.paimon.flink.action.cdc.schema.JdbcSchemasInfo;
import org.apache.paimon.schema.Schema;

import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.postgres.source.PostgresSourceBuilder;
import com.ververica.cdc.connectors.postgres.source.PostgresSourceBuilder.PostgresIncrementalSource;
import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.DebeziumOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.kafka.connect.json.JsonConverterConfig;

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

import static org.apache.paimon.flink.action.cdc.postgres.PostgresTypeUtils.toPaimonTypeVisitor;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Utils for Postgres Action. */
public class PostgresActionUtils {

    static Connection getConnection(Configuration postgresConfig) throws Exception {
        String url =
                String.format(
                        "jdbc:postgresql://%s:%d/%s",
                        postgresConfig.get(PostgresSourceOptions.HOSTNAME),
                        postgresConfig.get(PostgresSourceOptions.PG_PORT),
                        postgresConfig.get(PostgresSourceOptions.DATABASE_NAME));

        return DriverManager.getConnection(
                url,
                postgresConfig.get(PostgresSourceOptions.USERNAME),
                postgresConfig.get(PostgresSourceOptions.PASSWORD));
    }

    public static JdbcSchemasInfo getPostgresTableInfos(
            Configuration postgresConfig,
            Predicate<String> monitorTablePredication,
            List<Identifier> excludedTables,
            TypeMapping typeMapping)
            throws Exception {

        String databaseName = postgresConfig.get(PostgresSourceOptions.DATABASE_NAME);
        Pattern schemaPattern =
                Pattern.compile(postgresConfig.get(PostgresSourceOptions.SCHEMA_NAME));
        JdbcSchemasInfo jdbcSchemasInfo = new JdbcSchemasInfo();
        try (Connection conn = PostgresActionUtils.getConnection(postgresConfig)) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet schemas = metaData.getCatalogs()) {
                while (schemas.next()) {
                    String schemaName = schemas.getString("TABLE_SCHEMA");
                    Matcher schemaMatcher = schemaPattern.matcher(schemaName);
                    if (!schemaMatcher.matches()) {
                        continue;
                    }
                    try (ResultSet tables = metaData.getTables(databaseName, schemaName, "%", null)) {
                        while (tables.next()) {
                            String tableName = tables.getString("TABLE_NAME");
                            String tableComment = tables.getString("REMARKS");
                            Identifier identifier = Identifier.create(databaseName, tableName);
                            if (monitorTablePredication.test(tableName)) {
                                Schema schema =
                                        JdbcSchemaUtils.buildSchema(
                                                metaData,
                                                databaseName,
                                                schemaName,
                                                tableName,
                                                tableComment,
                                                typeMapping,
                                                toPaimonTypeVisitor());
                                jdbcSchemasInfo.addSchema(identifier, schema);
                            } else {
                                excludedTables.add(identifier);
                            }
                        }
                    }

                }
            }
        }

        return jdbcSchemasInfo;
    }

    public static JdbcIncrementalSource<String> buildPostgresSource(
            Configuration postgresConfig, String[] schemaList, String[] tableList) {
        validatePostgresConfig(postgresConfig);
        PostgresSourceBuilder<String> sourceBuilder = PostgresIncrementalSource.builder();

        sourceBuilder
                .hostname(postgresConfig.get(PostgresSourceOptions.HOSTNAME))
                .port(postgresConfig.get(PostgresSourceOptions.PG_PORT))
                .database(postgresConfig.get(PostgresSourceOptions.DATABASE_NAME))
                .schemaList(schemaList)
                .tableList(tableList)
                .slotName(postgresConfig.get(PostgresSourceOptions.SLOT_NAME))
                .username(postgresConfig.get(PostgresSourceOptions.USERNAME))
                .password(postgresConfig.get(PostgresSourceOptions.PASSWORD));

        // use pgoutput for PostgreSQL 10+
        postgresConfig
                .getOptional(PostgresSourceOptions.DECODING_PLUGIN_NAME)
                .ifPresent(sourceBuilder::decodingPluginName);

        // PostgreSQL CDC using increment snapshot, splitSize is used instead of fetchSize (as in
        // JDBC
        // connector). splitSize is the number of records in each snapshot split.
        postgresConfig
                .getOptional(PostgresSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE)
                .ifPresent(sourceBuilder::splitSize);
        postgresConfig
                .getOptional(PostgresSourceOptions.CONNECT_TIMEOUT)
                .ifPresent(sourceBuilder::connectTimeout);
        postgresConfig
                .getOptional(PostgresSourceOptions.CONNECT_MAX_RETRIES)
                .ifPresent(sourceBuilder::connectMaxRetries);
        postgresConfig
                .getOptional(PostgresSourceOptions.CONNECTION_POOL_SIZE)
                .ifPresent(sourceBuilder::connectionPoolSize);
        postgresConfig
                .getOptional(PostgresSourceOptions.HEARTBEAT_INTERVAL)
                .ifPresent(sourceBuilder::heartbeatInterval);

        String startupMode = postgresConfig.get(PostgresSourceOptions.SCAN_STARTUP_MODE);
        // see
        // https://github.com/ververica/flink-cdc-connectors/blob/master/flink-connector-mysql-cdc/src/main/java/com/ververica/cdc/connectors/mysql/table/MySqlTableSourceFactory.java#L196
        if ("initial".equalsIgnoreCase(startupMode)) {
            sourceBuilder.startupOptions(StartupOptions.initial());
        } else if ("earliest-offset".equalsIgnoreCase(startupMode)) {
            sourceBuilder.startupOptions(StartupOptions.earliest());
        } else if ("latest-offset".equalsIgnoreCase(startupMode)) {
            sourceBuilder.startupOptions(StartupOptions.latest());
        } else if ("specific-offset".equalsIgnoreCase(startupMode)) {
            String file =
                    postgresConfig.get(PostgresSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_FILE);
            Integer pos =
                    postgresConfig.get(PostgresSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_POS);
            sourceBuilder.startupOptions(StartupOptions.specificOffset(file, pos));
        } else if ("timestamp".equalsIgnoreCase(startupMode)) {
            sourceBuilder.startupOptions(
                    StartupOptions.timestamp(
                            postgresConfig.get(
                                    PostgresSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS)));
        }

        Properties debeziumProperties = new Properties();
        for (Map.Entry<String, String> entry : postgresConfig.toMap().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.startsWith(DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX)) {
                debeziumProperties.put(
                        key.substring(DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX.length()), value);
            }
        }
        sourceBuilder.debeziumProperties(debeziumProperties);

        Map<String, Object> customConverterConfigs = new HashMap<>();
        customConverterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");
        JsonDebeziumDeserializationSchema schema =
                new JsonDebeziumDeserializationSchema(true, customConverterConfigs);
        return sourceBuilder.deserializer(schema).includeSchemaChanges(true).build();
    }

    private static void validatePostgresConfig(Configuration postgresConfig) {
        checkArgument(
                postgresConfig.get(PostgresSourceOptions.HOSTNAME) != null,
                String.format(
                        "postgres-conf [%s] must be specified.",
                        PostgresSourceOptions.HOSTNAME.key()));

        checkArgument(
                postgresConfig.get(PostgresSourceOptions.USERNAME) != null,
                String.format(
                        "postgres-conf [%s] must be specified.",
                        PostgresSourceOptions.USERNAME.key()));

        checkArgument(
                postgresConfig.get(PostgresSourceOptions.PASSWORD) != null,
                String.format(
                        "postgres-conf [%s] must be specified.",
                        PostgresSourceOptions.PASSWORD.key()));

        checkArgument(
                postgresConfig.get(PostgresSourceOptions.DATABASE_NAME) != null,
                String.format(
                        "postgres-conf [%s] must be specified.",
                        PostgresSourceOptions.DATABASE_NAME.key()));
    }
}