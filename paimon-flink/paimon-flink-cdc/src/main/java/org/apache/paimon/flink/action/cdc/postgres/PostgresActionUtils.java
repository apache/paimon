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
import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.schema.JdbcSchemaUtils;
import org.apache.paimon.flink.action.cdc.schema.JdbcSchemasInfo;
import org.apache.paimon.flink.action.cdc.serialization.CdcDebeziumDeserializationSchema;
import org.apache.paimon.options.OptionsUtils;
import org.apache.paimon.schema.Schema;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder.PostgresIncrementalSource;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceOptions;
import org.apache.flink.cdc.debezium.table.DebeziumOptions;
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
            try (ResultSet schemas = metaData.getSchemas()) {
                while (schemas.next()) {
                    String schemaName = schemas.getString("TABLE_SCHEM");
                    Matcher schemaMatcher = schemaPattern.matcher(schemaName);
                    if (!schemaMatcher.matches()) {
                        continue;
                    }
                    try (ResultSet tables =
                            metaData.getTables(
                                    databaseName, schemaName, "%", new String[] {"TABLE"})) {
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
                                jdbcSchemasInfo.addSchema(identifier, schemaName, schema);
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

    public static JdbcIncrementalSource<CdcSourceRecord> buildPostgresSource(
            Configuration postgresConfig, String[] schemaList, String[] tableList) {
        PostgresSourceBuilder<CdcSourceRecord> sourceBuilder = PostgresIncrementalSource.builder();

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

        // Postgres CDC using increment snapshot, splitSize is used instead of fetchSize (as in JDBC
        // connector). splitSize is the number of records in each snapshot split. see
        // https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/postgres-cdc/#incremental-snapshot-options
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

        if ("initial".equalsIgnoreCase(startupMode)) {
            sourceBuilder.startupOptions(StartupOptions.initial());
        } else if ("latest-offset".equalsIgnoreCase(startupMode)) {
            sourceBuilder.startupOptions(StartupOptions.latest());
        }

        Properties debeziumProperties = new Properties();
        debeziumProperties.putAll(
                OptionsUtils.convertToPropertiesPrefixKey(
                        postgresConfig.toMap(), DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX));
        sourceBuilder.debeziumProperties(debeziumProperties);

        Map<String, Object> customConverterConfigs = new HashMap<>();
        customConverterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");
        CdcDebeziumDeserializationSchema schema =
                new CdcDebeziumDeserializationSchema(true, customConverterConfigs);
        return sourceBuilder.deserializer(schema).includeSchemaChanges(true).build();
    }

    public static void registerJdbcDriver() {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(
                    "No suitable driver found. Cannot find class org.postgresql.Driver.");
        }
    }
}
