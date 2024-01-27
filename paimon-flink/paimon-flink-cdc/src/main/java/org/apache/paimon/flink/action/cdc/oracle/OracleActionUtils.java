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

package org.apache.paimon.flink.action.cdc.oracle;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.schema.JdbcSchemaUtils;
import org.apache.paimon.flink.action.cdc.schema.JdbcSchemasInfo;
import org.apache.paimon.options.OptionsUtils;
import org.apache.paimon.schema.Schema;

import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.oracle.source.OracleSourceBuilder;
import com.ververica.cdc.connectors.oracle.source.config.OracleSourceOptions;
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

import static org.apache.paimon.flink.action.cdc.oracle.OracleTypeUtils.toPaimonTypeVisitor;

/** Utils for Oracle Action. */
public class OracleActionUtils {

    static Connection getConnection(Configuration oracleConfig) throws Exception {
        String url =
                String.format(
                        "jdbc:oracle:thin:@%s:%d:%s",
                        oracleConfig.get(OracleSourceOptions.HOSTNAME),
                        oracleConfig.get(OracleSourceOptions.PORT),
                        oracleConfig.get(OracleSourceOptions.DATABASE_NAME));

        return DriverManager.getConnection(
                url,
                oracleConfig.get(OracleSourceOptions.USERNAME),
                oracleConfig.get(OracleSourceOptions.PASSWORD));
    }

    public static JdbcSchemasInfo getOracleTableInfos(
            Configuration oracleConfig,
            Predicate<String> monitorTablePredication,
            List<Identifier> excludedTables,
            TypeMapping typeMapping)
            throws Exception {

        String databaseName = oracleConfig.get(OracleSourceOptions.DATABASE_NAME);
        Pattern schemaPattern = Pattern.compile(oracleConfig.get(OracleSourceOptions.SCHEMA_NAME));
        JdbcSchemasInfo jdbcSchemasInfo = new JdbcSchemasInfo();
        try (Connection conn = OracleActionUtils.getConnection(oracleConfig)) {
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
                            System.out.println(tableName);
                            String tableComment = tables.getString("REMARKS");
                            System.out.println(tableComment);
                            System.out.println("============");
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

    public static JdbcIncrementalSource<String> buildOracleSource(
            Configuration oracleConfig, String[] schemaList, String[] tableList) {
        OracleSourceBuilder<String> sourceBuilder =
                OracleSourceBuilder.OracleIncrementalSource.builder();

        sourceBuilder
                .hostname(oracleConfig.get(OracleSourceOptions.HOSTNAME))
                .port(oracleConfig.get(OracleSourceOptions.PORT))
                .databaseList(oracleConfig.get(OracleSourceOptions.DATABASE_NAME))
                .schemaList(schemaList)
                .tableList(tableList)
                .username(oracleConfig.get(OracleSourceOptions.USERNAME))
                .password(oracleConfig.get(OracleSourceOptions.PASSWORD));

        // Oracle CDC using increment snapshot, splitSize is used instead of fetchSize (as in JDBC
        // connector). splitSize is the number of records in each snapshot split. see
        // https://ververica.github.io/flink-cdc-connectors/master/content/connectors/oracle-cdc.html#incremental-snapshot-options
        oracleConfig
                .getOptional(OracleSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE)
                .ifPresent(sourceBuilder::splitSize);
        oracleConfig
                .getOptional(OracleSourceOptions.CONNECT_TIMEOUT)
                .ifPresent(sourceBuilder::connectTimeout);
        oracleConfig
                .getOptional(OracleSourceOptions.CONNECT_MAX_RETRIES)
                .ifPresent(sourceBuilder::connectMaxRetries);
        oracleConfig
                .getOptional(OracleSourceOptions.CONNECTION_POOL_SIZE)
                .ifPresent(sourceBuilder::connectionPoolSize);

        String startupMode = oracleConfig.get(OracleSourceOptions.SCAN_STARTUP_MODE);

        if ("initial".equalsIgnoreCase(startupMode)) {
            sourceBuilder.startupOptions(StartupOptions.initial());
        } else if ("latest-offset".equalsIgnoreCase(startupMode)) {
            sourceBuilder.startupOptions(StartupOptions.latest());
        }

        Properties debeziumProperties = new Properties();
        debeziumProperties.putAll(
                OptionsUtils.convertToPropertiesPrefixKey(
                        oracleConfig.toMap(), DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX));
        sourceBuilder.debeziumProperties(debeziumProperties);

        Map<String, Object> customConverterConfigs = new HashMap<>();
        customConverterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");
        JsonDebeziumDeserializationSchema schema =
                new JsonDebeziumDeserializationSchema(true, customConverterConfigs);
        return sourceBuilder.deserializer(schema).includeSchemaChanges(true).build();
    }

    public static void registerJdbcDriver() {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(
                    "No suitable driver found. Cannot find class oracle.jdbc.driver.OracleDriver.");
        }
    }
}
