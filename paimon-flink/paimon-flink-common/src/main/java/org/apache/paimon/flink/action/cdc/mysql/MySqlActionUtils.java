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

import org.apache.paimon.flink.sink.cdc.UpdatedDataFieldsProcessFunction;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
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
import org.apache.flink.configuration.Configuration;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.paimon.utils.Preconditions.checkArgument;

class MySqlActionUtils {

    static Connection getConnection(Configuration mySqlConfig) throws Exception {
        return DriverManager.getConnection(
                String.format(
                        "jdbc:mysql://%s:%d/",
                        mySqlConfig.get(MySqlSourceOptions.HOSTNAME),
                        mySqlConfig.get(MySqlSourceOptions.PORT)),
                mySqlConfig.get(MySqlSourceOptions.USERNAME),
                mySqlConfig.get(MySqlSourceOptions.PASSWORD));
    }

    static void assertSchemaCompatible(TableSchema tableSchema, MySqlSchema mySqlSchema) {
        if (!schemaCompatible(tableSchema, mySqlSchema)) {
            throw new IllegalArgumentException(
                    "Paimon schema and MySQL schema are not compatible.\n"
                            + "Paimon fields are: "
                            + tableSchema.fields()
                            + ".\nMySQL fields are: "
                            + mySqlSchema.fields());
        }
    }

    static boolean schemaCompatible(TableSchema tableSchema, MySqlSchema mySqlSchema) {
        for (Map.Entry<String, DataType> entry : mySqlSchema.fields().entrySet()) {
            int idx = tableSchema.fieldNames().indexOf(entry.getKey());
            if (idx < 0) {
                return false;
            }
            DataType type = tableSchema.fields().get(idx).type();
            if (UpdatedDataFieldsProcessFunction.canConvert(entry.getValue(), type)
                    != UpdatedDataFieldsProcessFunction.ConvertAction.CONVERT) {
                return false;
            }
        }
        return true;
    }

    static Schema buildPaimonSchema(
            MySqlSchema mySqlSchema,
            List<String> specifiedPartitionKeys,
            List<String> specifiedPrimaryKeys,
            Map<String, String> paimonConfig) {
        Schema.Builder builder = Schema.newBuilder();
        builder.options(paimonConfig);

        for (Map.Entry<String, DataType> entry : mySqlSchema.fields().entrySet()) {
            builder.column(entry.getKey(), entry.getValue());
        }

        if (specifiedPrimaryKeys.size() > 0) {
            for (String key : specifiedPrimaryKeys) {
                if (!mySqlSchema.fields().containsKey(key)) {
                    throw new IllegalArgumentException(
                            "Specified primary key " + key + " does not exist in MySQL tables");
                }
            }
            builder.primaryKey(specifiedPrimaryKeys);
        } else if (mySqlSchema.primaryKeys().size() > 0) {
            builder.primaryKey(mySqlSchema.primaryKeys());
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

    static MySqlSource<String> buildMySqlSource(Configuration mySqlConfig) {
        validateMySqlConfig(mySqlConfig);
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

        checkArgument(
                mySqlConfig.get(MySqlSourceOptions.TABLE_NAME) != null,
                String.format(
                        "mysql-conf [%s] must be specified.", MySqlSourceOptions.TABLE_NAME.key()));
    }
}
