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

package org.apache.paimon.flink.action.cdc.postgresql;

import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Utils for PostgreSqlAction. */
public class PostgreSqlActionUtils {

    static Connection getConnection(Configuration postgreSqlConfig) throws Exception {
        return DriverManager.getConnection(
                String.format(
                        "jdbc:postgresql://%s:%d/%s",
                        postgreSqlConfig.get(PostgresSourceOptions.HOSTNAME),
                        postgreSqlConfig.get(PostgresSourceOptions.PG_PORT),
                        postgreSqlConfig.get(PostgresSourceOptions.DATABASE_NAME)),
                postgreSqlConfig.get(PostgresSourceOptions.USERNAME),
                postgreSqlConfig.get(PostgresSourceOptions.PASSWORD));
    }

    static SourceFunction<String> buildPostgreSqlSource(Configuration postgreSqlConfig) {
        validatePostgreSqlConfig(postgreSqlConfig);
        PostgreSQLSource.Builder<String> sourceBuilder = PostgreSQLSource.builder();

        String schemaName = postgreSqlConfig.get(PostgresSourceOptions.SCHEMA_NAME);
        String tableName = postgreSqlConfig.get(PostgresSourceOptions.TABLE_NAME);
        sourceBuilder
                .hostname(postgreSqlConfig.get(PostgresSourceOptions.HOSTNAME))
                .port(postgreSqlConfig.get(PostgresSourceOptions.PG_PORT))
                .username(postgreSqlConfig.get(PostgresSourceOptions.USERNAME))
                .password(postgreSqlConfig.get(PostgresSourceOptions.PASSWORD))
                .database(postgreSqlConfig.get(PostgresSourceOptions.DATABASE_NAME))
                .schemaList(schemaName)
                .tableList(schemaName + "." + tableName);

        postgreSqlConfig
                .getOptional(PostgresSourceOptions.DECODING_PLUGIN_NAME)
                .ifPresent(sourceBuilder::decodingPluginName);

        postgreSqlConfig
                .getOptional(PostgresSourceOptions.SLOT_NAME)
                .ifPresent(sourceBuilder::slotName);

        Map<String, Object> customConverterConfigs = new HashMap<>();
        customConverterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");
        JsonDebeziumDeserializationSchema schema =
                new JsonDebeziumDeserializationSchema(true, customConverterConfigs);
        return sourceBuilder.deserializer(schema).build();
    }

    private static void validatePostgreSqlConfig(Configuration postgreSqlConfig) {
        ConfigOption<?>[] options = {
            PostgresSourceOptions.HOSTNAME,
            PostgresSourceOptions.USERNAME,
            PostgresSourceOptions.PASSWORD,
            PostgresSourceOptions.DATABASE_NAME,
            PostgresSourceOptions.SCHEMA_NAME,
            PostgresSourceOptions.TABLE_NAME
        };

        for (ConfigOption<?> option : options) {
            validateConfig(postgreSqlConfig, option);
        }
    }

    private static void validateConfig(Configuration config, ConfigOption<?> option) {
        checkArgument(
                config.contains(option),
                String.format("postgresql-conf [%s] must be specified.", option.key()));
    }
}
