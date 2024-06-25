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

package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.flink.action.cdc.format.DataFormat;
import org.apache.paimon.flink.action.cdc.kafka.KafkaActionUtils;
import org.apache.paimon.flink.action.cdc.mongodb.MongoDBRecordParser;
import org.apache.paimon.flink.action.cdc.mysql.MySqlActionUtils;
import org.apache.paimon.flink.action.cdc.mysql.MySqlRecordParser;
import org.apache.paimon.flink.action.cdc.postgres.PostgresActionUtils;
import org.apache.paimon.flink.action.cdc.postgres.PostgresRecordParser;
import org.apache.paimon.flink.action.cdc.pulsar.PulsarActionUtils;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;

import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceOptions;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceOptions;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.common.config.PulsarOptions;
import org.apache.flink.connector.pulsar.source.PulsarSourceOptions;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions;

import java.util.List;
import java.util.Map;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.KAFKA_CONF;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.MONGODB_CONF;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.MYSQL_CONF;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.POSTGRES_CONF;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.PULSAR_CONF;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.checkOneRequiredOption;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.checkRequiredOptions;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Provide different function according to CDC source type. */
public class SyncJobHandler {

    private final SourceType sourceType;
    private final Configuration cdcSourceConfig;
    private final boolean isTableSync;
    private final String sinkLocation;

    public SyncJobHandler(
            SourceType sourceType, Map<String, String> cdcSourceConfig, String database) {
        this.sourceType = sourceType;
        this.cdcSourceConfig = Configuration.fromMap(cdcSourceConfig);
        this.isTableSync = false;
        this.sinkLocation = database;
    }

    public SyncJobHandler(
            SourceType sourceType,
            Map<String, String> cdcSourceConfig,
            String database,
            String table) {
        this.sourceType = sourceType;
        this.cdcSourceConfig = Configuration.fromMap(cdcSourceConfig);
        this.isTableSync = true;
        this.sinkLocation = database + "." + table;
    }

    public String provideSourceName() {
        return sourceType.sourceName;
    }

    public String provideDefaultJobName() {
        return String.format(
                sourceType.defaultJobNameFormat, isTableSync ? "Table" : "Database", sinkLocation);
    }

    public void registerJdbcDriver() {
        if (sourceType == SourceType.MYSQL) {
            MySqlActionUtils.registerJdbcDriver();
        } else if (sourceType == SourceType.POSTGRES) {
            PostgresActionUtils.registerJdbcDriver();
        }
    }

    public void checkRequiredOption() {
        switch (sourceType) {
            case MYSQL:
                checkRequiredOptions(
                        cdcSourceConfig,
                        MYSQL_CONF,
                        MySqlSourceOptions.HOSTNAME,
                        MySqlSourceOptions.USERNAME,
                        MySqlSourceOptions.PASSWORD,
                        MySqlSourceOptions.DATABASE_NAME);
                if (isTableSync) {
                    checkRequiredOptions(
                            cdcSourceConfig, MYSQL_CONF, MySqlSourceOptions.TABLE_NAME);
                } else {
                    checkArgument(
                            !cdcSourceConfig.contains(MySqlSourceOptions.TABLE_NAME),
                            MySqlSourceOptions.TABLE_NAME.key()
                                    + " cannot be set for mysql_sync_database. "
                                    + "If you want to sync several MySQL tables into one Paimon table, "
                                    + "use mysql_sync_table instead.");
                }
                break;
            case POSTGRES:
                checkRequiredOptions(
                        cdcSourceConfig,
                        POSTGRES_CONF,
                        PostgresSourceOptions.HOSTNAME,
                        PostgresSourceOptions.USERNAME,
                        PostgresSourceOptions.PASSWORD,
                        PostgresSourceOptions.DATABASE_NAME,
                        PostgresSourceOptions.SCHEMA_NAME,
                        PostgresSourceOptions.SLOT_NAME);
                if (isTableSync) {
                    checkRequiredOptions(
                            cdcSourceConfig, POSTGRES_CONF, PostgresSourceOptions.TABLE_NAME);
                } else {
                    checkArgument(
                            !cdcSourceConfig.contains(PostgresSourceOptions.TABLE_NAME),
                            PostgresSourceOptions.TABLE_NAME.key()
                                    + " cannot be set for postgres_sync_database. "
                                    + "If you want to sync several PostgreSQL tables into one Paimon table, "
                                    + "use postgres_sync_table instead.");
                }
                break;
            case KAFKA:
                checkRequiredOptions(
                        cdcSourceConfig,
                        KAFKA_CONF,
                        KafkaConnectorOptions.VALUE_FORMAT,
                        KafkaConnectorOptions.PROPS_BOOTSTRAP_SERVERS);
                checkOneRequiredOption(
                        cdcSourceConfig,
                        KAFKA_CONF,
                        KafkaConnectorOptions.TOPIC,
                        KafkaConnectorOptions.TOPIC_PATTERN);
                break;
            case PULSAR:
                checkRequiredOptions(
                        cdcSourceConfig,
                        PULSAR_CONF,
                        PulsarActionUtils.VALUE_FORMAT,
                        PulsarOptions.PULSAR_SERVICE_URL,
                        PulsarOptions.PULSAR_ADMIN_URL,
                        PulsarSourceOptions.PULSAR_SUBSCRIPTION_NAME);
                checkOneRequiredOption(
                        cdcSourceConfig,
                        PULSAR_CONF,
                        PulsarActionUtils.TOPIC,
                        PulsarActionUtils.TOPIC_PATTERN);
                break;
            case MONGODB:
                checkRequiredOptions(
                        cdcSourceConfig,
                        MONGODB_CONF,
                        MongoDBSourceOptions.HOSTS,
                        MongoDBSourceOptions.DATABASE);
                if (isTableSync) {
                    checkRequiredOptions(
                            cdcSourceConfig, MONGODB_CONF, MongoDBSourceOptions.COLLECTION);
                }
                break;
            default:
                throw new UnsupportedOperationException("Unknown source type " + sourceType);
        }
    }

    public Source<CdcSourceRecord, ?, ?> provideSource() {
        switch (sourceType) {
            case KAFKA:
                return KafkaActionUtils.buildKafkaSource(
                        cdcSourceConfig,
                        provideDataFormat().createKafkaDeserializer(cdcSourceConfig));
            case PULSAR:
                return PulsarActionUtils.buildPulsarSource(
                        cdcSourceConfig,
                        provideDataFormat().createPulsarDeserializer(cdcSourceConfig));
            default:
                throw new UnsupportedOperationException(
                        "Cannot get source from source type" + sourceType);
        }
    }

    public FlatMapFunction<CdcSourceRecord, RichCdcMultiplexRecord> provideRecordParser(
            List<ComputedColumn> computedColumns,
            TypeMapping typeMapping,
            CdcMetadataConverter[] metadataConverters) {
        switch (sourceType) {
            case MYSQL:
                return new MySqlRecordParser(
                        cdcSourceConfig, computedColumns, typeMapping, metadataConverters);
            case POSTGRES:
                return new PostgresRecordParser(
                        cdcSourceConfig, computedColumns, typeMapping, metadataConverters);
            case KAFKA:
            case PULSAR:
                DataFormat dataFormat = provideDataFormat();
                return dataFormat.createParser(typeMapping, computedColumns);
            case MONGODB:
                return new MongoDBRecordParser(computedColumns, cdcSourceConfig);
            default:
                throw new UnsupportedOperationException("Unknown source type " + sourceType);
        }
    }

    public DataFormat provideDataFormat() {
        switch (sourceType) {
            case KAFKA:
                return KafkaActionUtils.getDataFormat(cdcSourceConfig);
            case PULSAR:
                return PulsarActionUtils.getDataFormat(cdcSourceConfig);
            default:
                throw new UnsupportedOperationException(
                        "Cannot get DataFormat from source type" + sourceType);
        }
    }

    public MessageQueueSchemaUtils.ConsumerWrapper provideConsumer() {
        switch (sourceType) {
            case KAFKA:
                return KafkaActionUtils.getKafkaEarliestConsumer(
                        cdcSourceConfig,
                        provideDataFormat().createKafkaDeserializer(cdcSourceConfig));
            case PULSAR:
                return PulsarActionUtils.createPulsarConsumer(
                        cdcSourceConfig,
                        provideDataFormat().createPulsarDeserializer(cdcSourceConfig));
            default:
                throw new UnsupportedOperationException(
                        "Cannot get consumer from source type" + sourceType);
        }
    }

    public CdcMetadataConverter provideMetadataConverter(String column) {
        return CdcMetadataProcessor.converter(sourceType, column);
    }

    /** CDC source type. */
    public enum SourceType {
        MYSQL("MySQL Source", "MySQL-Paimon %s Sync: %s"),
        KAFKA("Kafka Source", "Kafka-Paimon %s Sync: %s"),
        MONGODB("MongoDB Source", "MongoDB-Paimon %s Sync: %s"),
        PULSAR("Pulsar Source", "Pulsar-Paimon %s Sync: %s"),
        POSTGRES("Postgres Source", "Postgres-Paimon %s Sync: %s");

        private final String sourceName;
        private final String defaultJobNameFormat;

        SourceType(String sourceName, String defaultJobNameFormat) {
            this.sourceName = sourceName;
            this.defaultJobNameFormat = defaultJobNameFormat;
        }
    }
}
