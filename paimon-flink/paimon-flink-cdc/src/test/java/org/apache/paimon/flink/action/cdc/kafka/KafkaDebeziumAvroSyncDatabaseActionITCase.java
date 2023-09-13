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

package org.apache.paimon.flink.action.cdc.kafka;

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** IT cases for {@link KafkaSyncDatabaseAction}. */
public class KafkaDebeziumAvroSyncDatabaseActionITCase extends KafkaActionITCaseBase {

    private static final String T1_KEY_SCHEMA_V1 =
            "{\"type\":\"record\",\"name\":\"Key\",\"namespace\":\"test_avro.workdb.t1\",\"fields\":[{\"name\":\"k\",\"type\":\"int\"}],\"connect.name\":\"test_avro.workdb.t1.Key\"}";
    private static final String T1_VALUE_SCHEMA_V1 =
            "{\"type\":\"record\",\"name\":\"Envelope\",\"namespace\":\"test_avro.workdb.t1\",\"fields\":[{\"name\":\"before\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Value\",\"fields\":[{\"name\":\"k\",\"type\":{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"INT\",\"__debezium.source.column.name\":\"k\"}}},{\"name\":\"v1\",\"type\":{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"VARCHAR\",\"__debezium.source.column.length\":\"10\",\"__debezium.source.column.name\":\"v1\"},\"connect.default\":\"1.23\"},\"default\":\"1.23\"}],\"connect.name\":\"test_avro.workdb.t1.Value\"}],\"default\":null},{\"name\":\"after\",\"type\":[\"null\",\"Value\"],\"default\":null},{\"name\":\"source\",\"type\":{\"type\":\"record\",\"name\":\"Source\",\"namespace\":\"io.debezium.connector.mysql\",\"fields\":[{\"name\":\"version\",\"type\":\"string\"},{\"name\":\"connector\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"ts_ms\",\"type\":\"long\"},{\"name\":\"snapshot\",\"type\":[{\"type\":\"string\",\"connect.version\":1,\"connect.parameters\":{\"allowed\":\"true,last,false,incremental\"},\"connect.default\":\"false\",\"connect.name\":\"io.debezium.data.Enum\"},\"null\"],\"default\":\"false\"},{\"name\":\"db\",\"type\":\"string\"},{\"name\":\"sequence\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"table\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"server_id\",\"type\":\"long\"},{\"name\":\"gtid\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"file\",\"type\":\"string\"},{\"name\":\"pos\",\"type\":\"long\"},{\"name\":\"row\",\"type\":\"int\"},{\"name\":\"thread\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"query\",\"type\":[\"null\",\"string\"],\"default\":null}],\"connect.name\":\"io.debezium.connector.mysql.Source\"}},{\"name\":\"op\",\"type\":\"string\"},{\"name\":\"ts_ms\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"transaction\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"block\",\"namespace\":\"event\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"total_order\",\"type\":\"long\"},{\"name\":\"data_collection_order\",\"type\":\"long\"}],\"connect.version\":1,\"connect.name\":\"event.block\"}],\"default\":null}],\"connect.version\":1,\"connect.name\":\"test_avro.workdb.t1.Envelope\"}";
    private static final String T1_VALUE_SCHEMA_V2 =
            "{\"type\":\"record\",\"name\":\"Envelope\",\"namespace\":\"test_avro.workdb.t1\",\"fields\":[{\"name\":\"before\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Value\",\"fields\":[{\"name\":\"k\",\"type\":{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"INT\",\"__debezium.source.column.name\":\"k\"}}},{\"name\":\"v1\",\"type\":{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"VARCHAR\",\"__debezium.source.column.length\":\"10\",\"__debezium.source.column.name\":\"v1\"},\"connect.default\":\"1.23\"},\"default\":\"1.23\"},{\"name\":\"v2\",\"type\":[\"null\",{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"INT\",\"__debezium.source.column.name\":\"v2\"}}],\"default\":null}],\"connect.name\":\"test_avro.workdb.t1.Value\"}],\"default\":null},{\"name\":\"after\",\"type\":[\"null\",\"Value\"],\"default\":null},{\"name\":\"source\",\"type\":{\"type\":\"record\",\"name\":\"Source\",\"namespace\":\"io.debezium.connector.mysql\",\"fields\":[{\"name\":\"version\",\"type\":\"string\"},{\"name\":\"connector\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"ts_ms\",\"type\":\"long\"},{\"name\":\"snapshot\",\"type\":[{\"type\":\"string\",\"connect.version\":1,\"connect.parameters\":{\"allowed\":\"true,last,false,incremental\"},\"connect.default\":\"false\",\"connect.name\":\"io.debezium.data.Enum\"},\"null\"],\"default\":\"false\"},{\"name\":\"db\",\"type\":\"string\"},{\"name\":\"sequence\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"table\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"server_id\",\"type\":\"long\"},{\"name\":\"gtid\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"file\",\"type\":\"string\"},{\"name\":\"pos\",\"type\":\"long\"},{\"name\":\"row\",\"type\":\"int\"},{\"name\":\"thread\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"query\",\"type\":[\"null\",\"string\"],\"default\":null}],\"connect.name\":\"io.debezium.connector.mysql.Source\"}},{\"name\":\"op\",\"type\":\"string\"},{\"name\":\"ts_ms\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"transaction\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"block\",\"namespace\":\"event\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"total_order\",\"type\":\"long\"},{\"name\":\"data_collection_order\",\"type\":\"long\"}],\"connect.version\":1,\"connect.name\":\"event.block\"}],\"default\":null}],\"connect.version\":1,\"connect.name\":\"test_avro.workdb.t1.Envelope\"}";
    private static final String T2_KEY_SCHEMA_V1 =
            "{\"type\":\"record\",\"name\":\"Key\",\"namespace\":\"test_avro.workdb.t2\",\"fields\":[{\"name\":\"k1\",\"type\":\"int\"},{\"name\":\"k2\",\"type\":\"string\"}],\"connect.name\":\"test_avro.workdb.t2.Key\"}";
    private static final String T2_VALUE_SCHEMA_V1 =
            "{\"type\":\"record\",\"name\":\"Envelope\",\"namespace\":\"test_avro.workdb.t2\",\"fields\":[{\"name\":\"before\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Value\",\"fields\":[{\"name\":\"k1\",\"type\":{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"INT\",\"__debezium.source.column.name\":\"k1\"}}},{\"name\":\"k2\",\"type\":{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"VARCHAR\",\"__debezium.source.column.length\":\"10\",\"__debezium.source.column.name\":\"k2\"}}},{\"name\":\"v1\",\"type\":[\"null\",{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"INT\",\"__debezium.source.column.name\":\"v1\"}}],\"default\":null},{\"name\":\"v2\",\"type\":[\"null\",{\"type\":\"long\",\"connect.parameters\":{\"__debezium.source.column.type\":\"BIGINT\",\"__debezium.source.column.name\":\"v2\"}}],\"default\":null}],\"connect.name\":\"test_avro.workdb.t2.Value\"}],\"default\":null},{\"name\":\"after\",\"type\":[\"null\",\"Value\"],\"default\":null},{\"name\":\"source\",\"type\":{\"type\":\"record\",\"name\":\"Source\",\"namespace\":\"io.debezium.connector.mysql\",\"fields\":[{\"name\":\"version\",\"type\":\"string\"},{\"name\":\"connector\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"ts_ms\",\"type\":\"long\"},{\"name\":\"snapshot\",\"type\":[{\"type\":\"string\",\"connect.version\":1,\"connect.parameters\":{\"allowed\":\"true,last,false,incremental\"},\"connect.default\":\"false\",\"connect.name\":\"io.debezium.data.Enum\"},\"null\"],\"default\":\"false\"},{\"name\":\"db\",\"type\":\"string\"},{\"name\":\"sequence\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"table\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"server_id\",\"type\":\"long\"},{\"name\":\"gtid\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"file\",\"type\":\"string\"},{\"name\":\"pos\",\"type\":\"long\"},{\"name\":\"row\",\"type\":\"int\"},{\"name\":\"thread\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"query\",\"type\":[\"null\",\"string\"],\"default\":null}],\"connect.name\":\"io.debezium.connector.mysql.Source\"}},{\"name\":\"op\",\"type\":\"string\"},{\"name\":\"ts_ms\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"transaction\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"block\",\"namespace\":\"event\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"total_order\",\"type\":\"long\"},{\"name\":\"data_collection_order\",\"type\":\"long\"}],\"connect.version\":1,\"connect.name\":\"event.block\"}],\"default\":null}],\"connect.version\":1,\"connect.name\":\"test_avro.workdb.t2.Envelope\"}";
    private static final String T2_VALUE_SCHEMA_V2 =
            "{\"type\":\"record\",\"name\":\"Envelope\",\"namespace\":\"test_avro.workdb.t2\",\"fields\":[{\"name\":\"before\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Value\",\"fields\":[{\"name\":\"k1\",\"type\":{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"INT\",\"__debezium.source.column.name\":\"k1\"}}},{\"name\":\"k2\",\"type\":{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"VARCHAR\",\"__debezium.source.column.length\":\"10\",\"__debezium.source.column.name\":\"k2\"}}},{\"name\":\"v1\",\"type\":[\"null\",{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"INT\",\"__debezium.source.column.name\":\"v1\"}}],\"default\":null},{\"name\":\"v2\",\"type\":[\"null\",{\"type\":\"long\",\"connect.parameters\":{\"__debezium.source.column.type\":\"BIGINT\",\"__debezium.source.column.name\":\"v2\"}}],\"default\":null},{\"name\":\"v3\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"VARCHAR\",\"__debezium.source.column.length\":\"10\",\"__debezium.source.column.name\":\"v3\"}}],\"default\":null}],\"connect.name\":\"test_avro.workdb.t2.Value\"}],\"default\":null},{\"name\":\"after\",\"type\":[\"null\",\"Value\"],\"default\":null},{\"name\":\"source\",\"type\":{\"type\":\"record\",\"name\":\"Source\",\"namespace\":\"io.debezium.connector.mysql\",\"fields\":[{\"name\":\"version\",\"type\":\"string\"},{\"name\":\"connector\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"ts_ms\",\"type\":\"long\"},{\"name\":\"snapshot\",\"type\":[{\"type\":\"string\",\"connect.version\":1,\"connect.parameters\":{\"allowed\":\"true,last,false,incremental\"},\"connect.default\":\"false\",\"connect.name\":\"io.debezium.data.Enum\"},\"null\"],\"default\":\"false\"},{\"name\":\"db\",\"type\":\"string\"},{\"name\":\"sequence\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"table\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"server_id\",\"type\":\"long\"},{\"name\":\"gtid\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"file\",\"type\":\"string\"},{\"name\":\"pos\",\"type\":\"long\"},{\"name\":\"row\",\"type\":\"int\"},{\"name\":\"thread\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"query\",\"type\":[\"null\",\"string\"],\"default\":null}],\"connect.name\":\"io.debezium.connector.mysql.Source\"}},{\"name\":\"op\",\"type\":\"string\"},{\"name\":\"ts_ms\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"transaction\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"block\",\"namespace\":\"event\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"total_order\",\"type\":\"long\"},{\"name\":\"data_collection_order\",\"type\":\"long\"}],\"connect.version\":1,\"connect.name\":\"event.block\"}],\"default\":null}],\"connect.version\":1,\"connect.name\":\"test_avro.workdb.t2.Envelope\"}";
    private static final String TOPIC_1 = "test_json.workdb.t1";
    private static final String TOPIC_2 = "test_json.workdb.t2";

    private Schema t1KeySchema;
    private Schema t1ValueSchema;
    private Schema t1ValueSchemaV2;
    private Schema t2KeySchema;
    private Schema t2ValueSchema;
    private Schema t2ValueSchemaV2;
    private Schema debeziumSourceSchema;

    @BeforeEach
    public void setup() {
        super.setup();
        // Set database name from debezium's source property
        database = "workdb";
        // Init kafka key/value schema
        Parser parser = new Parser();
        t1KeySchema = parser.parse(T1_KEY_SCHEMA_V1);
        t1ValueSchema = parser.parse(T1_VALUE_SCHEMA_V1);
        t1ValueSchemaV2 = new Parser().parse(T1_VALUE_SCHEMA_V2);
        parser = new Parser();
        t2KeySchema = parser.parse(T2_KEY_SCHEMA_V1);
        t2ValueSchema = parser.parse(T2_VALUE_SCHEMA_V1);
        t2ValueSchemaV2 = new Parser().parse(T2_VALUE_SCHEMA_V2);
        debeziumSourceSchema = t1ValueSchema.getField("source").schema();
    }

    @Test
    @Timeout(60)
    public void testSchemaEvolutionMultiTopic() throws Exception {
        List<String> topics = Arrays.asList(TOPIC_1, TOPIC_2);
        int fileCount = topics.size();
        topics.forEach(topic -> createTestTopic(topic, 1, 1));

        // ---------- Write the Debezium avro into Kafka -------------------

        for (int i = 0; i < fileCount; i++) {
            try {
                writeRecordsToKafka(
                        topics.get(i),
                        readLines(
                                "kafka/debezium/database/schemaevolution/topic"
                                        + i
                                        + "/debezium-data-1.txt"),
                        1);
            } catch (Exception e) {
                throw new Exception("Failed to write debezium avro data to Kafka.", e);
            }
        }

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "debezium-avro");
        kafkaConfig.put("topic", String.join(";", topics));
        KafkaSyncDatabaseAction action =
                syncDatabaseActionBuilder(kafkaConfig)
                        .withTableConfig(getBasicTableConfig())
                        .withSchemaRegistry(getSchemaRegistryUrl())
                        .build();
        runActionWithDefaultEnv(action);

        testSchemaEvolutionImpl(topics, fileCount);
    }

    private void testSchemaEvolutionImpl(List<String> topics, int fileCount) throws Exception {
        waitingTables("t1", "t2");

        FileStoreTable table1 = getFileStoreTable("t1");
        FileStoreTable table2 = getFileStoreTable("t2");

        RowType rowType1 =
                RowType.of(
                        new DataType[] {DataTypes.INT().notNull(), DataTypes.VARCHAR(10)},
                        new String[] {"k", "v1"});
        List<String> primaryKeys1 = Collections.singletonList("k");
        List<String> expected = Arrays.asList("+I[1, one]", "+I[3, three]");
        waitForResult(expected, table1, rowType1, primaryKeys1);

        RowType rowType2 =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(10).notNull(),
                            DataTypes.INT(),
                            DataTypes.BIGINT()
                        },
                        new String[] {"k1", "k2", "v1", "v2"});
        List<String> primaryKeys2 = Arrays.asList("k1", "k2");
        expected = Arrays.asList("+I[2, two, 20, 200]", "+I[4, four, 40, 400]");
        waitForResult(expected, table2, rowType2, primaryKeys2);

        // Adding a new column
        for (int i = 0; i < fileCount; i++) {
            try {
                writeRecordsToKafka(
                        topics.get(i),
                        readLines(
                                "kafka/debezium/database/schemaevolution/topic"
                                        + i
                                        + "/debezium-data-2.txt"),
                        2);
            } catch (Exception e) {
                throw new Exception("Failed to write debezium avro data to Kafka.", e);
            }
        }

        rowType1 =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(), DataTypes.VARCHAR(10), DataTypes.INT()
                        },
                        new String[] {"k", "v1", "v2"});
        expected =
                Arrays.asList(
                        "+I[1, one, NULL]",
                        "+I[3, three, NULL]",
                        "+I[5, five, 50]",
                        "+I[7, seven, 70]");
        waitForResult(expected, table1, rowType1, primaryKeys1);

        rowType2 =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(10).notNull(),
                            DataTypes.INT(),
                            DataTypes.BIGINT(),
                            DataTypes.VARCHAR(10)
                        },
                        new String[] {"k1", "k2", "v1", "v2", "v3"});
        expected =
                Arrays.asList(
                        "+I[2, two, 20, 200, NULL]",
                        "+I[4, four, 40, 400, NULL]",
                        "+I[6, six, 60, 600, string_6]",
                        "+I[8, eight, 80, 800, string_8]");
        waitForResult(expected, table2, rowType2, primaryKeys2);
    }

    private void writeRecordsToKafka(String topic, List<String> lines, int fileIndex)
            throws Exception {
        Properties producerProperties = getStandardProps();
        producerProperties.setProperty("retries", "0");
        producerProperties.put("key.serializer", ByteArraySerializer.class.getName());
        producerProperties.put("value.serializer", ByteArraySerializer.class.getName());

        try (KafkaProducer<byte[], byte[]> kafkaProducer =
                new KafkaProducer<>(producerProperties)) {
            lines = filterOutLicenseLine(lines);
            for (int i = 0; i < lines.size(); i += 2) {
                JsonNode key = objectMapper.readTree(lines.get(i));
                JsonNode value = objectMapper.readTree(lines.get(i + 1));
                JsonNode keyPayload = key.get("payload");
                JsonNode valuePayload = value.get("payload");
                JsonNode source = valuePayload.get("source");
                JsonNode after = valuePayload.get("after");

                GenericRecord avroKey;
                GenericRecord avroValue;
                GenericRecord afterAvroValue;

                if (topic.equalsIgnoreCase(TOPIC_1)) {
                    avroKey = new GenericData.Record(t1KeySchema);
                    avroKey.put("k", keyPayload.get("k").asInt());

                    if (fileIndex == 1) {
                        avroValue = new GenericData.Record(t1ValueSchema);
                        afterAvroValue =
                                new GenericData.Record(
                                        sanitizedSchema(t1ValueSchema.getField("before").schema()));
                        afterAvroValue.put("k", after.get("k").asInt());
                        afterAvroValue.put("v1", after.get("v1").asText());
                    } else {
                        avroValue = new GenericData.Record(t1ValueSchemaV2);
                        afterAvroValue =
                                new GenericData.Record(
                                        sanitizedSchema(
                                                t1ValueSchemaV2.getField("before").schema()));
                        afterAvroValue.put("k", after.get("k").asInt());
                        afterAvroValue.put("v1", after.get("v1").asText());
                        afterAvroValue.put("v2", after.get("v2").asInt());
                    }
                    avroValue.put("after", afterAvroValue);
                } else {
                    avroKey = new GenericData.Record(t2KeySchema);
                    avroKey.put("k1", keyPayload.get("k1").asInt());
                    avroKey.put("k2", keyPayload.get("k2").asText());

                    if (fileIndex == 1) {
                        avroValue = new GenericData.Record(t2ValueSchema);
                        afterAvroValue =
                                new GenericData.Record(
                                        sanitizedSchema(t2ValueSchema.getField("before").schema()));
                        afterAvroValue.put("k1", after.get("k1").asInt());
                        afterAvroValue.put("k2", after.get("k2").asText());
                        afterAvroValue.put("v1", after.get("v1").asInt());
                        afterAvroValue.put("v2", after.get("v2").asLong());
                    } else {
                        avroValue = new GenericData.Record(t2ValueSchemaV2);
                        afterAvroValue =
                                new GenericData.Record(
                                        sanitizedSchema(
                                                t2ValueSchemaV2.getField("before").schema()));
                        afterAvroValue.put("k1", after.get("k1").asInt());
                        afterAvroValue.put("k2", after.get("k2").asText());
                        afterAvroValue.put("v1", after.get("v1").asInt());
                        afterAvroValue.put("v2", after.get("v2").asLong());
                        afterAvroValue.put("v3", after.get("v3").asText());
                    }
                    avroValue.put("after", afterAvroValue);
                }
                // Common properties
                avroValue.put("source", buildDebeziumSourceProperty(debeziumSourceSchema, source));
                avroValue.put("op", valuePayload.get("op").asText());
                avroValue.put("ts_ms", valuePayload.get("ts_ms").asLong());

                // Write to kafka
                kafkaProducer.send(
                        new ProducerRecord<>(
                                topic,
                                kafkaKeyAvroSerializer.serialize(topic, avroKey),
                                kafkaValueAvroSerializer.serialize(topic, avroValue)));
            }
        }
    }
}
