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
import org.apache.paimon.testutils.assertj.AssertionUtils;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for {@link KafkaSyncDatabaseAction}. */
public class KafkaDebeziumSyncDatabaseActionITCase extends KafkaActionITCaseBase {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @Timeout(120)
    public void testSchemaEvolutionMultiTopic() throws Exception {
        final String topic1 = "schema_evolution_0";
        final String topic2 = "schema_evolution_1";
        boolean writeOne = false;
        int fileCount = 2;
        List<String> topics = Arrays.asList(topic1, topic2);
        topics.forEach(topic -> createTestTopic(topic, 1, 1));

        // ---------- Write the debezium json into Kafka -------------------

        for (int i = 0; i < fileCount; i++) {
            try {
                writeRecordsToKafka(
                        topics.get(i),
                        readLines(
                                "kafka/debezium/database/schemaevolution/topic"
                                        + i
                                        + "/debezium-data-1.txt"));
            } catch (Exception e) {
                throw new Exception("Failed to write debezium data to Kafka.", e);
            }
        }

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "debezium-json");
        kafkaConfig.put("topic", String.join(";", topics));
        KafkaSyncDatabaseAction action =
                syncDatabaseActionBuilder(kafkaConfig)
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        testSchemaEvolutionImpl(topics, writeOne, fileCount);
    }

    @Test
    @Timeout(120)
    public void testSchemaEvolutionOneTopic() throws Exception {
        final String topic = "schema_evolution";
        boolean writeOne = true;
        int fileCount = 2;
        List<String> topics = Collections.singletonList(topic);
        topics.forEach(t -> createTestTopic(t, 1, 1));

        // ---------- Write the debezium json into Kafka -------------------

        for (int i = 0; i < fileCount; i++) {
            try {
                writeRecordsToKafka(
                        topics.get(0),
                        readLines(
                                "kafka/debezium/database/schemaevolution/topic"
                                        + i
                                        + "/debezium-data-1.txt"));
            } catch (Exception e) {
                throw new Exception("Failed to write debezium data to Kafka.", e);
            }
        }

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "debezium-json");
        kafkaConfig.put("topic", String.join(";", topics));
        KafkaSyncDatabaseAction action =
                syncDatabaseActionBuilder(kafkaConfig)
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        testSchemaEvolutionImpl(topics, writeOne, fileCount);
    }

    private void testSchemaEvolutionImpl(List<String> topics, boolean writeOne, int fileCount)
            throws Exception {
        waitingTables("t1", "t2");

        FileStoreTable table1 = getFileStoreTable("t1");
        FileStoreTable table2 = getFileStoreTable("t2");

        RowType rowType1 =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"id", "name", "description", "weight"});
        List<String> primaryKeys1 = Collections.singletonList("id");
        List<String> expected =
                Arrays.asList(
                        "+I[101, scooter, Small 2-wheel scooter, 3.14]",
                        "+I[102, car battery, 12V car battery, 8.1]");
        waitForResult(expected, table1, rowType1, primaryKeys1);

        RowType rowType2 =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"id", "name", "description", "weight"});
        List<String> primaryKeys2 = Collections.singletonList("id");
        List<String> expected2 =
                Arrays.asList(
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.75]");
        waitForResult(expected2, table2, rowType2, primaryKeys2);

        for (int i = 0; i < fileCount; i++) {
            try {
                writeRecordsToKafka(
                        writeOne ? topics.get(0) : topics.get(i),
                        readLines(
                                "kafka/debezium/database/schemaevolution/topic"
                                        + i
                                        + "/debezium-data-2.txt"));
            } catch (Exception e) {
                throw new Exception("Failed to write debezium data to Kafka.", e);
            }
        }

        rowType1 =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"id", "name", "description", "weight", "age"});
        expected =
                Arrays.asList(
                        "+I[101, scooter, Small 2-wheel scooter, 3.14, NULL]",
                        "+I[102, car battery, 12V car battery, 8.1, NULL]",
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, 19]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.75, 25]");
        waitForResult(expected, table1, rowType1, primaryKeys1);

        rowType2 =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"id", "name", "description", "weight", "address"});
        expected =
                Arrays.asList(
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, Beijing]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.75, Shanghai]");
        waitForResult(expected, table2, rowType2, primaryKeys2);
    }

    @Test
    public void testTopicIsEmpty() {
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "debezium-json");

        KafkaSyncDatabaseAction action = syncDatabaseActionBuilder(kafkaConfig).build();

        assertThatThrownBy(action::run)
                .satisfies(
                        AssertionUtils.anyCauseMatches(
                                IllegalArgumentException.class,
                                "kafka-conf [topic] must be specified."));
    }

    @Test
    @Timeout(120)
    public void testTableAffixMultiTopic() throws Exception {
        // create table t1
        createFileStoreTable(
                "test_prefix_t1_test_suffix",
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"id", "name", "description", "weight"}),
                Collections.emptyList(),
                Collections.singletonList("id"),
                Collections.emptyMap());

        final String topic1 = "prefix_suffix_0";
        final String topic2 = "prefix_suffix_1";
        boolean writeOne = false;
        int fileCount = 2;
        List<String> topics = Arrays.asList(topic1, topic2);
        topics.forEach(topic -> createTestTopic(topic, 1, 1));

        // ---------- Write the debezium json into Kafka -------------------

        for (int i = 0; i < topics.size(); i++) {
            try {
                writeRecordsToKafka(
                        topics.get(i),
                        readLines(
                                "kafka/debezium/database/prefixsuffix/topic"
                                        + i
                                        + "/debezium-data-1.txt"));
            } catch (Exception e) {
                throw new Exception("Failed to write debezium data to Kafka.", e);
            }
        }

        // try synchronization
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "debezium-json");
        kafkaConfig.put("topic", String.join(";", topics));
        KafkaSyncDatabaseAction action =
                syncDatabaseActionBuilder(kafkaConfig)
                        .withTablePrefix("test_prefix_")
                        .withTableSuffix("_test_suffix")
                        .withTableConfig(getBasicTableConfig())
                        // test including check with affix
                        .includingTables(ThreadLocalRandom.current().nextBoolean() ? "t1|t2" : ".*")
                        .build();
        runActionWithDefaultEnv(action);

        testTableAffixImpl(topics, writeOne, fileCount);
    }

    @Test
    @Timeout(120)
    public void testTableAffixOneTopic() throws Exception {
        // create table t1
        createFileStoreTable(
                "test_prefix_t1_test_suffix",
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"id", "name", "description", "weight"}),
                Collections.emptyList(),
                Collections.singletonList("id"),
                Collections.emptyMap());

        final String topic1 = "prefix_suffix";
        List<String> topics = Collections.singletonList(topic1);
        boolean writeOne = true;
        int fileCount = 2;
        topics.forEach(topic -> createTestTopic(topic, 1, 1));

        // ---------- Write the debezium json into Kafka -------------------

        for (int i = 0; i < fileCount; i++) {
            try {
                writeRecordsToKafka(
                        topics.get(0),
                        readLines(
                                "kafka/debezium/database/prefixsuffix/topic"
                                        + i
                                        + "/debezium-data-1.txt"));
            } catch (Exception e) {
                throw new Exception("Failed to write debezium data to Kafka.", e);
            }
        }

        // try synchronization
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "debezium-json");
        kafkaConfig.put("topic", String.join(";", topics));
        KafkaSyncDatabaseAction action =
                syncDatabaseActionBuilder(kafkaConfig)
                        .withTablePrefix("test_prefix_")
                        .withTableSuffix("_test_suffix")
                        .withTableConfig(getBasicTableConfig())
                        // test including check with affix
                        .includingTables(ThreadLocalRandom.current().nextBoolean() ? "t1|t2" : ".*")
                        .build();
        runActionWithDefaultEnv(action);

        testTableAffixImpl(topics, writeOne, fileCount);
    }

    private void testTableAffixImpl(List<String> topics, boolean writeOne, int fileCount)
            throws Exception {
        waitingTables("test_prefix_t1_test_suffix", "test_prefix_t2_test_suffix");

        FileStoreTable table1 = getFileStoreTable("test_prefix_t1_test_suffix");
        FileStoreTable table2 = getFileStoreTable("test_prefix_t2_test_suffix");

        RowType rowType1 =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"id", "name", "description", "weight"});
        List<String> primaryKeys1 = Collections.singletonList("id");
        List<String> expected =
                Arrays.asList(
                        "+I[101, scooter, Small 2-wheel scooter, 3.14]",
                        "+I[102, car battery, 12V car battery, 8.1]");
        waitForResult(expected, table1, rowType1, primaryKeys1);

        RowType rowType2 =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"id", "name", "description", "weight"});
        List<String> primaryKeys2 = Collections.singletonList("id");
        expected =
                Arrays.asList(
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.75]");
        waitForResult(expected, table2, rowType2, primaryKeys2);

        for (int i = 0; i < fileCount; i++) {
            try {
                writeRecordsToKafka(
                        writeOne ? topics.get(0) : topics.get(i),
                        readLines(
                                "kafka/debezium/database/prefixsuffix/topic"
                                        + i
                                        + "/debezium-data-2.txt"));
            } catch (Exception e) {
                throw new Exception("Failed to write debezium data to Kafka.", e);
            }
        }
        rowType1 =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"id", "name", "description", "weight", "address"});
        expected =
                Arrays.asList(
                        "+I[101, scooter, Small 2-wheel scooter, 3.14, Beijing]",
                        "+I[102, car battery, 12V car battery, 8.1, Shanghai]");
        waitForResult(expected, table1, rowType1, primaryKeys1);

        rowType2 =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"id", "name", "description", "weight", "age"});
        expected =
                Arrays.asList(
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, 19]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.75, 25]");
        waitForResult(expected, table2, rowType2, primaryKeys2);
    }

    @Test
    @Timeout(60)
    public void testIncludingTables() throws Exception {
        includingAndExcludingTablesImpl(
                "flink|paimon.+",
                null,
                Arrays.asList("flink", "paimon_1", "paimon_2"),
                Collections.singletonList("ignore"));
    }

    @Test
    @Timeout(60)
    public void testExcludingTables() throws Exception {
        includingAndExcludingTablesImpl(
                null,
                "flink|paimon.+",
                Collections.singletonList("ignore"),
                Arrays.asList("flink", "paimon_1", "paimon_2"));
    }

    @Test
    @Timeout(120)
    public void testIncludingAndExcludingTables() throws Exception {
        includingAndExcludingTablesImpl(
                "flink|paimon.+",
                "paimon_1",
                Arrays.asList("flink", "paimon_2"),
                Arrays.asList("paimon_1", "ignore"));
    }

    private void includingAndExcludingTablesImpl(
            @Nullable String includingTables,
            @Nullable String excludingTables,
            List<String> existedTables,
            List<String> notExistedTables)
            throws Exception {
        final String topic1 = "include_exclude" + UUID.randomUUID();
        List<String> topics = Collections.singletonList(topic1);
        topics.forEach(topic -> createTestTopic(topic, 1, 1));

        // ---------- Write the debezium json into Kafka -------------------
        try {
            writeRecordsToKafka(
                    topics.get(0),
                    readLines("kafka/debezium/database/include/topic0/debezium-data-1.txt"));
        } catch (Exception e) {
            throw new Exception("Failed to write debezium data to Kafka.", e);
        }
        // try synchronization
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "debezium-json");
        kafkaConfig.put("topic", String.join(";", topics));
        KafkaSyncDatabaseAction action =
                syncDatabaseActionBuilder(kafkaConfig)
                        .includingTables(includingTables)
                        .excludingTables(excludingTables)
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        // check paimon tables
        waitingTables(existedTables);
        assertTableNotExists(notExistedTables);
    }

    @Override
    protected void writeRecordsToKafka(String topic, List<String> lines) throws Exception {
        Properties producerProperties = getStandardProps();
        producerProperties.setProperty("retries", "0");
        producerProperties.put(
                "key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put(
                "value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer kafkaProducer = new KafkaProducer(producerProperties);
        for (int i = 0; i < lines.size(); i++) {
            try {
                String[] keyValue = lines.get(i).split(";");
                if (keyValue.length < 2) {
                    continue;
                }
                objectMapper.readTree(keyValue[0]);
                objectMapper.readTree(keyValue[1]);
                kafkaProducer.send(new ProducerRecord<>(topic, keyValue[0], keyValue[1]));
            } catch (Exception e) {
                // ignore
            }
        }
        kafkaProducer.close();
    }
}
