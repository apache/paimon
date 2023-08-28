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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.testutils.assertj.AssertionUtils;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for {@link KafkaSyncDatabaseAction}. */
public class KafkaOggSyncDatabaseActionITCase extends KafkaActionITCaseBase {

    @Test
    @Timeout(60)
    public void testSchemaEvolutionMultiTopic() throws Exception {

        final String topic1 = "schema_evolution_0";
        final String topic2 = "schema_evolution_1";
        boolean writeOne = false;
        int fileCount = 2;
        List<String> topics = Arrays.asList(topic1, topic2);
        topics.forEach(
                topic -> {
                    createTestTopic(topic, 1, 1);
                });

        // ---------- Write the ogg json into Kafka -------------------

        for (int i = 0; i < fileCount; i++) {
            try {
                writeRecordsToKafka(
                        topics.get(i),
                        readLines(
                                "kafka/ogg/database/schemaevolution/topic"
                                        + i
                                        + "/ogg-data-1.txt"));
            } catch (Exception e) {
                throw new Exception("Failed to write ogg data to Kafka.", e);
            }
        }

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "ogg-json");
        kafkaConfig.put("topic", String.join(";", topics));
        KafkaSyncDatabaseAction action =
                new KafkaSyncDatabaseAction(
                        kafkaConfig,
                        warehouse,
                        database,
                        Collections.emptyMap(),
                        getBasicTableConfig());
        action.build(env);
        JobClient client = env.executeAsync();
        waitJobRunning(client);

        testSchemaEvolutionImpl(topics, writeOne, fileCount);
    }

    @Test
    @Timeout(60)
    public void testSchemaEvolutionOneTopic() throws Exception {

        final String topic = "schema_evolution";
        boolean writeOne = true;
        int fileCount = 2;
        List<String> topics = Collections.singletonList(topic);
        topics.forEach(
                t -> {
                    createTestTopic(t, 1, 1);
                });

        // ---------- Write the ogg json into Kafka -------------------

        for (int i = 0; i < fileCount; i++) {
            try {
                writeRecordsToKafka(
                        topics.get(0),
                        readLines(
                                "kafka/ogg/database/schemaevolution/topic"
                                        + i
                                        + "/ogg-data-1.txt"));
            } catch (Exception e) {
                throw new Exception("Failed to write ogg data to Kafka.", e);
            }
        }

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "ogg-json");
        kafkaConfig.put("topic", String.join(";", topics));
        KafkaSyncDatabaseAction action =
                new KafkaSyncDatabaseAction(
                        kafkaConfig,
                        warehouse,
                        database,
                        Collections.emptyMap(),
                        getBasicTableConfig());
        action.build(env);
        JobClient client = env.executeAsync();
        waitJobRunning(client);

        testSchemaEvolutionImpl(topics, writeOne, fileCount);
    }

    private void testSchemaEvolutionImpl(List<String> topics, boolean writeOne, int fileCount)
            throws Exception {
        waitTablesCreated("T1", "T2");

        FileStoreTable table1 = getFileStoreTable("T1");
        FileStoreTable table2 = getFileStoreTable("T2");

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
                        "+I[101, scooter, Small 2-wheel scooter, 3.140000104904175]",
                        "+I[102, car battery, 12V car battery, 8.100000381469727]");
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
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.800000011920929]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.75]");
        waitForResult(expected2, table2, rowType2, primaryKeys2);

        for (int i = 0; i < fileCount; i++) {
            try {
                writeRecordsToKafka(
                        writeOne ? topics.get(0) : topics.get(i),
                        readLines(
                                "kafka/ogg/database/schemaevolution/topic"
                                        + i
                                        + "/ogg-data-2.txt"));
            } catch (Exception e) {
                throw new Exception("Failed to write ogg data to Kafka.", e);
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
                        "+I[101, scooter, Small 2-wheel scooter, 3.140000104904175, NULL]",
                        "+I[102, car battery, 12V car battery, 8.100000381469727, NULL]",
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.800000011920929, 19]",
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
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.800000011920929, Beijing]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.75, Shanghai]");
        waitForResult(expected, table2, rowType2, primaryKeys2);
    }

    @Test
    public void testTopicIsEmpty() {

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "ogg-json");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSyncDatabaseAction action =
                new KafkaSyncDatabaseAction(
                        kafkaConfig,
                        warehouse,
                        database,
                        Collections.emptyMap(),
                        Collections.emptyMap());

        assertThatThrownBy(() -> action.build(env))
                .satisfies(
                        AssertionUtils.anyCauseMatches(
                                IllegalArgumentException.class,
                                "kafka-conf [topic] must be specified."));
    }

    @Test
    @Timeout(60)
    public void testTableAffixMultiTopic() throws Exception {
        // create table t1
        Catalog catalog = catalog();
        catalog.createDatabase(database, true);
        Identifier identifier = Identifier.create(database, "TEST_PREFIX_T1_TEST_SUFFIX");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.STRING().notNull())
                        .column("name", DataTypes.STRING())
                        .column("description", DataTypes.STRING())
                        .column("weight", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        catalog.createTable(identifier, schema, false);

        final String topic1 = "prefix_suffix_0";
        final String topic2 = "prefix_suffix_1";
        boolean writeOne = false;
        int fileCount = 2;
        List<String> topics = Arrays.asList(topic1, topic2);
        topics.forEach(
                topic -> {
                    createTestTopic(topic, 1, 1);
                });

        // ---------- Write the ogg json into Kafka -------------------

        for (int i = 0; i < topics.size(); i++) {
            try {
                writeRecordsToKafka(
                        topics.get(i),
                        readLines("kafka/ogg/database/prefixsuffix/topic" + i + "/ogg-data-1.txt"));
            } catch (Exception e) {
                throw new Exception("Failed to write ogg data to Kafka.", e);
            }
        }

        // try synchronization
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "ogg-json");
        kafkaConfig.put("topic", String.join(";", topics));
        KafkaSyncDatabaseAction action =
                new KafkaSyncDatabaseAction(
                        kafkaConfig,
                        warehouse,
                        database,
                        "TEST_PREFIX_",
                        "_TEST_SUFFIX",
                        null,
                        null,
                        Collections.emptyMap(),
                        getBasicTableConfig());
        action.build(env);
        JobClient client = env.executeAsync();
        waitJobRunning(client);

        testTableAffixImpl(topics, writeOne, fileCount);
    }

    @Test
    @Timeout(60)
    public void testTableAffixOneTopic() throws Exception {
        // create table t1
        Catalog catalog = catalog();
        catalog.createDatabase(database, true);
        Identifier identifier = Identifier.create(database, "TEST_PREFIX_T1_TEST_SUFFIX");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.STRING().notNull())
                        .column("name", DataTypes.STRING())
                        .column("description", DataTypes.STRING())
                        .column("weight", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        catalog.createTable(identifier, schema, false);

        final String topic1 = "prefix_suffix";
        List<String> topics = Collections.singletonList(topic1);
        boolean writeOne = true;
        int fileCount = 2;
        topics.forEach(
                topic -> {
                    createTestTopic(topic, 1, 1);
                });

        // ---------- Write the ogg json into Kafka -------------------

        for (int i = 0; i < fileCount; i++) {
            try {
                writeRecordsToKafka(
                        topics.get(0),
                        readLines("kafka/ogg/database/prefixsuffix/topic" + i + "/ogg-data-1.txt"));
            } catch (Exception e) {
                throw new Exception("Failed to write ogg data to Kafka.", e);
            }
        }

        // try synchronization
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "ogg-json");
        kafkaConfig.put("topic", String.join(";", topics));
        KafkaSyncDatabaseAction action =
                new KafkaSyncDatabaseAction(
                        kafkaConfig,
                        warehouse,
                        database,
                        "TEST_PREFIX_",
                        "_TEST_SUFFIX",
                        null,
                        null,
                        Collections.emptyMap(),
                        getBasicTableConfig());
        action.build(env);
        JobClient client = env.executeAsync();
        waitJobRunning(client);

        testTableAffixImpl(topics, writeOne, fileCount);
    }

    private void testTableAffixImpl(List<String> topics, boolean writeOne, int fileCount)
            throws Exception {
        waitTablesCreated("TEST_PREFIX_T1_TEST_SUFFIX", "TEST_PREFIX_T2_TEST_SUFFIX");

        FileStoreTable table1 = getFileStoreTable("TEST_PREFIX_T1_TEST_SUFFIX");
        FileStoreTable table2 = getFileStoreTable("TEST_PREFIX_T2_TEST_SUFFIX");

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
                        "+I[101, scooter, Small 2-wheel scooter, 3.140000104904175]",
                        "+I[102, car battery, 12V car battery, 8.100000381469727]");
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
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.800000011920929]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.75]");
        waitForResult(expected, table2, rowType2, primaryKeys2);

        for (int i = 0; i < fileCount; i++) {
            try {
                writeRecordsToKafka(
                        writeOne ? topics.get(0) : topics.get(i),
                        readLines("kafka/ogg/database/prefixsuffix/topic" + i + "/ogg-data-2.txt"));
            } catch (Exception e) {
                throw new Exception("Failed to write ogg data to Kafka.", e);
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
                        "+I[101, scooter, Small 2-wheel scooter, 3.140000104904175, Beijing]",
                        "+I[102, car battery, 12V car battery, 8.100000381469727, Shanghai]");
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
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.800000011920929, 19]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.75, 25]");
        waitForResult(expected, table2, rowType2, primaryKeys2);
    }

    @Test
    @Timeout(60)
    public void testIncludingTables() throws Exception {
        includingAndExcludingTablesImpl(
                "FLINK|PAIMON.+",
                null,
                Arrays.asList("FLINK", "PAIMON_1", "PAIMON_2"),
                Collections.singletonList("IGNORE"));
    }

    @Test
    @Timeout(60)
    public void testExcludingTables() throws Exception {
        includingAndExcludingTablesImpl(
                null,
                "FLINK|PAIMON.+",
                Collections.singletonList("IGNORE"),
                Arrays.asList("FLINK", "PAIMON_1", "PAIMON_2"));
    }

    @Test
    @Timeout(60)
    public void testIncludingAndExcludingTables() throws Exception {
        includingAndExcludingTablesImpl(
                "FLINK|PAIMON.+",
                "PAIMON_1",
                Arrays.asList("FLINK", "PAIMON_2"),
                Arrays.asList("PAIMON_1", "IGNORE"));
    }

    private void includingAndExcludingTablesImpl(
            @Nullable String includingTables,
            @Nullable String excludingTables,
            List<String> existedTables,
            List<String> notExistedTables)
            throws Exception {
        final String topic1 = "include_exclude" + UUID.randomUUID();
        List<String> topics = Collections.singletonList(topic1);
        topics.forEach(
                topic -> {
                    createTestTopic(topic, 1, 1);
                });

        // ---------- Write the ogg json into Kafka -------------------

        try {
            writeRecordsToKafka(
                    topics.get(0), readLines("kafka/ogg/database/include/topic0/ogg-data-1.txt"));
        } catch (Exception e) {
            throw new Exception("Failed to write ogg data to Kafka.", e);
        }
        // try synchronization
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "ogg-json");
        kafkaConfig.put("topic", String.join(";", topics));
        KafkaSyncDatabaseAction action =
                new KafkaSyncDatabaseAction(
                        kafkaConfig,
                        warehouse,
                        database,
                        null,
                        null,
                        includingTables,
                        excludingTables,
                        Collections.emptyMap(),
                        getBasicTableConfig());
        action.build(env);
        JobClient client = env.executeAsync();
        waitJobRunning(client);

        // check paimon tables
        waitTablesCreated(existedTables.toArray(new String[0]));
        assertTableNotExists(notExistedTables);
    }

    private void assertTableNotExists(List<String> tableNames) {
        Catalog catalog = catalog();
        for (String tableName : tableNames) {
            Identifier identifier = Identifier.create(database, tableName);
            assertThat(catalog.tableExists(identifier)).isFalse();
        }
    }
}
