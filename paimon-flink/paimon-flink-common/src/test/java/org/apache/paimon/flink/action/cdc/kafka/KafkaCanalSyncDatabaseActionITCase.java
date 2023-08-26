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
public class KafkaCanalSyncDatabaseActionITCase extends KafkaActionITCaseBase {

    @Test
    @Timeout(60)
    public void testSchemaEvolutionMultiTopic() throws Exception {

        final String topic1 = "schema_evolution_0";
        final String topic2 = "schema_evolution_1";
        final String topic3 = "schema_evolution_2";
        boolean writeOne = false;
        int fileCount = 3;
        List<String> topics = Arrays.asList(topic1, topic2, topic3);
        topics.forEach(
                topic -> {
                    createTestTopic(topic, 1, 1);
                });

        // ---------- Write the Canal json into Kafka -------------------

        for (int i = 0; i < fileCount; i++) {
            try {
                writeRecordsToKafka(
                        topics.get(i),
                        readLines(
                                "kafka/canal/database/schemaevolution/topic"
                                        + i
                                        + "/canal-data-1.txt"));
            } catch (Exception e) {
                throw new Exception("Failed to write canal data to Kafka.", e);
            }
        }

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "canal-json");
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
        int fileCount = 3;
        List<String> topics = Collections.singletonList(topic);
        topics.forEach(
                t -> {
                    createTestTopic(t, 1, 1);
                });

        // ---------- Write the Canal json into Kafka -------------------

        for (int i = 0; i < fileCount; i++) {
            try {
                writeRecordsToKafka(
                        topics.get(0),
                        readLines(
                                "kafka/canal/database/schemaevolution/topic"
                                        + i
                                        + "/canal-data-1.txt"));
            } catch (Exception e) {
                throw new Exception("Failed to write canal data to Kafka.", e);
            }
        }

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "canal-json");
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
        waitTablesCreated("t1", "t2");

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

        for (int i = 0; i < fileCount; i++) {
            try {
                writeRecordsToKafka(
                        writeOne ? topics.get(0) : topics.get(i),
                        readLines(
                                "kafka/canal/database/schemaevolution/topic"
                                        + i
                                        + "/canal-data-2.txt"));
            } catch (Exception e) {
                throw new Exception("Failed to write canal data to Kafka.", e);
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

        for (int i = 0; i < fileCount; i++) {
            try {
                writeRecordsToKafka(
                        writeOne ? topics.get(0) : topics.get(i),
                        readLines(
                                "kafka/canal/database/schemaevolution/topic"
                                        + i
                                        + "/canal-data-3.txt"));
            } catch (Exception e) {
                throw new Exception("Failed to write canal data to Kafka.", e);
            }
        }

        rowType1 =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(), DataTypes.VARCHAR(10), DataTypes.BIGINT()
                        },
                        new String[] {"k", "v1", "v2"});
        expected =
                Arrays.asList(
                        "+I[1, one, NULL]",
                        "+I[3, three, NULL]",
                        "+I[5, five, 50]",
                        "+I[7, seven, 70]",
                        "+I[9, nine, 9000000000000]");
        waitForResult(expected, table1, rowType1, primaryKeys1);

        rowType2 =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(10).notNull(),
                            DataTypes.INT(),
                            DataTypes.BIGINT(),
                            DataTypes.VARCHAR(20)
                        },
                        new String[] {"k1", "k2", "v1", "v2", "v3"});
        expected =
                Arrays.asList(
                        "+I[2, two, 20, 200, NULL]",
                        "+I[4, four, 40, 400, NULL]",
                        "+I[6, six, 60, 600, string_6]",
                        "+I[8, eight, 80, 800, string_8]",
                        "+I[10, ten, 100, 1000, long_long_string_10]");
        waitForResult(expected, table2, rowType2, primaryKeys2);
    }

    @Test
    public void testTopicIsEmpty() {
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "canal-json");

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
        Identifier identifier = Identifier.create(database, "test_prefix_t1_test_suffix");
        Schema schema =
                Schema.newBuilder()
                        .column("k1", DataTypes.INT().notNull())
                        .column("v0", DataTypes.VARCHAR(10))
                        .primaryKey("k1")
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

        // ---------- Write the Canal json into Kafka -------------------

        for (int i = 0; i < topics.size(); i++) {
            try {
                writeRecordsToKafka(
                        topics.get(i),
                        readLines(
                                "kafka/canal/database/prefixsuffix/topic"
                                        + i
                                        + "/canal-data-1.txt"));
            } catch (Exception e) {
                throw new Exception("Failed to write canal data to Kafka.", e);
            }
        }

        // try synchronization
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "canal-json");
        kafkaConfig.put("topic", String.join(";", topics));
        KafkaSyncDatabaseAction action =
                new KafkaSyncDatabaseAction(
                        kafkaConfig,
                        warehouse,
                        database,
                        "test_prefix_",
                        "_test_suffix",
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
        Identifier identifier = Identifier.create(database, "test_prefix_t1_test_suffix");
        Schema schema =
                Schema.newBuilder()
                        .column("k1", DataTypes.INT().notNull())
                        .column("v0", DataTypes.VARCHAR(10))
                        .primaryKey("k1")
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

        // ---------- Write the Canal json into Kafka -------------------

        for (int i = 0; i < fileCount; i++) {
            try {
                writeRecordsToKafka(
                        topics.get(0),
                        readLines(
                                "kafka/canal/database/prefixsuffix/topic"
                                        + i
                                        + "/canal-data-1.txt"));
            } catch (Exception e) {
                throw new Exception("Failed to write canal data to Kafka.", e);
            }
        }

        // try synchronization
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "canal-json");
        kafkaConfig.put("topic", String.join(";", topics));
        KafkaSyncDatabaseAction action =
                new KafkaSyncDatabaseAction(
                        kafkaConfig,
                        warehouse,
                        database,
                        "test_prefix_",
                        "_test_suffix",
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
        waitTablesCreated("test_prefix_t1_test_suffix", "test_prefix_t2_test_suffix");

        FileStoreTable table1 = getFileStoreTable("test_prefix_t1_test_suffix");
        FileStoreTable table2 = getFileStoreTable("test_prefix_t2_test_suffix");

        RowType rowType1 =
                RowType.of(
                        new DataType[] {DataTypes.INT().notNull(), DataTypes.VARCHAR(10)},
                        new String[] {"k1", "v0"});
        List<String> primaryKeys1 = Collections.singletonList("k1");
        List<String> expected = Arrays.asList("+I[1, one]", "+I[3, three]");
        waitForResult(expected, table1, rowType1, primaryKeys1);

        RowType rowType2 =
                RowType.of(
                        new DataType[] {DataTypes.INT().notNull(), DataTypes.VARCHAR(10)},
                        new String[] {"k2", "v0"});
        List<String> primaryKeys2 = Collections.singletonList("k2");
        expected = Arrays.asList("+I[2, two]", "+I[4, four]");
        waitForResult(expected, table2, rowType2, primaryKeys2);

        for (int i = 0; i < fileCount; i++) {
            try {
                writeRecordsToKafka(
                        writeOne ? topics.get(0) : topics.get(i),
                        readLines(
                                "kafka/canal/database/prefixsuffix/topic"
                                        + i
                                        + "/canal-data-2.txt"));
            } catch (Exception e) {
                throw new Exception("Failed to write canal data to Kafka.", e);
            }
        }
        rowType1 =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(), DataTypes.VARCHAR(10), DataTypes.INT()
                        },
                        new String[] {"k1", "v0", "v1"});
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
                            DataTypes.INT().notNull(), DataTypes.VARCHAR(10), DataTypes.VARCHAR(10)
                        },
                        new String[] {"k2", "v0", "v1"});
        expected =
                Arrays.asList(
                        "+I[2, two, NULL]",
                        "+I[4, four, NULL]",
                        "+I[6, six, s_6]",
                        "+I[8, eight, s_8]");
        waitForResult(expected, table2, rowType2, primaryKeys2);

        for (int i = 0; i < fileCount; i++) {
            try {
                writeRecordsToKafka(
                        writeOne ? topics.get(0) : topics.get(i),
                        readLines(
                                "kafka/canal/database/prefixsuffix/topic"
                                        + i
                                        + "/canal-data-3.txt"));
            } catch (Exception e) {
                throw new Exception("Failed to write canal data to Kafka.", e);
            }
        }

        rowType1 =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(), DataTypes.VARCHAR(10), DataTypes.BIGINT()
                        },
                        new String[] {"k1", "v0", "v1"});
        expected =
                Arrays.asList(
                        "+I[1, one, NULL]",
                        "+I[3, three, NULL]",
                        "+I[5, five, 50]",
                        "+I[7, seven, 70]",
                        "+I[9, nine, 9000000000000]");
        waitForResult(expected, table1, rowType1, primaryKeys1);

        rowType2 =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(), DataTypes.VARCHAR(10), DataTypes.VARCHAR(20)
                        },
                        new String[] {"k2", "v0", "v1"});
        expected =
                Arrays.asList(
                        "+I[2, two, NULL]",
                        "+I[4, four, NULL]",
                        "+I[6, six, s_6]",
                        "+I[8, eight, s_8]",
                        "+I[10, ten, long_s_10]");
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
    @Timeout(60)
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
        topics.forEach(
                topic -> {
                    createTestTopic(topic, 1, 1);
                });

        // ---------- Write the Canal json into Kafka -------------------

        try {
            writeRecordsToKafka(
                    topics.get(0),
                    readLines("kafka/canal/database/include/topic0/canal-data-1.txt"));
        } catch (Exception e) {
            throw new Exception("Failed to write canal data to Kafka.", e);
        }
        // try synchronization
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "canal-json");
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
