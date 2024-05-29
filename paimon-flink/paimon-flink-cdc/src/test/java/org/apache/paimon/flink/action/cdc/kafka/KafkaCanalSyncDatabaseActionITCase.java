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

import org.apache.paimon.catalog.FileSystemCatalogOptions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TOPIC_PATTERN;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.VALUE_FORMAT;
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TO_STRING;
import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
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
        topics.forEach(topic -> createTestTopic(topic, 1, 1));

        for (int i = 0; i < fileCount; i++) {
            writeRecordsToKafka(
                    topics.get(i),
                    "kafka/canal/database/schemaevolution/topic%s/canal-data-1.txt",
                    i);
        }

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), "canal-json");
        if (ThreadLocalRandom.current().nextBoolean()) {
            kafkaConfig.put(TOPIC.key(), String.join(";", topics));
        } else {
            kafkaConfig.put(TOPIC_PATTERN.key(), "schema_evolution_.+");
        }

        KafkaSyncDatabaseAction action =
                syncDatabaseActionBuilder(kafkaConfig)
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        testSchemaEvolutionImpl(topics, writeOne, fileCount);
    }

    @Test
    @Timeout(60)
    public void testSchemaEvolutionOneTopic() throws Exception {
        final String topic = "schema_evolution";
        boolean writeOne = true;
        int fileCount = 3;
        List<String> topics = Collections.singletonList(topic);
        topics.forEach(t -> createTestTopic(t, 1, 1));

        for (int i = 0; i < fileCount; i++) {
            writeRecordsToKafka(
                    topics.get(0),
                    "kafka/canal/database/schemaevolution/topic%s/canal-data-1.txt",
                    i);
        }

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), "canal-json");
        kafkaConfig.put(TOPIC.key(), String.join(";", topics));

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
            writeRecordsToKafka(
                    writeOne ? topics.get(0) : topics.get(i),
                    "kafka/canal/database/schemaevolution/topic%s/canal-data-2.txt",
                    i);
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
            writeRecordsToKafka(
                    writeOne ? topics.get(0) : topics.get(i),
                    "kafka/canal/database/schemaevolution/topic%s/canal-data-3.txt",
                    i);
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
        kafkaConfig.put(VALUE_FORMAT.key(), "canal-json");

        KafkaSyncDatabaseAction action = syncDatabaseActionBuilder(kafkaConfig).build();

        assertThatThrownBy(action::run)
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "kafka_conf must and can only set one of the following options: topic,topic-pattern."));
    }

    @Test
    @Timeout(60)
    public void testTableAffixMultiTopic() throws Exception {
        // create table t1
        createFileStoreTable(
                "test_prefix_t1_test_suffix",
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.VARCHAR(10)},
                        new String[] {"k1", "v0"}),
                Collections.emptyList(),
                Collections.singletonList("k1"),
                Collections.emptyList(),
                Collections.emptyMap());

        final String topic1 = "prefix_suffix_0";
        final String topic2 = "prefix_suffix_1";
        boolean writeOne = false;
        int fileCount = 2;
        List<String> topics = Arrays.asList(topic1, topic2);
        topics.forEach(topic -> createTestTopic(topic, 1, 1));

        for (int i = 0; i < topics.size(); i++) {
            writeRecordsToKafka(
                    topics.get(i), "kafka/canal/database/prefixsuffix/topic%s/canal-data-1.txt", i);
        }

        // try synchronization
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), "canal-json");
        kafkaConfig.put(TOPIC.key(), String.join(";", topics));
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
    @Timeout(60)
    public void testTableAffixOneTopic() throws Exception {
        // create table t1
        createFileStoreTable(
                "test_prefix_t1_test_suffix",
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.VARCHAR(10)},
                        new String[] {"k1", "v0"}),
                Collections.emptyList(),
                Collections.singletonList("k1"),
                Collections.emptyList(),
                Collections.emptyMap());

        final String topic1 = "prefix_suffix";
        List<String> topics = Collections.singletonList(topic1);
        boolean writeOne = true;
        int fileCount = 2;
        topics.forEach(topic -> createTestTopic(topic, 1, 1));

        for (int i = 0; i < fileCount; i++) {
            writeRecordsToKafka(
                    topics.get(0), "kafka/canal/database/prefixsuffix/topic%s/canal-data-1.txt", i);
        }

        // try synchronization
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), "canal-json");
        kafkaConfig.put(TOPIC.key(), String.join(";", topics));
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
            writeRecordsToKafka(
                    writeOne ? topics.get(0) : topics.get(i),
                    "kafka/canal/database/prefixsuffix/topic%s/canal-data-2.txt",
                    i);
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
            writeRecordsToKafka(
                    writeOne ? topics.get(0) : topics.get(i),
                    "kafka/canal/database/prefixsuffix/topic%s/canal-data-3.txt",
                    i);
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
        topics.forEach(topic -> createTestTopic(topic, 1, 1));
        writeRecordsToKafka(topics.get(0), "kafka/canal/database/include/topic0/canal-data-1.txt");

        // try synchronization
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), "canal-json");
        kafkaConfig.put(TOPIC.key(), String.join(";", topics));
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

    @Test
    @Timeout(60)
    public void testTypeMappingToString() throws Exception {
        final String topic = "map-to-string";
        createTestTopic(topic, 1, 1);
        writeRecordsToKafka(topic, "kafka/canal/database/tostring/canal-data-1.txt");

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), "canal-json");
        kafkaConfig.put(TOPIC.key(), topic);

        KafkaSyncDatabaseAction action =
                syncDatabaseActionBuilder(kafkaConfig)
                        .withTableConfig(getBasicTableConfig())
                        .withTypeMappingModes(TO_STRING.configString())
                        .build();
        runActionWithDefaultEnv(action);

        waitingTables("t1");

        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING().notNull(), DataTypes.STRING(), DataTypes.STRING()
                        },
                        new String[] {"k1", "v0", "v1"});
        waitForResult(
                Arrays.asList("+I[5, five, 50]", "+I[7, seven, 70]"),
                getFileStoreTable("t1"),
                rowType,
                Collections.singletonList("k1"));
    }

    @Test
    public void testCatalogAndTableConfig() {
        KafkaSyncDatabaseAction action =
                syncDatabaseActionBuilder(getBasicKafkaConfig())
                        .withCatalogConfig(Collections.singletonMap("catalog-key", "catalog-value"))
                        .withTableConfig(Collections.singletonMap("table-key", "table-value"))
                        .build();

        assertThat(action.catalogConfig()).containsEntry("catalog-key", "catalog-value");
        assertThat(action.tableConfig())
                .containsExactlyEntriesOf(Collections.singletonMap("table-key", "table-value"));
    }

    @Test
    @Timeout(60)
    public void testCaseInsensitive() throws Exception {
        final String topic = "case-insensitive";
        createTestTopic(topic, 1, 1);
        writeRecordsToKafka(topic, "kafka/canal/database/case-insensitive/canal-data-1.txt");

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), "canal-json");
        kafkaConfig.put(TOPIC.key(), topic);

        KafkaSyncDatabaseAction action =
                syncDatabaseActionBuilder(kafkaConfig)
                        .withTableConfig(getBasicTableConfig())
                        .withCatalogConfig(
                                Collections.singletonMap(
                                        FileSystemCatalogOptions.CASE_SENSITIVE.key(), "false"))
                        .build();
        runActionWithDefaultEnv(action);

        waitingTables("t1");
        FileStoreTable table = getFileStoreTable("t1");
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(), DataTypes.VARCHAR(10), DataTypes.INT()
                        },
                        new String[] {"k1", "v0", "v1"});
        waitForResult(
                Arrays.asList("+I[5, five, 50]", "+I[7, seven, 70]"),
                table,
                rowType,
                Collections.singletonList("k1"));
    }

    @Test
    @Timeout(60)
    public void testCannotSynchronizeIncompleteJson() throws Exception {
        final String topic = "incomplete";
        createTestTopic(topic, 1, 1);
        writeRecordsToKafka(topic, "kafka/canal/database/incomplete/canal-data-1.txt");

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), "canal-json");
        kafkaConfig.put(TOPIC.key(), topic);

        KafkaSyncDatabaseAction action =
                syncDatabaseActionBuilder(kafkaConfig)
                        .withTableConfig(getBasicTableConfig())
                        .build();
        action.withStreamExecutionEnvironment(env).build();

        assertThatThrownBy(() -> env.execute())
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Cannot synchronize record when database name or table name is unknown. "
                                        + "Invalid record is:\n"
                                        + "{databaseName=null, tableName=null, fields=[`k` STRING, `v0` STRING, `v1` STRING], "
                                        + "primaryKeys=[], cdcRecord=+I {v0=five, k=5, v1=50}}"));
    }

    @Test
    @Timeout(60)
    public void testMergeShards() throws Exception {
        final String topic = "merge-shards";
        createTestTopic(topic, 1, 1);
        writeRecordsToKafka(topic, "kafka/canal/database/mergeshards/canal-data-1.txt");

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), "canal-json");
        kafkaConfig.put(TOPIC.key(), topic);

        KafkaSyncDatabaseAction action =
                syncDatabaseActionBuilder(kafkaConfig)
                        .withTableConfig(getBasicTableConfig())
                        .mergeShards(false)
                        .build();
        runActionWithDefaultEnv(action);

        waitingTables("paimon_sync_database_merge_shard_1_t1","paimon_sync_database_merge_shard_2_t1");

        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(), DataTypes.VARCHAR(10), DataTypes.INT()
                        },
                        new String[] {"k1", "v0", "v1"});
        waitForResult(
                Arrays.asList("+I[5, five, 50]"),
                getFileStoreTable("paimon_sync_database_merge_shard_1_t1"),
                rowType,
                Collections.singletonList("k1"));

        waitForResult(
                Arrays.asList("+I[7, seven, 70]"),
                getFileStoreTable("paimon_sync_database_merge_shard_2_t1"),
                rowType,
                Collections.singletonList("k1"));
    }
}
