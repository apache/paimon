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

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.VALUE_FORMAT;
import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for {@link KafkaSyncDatabaseAction}. */
public class KafkaSyncDatabaseActionITCase extends KafkaActionITCaseBase {

    protected void testSchemaEvolutionMultiTopic(String format) throws Exception {
        final String topic1 = "schema_evolution_0";
        final String topic2 = "schema_evolution_1";
        boolean writeOne = false;
        int fileCount = 2;
        List<String> topics = Arrays.asList(topic1, topic2);
        topics.forEach(topic -> createTestTopic(topic, 1, 1));

        for (int i = 0; i < fileCount; i++) {
            writeRecordsToKafka(
                    topics.get(i),
                    "kafka/%s/database/schemaevolution/topic%s/%s-data-1.txt",
                    format,
                    i,
                    format);
        }

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), format);
        kafkaConfig.put(TOPIC.key(), String.join(";", topics));
        KafkaSyncDatabaseAction action =
                syncDatabaseActionBuilder(kafkaConfig)
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        testSchemaEvolutionImpl(topics, writeOne, fileCount, format);
    }

    protected void testSchemaEvolutionOneTopic(String format) throws Exception {
        final String topic = "schema_evolution";
        boolean writeOne = true;
        int fileCount = 2;
        List<String> topics = Collections.singletonList(topic);
        topics.forEach(t -> createTestTopic(t, 1, 1));

        for (int i = 0; i < fileCount; i++) {
            writeRecordsToKafka(
                    topics.get(0),
                    "kafka/%s/database/schemaevolution/topic%s/%s-data-1.txt",
                    format,
                    i,
                    format);
        }

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), format);
        kafkaConfig.put(TOPIC.key(), String.join(";", topics));
        KafkaSyncDatabaseAction action =
                syncDatabaseActionBuilder(kafkaConfig)
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        testSchemaEvolutionImpl(topics, writeOne, fileCount, format);
    }

    private void testSchemaEvolutionImpl(
            List<String> topics, boolean writeOne, int fileCount, String format) throws Exception {
        waitingTables("t1", "t2");

        FileStoreTable table1 = getFileStoreTable("t1");
        FileStoreTable table2 = getFileStoreTable("t2");

        RowType rowType1 =
                RowType.of(
                        new DataType[] {
                            getDataType(format),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"id", "name", "description", "weight"});

        List<String> expected =
                Arrays.asList(
                        "+I[101, scooter, Small 2-wheel scooter, 3.14]",
                        "+I[102, car battery, 12V car battery, 8.1]");
        waitForResult(expected, table1, rowType1, getPrimaryKey(format));

        RowType rowType2 =
                RowType.of(
                        new DataType[] {
                            getDataType(format),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"id", "name", "description", "weight"});

        List<String> expected2 =
                Arrays.asList(
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.75]");
        waitForResult(expected2, table2, rowType2, getPrimaryKey(format));

        for (int i = 0; i < fileCount; i++) {
            writeRecordsToKafka(
                    writeOne ? topics.get(0) : topics.get(i),
                    "kafka/%s/database/schemaevolution/topic%s/%s-data-2.txt",
                    format,
                    i,
                    format);
        }

        rowType1 =
                RowType.of(
                        new DataType[] {
                            getDataType(format),
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
        waitForResult(expected, table1, rowType1, getPrimaryKey(format));

        rowType2 =
                RowType.of(
                        new DataType[] {
                            getDataType(format),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"id", "name", "description", "weight", "address"});

        if (format.equals("debezium")) {
            expected =
                    Arrays.asList(
                            "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, NULL]",
                            "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, Beijing]",
                            "+I[104, hammer, 12oz carpenter's hammer, 0.75, NULL]",
                            "+I[104, hammer, 12oz carpenter's hammer, 0.75, Shanghai]");
        } else {
            expected =
                    Arrays.asList(
                            "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, Beijing]",
                            "+I[104, hammer, 12oz carpenter's hammer, 0.75, Shanghai]");
        }

        waitForResult(expected, table2, rowType2, getPrimaryKey(format));
    }

    protected void testTopicIsEmpty(String format) {
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), format);

        KafkaSyncDatabaseAction action = syncDatabaseActionBuilder(kafkaConfig).build();

        assertThatThrownBy(action::run)
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "kafka_conf must and can only set one of the following options: topic,topic-pattern."));
    }

    protected void testTableAffixMultiTopic(String format) throws Exception {
        // create table t1
        createFileStoreTable(
                "test_prefix_t1_test_suffix",
                RowType.of(
                        new DataType[] {
                            getDataType(format),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"id", "name", "description", "weight"}),
                Collections.emptyList(),
                getPrimaryKey(format),
                Collections.emptyMap());

        final String topic1 = "prefix_suffix_0";
        final String topic2 = "prefix_suffix_1";
        boolean writeOne = false;
        int fileCount = 2;
        List<String> topics = Arrays.asList(topic1, topic2);
        topics.forEach(topic -> createTestTopic(topic, 1, 1));

        for (int i = 0; i < topics.size(); i++) {
            writeRecordsToKafka(
                    topics.get(i),
                    "kafka/%s/database/prefixsuffix/topic%s/%s-data-1.txt",
                    format,
                    i,
                    format);
        }

        // try synchronization
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), format);
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

        testTableAffixImpl(topics, writeOne, fileCount, format);
    }

    protected void testTableAffixOneTopic(String format) throws Exception {
        // create table t1
        createFileStoreTable(
                "test_prefix_t1_test_suffix",
                RowType.of(
                        new DataType[] {
                            getDataType(format),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"id", "name", "description", "weight"}),
                Collections.emptyList(),
                getPrimaryKey(format),
                Collections.emptyMap());

        final String topic1 = "prefix_suffix";
        List<String> topics = Collections.singletonList(topic1);
        boolean writeOne = true;
        int fileCount = 2;
        topics.forEach(topic -> createTestTopic(topic, 1, 1));

        for (int i = 0; i < fileCount; i++) {
            writeRecordsToKafka(
                    topics.get(0),
                    String.format(
                            "kafka/%s/database/prefixsuffix/topic%s/%s-data-1.txt",
                            format, i, format));
        }

        // try synchronization
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), format);
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

        testTableAffixImpl(topics, writeOne, fileCount, format);
    }

    private void testTableAffixImpl(
            List<String> topics, boolean writeOne, int fileCount, String format) throws Exception {
        waitingTables("test_prefix_t1_test_suffix", "test_prefix_t2_test_suffix");

        FileStoreTable table1 = getFileStoreTable("test_prefix_t1_test_suffix");
        FileStoreTable table2 = getFileStoreTable("test_prefix_t2_test_suffix");

        RowType rowType1 =
                RowType.of(
                        new DataType[] {
                            getDataType(format),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"id", "name", "description", "weight"});

        List<String> expected =
                Arrays.asList(
                        "+I[101, scooter, Small 2-wheel scooter, 3.14]",
                        "+I[102, car battery, 12V car battery, 8.1]");
        waitForResult(expected, table1, rowType1, getPrimaryKey(format));

        RowType rowType2 =
                RowType.of(
                        new DataType[] {
                            getDataType(format),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"id", "name", "description", "weight"});

        expected =
                Arrays.asList(
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.75]");
        waitForResult(expected, table2, rowType2, getPrimaryKey(format));

        for (int i = 0; i < fileCount; i++) {
            writeRecordsToKafka(
                    writeOne ? topics.get(0) : topics.get(i),
                    "kafka/%s/database/prefixsuffix/topic%s/%s-data-2.txt",
                    format,
                    i,
                    format);
        }
        rowType1 =
                RowType.of(
                        new DataType[] {
                            getDataType(format),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"id", "name", "description", "weight", "address"});
        if (format.equals("debezium")) {
            expected =
                    Arrays.asList(
                            "+I[101, scooter, Small 2-wheel scooter, 3.14, Beijing]",
                            "+I[101, scooter, Small 2-wheel scooter, 3.14, NULL]",
                            "+I[102, car battery, 12V car battery, 8.1, Shanghai]",
                            "+I[102, car battery, 12V car battery, 8.1, NULL]");
        } else {
            expected =
                    Arrays.asList(
                            "+I[101, scooter, Small 2-wheel scooter, 3.14, Beijing]",
                            "+I[102, car battery, 12V car battery, 8.1, Shanghai]");
        }
        waitForResult(expected, table1, rowType1, getPrimaryKey(format));

        rowType2 =
                RowType.of(
                        new DataType[] {
                            getDataType(format),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"id", "name", "description", "weight", "age"});
        if (format.equals("debezium")) {
            expected =
                    Arrays.asList(
                            "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, 19]",
                            "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, NULL]",
                            "+I[104, hammer, 12oz carpenter's hammer, 0.75, 25]",
                            "+I[104, hammer, 12oz carpenter's hammer, 0.75, NULL]");
        } else {
            expected =
                    Arrays.asList(
                            "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, 19]",
                            "+I[104, hammer, 12oz carpenter's hammer, 0.75, 25]");
        }
        waitForResult(expected, table2, rowType2, getPrimaryKey(format));
    }

    protected void testIncludingTables(String format) throws Exception {
        includingAndExcludingTablesImpl(
                "flink|paimon.+",
                null,
                Arrays.asList("flink", "paimon_1", "paimon_2"),
                Collections.singletonList("ignore"),
                format);
    }

    protected void testExcludingTables(String format) throws Exception {
        includingAndExcludingTablesImpl(
                null,
                "flink|paimon.+",
                Collections.singletonList("ignore"),
                Arrays.asList("flink", "paimon_1", "paimon_2"),
                format);
    }

    protected void testIncludingAndExcludingTables(String format) throws Exception {
        includingAndExcludingTablesImpl(
                "flink|paimon.+",
                "paimon_1",
                Arrays.asList("flink", "paimon_2"),
                Arrays.asList("paimon_1", "ignore"),
                format);
    }

    private void includingAndExcludingTablesImpl(
            @Nullable String includingTables,
            @Nullable String excludingTables,
            List<String> existedTables,
            List<String> notExistedTables,
            String format)
            throws Exception {
        final String topic1 = "include_exclude" + UUID.randomUUID();
        List<String> topics = Collections.singletonList(topic1);
        topics.forEach(topic -> createTestTopic(topic, 1, 1));

        writeRecordsToKafka(
                topics.get(0), "kafka/%s/database/include/topic0/%s-data-1.txt", format, format);

        // try synchronization
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), format);
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

    protected void testCaseInsensitive(String format) throws Exception {
        final String topic = "case-insensitive";
        createTestTopic(topic, 1, 1);

        writeRecordsToKafka(
                topic, "kafka/%s/database/case-insensitive/%s-data-1.txt", format, format);

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), format);
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
                            getDataType(format),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"id", "name", "description", "weight"});

        List<String> expected =
                Arrays.asList(
                        "+I[101, scooter, Small 2-wheel scooter, 3.14]",
                        "+I[102, car battery, 12V car battery, 8.1]");
        waitForResult(expected, table, rowType, getPrimaryKey(format));
    }

    private DataType getDataType(String format) {
        return format.equals("debezium") ? DataTypes.STRING() : DataTypes.STRING().notNull();
    }

    private List<String> getPrimaryKey(String format) {
        return format.equals("debezium")
                ? Collections.emptyList()
                : Collections.singletonList("id");
    }
}
