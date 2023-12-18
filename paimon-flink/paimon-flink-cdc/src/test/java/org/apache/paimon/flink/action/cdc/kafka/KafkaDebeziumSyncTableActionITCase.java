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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.VALUE_FORMAT;

/** IT cases for {@link KafkaDebeziumSyncTableActionITCase}. */
public class KafkaDebeziumSyncTableActionITCase extends KafkaActionITCaseBase {

    @Test
    @Timeout(60)
    public void testSchemaEvolution() throws Exception {
        runSingleTableSchemaEvolution("schemaevolution");
    }

    private void runSingleTableSchemaEvolution(String sourceDir) throws Exception {
        final String topic = "schema_evolution";
        createTestTopic(topic, 1, 1);
        // ---------- Write the debezium json into Kafka -------------------
        List<String> lines =
                readLines(String.format("kafka/debezium/table/%s/debezium-data-1.txt", sourceDir));
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception("Failed to write debezium data to Kafka.", e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), "debezium-json");
        kafkaConfig.put(TOPIC.key(), topic);
        KafkaSyncTableAction action =
                syncTableActionBuilder(kafkaConfig)
                        .withPrimaryKeys("id")
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        testSchemaEvolutionImpl(topic, sourceDir);
    }

    private void testSchemaEvolutionImpl(String topic, String sourceDir) throws Exception {
        FileStoreTable table = getFileStoreTable(tableName);

        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"id", "name", "description", "weight"});
        List<String> primaryKeys = Collections.singletonList("id");
        List<String> expected =
                Arrays.asList(
                        "+I[101, scooter, Small 2-wheel scooter, 3.14]",
                        "+I[102, car battery, 12V car battery, 8.1]");
        waitForResult(expected, table, rowType, primaryKeys);

        try {
            writeRecordsToKafka(
                    topic,
                    readLines(
                            String.format(
                                    "kafka/debezium/table/%s/debezium-data-2.txt", sourceDir)));
        } catch (Exception e) {
            throw new Exception("Failed to write debezium data to Kafka.", e);
        }
        rowType =
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
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, 18]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.75, 24]");
        waitForResult(expected, table, rowType, primaryKeys);

        try {
            writeRecordsToKafka(
                    topic,
                    readLines(
                            String.format(
                                    "kafka/debezium/table/%s/debezium-data-3.txt", sourceDir)));
        } catch (Exception e) {
            throw new Exception("Failed to write debezium data to Kafka.", e);
        }
        rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"id", "name", "description", "weight", "age", "address"});
        expected =
                Arrays.asList(
                        "+I[102, car battery, 12V car battery, 8.1, NULL, NULL]",
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, 18, NULL]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.75, 24, NULL]",
                        "+I[105, hammer, 14oz carpenter's hammer, 0.875, NULL, Beijing]",
                        "+I[107, rocks, box of assorted rocks, 5.3, NULL, NULL]");
        waitForResult(expected, table, rowType, primaryKeys);

        try {
            writeRecordsToKafka(
                    topic,
                    readLines(
                            String.format(
                                    "kafka/debezium/table/%s/debezium-data-4.txt", sourceDir)));
        } catch (Exception e) {
            throw new Exception("Failed to write debezium data to Kafka.", e);
        }
        rowType =
                RowType.of(
                        new DataType[] {
                                DataTypes.STRING().notNull(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING()
                        },
                        new String[] {"id", "name", "row"});
        expected =
                Arrays.asList(
                        "+I[102, car battery, 12V car battery, 8.1, NULL, NULL]",
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, 18, NULL]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.75, 24, NULL]",
                        "+I[105, hammer, 14oz carpenter's hammer, 0.875, NULL, Beijing]",
                        "+I[107, rocks, box of assorted rocks, 5.3, NULL, NULL]");
        waitForResult(expected, table, rowType, primaryKeys);
    }

    @Test
    @Timeout(60)
    public void testComputedColumn() throws Exception {
        String topic = "computed_column";
        createTestTopic(topic, 1, 1);

        List<String> lines = readLines("kafka/debezium/table/computedcolumn/debezium-data-1.txt");
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception("Failed to write canal data to Kafka.", e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), "debezium-json");
        kafkaConfig.put(TOPIC.key(), topic);
        KafkaSyncTableAction action =
                syncTableActionBuilder(kafkaConfig)
                        .withPartitionKeys("_year")
                        .withPrimaryKeys("id", "_year")
                        .withComputedColumnArgs("_year=year(date)")
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING().notNull(),
                            DataTypes.STRING(),
                            DataTypes.INT().notNull()
                        },
                        new String[] {"id", "date", "_year"});
        waitForResult(
                Collections.singletonList("+I[101, 2023-03-23, 2023]"),
                getFileStoreTable(tableName),
                rowType,
                Arrays.asList("id", "_year"));
    }

    @Test
    @Timeout(160)
    public void testRecordWithNestedDataType() throws Exception {
        String topic = "nested_type";
        createTestTopic(topic, 1, 1);

        List<String> lines = readLines("kafka/debezium/table/nestedtype/debezium-data-1.txt");
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception("Failed to write canal data to Kafka.", e);
        }

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), "debezium-json");
        kafkaConfig.put(TOPIC.key(), topic);
        KafkaSyncTableAction action =
                syncTableActionBuilder(kafkaConfig)
                        .withPrimaryKeys("id")
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        FileStoreTable table = getFileStoreTable(tableName);

        RowType rowType =
                RowType.of(
                        new DataType[] {
                                DataTypes.STRING().notNull(),
                                DataTypes.STRING()
                        },
                        new String[] {"id", "name", "row"});
        List<String> primaryKeys = Collections.singletonList("id");
        List<String> expected =
                Collections.singletonList(
                        "+I[101, scooter, {\"row_key\":\"value\"}]");
        waitForResult(expected, table, rowType, primaryKeys);
    }
}
