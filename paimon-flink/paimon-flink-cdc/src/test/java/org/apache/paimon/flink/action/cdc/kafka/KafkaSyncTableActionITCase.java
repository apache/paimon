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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.cdc.MessageQueueSchemaUtils;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.AbstractFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.configuration.Configuration;

import java.util.*;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SCAN_STARTUP_SPECIFIC_OFFSETS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.ScanStartupMode.EARLIEST_OFFSET;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.ScanStartupMode.GROUP_OFFSETS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.ScanStartupMode.LATEST_OFFSET;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.ScanStartupMode.SPECIFIC_OFFSETS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.ScanStartupMode.TIMESTAMP;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.VALUE_FORMAT;
import static org.apache.paimon.flink.action.cdc.kafka.KafkaActionUtils.getDataFormat;
import static org.apache.paimon.flink.action.cdc.kafka.KafkaActionUtils.getKafkaEarliestConsumer;
import static org.apache.paimon.testutils.assertj.AssertionUtils.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for {@link KafkaSyncTableAction}. */
public class KafkaSyncTableActionITCase extends KafkaActionITCaseBase {

    protected void runSingleTableSchemaEvolution(String sourceDir, String format) throws Exception {
        final String topic = "schema_evolution";
        createTestTopic(topic, 1, 1);
        // ---------- Write the data into Kafka -------------------
        List<String> lines =
                readLines(
                        String.format(
                                "kafka/%s/table/%s/%s-data-1.txt", format, sourceDir, format));
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception(String.format("Failed to write %s data to Kafka.", format), e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), format + "-json");
        kafkaConfig.put(TOPIC.key(), topic);
        KafkaSyncTableAction action =
                syncTableActionBuilder(kafkaConfig)
                        .withPrimaryKeys("id")
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        testSchemaEvolutionImpl(topic, sourceDir, format);
    }

    private void testSchemaEvolutionImpl(String topic, String sourceDir, String format)
            throws Exception {
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
                                    "kafka/%s/table/%s/%s-data-2.txt", format, sourceDir, format)));
        } catch (Exception e) {
            throw new Exception(String.format("Failed to write %s data to Kafka.", format), e);
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
                                    "kafka/%s/table/%s/%s-data-3.txt", format, sourceDir, format)));
        } catch (Exception e) {
            throw new Exception(String.format("Failed to write %s data to Kafka.", format), e);
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
    }

    public void testNotSupportFormat(String format) throws Exception {
        final String topic = "not_support";
        createTestTopic(topic, 1, 1);
        // ---------- Write the data into Kafka -------------------
        List<String> lines =
                readLines(
                        String.format(
                                "kafka/%s/table/schemaevolution/%s-data-1.txt", format, format));
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception(String.format("Failed to write %s data to Kafka.", format), e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), "togg-json");
        kafkaConfig.put(TOPIC.key(), topic);
        KafkaSyncTableAction action =
                syncTableActionBuilder(kafkaConfig)
                        .withPrimaryKeys("id")
                        .withTableConfig(getBasicTableConfig())
                        .build();

        assertThatThrownBy(action::run)
                .satisfies(
                        anyCauseMatches(
                                UnsupportedOperationException.class,
                                "This format: togg-json is not supported."));
    }

    protected void testAssertSchemaCompatible(String format) throws Exception {
        final String topic = "assert_schema_compatible";
        createTestTopic(topic, 1, 1);
        // ---------- Write the data into Kafka -------------------
        List<String> lines =
                readLines(
                        String.format(
                                "kafka/%s/table/schemaevolution/%s-data-1.txt", format, format));
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception(String.format("Failed to write %s data to Kafka.", format), e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), format + "-json");
        kafkaConfig.put(TOPIC.key(), topic);

        // create an incompatible table
        createFileStoreTable(
                RowType.of(
                        new DataType[] {DataTypes.STRING(), DataTypes.STRING()},
                        new String[] {"k", "v1"}),
                Collections.emptyList(),
                Collections.singletonList("k"),
                Collections.emptyMap());

        KafkaSyncTableAction action =
                syncTableActionBuilder(kafkaConfig)
                        .withPrimaryKeys("id")
                        .withTableConfig(getBasicTableConfig())
                        .build();

        assertThatThrownBy(action::run)
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Paimon schema and source table schema are not compatible.\n"
                                        + "Paimon fields are: [`k` STRING NOT NULL, `v1` STRING].\n"
                                        + "Source table fields are: [`id` STRING NOT NULL, `name` STRING, `description` STRING, `weight` STRING]"));
    }

    protected void testStarUpOptionSpecific(String format) throws Exception {
        final String topic = "start_up_specific";
        createTestTopic(topic, 1, 1);
        // ---------- Write the data into Kafka -------------------
        List<String> lines =
                readLines(
                        String.format("kafka/%s/table/startupmode/%s-data-1.txt", format, format));
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception(String.format("Failed to write %s data to Kafka.", format), e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), format + "-json");
        kafkaConfig.put(TOPIC.key(), topic);
        kafkaConfig.put(SCAN_STARTUP_MODE.key(), SPECIFIC_OFFSETS.toString());
        kafkaConfig.put(SCAN_STARTUP_SPECIFIC_OFFSETS.key(), "partition:0,offset:1");
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
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"id", "name", "description", "weight"});
        List<String> primaryKeys = Collections.singletonList("id");
        // topic has two records we read two
        List<String> expected =
                Collections.singletonList("+I[102, car battery, 12V car battery, 8.1]");
        waitForResult(expected, table, rowType, primaryKeys);
    }

    protected void testStarUpOptionLatest(String format) throws Exception {
        final String topic = "start_up_latest";
        createTestTopic(topic, 1, 1);
        // ---------- Write the data into Kafka -------------------
        List<String> lines =
                readLines(
                        String.format("kafka/%s/table/startupmode/%s-data-1.txt", format, format));
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception(String.format("Failed to write %s data to Kafka.", format), e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), format + "-json");
        kafkaConfig.put(TOPIC.key(), topic);
        kafkaConfig.put(SCAN_STARTUP_MODE.key(), LATEST_OFFSET.toString());
        KafkaSyncTableAction action =
                syncTableActionBuilder(kafkaConfig)
                        .withPrimaryKeys("id")
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        Thread.sleep(5000);
        FileStoreTable table = getFileStoreTable(tableName);
        try {
            writeRecordsToKafka(
                    topic,
                    readLines(
                            String.format(
                                    "kafka/%s/table/startupmode/%s-data-2.txt", format, format)));
        } catch (Exception e) {
            throw new Exception(String.format("Failed to write %s data to Kafka.", format), e);
        }

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
        // topic has four records we read two
        List<String> expected =
                Arrays.asList(
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.75]");
        waitForResult(expected, table, rowType, primaryKeys);
    }

    public void testStarUpOptionTimestamp(String format) throws Exception {
        final String topic = "start_up_timestamp";
        createTestTopic(topic, 1, 1);
        // ---------- Write the data into Kafka -------------------
        List<String> lines =
                readLines(
                        String.format("kafka/%s/table/startupmode/%s-data-1.txt", format, format));
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception(String.format("Failed to write %s data to Kafka.", format), e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), format + "-json");
        kafkaConfig.put(TOPIC.key(), topic);
        kafkaConfig.put(SCAN_STARTUP_MODE.key(), TIMESTAMP.toString());
        kafkaConfig.put(
                SCAN_STARTUP_TIMESTAMP_MILLIS.key(), String.valueOf(System.currentTimeMillis()));
        KafkaSyncTableAction action =
                syncTableActionBuilder(kafkaConfig)
                        .withPrimaryKeys("id")
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        try {
            writeRecordsToKafka(
                    topic,
                    readLines(
                            String.format(
                                    "kafka/%s/table/startupmode/%s-data-2.txt", format, format)));
        } catch (Exception e) {
            throw new Exception(String.format("Failed to write %s data to Kafka.", format), e);
        }
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
        // topic has four records we read two
        List<String> expected =
                Arrays.asList(
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.75]");
        waitForResult(expected, table, rowType, primaryKeys);
    }

    public void testStarUpOptionEarliest(String format) throws Exception {
        final String topic = "start_up_earliest";
        createTestTopic(topic, 1, 1);
        // ---------- Write the data into Kafka -------------------
        List<String> lines =
                readLines(
                        String.format("kafka/%s/table/startupmode/%s-data-1.txt", format, format));
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception(String.format("Failed to write %s data to Kafka.", format), e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), format + "-json");
        kafkaConfig.put(TOPIC.key(), topic);
        kafkaConfig.put(SCAN_STARTUP_MODE.key(), EARLIEST_OFFSET.toString());
        KafkaSyncTableAction action =
                syncTableActionBuilder(kafkaConfig)
                        .withPrimaryKeys("id")
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        try {
            writeRecordsToKafka(
                    topic,
                    readLines(
                            String.format(
                                    "kafka/%s/table/startupmode/%s-data-2.txt", format, format)));
        } catch (Exception e) {
            throw new Exception(String.format("Failed to write %s data to Kafka.", format), e);
        }
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
        // topic has four records we read all
        List<String> expected =
                Arrays.asList(
                        "+I[101, scooter, Small 2-wheel scooter, 3.14]",
                        "+I[102, car battery, 12V car battery, 8.1]",
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.75]");
        waitForResult(expected, table, rowType, primaryKeys);
    }

    public void testStarUpOptionGroup(String format) throws Exception {
        final String topic = "start_up_group";
        createTestTopic(topic, 1, 1);
        // ---------- Write the data into Kafka -------------------
        List<String> lines =
                readLines(
                        String.format("kafka/%s/table/startupmode/%s-data-1.txt", format, format));
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception(String.format("Failed to write %s data to Kafka.", format), e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), format + "-json");
        kafkaConfig.put(TOPIC.key(), topic);
        kafkaConfig.put(SCAN_STARTUP_MODE.key(), GROUP_OFFSETS.toString());
        KafkaSyncTableAction action =
                syncTableActionBuilder(kafkaConfig)
                        .withPrimaryKeys("id")
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        try {
            writeRecordsToKafka(
                    topic,
                    readLines(
                            String.format(
                                    "kafka/%s/table/startupmode/%s-data-2.txt", format, format)));
        } catch (Exception e) {
            throw new Exception(String.format("Failed to write %s data to Kafka.", format), e);
        }
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
        // topic has four records we read all
        List<String> expected =
                Arrays.asList(
                        "+I[101, scooter, Small 2-wheel scooter, 3.14]",
                        "+I[102, car battery, 12V car battery, 8.1]",
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.75]");
        waitForResult(expected, table, rowType, primaryKeys);
    }

    public void testComputedColumn(String format) throws Exception {
        String topic = "computed_column";
        createTestTopic(topic, 1, 1);

        List<String> lines =
                readLines(
                        String.format(
                                "kafka/%s/table/computedcolumn/%s-data-1.txt", format, format));
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception(String.format("Failed to write %s data to Kafka.", format), e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), format + "-json");
        kafkaConfig.put(TOPIC.key(), topic);
        KafkaSyncTableAction action =
                syncTableActionBuilder(kafkaConfig)
                        .withPartitionKeys("_year")
                        .withPrimaryKeys("_id", "_year")
                        .withComputedColumnArgs("_year=year(_date)")
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
                        new String[] {"_id", "_date", "_year"});
        waitForResult(
                Collections.singletonList("+I[101, 2023-03-23, 2023]"),
                getFileStoreTable(tableName),
                rowType,
                Arrays.asList("_id", "_year"));
    }

    protected void testCDCOperations(String format) throws Exception {
        String topic = "event";
        createTestTopic(topic, 1, 1);

        List<String> lines =
                readLines(String.format("kafka/%s/table/event/event-insert.txt", format));
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception(String.format("Failed to write %s data to Kafka.", format), e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), "ogg-json");
        kafkaConfig.put(TOPIC.key(), topic);
        KafkaSyncTableAction action =
                syncTableActionBuilder(kafkaConfig)
                        .withPrimaryKeys("id")
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        FileStoreTable table = getFileStoreTable(tableName);
        List<String> primaryKeys = Collections.singletonList("id");
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"id", "name", "description", "weight"});

        // For the INSERT operation
        List<String> expectedInsert =
                Arrays.asList(
                        "+I[101, scooter, Small 2-wheel scooter, 3.14]",
                        "+I[102, car battery, 12V car battery, 8.1]",
                        "+I[103, scooter, Big 2-wheel scooter , 5.1]");
        waitForResult(expectedInsert, table, rowType, primaryKeys);

        try {
            writeRecordsToKafka(
                    topic,
                    readLines(String.format("kafka/%s/table/event/event-update.txt", format)));
        } catch (Exception e) {
            throw new Exception(String.format("Failed to write %s data to Kafka.", format), e);
        }
        // For the UPDATE operation
        List<String> expectedUpdate =
                Arrays.asList(
                        "+I[101, scooter, Small 2-wheel scooter, 3.14]",
                        "+I[102, car battery, 12V car battery, 8.1]",
                        "+I[103, scooter, Big 2-wheel scooter , 8.1]");
        waitForResult(expectedUpdate, table, rowType, primaryKeys);

        try {
            writeRecordsToKafka(
                    topic,
                    readLines(String.format("kafka/%s/table/event/event-delete.txt", format)));
        } catch (Exception e) {
            throw new Exception(String.format("Failed to write %s data to Kafka.", format), e);
        }

        // For the REPLACE operation
        List<String> expectedReplace =
                Arrays.asList(
                        "+I[101, scooter, Small 2-wheel scooter, 3.14]",
                        "+I[102, car battery, 12V car battery, 8.1]");
        waitForResult(expectedReplace, table, rowType, primaryKeys);
    }

    public void testKafkaBuildSchemaWithDelete(String format) throws Exception {
        final String topic = "test_kafka_schema";
        createTestTopic(topic, 1, 1);
        // ---------- Write the Debezium json into Kafka -------------------
        List<String> lines =
                readLines(
                        String.format(
                                "kafka/%s/table/schema/schemaevolution/%s-data-4.txt",
                                format, format));
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception(String.format("Failed to write %s data to Kafka.", format), e);
        }
        Configuration kafkaConfig = Configuration.fromMap(getBasicKafkaConfig());
        kafkaConfig.setString(VALUE_FORMAT.key(), format + "-json");
        kafkaConfig.setString(TOPIC.key(), topic);

        Schema kafkaSchema =
                MessageQueueSchemaUtils.getSchema(
                        getKafkaEarliestConsumer(kafkaConfig),
                        getDataFormat(kafkaConfig),
                        TypeMapping.defaultMapping());
        List<DataField> fields = new ArrayList<>();
        // {"id": 101, "name": "scooter", "description": "Small 2-wheel scooter", "weight": 3.14}
        fields.add(new DataField(0, "id", DataTypes.STRING()));
        fields.add(new DataField(1, "name", DataTypes.STRING()));
        fields.add(new DataField(2, "description", DataTypes.STRING()));
        fields.add(new DataField(3, "weight", DataTypes.STRING()));
        assertThat(kafkaSchema.fields()).isEqualTo(fields);
    }

    public void testWaterMarkSyncTable(String format) throws Exception {
        String topic = "watermark";
        createTestTopic(topic, 1, 1);
        writeRecordsToKafka(
                topic,
                readLines(String.format("kafka/%s/table/watermark/%s-data-1.txt", format, format)));

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), format + "-json");
        kafkaConfig.put(TOPIC.key(), topic);

        Map<String, String> config = getBasicTableConfig();
        config.put("tag.automatic-creation", "watermark");
        config.put("tag.creation-period", "hourly");
        config.put("scan.watermark.alignment.group", "alignment-group-1");
        config.put("scan.watermark.alignment.max-drift", "20 s");
        config.put("scan.watermark.alignment.update-interval", "1 s");

        KafkaSyncTableAction action =
                syncTableActionBuilder(kafkaConfig).withTableConfig(config).build();
        runActionWithDefaultEnv(action);

        AbstractFileStoreTable table =
                (AbstractFileStoreTable) catalog.getTable(new Identifier(database, tableName));
        while (true) {
            if (table.snapshotManager().snapshotCount() > 0
                    && table.snapshotManager().latestSnapshot().watermark()
                            != -9223372036854775808L) {
                return;
            }
            Thread.sleep(1000);
        }
    }
}
