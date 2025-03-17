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
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for {@link KafkaSyncTableAction}. */
public class KafkaSyncTableActionITCase extends KafkaActionITCaseBase {

    protected void runSingleTableSchemaEvolution(String sourceDir, String format) throws Exception {
        final String topic = "schema_evolution";
        createTestTopic(topic, 1, 1);
        writeRecordsToKafka(topic, "kafka/%s/table/%s/%s-data-1.txt", format, sourceDir, format);

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

        writeRecordsToKafka(topic, "kafka/%s/table/%s/%s-data-2.txt", format, sourceDir, format);

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

        writeRecordsToKafka(topic, "kafka/%s/table/%s/%s-data-3.txt", format, sourceDir, format);

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

    protected void runSingleTableSchemaEvolutionWithSchemaIncludeRecord(
            String sourceDir, String format) throws Exception {
        final String topic = "schema_evolution";
        createTestTopic(topic, 1, 1);
        writeRecordsToKafka(
                topic, "kafka/%s/table/schema/%s/%s-data-1.txt", format, sourceDir, format);

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), format + "-json");
        kafkaConfig.put(TOPIC.key(), topic);
        KafkaSyncTableAction action =
                syncTableActionBuilder(kafkaConfig)
                        .withPrimaryKeys("id")
                        .withTableConfig(getBasicTableConfig())
                        .build();
        JobClient jobClient = runActionWithDefaultEnv(action);

        FileStoreTable table = getFileStoreTable(tableName);

        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.DOUBLE()
                        },
                        new String[] {"id", "name", "description", "weight"});
        List<String> primaryKeys = Collections.singletonList("id");
        List<String> expected =
                Collections.singletonList("+I[101, scooter, Small 2-wheel scooter, 3.14]");
        waitForResult(expected, table, rowType, primaryKeys);

        // add column
        writeRecordsToKafka(
                topic, "kafka/%s/table/schema/%s/%s-data-2.txt", format, sourceDir, format);

        rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.DOUBLE(),
                            DataTypes.INT()
                        },
                        new String[] {"id", "name", "description", "weight", "age"});
        expected =
                Arrays.asList(
                        "+I[101, scooter, Small 2-wheel scooter, 3.14, NULL]",
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, 18]");
        waitForResult(expected, table, rowType, primaryKeys);

        // column type promotion (int32 -> int64)
        writeRecordsToKafka(
                topic, "kafka/%s/table/schema/%s/%s-data-3.txt", format, sourceDir, format);

        rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.DOUBLE(),
                            DataTypes.BIGINT()
                        },
                        new String[] {"id", "name", "description", "weight", "age"});
        expected =
                Arrays.asList(
                        "+I[101, scooter, Small 2-wheel scooter, 3.14, NULL]",
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, 18]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.75, 24]");
        waitForResult(expected, table, rowType, primaryKeys);

        // column type changed ignore (int64 -> int32)
        writeRecordsToKafka(
                topic, "kafka/%s/table/schema/%s/%s-data-4.txt", format, sourceDir, format);

        rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.DOUBLE(),
                            DataTypes.BIGINT()
                        },
                        new String[] {"id", "name", "description", "weight", "age"});
        expected =
                Arrays.asList(
                        "+I[101, scooter, Small 2-wheel scooter, 3.14, NULL]",
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, 18]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.75, 24]",
                        "+I[105, hammer, 14oz carpenter's hammer, 0.875, 24]");
        waitForResult(expected, table, rowType, primaryKeys);

        // column type covert exception (int64 -> string)
        writeRecordsToKafka(
                topic, "kafka/%s/table/schema/%s/%s-data-5.txt", format, sourceDir, format);

        while (true) {
            JobStatus status = jobClient.getJobStatus().get();
            if (status != JobStatus.RUNNING) {
                assertThatThrownBy(() -> jobClient.getJobExecutionResult().get())
                        .satisfies(
                                anyCauseMatches(
                                        UnsupportedOperationException.class,
                                        "Cannot convert field age from type BIGINT to STRING of Paimon table"));
                break;
            }
            Thread.sleep(1000);
        }
    }

    public void testNotSupportFormat(String format) throws Exception {
        final String topic = "not_support";
        createTestTopic(topic, 1, 1);
        writeRecordsToKafka(topic, "kafka/%s/table/schemaevolution/%s-data-1.txt", format, format);

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
        writeRecordsToKafka(topic, "kafka/%s/table/schemaevolution/%s-data-1.txt", format, format);

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
                Collections.emptyList(),
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
        writeRecordsToKafka(topic, "kafka/%s/table/startupmode/%s-data-1.txt", format, format);

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
        writeRecordsToKafka(
                topic, true, "kafka/%s/table/startupmode/%s-data-1.txt", format, format);

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

        writeRecordsToKafka(topic, "kafka/%s/table/startupmode/%s-data-2.txt", format, format);

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
        writeRecordsToKafka(
                topic, true, "kafka/%s/table/startupmode/%s-data-1.txt", format, format);

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

        writeRecordsToKafka(topic, "kafka/%s/table/startupmode/%s-data-2.txt", format, format);

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
        writeRecordsToKafka(topic, "kafka/%s/table/startupmode/%s-data-1.txt", format, format);

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

        writeRecordsToKafka(topic, "kafka/%s/table/startupmode/%s-data-2.txt", format, format);

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
        writeRecordsToKafka(topic, "kafka/%s/table/startupmode/%s-data-1.txt", format, format);

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

        writeRecordsToKafka(topic, "kafka/%s/table/startupmode/%s-data-2.txt", format, format);

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
        writeRecordsToKafka(topic, "kafka/%s/table/computedcolumn/%s-data-1.txt", format, format);

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
        writeRecordsToKafka(topic, "kafka/%s/table/event/event-insert.txt", format);

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

        writeRecordsToKafka(topic, "kafka/%s/table/event/event-update.txt", format);

        // For the UPDATE operation
        List<String> expectedUpdate =
                Arrays.asList(
                        "+I[101, scooter, Small 2-wheel scooter, 3.14]",
                        "+I[102, car battery, 12V car battery, 8.1]",
                        "+I[103, scooter, Big 2-wheel scooter , 8.1]");
        waitForResult(expectedUpdate, table, rowType, primaryKeys);

        writeRecordsToKafka(topic, "kafka/%s/table/event/event-delete.txt", format);

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
        writeRecordsToKafka(
                topic,
                "kafka/%s/table/schema/schemaevolution/%s-data-with-delete.txt",
                format,
                format);

        Configuration kafkaConfig = Configuration.fromMap(getBasicKafkaConfig());
        kafkaConfig.setString(VALUE_FORMAT.key(), format + "-json");
        kafkaConfig.setString(TOPIC.key(), topic);

        Schema kafkaSchema =
                MessageQueueSchemaUtils.getSchema(
                        getKafkaEarliestConsumer(
                                kafkaConfig, new KafkaDebeziumJsonDeserializationSchema()),
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
        writeRecordsToKafka(topic, "kafka/%s/table/watermark/%s-data-1.txt", format, format);

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), format + "-json");
        kafkaConfig.put(TOPIC.key(), topic);

        Map<String, String> config = getBasicTableConfig();
        if ("debezium".equals(format)) {
            // debezium has no key
            // append mode never stop with compaction
            config.remove("bucket");
            config.put("write-only", "true");
        }
        config.put("tag.automatic-creation", "watermark");
        config.put("tag.creation-period", "hourly");
        config.put("scan.watermark.alignment.group", "alignment-group-1");
        config.put("scan.watermark.alignment.max-drift", "20 s");
        config.put("scan.watermark.alignment.update-interval", "1 s");

        KafkaSyncTableAction action =
                syncTableActionBuilder(kafkaConfig).withTableConfig(config).build();
        runActionWithDefaultEnv(action);

        FileStoreTable table =
                (FileStoreTable) catalog.getTable(new Identifier(database, tableName));
        while (true) {
            if (table.snapshotManager().snapshotCount() > 0
                    && table.snapshotManager().latestSnapshot().watermark()
                            != -9223372036854775808L) {
                return;
            }
            Thread.sleep(1000);
        }
    }

    public void testSchemaIncludeRecord(String format) throws Exception {
        String topic = "schema_include";
        createTestTopic(topic, 1, 1);
        writeRecordsToKafka(topic, "kafka/debezium/table/schema/include/debezium-data-1.txt");

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), format + "-json");
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
                            DataTypes.INT().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.DOUBLE()
                        },
                        new String[] {"id", "name", "description", "weight"});
        List<String> primaryKeys = Collections.singletonList("id");
        List<String> expected =
                Collections.singletonList(
                        "+I[101, scooter, Small 2-wheel scooter, 3.140000104904175]");
        waitForResult(expected, table, rowType, primaryKeys);
    }

    // TODO some types are different from mysql cdc; maybe need to fix
    public void testAllTypesWithSchemaImpl(String format) throws Exception {
        String topic = "schema_include_all_type";
        createTestTopic(topic, 1, 1);
        writeRecordsToKafka(topic, "kafka/debezium/table/schema/alltype/debezium-data-1.txt");

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), format + "-json");
        kafkaConfig.put(TOPIC.key(), topic);
        KafkaSyncTableAction action =
                syncTableActionBuilder(kafkaConfig)
                        .withPartitionKeys("pt")
                        .withPrimaryKeys("pt", "_id")
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        waitingTables(tableName);
        FileStoreTable table = getFileStoreTable(tableName);

        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(), // _id
                            DataTypes.DECIMAL(2, 1).notNull(), // pt
                            DataTypes.BOOLEAN(), // _bit1
                            DataTypes.BINARY(8), // _bit
                            DataTypes.SMALLINT(), // _tinyint1 different from mysql cdc
                            DataTypes.SMALLINT(), // _boolean different from mysql cdc
                            DataTypes.SMALLINT(), // _bool different from mysql cdc
                            DataTypes.SMALLINT(), // _tinyint different from mysql cdc
                            DataTypes.SMALLINT(), // _tinyint_unsigned
                            DataTypes.SMALLINT(), // _tinyint_unsigned_zerofill
                            DataTypes.SMALLINT(), // _smallint
                            DataTypes.INT(), // _smallint_unsigned
                            DataTypes.INT(), // _smallint_unsigned_zerofill
                            DataTypes.INT(), // _mediumint
                            DataTypes.INT(), // _mediumint_unsigned different from mysql cdc
                            DataTypes
                                    .INT(), // _mediumint_unsigned_zerofill different from mysql cdc
                            DataTypes.INT(), // _int
                            DataTypes.BIGINT(), // _int_unsigned
                            DataTypes.BIGINT(), // _int_unsigned_zerofill
                            DataTypes.BIGINT(), // _bigint
                            DataTypes.DECIMAL(20, 0), // _bigint_unsigned
                            DataTypes.DECIMAL(20, 0), // _bigint_unsigned_zerofill
                            DataTypes.DECIMAL(20, 0), // _serial different from mysql cdc
                            DataTypes.DOUBLE(), // _float different from mysql cdc
                            DataTypes.DOUBLE(), // _float_unsigned different from mysql cdc
                            DataTypes.DOUBLE(), // _float_unsigned_zerofill different from mysql cdc
                            DataTypes.DOUBLE(), // _real
                            DataTypes.DOUBLE(), // _real_unsigned
                            DataTypes.DOUBLE(), // _real_unsigned_zerofill
                            DataTypes.DOUBLE(), // _double
                            DataTypes.DOUBLE(), // _double_unsigned
                            DataTypes.DOUBLE(), // _double_unsigned_zerofill
                            DataTypes.DOUBLE(), // _double_precision
                            DataTypes.DOUBLE(), // _double_precision_unsigned
                            DataTypes.DOUBLE(), // _double_precision_unsigned_zerofill
                            DataTypes.DECIMAL(8, 3), // _numeric
                            DataTypes.DECIMAL(8, 3), // _numeric_unsigned
                            DataTypes.DECIMAL(8, 3), // _numeric_unsigned_zerofill
                            DataTypes.STRING(), // _fixed
                            DataTypes.STRING(), // _fixed_unsigned
                            DataTypes.STRING(), // _fixed_unsigned_zerofill
                            DataTypes.DECIMAL(8, 0), // _decimal
                            DataTypes.DECIMAL(8, 0), // _decimal_unsigned
                            DataTypes.DECIMAL(8, 0), // _decimal_unsigned_zerofill
                            DataTypes.DECIMAL(38, 10), // _big_decimal
                            DataTypes.DATE(), // _date
                            DataTypes.TIMESTAMP(3), // _datetime different from mysql cdc
                            DataTypes.TIMESTAMP(3), // _datetime3
                            DataTypes.TIMESTAMP(6), // _datetime6
                            DataTypes.TIMESTAMP(3), // _datetime_p different from mysql cdc
                            DataTypes.TIMESTAMP(3), // _datetime_p2 different from mysql cdc
                            DataTypes.TIMESTAMP(6), // _timestamp
                            DataTypes.TIMESTAMP(6), // _timestamp0 different from mysql cdc
                            DataTypes.STRING(), // _char different from mysql cdc
                            DataTypes.STRING(), // _varchar different from mysql cdc
                            DataTypes.STRING(), // _tinytext
                            DataTypes.STRING(), // _text
                            DataTypes.STRING(), // _mediumtext
                            DataTypes.STRING(), // _longtext
                            DataTypes.BYTES(), // _bin different from mysql cdc
                            DataTypes.BYTES(), // _varbin different from mysql cdc
                            DataTypes.BYTES(), // _tinyblob
                            DataTypes.BYTES(), // _blob
                            DataTypes.BYTES(), // _mediumblob
                            DataTypes.BYTES(), // _longblob
                            DataTypes.STRING(), // _json
                            DataTypes.STRING(), // _enum
                            DataTypes.INT(), // _year
                            DataTypes.TIME(), // _time
                            DataTypes.STRING(), // _point
                            DataTypes.STRING(), // _geometry
                            DataTypes.STRING(), // _linestring
                            DataTypes.STRING(), // _polygon
                            DataTypes.STRING(), // _multipoint
                            DataTypes.STRING(), // _multiline
                            DataTypes.STRING(), // _multipolygon
                            DataTypes.STRING(), // _geometrycollection
                            DataTypes.STRING() // _set different from mysql cdc
                        },
                        new String[] {
                            "_id",
                            "pt",
                            "_bit1",
                            "_bit",
                            "_tinyint1",
                            "_boolean",
                            "_bool",
                            "_tinyint",
                            "_tinyint_unsigned",
                            "_tinyint_unsigned_zerofill",
                            "_smallint",
                            "_smallint_unsigned",
                            "_smallint_unsigned_zerofill",
                            "_mediumint",
                            "_mediumint_unsigned",
                            "_mediumint_unsigned_zerofill",
                            "_int",
                            "_int_unsigned",
                            "_int_unsigned_zerofill",
                            "_bigint",
                            "_bigint_unsigned",
                            "_bigint_unsigned_zerofill",
                            "_serial",
                            "_float",
                            "_float_unsigned",
                            "_float_unsigned_zerofill",
                            "_real",
                            "_real_unsigned",
                            "_real_unsigned_zerofill",
                            "_double",
                            "_double_unsigned",
                            "_double_unsigned_zerofill",
                            "_double_precision",
                            "_double_precision_unsigned",
                            "_double_precision_unsigned_zerofill",
                            "_numeric",
                            "_numeric_unsigned",
                            "_numeric_unsigned_zerofill",
                            "_fixed",
                            "_fixed_unsigned",
                            "_fixed_unsigned_zerofill",
                            "_decimal",
                            "_decimal_unsigned",
                            "_decimal_unsigned_zerofill",
                            "_big_decimal",
                            "_date",
                            "_datetime",
                            "_datetime3",
                            "_datetime6",
                            "_datetime_p",
                            "_datetime_p2",
                            "_timestamp",
                            "_timestamp0",
                            "_char",
                            "_varchar",
                            "_tinytext",
                            "_text",
                            "_mediumtext",
                            "_longtext",
                            "_bin",
                            "_varbin",
                            "_tinyblob",
                            "_blob",
                            "_mediumblob",
                            "_longblob",
                            "_json",
                            "_enum",
                            "_year",
                            "_time",
                            "_point",
                            "_geometry",
                            "_linestring",
                            "_polygon",
                            "_multipoint",
                            "_multiline",
                            "_multipolygon",
                            "_geometrycollection",
                            "_set",
                        });

        // BIT(64) data: 0B11111000111 -> 0B00000111_11000111
        String bits =
                Arrays.toString(
                        new byte[] {0, 0, 0, 0, 0, 0, (byte) 0B00000111, (byte) 0B11000111});
        List<String> expected =
                Collections.singletonList(
                        "+I["
                                + "1, 1.1, "
                                + String.format("true, %s, ", bits)
                                + "1, 1, 0, 1, 2, 3, "
                                + "1000, 2000, 3000, "
                                + "100000, 200000, 300000, "
                                + "1000000, 2000000, 3000000, "
                                + "10000000000, 20000000000, 30000000000, 40000000000, "
                                + "1.5, 2.5, 3.5, "
                                + "1.000001, 2.000002, 3.000003, "
                                + "1.000011, 2.000022, 3.000033, "
                                + "1.000111, 2.000222, 3.000333, "
                                + "12345.110, 12345.220, 12345.330, "
                                + "123456789876543212345678987654321.11, 123456789876543212345678987654321.22, 123456789876543212345678987654321.33, "
                                + "11111, 22222, 33333, 2222222222222222300000001111.1234567890, "
                                + "19439, "
                                // display value of datetime is not affected by timezone
                                + "2023-03-23T14:30:05, 2023-03-23T14:30:05.123, 2023-03-23T14:30:05.123456, "
                                + "2023-03-24T14:30, 2023-03-24T14:30:05.120, "
                                // display value of timestamp is affected by timezone
                                // we store 2023-03-23T15:00:10.123456 in UTC-8 system timezone
                                // and query this timestamp in UTC-5 MySQL server timezone
                                // so the display value should increase by 3 hour
                                // TODO haven't handle zone
                                + "2023-03-23T22:00:10.123456, 2023-03-23T07:10, "
                                + "Paimon, Apache Paimon, Apache Paimon MySQL TINYTEXT Test Data, Apache Paimon MySQL Test Data, Apache Paimon MySQL MEDIUMTEXT Test Data, Apache Paimon MySQL Long Test Data, "
                                + "[98, 121, 116, 101, 115, 0, 0, 0, 0, 0], "
                                + "[109, 111, 114, 101, 32, 98, 121, 116, 101, 115], "
                                + "[84, 73, 78, 89, 66, 76, 79, 66, 32, 116, 121, 112, 101, 32, 116, 101, 115, 116, 32, 100, 97, 116, 97], "
                                + "[66, 76, 79, 66, 32, 116, 121, 112, 101, 32, 116, 101, 115, 116, 32, 100, 97, 116, 97], "
                                + "[77, 69, 68, 73, 85, 77, 66, 76, 79, 66, 32, 116, 121, 112, 101, 32, 116, 101, 115, 116, 32, 100, 97, 116, 97], "
                                + "[76, 79, 78, 71, 66, 76, 79, 66, 32, 32, 98, 121, 116, 101, 115, 32, 116, 101, 115, 116, 32, 100, 97, 116, 97], "
                                + "{\"a\": \"b\"}, "
                                + "value1, "
                                + "2023, "
                                + "36803000, "
                                + "{\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}, "
                                + "{\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0}, "
                                + "{\"coordinates\":[[3,0],[3,3],[3,5]],\"type\":\"LineString\",\"srid\":0}, "
                                + "{\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0}, "
                                + "{\"coordinates\":[[1,1],[2,2]],\"type\":\"MultiPoint\",\"srid\":0}, "
                                + "{\"coordinates\":[[[1,1],[2,2],[3,3]],[[4,4],[5,5]]],\"type\":\"MultiLineString\",\"srid\":0}, "
                                + "{\"coordinates\":[[[[0,0],[10,0],[10,10],[0,10],[0,0]]],[[[5,5],[7,5],[7,7],[5,7],[5,5]]]],\"type\":\"MultiPolygon\",\"srid\":0}, "
                                + "{\"geometries\":[{\"type\":\"Point\",\"coordinates\":[10,10]},{\"type\":\"Point\",\"coordinates\":[30,30]},{\"type\":\"LineString\",\"coordinates\":[[15,15],[20,20]]}],\"type\":\"GeometryCollection\",\"srid\":0}, "
                                // TODO fix set
                                + "a,b"
                                + "]");

        List<String> primaryKeys = Arrays.asList("pt", "_id");

        waitForResult(expected, table, rowType, primaryKeys);
    }

    protected void testTableFiledValNull(String format) throws Exception {
        final String topic = "table_filed_val_null";
        createTestTopic(topic, 1, 1);
        writeRecordsToKafka(topic, "kafka/%s/table/schemaevolution/%s-data-4.txt", format, format);

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

        Thread.sleep(5000);
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
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, null]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.75]");
        waitForResult(expected, table, rowType, primaryKeys);
    }
}
