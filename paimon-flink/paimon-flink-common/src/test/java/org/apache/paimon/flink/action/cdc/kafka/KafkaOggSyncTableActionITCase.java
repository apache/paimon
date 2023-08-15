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
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for {@link KafkaOggSyncTableActionITCase}. */
public class KafkaOggSyncTableActionITCase extends KafkaActionITCaseBase {

    @Test
    @Timeout(60)
    public void testSchemaEvolution() throws Exception {
        runSingleTableSchemaEvolution("schemaevolution");
    }

    private void runSingleTableSchemaEvolution(String sourceDir) throws Exception {
        final String topic = "schema_evolution";
        createTestTopic(topic, 1, 1);
        // ---------- Write the ogg json into Kafka -------------------
        List<String> lines =
                readLines(String.format("kafka/ogg/table/%s/ogg-data-1.txt", sourceDir));
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception("Failed to write ogg data to Kafka.", e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "ogg-json");
        kafkaConfig.put("topic", topic);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.noRestart());

        ThreadLocalRandom random = ThreadLocalRandom.current();
        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put("bucket", String.valueOf(random.nextInt(3) + 1));
        tableConfig.put("sink.parallelism", String.valueOf(random.nextInt(3) + 1));
        KafkaSyncTableAction action =
                new KafkaSyncTableAction(
                        kafkaConfig,
                        warehouse,
                        database,
                        tableName,
                        Collections.emptyList(),
                        Collections.singletonList("id"),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        tableConfig);
        action.build(env);
        JobClient client = env.executeAsync();

        waitJobRunning(client);

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
                        "+I[101, scooter, Small 2-wheel scooter, 3.140000104904175]",
                        "+I[102, car battery, 12V car battery, 8.100000381469727]");
        waitForResult(expected, table, rowType, primaryKeys);

        try {
            writeRecordsToKafka(
                    topic,
                    readLines(String.format("kafka/ogg/table/%s/ogg-data-2.txt", sourceDir)));
        } catch (Exception e) {
            throw new Exception("Failed to write ogg data to Kafka.", e);
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
                        "+I[101, scooter, Small 2-wheel scooter, 3.140000104904175, NULL]",
                        "+I[102, car battery, 12V car battery, 8.100000381469727, NULL]",
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.800000011920929, 18]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.75, 24]");
        waitForResult(expected, table, rowType, primaryKeys);

        try {
            writeRecordsToKafka(
                    topic,
                    readLines(String.format("kafka/ogg/table/%s/ogg-data-3.txt", sourceDir)));
        } catch (Exception e) {
            throw new Exception("Failed to write ogg data to Kafka.", e);
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
                        "+I[102, car battery, 12V car battery, 8.100000381469727, NULL, NULL]",
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.800000011920929, 18, NULL]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.75, 24, NULL]",
                        "+I[107, rocks, box of assorted rocks, 5.300000190734863, NULL, NULL]",
                        "+U[105, hammer, 14oz carpenter's hammer, 0.875, NULL, Beijing]");
        waitForResult(expected, table, rowType, primaryKeys);
    }

    @Test
    @Timeout(60)
    public void testNotSupportFormat() throws Exception {
        final String topic = "not_support";
        createTestTopic(topic, 1, 1);
        // ---------- Write the ogg json into Kafka -------------------
        List<String> lines = readLines("kafka/ogg/table/schemaevolution/ogg-data-1.txt");
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception("Failed to write ogg data to Kafka.", e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "togg-json");
        kafkaConfig.put("topic", topic);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.noRestart());

        ThreadLocalRandom random = ThreadLocalRandom.current();
        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put("bucket", String.valueOf(random.nextInt(3) + 1));
        tableConfig.put("sink.parallelism", String.valueOf(random.nextInt(3) + 1));
        KafkaSyncTableAction action =
                new KafkaSyncTableAction(
                        kafkaConfig,
                        warehouse,
                        database,
                        tableName,
                        Collections.emptyList(),
                        Collections.singletonList("id"),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        tableConfig);

        assertThatThrownBy(() -> action.build(env))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("This format: togg-json is not supported.");
    }

    @Test
    @Timeout(60)
    public void testAssertSchemaCompatible() throws Exception {
        final String topic = "assert_schema_compatible";
        createTestTopic(topic, 1, 1);
        // ---------- Write the ogg json into Kafka -------------------
        List<String> lines = readLines("kafka/ogg/table/schemaevolution/ogg-data-1.txt");
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception("Failed to write ogg data to Kafka.", e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "ogg-json");
        kafkaConfig.put("topic", topic);

        // create an incompatible table
        Catalog catalog = catalog();
        catalog.createDatabase(database, true);
        Identifier identifier = Identifier.create(database, tableName);
        Schema schema =
                Schema.newBuilder()
                        .column("k", DataTypes.STRING())
                        .column("v1", DataTypes.STRING())
                        .primaryKey("k")
                        .build();
        catalog.createTable(identifier, schema, false);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.noRestart());

        ThreadLocalRandom random = ThreadLocalRandom.current();
        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put("bucket", String.valueOf(random.nextInt(3) + 1));
        tableConfig.put("sink.parallelism", String.valueOf(random.nextInt(3) + 1));
        KafkaSyncTableAction action =
                new KafkaSyncTableAction(
                        kafkaConfig,
                        warehouse,
                        database,
                        tableName,
                        Collections.emptyList(),
                        Collections.singletonList("id"),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        tableConfig);

        assertThatThrownBy(() -> action.build(env))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Paimon schema and Kafka schema are not compatible.\n"
                                + "Paimon fields are: [`k` STRING NOT NULL, `v1` STRING].\n"
                                + "Kafka fields are: [`id` STRING NOT NULL, `name` STRING, `description` STRING, `weight` STRING]");
    }

    @Test
    @Timeout(60)
    public void testStarUpOptionSpecific() throws Exception {
        final String topic = "start_up_specific";
        createTestTopic(topic, 1, 1);
        // ---------- Write the ogg json into Kafka -------------------
        List<String> lines = readLines("kafka/ogg/table/startupmode/ogg-data-1.txt");
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception("Failed to write ogg data to Kafka.", e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "ogg-json");
        kafkaConfig.put("topic", topic);
        kafkaConfig.put("scan.startup.mode", "specific-offsets");
        kafkaConfig.put("scan.startup.specific-offsets", "partition:0,offset:1");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.noRestart());

        ThreadLocalRandom random = ThreadLocalRandom.current();
        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put("bucket", String.valueOf(random.nextInt(3) + 1));
        tableConfig.put("sink.parallelism", String.valueOf(random.nextInt(3) + 1));
        KafkaSyncTableAction action =
                new KafkaSyncTableAction(
                        kafkaConfig,
                        warehouse,
                        database,
                        tableName,
                        Collections.emptyList(),
                        Collections.singletonList("id"),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        tableConfig);
        action.build(env);
        JobClient client = env.executeAsync();

        waitJobRunning(client);

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
                Collections.singletonList(
                        "+I[102, car battery, 12V car battery, 8.100000381469727]");
        waitForResult(expected, table, rowType, primaryKeys);
    }

    @Test
    @Timeout(60)
    public void testStarUpOptionLatest() throws Exception {
        final String topic = "start_up_latest";
        createTestTopic(topic, 1, 1);
        // ---------- Write the ogg json into Kafka -------------------
        List<String> lines = readLines("kafka/ogg/table/startupmode/ogg-data-1.txt");
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception("Failed to write ogg data to Kafka.", e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "ogg-json");
        kafkaConfig.put("topic", topic);
        kafkaConfig.put("scan.startup.mode", "latest-offset");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.noRestart());

        ThreadLocalRandom random = ThreadLocalRandom.current();
        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put("bucket", String.valueOf(random.nextInt(3) + 1));
        tableConfig.put("sink.parallelism", String.valueOf(random.nextInt(3) + 1));
        KafkaSyncTableAction action =
                new KafkaSyncTableAction(
                        kafkaConfig,
                        warehouse,
                        database,
                        tableName,
                        Collections.emptyList(),
                        Collections.singletonList("id"),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        tableConfig);
        action.build(env);
        JobClient client = env.executeAsync();

        waitJobRunning(client);
        waitTablesCreated(tableName);
        Thread.sleep(5000);
        FileStoreTable table = getFileStoreTable(tableName);
        try {
            writeRecordsToKafka(topic, readLines("kafka/ogg/table/startupmode/ogg-data-2.txt"));
        } catch (Exception e) {
            throw new Exception("Failed to write ogg data to Kafka.", e);
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
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.800000011920929]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.75]");
        waitForResult(expected, table, rowType, primaryKeys);
    }

    @Test
    @Timeout(60)
    public void testStarUpOptionTimestamp() throws Exception {
        final String topic = "start_up_timestamp";
        createTestTopic(topic, 1, 1);
        // ---------- Write the ogg json into Kafka -------------------
        List<String> lines = readLines("kafka/ogg/table/startupmode/ogg-data-1.txt");
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception("Failed to write ogg data to Kafka.", e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "ogg-json");
        kafkaConfig.put("topic", topic);
        kafkaConfig.put("scan.startup.mode", "timestamp");
        kafkaConfig.put(
                "scan.startup.timestamp-millis", String.valueOf(System.currentTimeMillis()));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.noRestart());

        ThreadLocalRandom random = ThreadLocalRandom.current();
        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put("bucket", String.valueOf(random.nextInt(3) + 1));
        tableConfig.put("sink.parallelism", String.valueOf(random.nextInt(3) + 1));
        KafkaSyncTableAction action =
                new KafkaSyncTableAction(
                        kafkaConfig,
                        warehouse,
                        database,
                        tableName,
                        Collections.emptyList(),
                        Collections.singletonList("id"),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        tableConfig);
        action.build(env);
        JobClient client = env.executeAsync();

        waitJobRunning(client);

        try {
            writeRecordsToKafka(topic, readLines("kafka/ogg/table/startupmode/ogg-data-2.txt"));
        } catch (Exception e) {
            throw new Exception("Failed to write ogg data to Kafka.", e);
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
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.800000011920929]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.75]");
        waitForResult(expected, table, rowType, primaryKeys);
    }

    @Test
    @Timeout(60)
    public void testStarUpOptionEarliest() throws Exception {
        final String topic = "start_up_earliest";
        createTestTopic(topic, 1, 1);
        // ---------- Write the ogg json into Kafka -------------------
        List<String> lines = readLines("kafka/ogg/table/startupmode/ogg-data-1.txt");
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception("Failed to write ogg data to Kafka.", e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "ogg-json");
        kafkaConfig.put("topic", topic);
        kafkaConfig.put("scan.startup.mode", "earliest-offset");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.noRestart());

        ThreadLocalRandom random = ThreadLocalRandom.current();
        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put("bucket", String.valueOf(random.nextInt(3) + 1));
        tableConfig.put("sink.parallelism", String.valueOf(random.nextInt(3) + 1));
        KafkaSyncTableAction action =
                new KafkaSyncTableAction(
                        kafkaConfig,
                        warehouse,
                        database,
                        tableName,
                        Collections.emptyList(),
                        Collections.singletonList("id"),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        tableConfig);
        action.build(env);
        JobClient client = env.executeAsync();

        waitJobRunning(client);

        try {
            writeRecordsToKafka(topic, readLines("kafka/ogg/table/startupmode/ogg-data-2.txt"));
        } catch (Exception e) {
            throw new Exception("Failed to write ogg data to Kafka.", e);
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
                        "+I[101, scooter, Small 2-wheel scooter, 3.140000104904175]",
                        "+I[102, car battery, 12V car battery, 8.100000381469727]",
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.800000011920929]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.75]");
        waitForResult(expected, table, rowType, primaryKeys);
    }

    @Test
    @Timeout(60)
    public void testStarUpOptionGroup() throws Exception {
        final String topic = "start_up_group";
        createTestTopic(topic, 1, 1);
        // ---------- Write the ogg json into Kafka -------------------
        List<String> lines = readLines("kafka/ogg/table/startupmode/ogg-data-1.txt");
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception("Failed to write ogg data to Kafka.", e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "ogg-json");
        kafkaConfig.put("topic", topic);
        kafkaConfig.put("scan.startup.mode", "group-offsets");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.noRestart());

        ThreadLocalRandom random = ThreadLocalRandom.current();
        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put("bucket", String.valueOf(random.nextInt(3) + 1));
        tableConfig.put("sink.parallelism", String.valueOf(random.nextInt(3) + 1));
        KafkaSyncTableAction action =
                new KafkaSyncTableAction(
                        kafkaConfig,
                        warehouse,
                        database,
                        tableName,
                        Collections.emptyList(),
                        Collections.singletonList("id"),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        tableConfig);
        action.build(env);
        JobClient client = env.executeAsync();

        waitJobRunning(client);

        try {
            writeRecordsToKafka(topic, readLines("kafka/ogg/table/startupmode/ogg-data-2.txt"));
        } catch (Exception e) {
            throw new Exception("Failed to write ogg data to Kafka.", e);
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
                        "+I[101, scooter, Small 2-wheel scooter, 3.140000104904175]",
                                "+I[102, car battery, 12V car battery, 8.100000381469727]",
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.800000011920929]",
                                "+I[104, hammer, 12oz carpenter's hammer, 0.75]");
        waitForResult(expected, table, rowType, primaryKeys);
    }
}
