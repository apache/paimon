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
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for {@link KafkaCanalSyncTableActionITCase}. */
public class KafkaCanalSyncTableActionITCase extends KafkaActionITCaseBase {

    @Test
    @Timeout(60)
    public void testSchemaEvolution() throws Exception {
        runSingleTableSchemaEvolution("schemaevolution");
    }

    @Test
    @Timeout(60)
    public void testSchemaEvolutionWithMissingDdl() throws Exception {
        runSingleTableSchemaEvolution("schemaevolutionmissingddl");
    }

    private void runSingleTableSchemaEvolution(String sourceDir) throws Exception {
        final String topic = "schema_evolution";
        createTestTopic(topic, 1, 1);
        // ---------- Write the Canal json into Kafka -------------------
        List<String> lines =
                readLines(String.format("kafka.canal/table/%s/canal-data-1.txt", sourceDir));
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception("Failed to write canal data to Kafka.", e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "canal-json");
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
                        Collections.singletonList("pt"),
                        Arrays.asList("pt", "_id"),
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
                            DataTypes.INT().notNull(),
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(10)
                        },
                        new String[] {"pt", "_id", "v1"});
        List<String> primaryKeys = Arrays.asList("pt", "_id");
        List<String> expected = Arrays.asList("+I[1, 1, one]", "+I[1, 2, two]", "+I[2, 4, four]");
        waitForResult(expected, table, rowType, primaryKeys);

        try {
            writeRecordsToKafka(
                    topic,
                    readLines(String.format("kafka.canal/table/%s/canal-data-2.txt", sourceDir)));
        } catch (Exception e) {
            throw new Exception("Failed to write canal data to Kafka.", e);
        }
        rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(10),
                            DataTypes.INT()
                        },
                        new String[] {"pt", "_id", "v1", "v2"});
        expected =
                Arrays.asList(
                        "+I[1, 1, one, NULL]",
                        "+I[1, 2, second, NULL]",
                        "+I[2, 3, three, 30]",
                        "+I[2, 4, four, NULL]",
                        "+I[1, 5, five, 50]",
                        "+I[1, 6, six, 60]");
        waitForResult(expected, table, rowType, primaryKeys);

        try {
            writeRecordsToKafka(
                    topic,
                    readLines(String.format("kafka.canal/table/%s/canal-data-3.txt", sourceDir)));
        } catch (Exception e) {
            throw new Exception("Failed to write canal data to Kafka.", e);
        }
        rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(10),
                            DataTypes.BIGINT()
                        },
                        new String[] {"pt", "_id", "v1", "v2"});
        expected =
                Arrays.asList(
                        "+I[1, 1, one, NULL]",
                        "+I[1, 2, second, NULL]",
                        "+I[2, 3, three, 30000000000]",
                        "+I[2, 4, four, NULL]",
                        "+I[1, 6, six, 60]",
                        "+I[2, 7, seven, 70000000000]",
                        "+I[2, 8, eight, 80000000000]");
        waitForResult(expected, table, rowType, primaryKeys);

        try {
            writeRecordsToKafka(
                    topic,
                    readLines(String.format("kafka.canal/table/%s/canal-data-4.txt", sourceDir)));
        } catch (Exception e) {
            throw new Exception("Failed to write canal data to Kafka.", e);
        }
        rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(20),
                            DataTypes.BIGINT(),
                            DataTypes.DECIMAL(8, 3),
                            DataTypes.VARBINARY(10),
                            DataTypes.FLOAT()
                        },
                        new String[] {"pt", "_id", "v1", "v2", "v3", "v4", "v5"});
        expected =
                Arrays.asList(
                        "+I[1, 1, one, NULL, NULL, NULL, NULL]",
                        "+I[1, 2, second, NULL, NULL, NULL, NULL]",
                        "+I[2, 3, three, 30000000000, NULL, NULL, NULL]",
                        "+I[2, 4, four, NULL, NULL, NULL, NULL]",
                        "+I[1, 6, six, 60, NULL, NULL, NULL]",
                        "+I[2, 7, seven, 70000000000, NULL, NULL, NULL]",
                        "+I[2, 8, very long string, 80000000000, NULL, NULL, NULL]",
                        "+I[1, 9, nine, 90000000000, 99999.999, [110, 105, 110, 101, 46, 98, 105, 110], 9.9]");
        waitForResult(expected, table, rowType, primaryKeys);

        try {
            writeRecordsToKafka(
                    topic,
                    readLines(String.format("kafka.canal/table/%s/canal-data-5.txt", sourceDir)));
        } catch (Exception e) {
            throw new Exception("Failed to write canal data to Kafka.", e);
        }
        rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(20),
                            DataTypes.BIGINT(),
                            DataTypes.DECIMAL(8, 3),
                            DataTypes.VARBINARY(20),
                            DataTypes.DOUBLE()
                        },
                        new String[] {"pt", "_id", "v1", "v2", "v3", "v4", "v5"});
        expected =
                Arrays.asList(
                        "+I[1, 1, one, NULL, NULL, NULL, NULL]",
                        "+I[1, 2, second, NULL, NULL, NULL, NULL]",
                        "+I[2, 3, three, 30000000000, NULL, NULL, NULL]",
                        "+I[2, 4, four, NULL, NULL, [102, 111, 117, 114, 46, 98, 105, 110, 46, 108, 111, 110, 103], 4.00000000004]",
                        "+I[1, 6, six, 60, NULL, NULL, NULL]",
                        "+I[2, 7, seven, 70000000000, NULL, NULL, NULL]",
                        "+I[2, 8, very long string, 80000000000, NULL, NULL, NULL]",
                        "+I[1, 9, nine, 90000000000, 99999.999, [110, 105, 110, 101, 46, 98, 105, 110, 46, 108, 111, 110, 103], 9.00000000009]");
        waitForResult(expected, table, rowType, primaryKeys);
    }

    @Test
    @Timeout(60)
    public void testMultipleSchemaEvolutions() throws Exception {
        final String topic = "schema_evolution_multiple";
        createTestTopic(topic, 1, 1);
        // ---------- Write the Canal json into Kafka -------------------
        List<String> lines =
                readLines("kafka.canal/table/schemaevolutionmultiple/canal-data-1.txt");
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception("Failed to write canal data to Kafka.", e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "canal-json");

        kafkaConfig.put("topic", topic);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.noRestart());

        KafkaSyncTableAction action =
                new KafkaSyncTableAction(
                        kafkaConfig,
                        warehouse,
                        database,
                        tableName,
                        Collections.emptyList(),
                        Collections.singletonList("_id"),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        action.build(env);
        JobClient client = env.executeAsync();

        waitJobRunning(client);

        testSchemaEvolutionMultipleImpl(topic);
    }

    private void testSchemaEvolutionMultipleImpl(String topic) throws Exception {
        FileStoreTable table = getFileStoreTable(tableName);

        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(10),
                            DataTypes.INT(),
                            DataTypes.VARCHAR(10)
                        },
                        new String[] {"_id", "v1", "v2", "v3"});
        List<String> primaryKeys = Collections.singletonList("_id");
        List<String> expected = Collections.singletonList("+I[1, one, 10, string_1]");
        waitForResult(expected, table, rowType, primaryKeys);

        try {
            writeRecordsToKafka(
                    topic, readLines("kafka.canal/table/schemaevolutionmultiple/canal-data-2.txt"));
        } catch (Exception e) {
            throw new Exception("Failed to write canal data to Kafka.", e);
        }
        rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(20),
                            DataTypes.BIGINT(),
                            DataTypes.VARCHAR(10),
                            DataTypes.INT(),
                            DataTypes.DOUBLE(),
                            DataTypes.DECIMAL(5, 3),
                            DataTypes.VARCHAR(10)
                        },
                        new String[] {"_id", "v1", "v2", "v3", "v4", "v5", "v6", "$% ^,& *("});
        expected =
                Arrays.asList(
                        "+I[1, one, 10, string_1, NULL, NULL, NULL, NULL]",
                        "+I[2, long_string_two, 2000000000000, string_2, 20, 20.5, 20.002, test_2]");
        waitForResult(expected, table, rowType, primaryKeys);
    }

    @Test
    @Timeout(60)
    public void testAllTypes() throws Exception {

        // the first round checks for table creation
        // the second round checks for running the action on an existing table
        for (int i = 0; i < 2; i++) {
            testAllTypesOnce();
            Thread.sleep(3000);
        }
    }

    private void testAllTypesOnce() throws Exception {
        final String topic = "all_type" + UUID.randomUUID();
        createTestTopic(topic, 1, 1);

        // ---------- Write the Canal json into Kafka -------------------
        List<String> lines = readLines("kafka.canal/table/alltype/canal-data.txt");
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception("Failed to write canal data to Kafka.", e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "canal-json");

        kafkaConfig.put("topic", topic);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.noRestart());

        KafkaSyncTableAction action =
                new KafkaSyncTableAction(
                        kafkaConfig,
                        warehouse,
                        database,
                        tableName,
                        Collections.singletonList("pt"),
                        Arrays.asList("pt", "_id"),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        action.build(env);
        JobClient client = env.executeAsync();

        waitJobRunning(client);

        testAllTypesImpl();
        client.cancel().get();
    }

    private void testAllTypesImpl() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(), // _id
                            DataTypes.DECIMAL(2, 1).notNull(), // pt
                            DataTypes.BOOLEAN(), // _tinyint1
                            DataTypes.BOOLEAN(), // _boolean
                            DataTypes.BOOLEAN(), // _bool
                            DataTypes.TINYINT(), // _tinyint
                            DataTypes.SMALLINT(), // _tinyint_unsigned
                            DataTypes.SMALLINT(), // _tinyint_unsigned_zerofill
                            DataTypes.SMALLINT(), // _smallint
                            DataTypes.INT(), // _smallint_unsigned
                            DataTypes.INT(), // _smallint_unsigned_zerofill
                            DataTypes.INT(), // _mediumint
                            DataTypes.BIGINT(), // _mediumint_unsigned
                            DataTypes.BIGINT(), // _mediumint_unsigned_zerofill
                            DataTypes.INT(), // _int
                            DataTypes.BIGINT(), // _int_unsigned
                            DataTypes.BIGINT(), // _int_unsigned_zerofill
                            DataTypes.BIGINT(), // _bigint
                            DataTypes.DECIMAL(20, 0), // _bigint_unsigned
                            DataTypes.DECIMAL(20, 0), // _bigint_unsigned_zerofill
                            DataTypes.DECIMAL(20, 0), // _serial
                            DataTypes.FLOAT(), // _float
                            DataTypes.FLOAT(), // _float_unsigned
                            DataTypes.FLOAT(), // _float_unsigned_zerofill
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
                            DataTypes.DATE(), // _date
                            DataTypes.TIMESTAMP(0), // _datetime
                            DataTypes.TIMESTAMP(3), // _datetime3
                            DataTypes.TIMESTAMP(6), // _datetime6
                            DataTypes.TIMESTAMP(0), // _datetime_p
                            DataTypes.TIMESTAMP(2), // _datetime_p2
                            DataTypes.TIMESTAMP(6), // _timestamp
                            DataTypes.TIMESTAMP(0), // _timestamp0
                            DataTypes.CHAR(10), // _char
                            DataTypes.VARCHAR(20), // _varchar
                            DataTypes.STRING(), // _tinytext
                            DataTypes.STRING(), // _text
                            DataTypes.STRING(), // _mediumtext
                            DataTypes.STRING(), // _longtext
                            DataTypes.VARBINARY(10), // _bin
                            DataTypes.VARBINARY(20), // _varbin
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
                            DataTypes.ARRAY(DataTypes.STRING()) // _set
                        },
                        new String[] {
                            "_id",
                            "pt",
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
        FileStoreTable table = getFileStoreTable(tableName);
        List<String> expected =
                Arrays.asList(
                        "+I["
                                + "1, 1.1, "
                                + "true, true, false, 1, 2, 3, "
                                + "1000, 2000, 3000, "
                                + "100000, 200000, 300000, "
                                + "1000000, 2000000, 3000000, "
                                + "10000000000, 20000000000, 30000000000, 40000000000, "
                                + "1.5, 2.5, 3.5, "
                                + "1.000001, 2.000002, 3.000003, "
                                + "1.000011, 2.000022, 3.000033, "
                                + "1.000111, 2.000222, 3.000333, "
                                + "12345.110, 12345.220, 12345.330, "
                                + "123456789876543212345678987654321.110, 123456789876543212345678987654321.220, 123456789876543212345678987654321.330, "
                                + "11111, 22222, 33333, "
                                + "19439, "
                                // display value of datetime is not affected by timezone
                                + "2023-03-23T14:30:05, 2023-03-23T14:30:05.123, 2023-03-23T14:30:05.123456, "
                                + "2023-03-24T14:30, 2023-03-24T14:30:05.120, "
                                + "2023-03-23T15:00:10.123456, 2023-03-23T00:10, "
                                + "Paimon, Apache Paimon, Apache Paimon MySQL TINYTEXT Test Data, Apache Paimon MySQL Test Data, Apache Paimon MySQL MEDIUMTEXT Test Data, Apache Paimon MySQL Long Test Data, "
                                + "[98, 121, 116, 101, 115], "
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
                                + "[a, b]"
                                + "]",
                        "+I["
                                + "2, 2.2, "
                                + "NULL, NULL, NULL, NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, 50000000000, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, "
                                + "NULL, NULL, "
                                + "NULL, NULL, NULL, NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, NULL, NULL, NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL, "
                                + "NULL"
                                + "]");

        waitForResult(expected, table, rowType, Arrays.asList("pt", "_id"));
    }

    @Test
    @Timeout(60)
    public void testNotSupportFormat() throws Exception {
        final String topic = "not_support";
        createTestTopic(topic, 1, 1);
        // ---------- Write the Canal json into Kafka -------------------
        List<String> lines = readLines("kafka.canal/table/schemaevolution/canal-data-1.txt");
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception("Failed to write canal data to Kafka.", e);
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
                        Collections.singletonList("pt"),
                        Arrays.asList("pt", "_id"),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        tableConfig);

        assertThatThrownBy(() -> action.build(env))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("This format: ogg-json is not support.");
    }

    @Test
    @Timeout(120)
    public void testKafkaNoNonDdlData() throws Exception {
        final String topic = "no_non_ddl_data";
        createTestTopic(topic, 1, 1);
        // ---------- Write the Canal json into Kafka -------------------
        List<String> lines = readLines("kafka.canal/table/nononddldata/canal-data-1.txt");
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception("Failed to write canal data to Kafka.", e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "canal-json");
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
                        Collections.singletonList("pt"),
                        Arrays.asList("pt", "_id"),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        tableConfig);

        assertThatThrownBy(() -> action.build(env))
                .isInstanceOf(Exception.class)
                .hasMessage("Could not get metadata from server,topic:no_non_ddl_data");
    }

    @Test
    @Timeout(60)
    public void testAssertSchemaCompatible() throws Exception {
        final String topic = "assert_schema_compatible";
        createTestTopic(topic, 1, 1);
        // ---------- Write the Canal json into Kafka -------------------
        List<String> lines = readLines("kafka.canal/table/schemaevolution/canal-data-1.txt");
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception("Failed to write canal data to Kafka.", e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "canal-json");
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
                        Collections.singletonList("pt"),
                        Arrays.asList("pt", "_id"),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        tableConfig);

        assertThatThrownBy(() -> action.build(env))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Paimon schema and Kafka schema are not compatible.\n"
                                + "Paimon fields are: [`k` STRING NOT NULL, `v1` STRING].\n"
                                + "Kafka fields are: [`pt` INT NOT NULL, `_id` INT NOT NULL, `v1` VARCHAR(10)]");
    }

    @Test
    @Timeout(60)
    public void testStarUpOptionSpecific() throws Exception {
        final String topic = "start_up_specific";
        createTestTopic(topic, 1, 1);
        // ---------- Write the Canal json into Kafka -------------------
        List<String> lines = readLines("kafka.canal/table/startupmode/canal-data-1.txt");
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception("Failed to write canal data to Kafka.", e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "canal-json");
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
                        Collections.singletonList("pt"),
                        Arrays.asList("pt", "_id"),
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
                            DataTypes.INT().notNull(),
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(10)
                        },
                        new String[] {"pt", "_id", "v1"});
        List<String> primaryKeys = Arrays.asList("pt", "_id");
        // topic has two records we read two
        List<String> expected = Collections.singletonList("+I[1, 2, two]");
        waitForResult(expected, table, rowType, primaryKeys);
    }

    @Test
    @Timeout(60)
    public void testStarUpOptionLatest() throws Exception {
        final String topic = "start_up_latest";
        createTestTopic(topic, 1, 1);
        // ---------- Write the Canal json into Kafka -------------------
        List<String> lines = readLines("kafka.canal/table/startupmode/canal-data-1.txt");
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception("Failed to write canal data to Kafka.", e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "canal-json");
        kafkaConfig.put("topic", topic);
        kafkaConfig.put("scan.startup.mode", "latest-offset");

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
                        Collections.singletonList("pt"),
                        Arrays.asList("pt", "_id"),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        tableConfig);
        action.build(env);
        JobClient client = env.executeAsync();

        waitJobRunning(client);

        try {
            writeRecordsToKafka(topic, readLines("kafka.canal/table/startupmode/canal-data-2.txt"));
        } catch (Exception e) {
            throw new Exception("Failed to write canal data to Kafka.", e);
        }
        FileStoreTable table = getFileStoreTable(tableName);

        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(10)
                        },
                        new String[] {"pt", "_id", "v1"});
        List<String> primaryKeys = Arrays.asList("pt", "_id");
        // topic has four records we read two
        List<String> expected = Arrays.asList("+I[1, 3, three]", "+I[1, 4, four]");
        waitForResult(expected, table, rowType, primaryKeys);
    }

    @Test
    @Timeout(60)
    public void testStarUpOptionTimestamp() throws Exception {
        final String topic = "start_up_timestamp";
        createTestTopic(topic, 1, 1);
        // ---------- Write the Canal json into Kafka -------------------
        List<String> lines = readLines("kafka.canal/table/startupmode/canal-data-1.txt");
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception("Failed to write canal data to Kafka.", e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "canal-json");
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
                        Collections.singletonList("pt"),
                        Arrays.asList("pt", "_id"),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        tableConfig);
        action.build(env);
        JobClient client = env.executeAsync();

        waitJobRunning(client);

        try {
            writeRecordsToKafka(topic, readLines("kafka.canal/table/startupmode/canal-data-2.txt"));
        } catch (Exception e) {
            throw new Exception("Failed to write canal data to Kafka.", e);
        }
        FileStoreTable table = getFileStoreTable(tableName);

        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(10)
                        },
                        new String[] {"pt", "_id", "v1"});
        List<String> primaryKeys = Arrays.asList("pt", "_id");
        // topic has four records we read two
        List<String> expected = Arrays.asList("+I[1, 3, three]", "+I[1, 4, four]");
        waitForResult(expected, table, rowType, primaryKeys);
    }

    @Test
    @Timeout(60)
    public void testStarUpOptionEarliest() throws Exception {
        final String topic = "start_up_earliest";
        createTestTopic(topic, 1, 1);
        // ---------- Write the Canal json into Kafka -------------------
        List<String> lines = readLines("kafka.canal/table/startupmode/canal-data-1.txt");
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception("Failed to write canal data to Kafka.", e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "canal-json");
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
                        Collections.singletonList("pt"),
                        Arrays.asList("pt", "_id"),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        tableConfig);
        action.build(env);
        JobClient client = env.executeAsync();

        waitJobRunning(client);

        try {
            writeRecordsToKafka(topic, readLines("kafka.canal/table/startupmode/canal-data-2.txt"));
        } catch (Exception e) {
            throw new Exception("Failed to write canal data to Kafka.", e);
        }
        FileStoreTable table = getFileStoreTable(tableName);

        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(10)
                        },
                        new String[] {"pt", "_id", "v1"});
        List<String> primaryKeys = Arrays.asList("pt", "_id");
        // topic has four records we read all
        List<String> expected =
                Arrays.asList(
                        "+I[1, 1, one]", "+I[1, 2, two]", "+I[1, 3, three]", "+I[1, 4, four]");
        waitForResult(expected, table, rowType, primaryKeys);
    }

    @Test
    @Timeout(60)
    public void testStarUpOptionGroup() throws Exception {
        final String topic = "start_up_group";
        createTestTopic(topic, 1, 1);
        // ---------- Write the Canal json into Kafka -------------------
        List<String> lines = readLines("kafka.canal/table/startupmode/canal-data-1.txt");
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception("Failed to write canal data to Kafka.", e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "canal-json");
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
                        Collections.singletonList("pt"),
                        Arrays.asList("pt", "_id"),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        tableConfig);
        action.build(env);
        JobClient client = env.executeAsync();

        waitJobRunning(client);

        try {
            writeRecordsToKafka(topic, readLines("kafka.canal/table/startupmode/canal-data-2.txt"));
        } catch (Exception e) {
            throw new Exception("Failed to write canal data to Kafka.", e);
        }
        FileStoreTable table = getFileStoreTable(tableName);

        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(10)
                        },
                        new String[] {"pt", "_id", "v1"});
        List<String> primaryKeys = Arrays.asList("pt", "_id");
        // topic has four records we read all
        List<String> expected =
                Arrays.asList(
                        "+I[1, 1, one]", "+I[1, 2, two]", "+I[1, 3, three]", "+I[1, 4, four]");
        waitForResult(expected, table, rowType, primaryKeys);
    }
}
