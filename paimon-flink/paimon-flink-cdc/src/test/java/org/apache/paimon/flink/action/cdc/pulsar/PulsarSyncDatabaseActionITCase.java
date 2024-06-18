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

package org.apache.paimon.flink.action.cdc.pulsar;

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
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_PARTITION_DISCOVERY_INTERVAL_MS;
import static org.apache.paimon.flink.action.cdc.pulsar.PulsarActionUtils.TOPIC;
import static org.apache.paimon.flink.action.cdc.pulsar.PulsarActionUtils.TOPIC_PATTERN;
import static org.apache.paimon.flink.action.cdc.pulsar.PulsarActionUtils.VALUE_FORMAT;

/** IT cases for {@link PulsarSyncDatabaseAction}. */
public class PulsarSyncDatabaseActionITCase extends PulsarActionITCaseBase {

    @Test
    @Timeout(60)
    public void testSchemaEvolutionMultiTopic() throws Exception {
        final String topic1 = "schema_evolution_0";
        final String topic2 = "schema_evolution_1";
        final String topic3 = "schema_evolution_2";
        boolean writeOne = false;
        int fileCount = 3;
        topics = Arrays.asList(topic1, topic2, topic3);
        topics.forEach(topic -> createTopic(topic, 1));

        // ---------- Write the Canal json into Pulsar -------------------

        for (int i = 0; i < fileCount; i++) {
            try {
                sendMessages(
                        topics.get(i),
                        getMessages(
                                "kafka/canal/database/schemaevolution/topic"
                                        + i
                                        + "/canal-data-1.txt"));
            } catch (Exception e) {
                throw new Exception("Failed to write canal data to Pulsar.", e);
            }
        }

        Map<String, String> pulsarConfig = getBasicPulsarConfig();
        pulsarConfig.put(PULSAR_PARTITION_DISCOVERY_INTERVAL_MS.key(), "-1");
        pulsarConfig.put(VALUE_FORMAT.key(), "canal-json");
        if (ThreadLocalRandom.current().nextBoolean()) {
            pulsarConfig.put(TOPIC.key(), String.join(";", topics));
        } else {
            pulsarConfig.put(TOPIC_PATTERN.key(), "schema_evolution_.+");
        }

        PulsarSyncDatabaseAction action =
                syncDatabaseActionBuilder(pulsarConfig)
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
        topics = Collections.singletonList(topic);
        topics.forEach(t -> createTopic(t, 1));

        // ---------- Write the Canal json into Pulsar -------------------

        for (int i = 0; i < fileCount; i++) {
            try {
                sendMessages(
                        topics.get(0),
                        getMessages(
                                "kafka/canal/database/schemaevolution/topic"
                                        + i
                                        + "/canal-data-1.txt"));
            } catch (Exception e) {
                throw new Exception("Failed to write canal data to Pulsar.", e);
            }
        }

        Map<String, String> pulsarConfig = getBasicPulsarConfig();
        pulsarConfig.put(PULSAR_PARTITION_DISCOVERY_INTERVAL_MS.key(), "-1");
        pulsarConfig.put(VALUE_FORMAT.key(), "canal-json");
        pulsarConfig.put(TOPIC.key(), String.join(";", topics));

        PulsarSyncDatabaseAction action =
                syncDatabaseActionBuilder(pulsarConfig)
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
            try {
                sendMessages(
                        writeOne ? topics.get(0) : topics.get(i),
                        getMessages(
                                "kafka/canal/database/schemaevolution/topic"
                                        + i
                                        + "/canal-data-2.txt"));
            } catch (Exception e) {
                throw new Exception("Failed to write canal data to Pulsar.", e);
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
                sendMessages(
                        writeOne ? topics.get(0) : topics.get(i),
                        getMessages(
                                "kafka/canal/database/schemaevolution/topic"
                                        + i
                                        + "/canal-data-3.txt"));
            } catch (Exception e) {
                throw new Exception("Failed to write canal data to Pulsar.", e);
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
}
