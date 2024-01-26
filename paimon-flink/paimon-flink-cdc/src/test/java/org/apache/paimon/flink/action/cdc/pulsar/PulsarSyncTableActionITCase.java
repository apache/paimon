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

import org.apache.paimon.catalog.Identifier;
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

/** IT cases for {@link PulsarSyncTableAction}. */
public class PulsarSyncTableActionITCase extends PulsarActionITCaseBase {

    @Test
    @Timeout(120)
    public void testSchemaEvolution() throws Exception {
        runSingleTableSchemaEvolution("schemaevolution");
    }

    private void runSingleTableSchemaEvolution(String sourceDir) throws Exception {
        final String topic = "schema_evolution";
        topics = Collections.singletonList(topic);
        createTopic(topic, 1);
        // ---------- Write the Canal json into Pulsar -------------------
        sendMessages(
                topic,
                getMessages(String.format("kafka/canal/table/%s/canal-data-1.txt", sourceDir)));

        Map<String, String> pulsarConfig = getBasicPulsarConfig();
        pulsarConfig.put(PULSAR_PARTITION_DISCOVERY_INTERVAL_MS.key(), "-1");
        if (ThreadLocalRandom.current().nextBoolean()) {
            pulsarConfig.put(TOPIC.key(), topic);
        } else {
            pulsarConfig.put(TOPIC_PATTERN.key(), "schema_.*");
        }
        pulsarConfig.put(VALUE_FORMAT.key(), "canal-json");

        PulsarSyncTableAction action =
                syncTableActionBuilder(pulsarConfig)
                        .withPartitionKeys("pt")
                        .withPrimaryKeys("pt", "_id")
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
                            DataTypes.INT().notNull(),
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(10)
                        },
                        new String[] {"pt", "_id", "v1"});
        List<String> primaryKeys = Arrays.asList("pt", "_id");
        List<String> expected = Arrays.asList("+I[1, 1, one]", "+I[1, 2, two]", "+I[2, 4, four]");
        waitForResult(expected, table, rowType, primaryKeys);

        sendMessages(
                topic,
                getMessages(String.format("kafka/canal/table/%s/canal-data-2.txt", sourceDir)));

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

        sendMessages(
                topic,
                getMessages(String.format("kafka/canal/table/%s/canal-data-3.txt", sourceDir)));

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

        sendMessages(
                topic,
                getMessages(String.format("kafka/canal/table/%s/canal-data-4.txt", sourceDir)));

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

        sendMessages(
                topic,
                getMessages(String.format("kafka/canal/table/%s/canal-data-5.txt", sourceDir)));

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
    public void testWaterMarkSyncTable() throws Exception {
        String topic = "watermark";
        topics = Collections.singletonList(topic);
        createTopic(topic, 1);
        sendMessages(topic, getMessages("kafka/canal/table/watermark/canal-data-1.txt"));

        Map<String, String> pulsarConfig = getBasicPulsarConfig();
        pulsarConfig.put(PULSAR_PARTITION_DISCOVERY_INTERVAL_MS.key(), "-1");
        pulsarConfig.put(TOPIC.key(), topic);
        pulsarConfig.put(VALUE_FORMAT.key(), "canal-json");
        Map<String, String> config = getBasicTableConfig();
        config.put("tag.automatic-creation", "watermark");
        config.put("tag.creation-period", "hourly");

        PulsarSyncTableAction action =
                syncTableActionBuilder(pulsarConfig)
                        .withPartitionKeys("pt")
                        .withPrimaryKeys("pt", "_id")
                        .withTableConfig(config)
                        .build();
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
}
