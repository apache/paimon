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

import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_PARTITION_DISCOVERY_INTERVAL_MS;
import static org.apache.paimon.flink.action.cdc.pulsar.PulsarActionUtils.TOPIC;
import static org.apache.paimon.flink.action.cdc.pulsar.PulsarActionUtils.VALUE_FORMAT;

/** IT cases for {@link PulsarDebeziumSyncTableActionITCase}. */
public class PulsarDebeziumSyncTableActionITCase extends PulsarActionITCaseBase {

    @Test
    @Timeout(120)
    public void testSchemaEvolution() throws Exception {
        runSingleTableSchemaEvolution("schemaevolution");
    }

    private void runSingleTableSchemaEvolution(String sourceDir) throws Exception {
        final String topic = "schema_evolution";
        createTopic(topic, 1);
        // ---------- Write the debezium json into Pulsar -------------------
        sendMessages(
                topic,
                getMessages(
                        String.format("kafka/debezium/table/%s/debezium-data-1.txt", sourceDir)));

        Map<String, String> pulsarConfig = getBasicPulsarConfig();
        pulsarConfig.put(PULSAR_PARTITION_DISCOVERY_INTERVAL_MS.key(), "-1");
        pulsarConfig.put(TOPIC.key(), topic);
        pulsarConfig.put(VALUE_FORMAT.key(), "debezium-json");

        PulsarSyncTableAction action =
                syncTableActionBuilder(pulsarConfig)
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

        // ---------- Write the debezium json into Kafka -------------------
        sendMessages(
                topic,
                getMessages(
                        String.format("kafka/debezium/table/%s/debezium-data-2.txt", sourceDir)));

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

        sendMessages(
                topic,
                getMessages(
                        String.format("kafka/debezium/table/%s/debezium-data-3.txt", sourceDir)));

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
}
