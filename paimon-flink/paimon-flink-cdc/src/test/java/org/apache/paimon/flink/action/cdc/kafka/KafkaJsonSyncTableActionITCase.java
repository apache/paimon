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

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.VALUE_FORMAT;

/** IT cases for {@link KafkaSyncTableAction}. */
public class KafkaJsonSyncTableActionITCase extends KafkaSyncTableActionITCase {

    @Test
    @Timeout(90)
    public void testSchemaEvolution() throws Exception {
        String topic = "schema-evolution";
        Map<String, String> tableOptions = new HashMap<>();

        createTestTopic(topic, 1, 1);
        writeRecordsToKafka(topic, "kafka/json/table/schemaevolution/json-data-1.txt");

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), "json");
        kafkaConfig.put(TOPIC.key(), topic);
        KafkaSyncTableAction action =
                syncTableActionBuilder(kafkaConfig)
                        .withTableConfig(getBasicTableConfig())
                        .withTableConfig(tableOptions)
                        .build();

        runActionWithDefaultEnv(action);

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()},
                        new String[] {"a", "b", "event_tm"});

        waitForResult(
                Arrays.asList("+I[a1, b1, 2024-05-22 09:50:40]", "+I[a2, b2, 2024-05-23 10:20:56]"),
                getFileStoreTable(tableName),
                rowType,
                Collections.emptyList());

        writeRecordsToKafka(
                topic, "kafka/json/table/schemaevolution/json-data-2.txt", "schemaevolution");

        rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"a", "b", "event_tm", "c"});

        waitForResult(
                Arrays.asList(
                        "+I[a1, b1, 2024-05-22 09:50:40, NULL]",
                        "+I[a2, b2, 2024-05-23 10:20:56, NULL]",
                        "+I[a3, b3, 2024-05-22 19:50:40, NULL]",
                        "+I[a4, b4, 2024-05-23 15:20:56, c4]"),
                getFileStoreTable(tableName),
                rowType,
                Collections.emptyList());
    }

    @Test
    @Timeout(90)
    public void testComputedColumn() throws Exception {
        String topic = "computed_column";
        Map<String, String> tableOptions = new HashMap<>();

        createTestTopic(topic, 1, 1);
        writeRecordsToKafka(topic, "kafka/json/table/computedcolumn/json-data-1.txt");

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), "json");
        kafkaConfig.put(TOPIC.key(), topic);
        KafkaSyncTableAction action =
                syncTableActionBuilder(kafkaConfig)
                        .withTableConfig(getBasicTableConfig())
                        .withPartitionKeys("pt")
                        .withTableConfig(tableOptions)
                        .withComputedColumnArgs("pt=date_format(event_tm, yyyyMMdd)")
                        .build();

        runActionWithDefaultEnv(action);

        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"a", "b", "event_tm", "pt"});
        waitForResult(
                Arrays.asList(
                        "+I[a1, b1, 2024-05-20 20:50:30, 20240520]",
                        "+I[a2, b2, 2024-05-21 18:10:46, 20240521]"),
                getFileStoreTable(tableName),
                rowType,
                Collections.emptyList());
    }
}
