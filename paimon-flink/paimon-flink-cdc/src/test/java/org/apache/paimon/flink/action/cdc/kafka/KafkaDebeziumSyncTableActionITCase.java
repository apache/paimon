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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.VALUE_FORMAT;

/** IT cases for {@link KafkaSyncTableAction}. */
public class KafkaDebeziumSyncTableActionITCase extends KafkaSyncTableActionITCase {

    private static final String DEBEZIUM = "debezium";

    @Test
    @Timeout(60)
    public void testSchemaEvolution() throws Exception {
        runSingleTableSchemaEvolution("schemaevolution", DEBEZIUM);
    }

    @Test
    @Timeout(60)
    public void testNotSupportFormat() throws Exception {
        testNotSupportFormat(DEBEZIUM);
    }

    @Test
    @Timeout(60)
    public void testAssertSchemaCompatible() throws Exception {
        testAssertSchemaCompatible(DEBEZIUM);
    }

    @Test
    @Timeout(60)
    public void testStarUpOptionSpecific() throws Exception {
        testStarUpOptionSpecific(DEBEZIUM);
    }

    @Test
    @Timeout(60)
    public void testStarUpOptionLatest() throws Exception {
        testStarUpOptionLatest(DEBEZIUM);
    }

    @Test
    @Timeout(60)
    public void testStarUpOptionTimestamp() throws Exception {
        testStarUpOptionTimestamp(DEBEZIUM);
    }

    @Test
    @Timeout(60)
    public void testStarUpOptionEarliest() throws Exception {
        testStarUpOptionEarliest(DEBEZIUM);
    }

    @Test
    @Timeout(60)
    public void testStarUpOptionGroup() throws Exception {
        testStarUpOptionGroup(DEBEZIUM);
    }

    @Test
    @Timeout(60)
    public void testComputedColumn() throws Exception {
        testComputedColumn(DEBEZIUM);
    }

    @Test
    @Timeout(60)
    public void testWaterMarkSyncTable() throws Exception {
        testWaterMarkSyncTable(DEBEZIUM);
    }

    @Test
    @Timeout(60)
    public void testKafkaBuildSchemaWithDelete() throws Exception {
        testKafkaBuildSchemaWithDelete(DEBEZIUM);
    }

    @Test
    @Timeout(60)
    public void testSchemaIncludeRecord() throws Exception {
        testSchemaIncludeRecord(DEBEZIUM);
    }

    @Test
    @Timeout(60)
    public void testSchemaEvolutionWithSchemaIncludeRecord() throws Exception {
        runSingleTableSchemaEvolutionWithSchemaIncludeRecord("schemaevolution", DEBEZIUM);
    }

    @Test
    @Timeout(60)
    public void testAllTypesWithSchema() throws Exception {
        testAllTypesWithSchemaImpl(DEBEZIUM);
    }

    @Test
    @Timeout(60)
    public void testMessageWithNullValue() throws Exception {
        final String topic = "test_null_value";
        createTestTopic(topic, 1, 1);

        writeRecordsToKafka(topic, "kafka/debezium/table/nullvalue/debezium-data-1.txt");
        // write null value
        kafkaProducer.send(new ProducerRecord<>(topic, null));
        writeRecordsToKafka(topic, "kafka/debezium/table/nullvalue/debezium-data-2.txt");

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), "debezium-json");
        kafkaConfig.put(TOPIC.key(), topic);
        KafkaSyncTableAction action =
                syncTableActionBuilder(kafkaConfig)
                        .withPrimaryKeys("id")
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.STRING().notNull(), DataTypes.STRING()},
                        new String[] {"id", "value"});
        waitForResult(
                Arrays.asList("+I[1, A]", "+I[2, B]"),
                getFileStoreTable(tableName),
                rowType,
                Collections.singletonList("id"));
    }
}
