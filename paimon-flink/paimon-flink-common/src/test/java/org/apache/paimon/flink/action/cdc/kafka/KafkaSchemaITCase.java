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

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KafkaSchema}. */
public class KafkaSchemaITCase extends KafkaActionITCaseBase {
    @Test
    @Timeout(60)
    public void testKafkaSchema() throws Exception {
        final String topic = "test_kafka_schema";
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

        KafkaSchema kafkaSchema =
                KafkaSchema.getKafkaSchema(Configuration.fromMap(kafkaConfig), topic);
        Map<String, DataType> fields = new LinkedHashMap<>();
        fields.put("pt", DataTypes.INT());
        fields.put("_id", DataTypes.INT());
        fields.put("v1", DataTypes.VARCHAR(10));
        String tableName = "schema_evolution_1";
        String databasesName = "paimon_sync_table";
        assertThat(kafkaSchema.fields()).isEqualTo(fields);
        assertThat(kafkaSchema.tableName()).isEqualTo(tableName);
        assertThat(kafkaSchema.databaseName()).isEqualTo(databasesName);
    }
}
