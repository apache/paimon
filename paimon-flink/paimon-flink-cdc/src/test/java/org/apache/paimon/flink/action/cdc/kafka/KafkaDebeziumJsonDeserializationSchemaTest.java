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

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

/**
 * Unit tests for {@link KafkaDebeziumJsonDeserializationSchema}. Ensures that deserialization
 * succeeds when Kafka keys are not valid JSON and verifies that the value payload is still parsed
 * correctly.
 */
public class KafkaDebeziumJsonDeserializationSchemaTest {

    @Test
    public void testDeserializeWithNonJsonKey() throws Exception {
        KafkaDebeziumJsonDeserializationSchema schema =
                new KafkaDebeziumJsonDeserializationSchema();

        byte[] rawKey = "non-json-key".getBytes(StandardCharsets.UTF_8);
        byte[] jsonValue = "{\"after\":{\"id\":1},\"op\":\"c\"}".getBytes(StandardCharsets.UTF_8);

        CdcSourceRecord record =
                schema.deserialize(new ConsumerRecord<>("topic", 0, 0L, rawKey, jsonValue));

        Assertions.assertNotNull(record, "Deserialization should succeed and return a record");
        Assertions.assertNull(record.getKey(), "Key should be null when the Kafka key is not JSON");

        JsonNode valueNode = (JsonNode) record.getValue();
        Assertions.assertEquals(
                1,
                valueNode.get("after").get("id").asInt(),
                "Value JSON should be parsed correctly");
    }
}
