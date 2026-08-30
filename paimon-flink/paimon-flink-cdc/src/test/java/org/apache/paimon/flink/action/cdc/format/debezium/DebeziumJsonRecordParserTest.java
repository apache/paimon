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

package org.apache.paimon.flink.action.cdc.format.debezium;

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.schema.Schema;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DebeziumJsonRecordParser}. */
public class DebeziumJsonRecordParserTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    public void testPrimaryKeysFromSchemaEnabledKey() throws Exception {
        JsonNode key =
                OBJECT_MAPPER.readTree(
                        "{\"schema\":{\"type\":\"struct\",\"fields\":["
                                + "{\"type\":\"int64\",\"optional\":false,\"field\":\"id\"},"
                                + "{\"type\":\"string\",\"optional\":false,\"field\":\"tenant\"}]},"
                                + "\"payload\":{\"id\":1,\"tenant\":\"A\"}}");

        assertPrimaryKeys(key, value(null), "id", "tenant");
    }

    @Test
    public void testPrimaryKeysFromSchemaLessKey() throws Exception {
        JsonNode key = OBJECT_MAPPER.readTree("{\"id\":1,\"tenant\":\"A\"}");

        assertPrimaryKeys(key, value(null), "id", "tenant");
    }

    @Test
    public void testPrimaryKeysFromValueTakePrecedence() throws Exception {
        JsonNode key = OBJECT_MAPPER.readTree("{\"id\":1}");

        assertPrimaryKeys(key, value("[\"tenant\"]"), "tenant");
    }

    @Test
    public void testEmptyPrimaryKeysFallBackToKey() throws Exception {
        JsonNode key = OBJECT_MAPPER.readTree("{\"id\":1}");

        assertPrimaryKeys(key, value("[]"), "id");
    }

    private static JsonNode value(String primaryKeys) throws Exception {
        String primaryKeyField = primaryKeys == null ? "" : "\"pkNames\":" + primaryKeys + ",";
        return OBJECT_MAPPER.readTree(
                "{"
                        + primaryKeyField
                        + "\"before\":null,"
                        + "\"after\":{\"id\":1,\"tenant\":\"A\",\"name\":\"Alice\"},"
                        + "\"source\":{\"db\":\"test\",\"table\":\"users\"},"
                        + "\"op\":\"c\"}");
    }

    private static void assertPrimaryKeys(JsonNode key, JsonNode value, String... primaryKeys) {
        DebeziumJsonRecordParser parser =
                new DebeziumJsonRecordParser(TypeMapping.defaultMapping(), Collections.emptyList());
        Schema schema = parser.buildSchema(new CdcSourceRecord("users", key, value));

        assertThat(schema).isNotNull();
        assertThat(schema.primaryKeys()).containsExactly(primaryKeys);
    }
}
