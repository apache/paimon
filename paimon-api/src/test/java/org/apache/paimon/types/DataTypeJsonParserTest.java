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

package org.apache.paimon.types;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DataTypeJsonParser}. */
class DataTypeJsonParserTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void parseDataFieldWithoutIdAutoAssigns() throws Exception {
        ObjectNode json = MAPPER.createObjectNode();
        json.put("name", "x");
        json.put("type", "INT");

        DataField field = DataTypeJsonParser.parseDataField(json);
        assertThat(field.id()).isZero();
        assertThat(field.name()).isEqualTo("x");
        assertThat(field.type()).isEqualTo(new IntType());
    }

    @Test
    void parseDataFieldKeepsExplicitId() throws Exception {
        ObjectNode json = MAPPER.createObjectNode();
        json.put("id", 7);
        json.put("name", "x");
        json.put("type", "INT");

        DataField field = DataTypeJsonParser.parseDataField(json);
        assertThat(field.id()).isEqualTo(7);
    }

    @Test
    void parseRowWithoutFieldIdsAutoAssignsSequentially() throws Exception {
        JsonNode json =
                MAPPER.readTree(
                        "{\"type\":\"ROW\",\"fields\":[{\"name\":\"a\",\"type\":\"INT\"},"
                                + "{\"name\":\"b\",\"type\":\"STRING\"}]}");

        DataType type = DataTypeJsonParser.parseDataType(json);
        assertThat(type)
                .isEqualTo(
                        new RowType(
                                Arrays.asList(
                                        new DataField(0, "a", new IntType()),
                                        new DataField(1, "b", DataTypes.STRING()))));
    }
}
