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

package org.apache.paimon.flink.action.cdc.format.canal;

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.RowKind;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link CanalRecordParser}. */
public class CanalRecordParserTest {

    /**
     * A Canal {@code UPDATE} event may carry two equal row images in one batch (natural for a table
     * without a primary key, or a bulk update that sets several rows to the same values). The old
     * positional pairing was built with {@code Collectors.toMap} keyed by the row node, so equal
     * images collided and the parser threw {@code IllegalStateException: Duplicate key}. Pairing by
     * position must instead emit a DELETE + INSERT for every row.
     */
    @Test
    public void testUpdateWithDuplicateRowImagesPairsByPosition() throws Exception {
        String json =
                "{"
                        + "\"database\":\"test_db\",\"table\":\"test_table\","
                        + "\"pkNames\":[\"id\"],\"isDdl\":false,\"type\":\"UPDATE\","
                        + "\"mysqlType\":{\"id\":\"int\",\"name\":\"varchar(20)\"},"
                        + "\"data\":[{\"id\":\"1\",\"name\":\"x\"},{\"id\":\"1\",\"name\":\"x\"}],"
                        + "\"old\":[{\"name\":\"a\"},{\"name\":\"b\"}]"
                        + "}";

        CanalRecordParser parser =
                new CanalRecordParser(TypeMapping.defaultMapping(), Collections.emptyList());
        JsonNode rootNode = new ObjectMapper().readValue(json, JsonNode.class);
        CdcSourceRecord cdcRecord = new CdcSourceRecord(rootNode);
        parser.buildSchema(cdcRecord);

        List<RichCdcMultiplexRecord> records = parser.extractRecords();

        // Two updated rows, each emits a DELETE (old image) + an INSERT (new image).
        assertThat(records).hasSize(4);
        assertThat(
                        records.stream()
                                .filter(
                                        r ->
                                                r.toRichCdcRecord().toCdcRecord().kind()
                                                        == RowKind.DELETE)
                                .count())
                .isEqualTo(2);
        assertThat(
                        records.stream()
                                .filter(
                                        r ->
                                                r.toRichCdcRecord().toCdcRecord().kind()
                                                        == RowKind.INSERT)
                                .count())
                .isEqualTo(2);
    }
}
