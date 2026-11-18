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

package org.apache.paimon.flink.action.cdc.format.ogg;

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.RowKind;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link OggRecordParser}. */
public class OggRecordParserTest {

    private static final String INSERT_EVENT =
            "{\"table\":\"TEST.CUSTOMER\",\"op_type\":\"I\","
                    + "\"op_ts\":\"2026-10-15 09:13:37.019234\","
                    + "\"current_ts\":\"2026-10-15T09:13:41.537004\","
                    + "\"pos\":\"00000627270001984891\","
                    + "\"primary_keys\":[\"ID\"],"
                    + "\"before\":null,"
                    + "\"after\":{\"ID\":\"1\",\"NAME\":null,\"EMPTY_VALUE\":\"\","
                    + "\"LITERAL_NULL\":\"null\"}}";

    /**
     * A JSON null is a SQL NULL, which must stay distinct from the empty string and from the
     * literal string "null". The parser used to convert null to the string "null".
     */
    @Test
    public void testJsonNullIsNotConvertedToString() throws Exception {
        OggRecordParser parser =
                new OggRecordParser(TypeMapping.defaultMapping(), Collections.emptyList());
        List<RichCdcMultiplexRecord> records = new ArrayList<>();

        parser.flatMap(toRecord(INSERT_EVENT), new ListCollector<>(records));
        assertThat(records).hasSize(1);

        CdcRecord cdcRecord = records.get(0).toRichCdcRecord().toCdcRecord();
        assertThat(cdcRecord.kind()).isEqualTo(RowKind.INSERT);

        Map<String, String> data = cdcRecord.data();
        assertThat(data).containsKey("NAME");
        assertThat(data.get("NAME")).isNull();
        assertThat(data).containsEntry("EMPTY_VALUE", "");
        assertThat(data).containsEntry("LITERAL_NULL", "null");
        assertThat(data).containsEntry("ID", "1");
    }

    private static CdcSourceRecord toRecord(String json) throws Exception {
        return new CdcSourceRecord(new ObjectMapper().readValue(json, JsonNode.class));
    }
}
