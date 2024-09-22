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

package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.flink.action.cdc.mongodb.MongoDBCdcTimestampExtractor;
import org.apache.paimon.flink.action.cdc.mysql.DebeziumEventTest;
import org.apache.paimon.flink.action.cdc.mysql.MysqlCdcTimestampExtractor;
import org.apache.paimon.flink.action.cdc.watermark.CdcDebeziumTimestampExtractor;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link CdcDebeziumTimestampExtractor}. */
public class CdcDebeziumTimestampExtractorITCase {

    private ObjectMapper objectMapper;

    @BeforeEach
    public void before() {
        objectMapper = new ObjectMapper();
        objectMapper
                .configure(JsonParser.Feature.ALLOW_COMMENTS, true)
                .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Test
    public void testMysqlCdcTimestampExtractor() throws Exception {
        MysqlCdcTimestampExtractor extractor = new MysqlCdcTimestampExtractor();

        JsonNode data = objectMapper.readValue("{\"payload\" : {\"ts_ms\": 1}}", JsonNode.class);
        CdcSourceRecord record = new CdcSourceRecord(data.toString());
        assertThat(extractor.extractTimestamp(record)).isEqualTo(1L);

        // If the record is a schema-change event `ts_ms` would be null, just ignore the record.
        final URL url =
                DebeziumEventTest.class
                        .getClassLoader()
                        .getResource("mysql/debezium-event-change.json");
        assertThat(url).isNotNull();
        JsonNode schemaChangeEvent = objectMapper.readValue(url, JsonNode.class);
        record = new CdcSourceRecord(schemaChangeEvent.toString());
        assertThat(extractor.extractTimestamp(record)).isEqualTo(Long.MIN_VALUE);
    }

    @Test
    public void testMongodbCdcTimestampExtractor() throws Exception {
        MongoDBCdcTimestampExtractor extractor = new MongoDBCdcTimestampExtractor();

        JsonNode data = objectMapper.readValue("{\"ts_ms\": 1}", JsonNode.class);
        CdcSourceRecord record = new CdcSourceRecord(data.toString());
        assertThat(extractor.extractTimestamp(record)).isEqualTo(1L);
    }
}
