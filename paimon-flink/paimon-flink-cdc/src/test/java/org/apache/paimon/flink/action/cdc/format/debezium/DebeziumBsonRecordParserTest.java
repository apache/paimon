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
import org.apache.paimon.flink.action.cdc.kafka.KafkaActionITCaseBase;
import org.apache.paimon.flink.action.cdc.watermark.MessageQueueCdcTimestampExtractor;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.RowKind;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Test for DebeziumBsonRecordParser. */
public class DebeziumBsonRecordParserTest extends KafkaActionITCaseBase {

    private static final Logger log = LoggerFactory.getLogger(DebeziumBsonRecordParserTest.class);
    private static List<String> insertList = new ArrayList<>();
    private static List<String> updateList = new ArrayList<>();
    private static List<String> deleteList = new ArrayList<>();

    private static ObjectMapper objMapper = new ObjectMapper();

    private static Map<String, String> beforeEvent = new HashMap<>();

    static {
        beforeEvent.put("_id", "67ab25755c0d5ac87eb8c632");
        beforeEvent.put("created_at", "1736207571013");
        beforeEvent.put("created_by", "peter");
        beforeEvent.put("tags", "[\"pending\"]");
        beforeEvent.put("updated_at", "1739455297970");
    }

    private static Map<String, String> afterEvent = new HashMap<>();

    static {
        afterEvent.put("_id", "67ab25755c0d5ac87eb8c632");
        afterEvent.put("created_at", "1736207571013");
        afterEvent.put("created_by", "peter");
        afterEvent.put("tags", "[\"succeed\"]");
        afterEvent.put("updated_at", "1739455397970");
    }

    @Before
    public void setup() {
        String insertRes = "kafka/debezium-bson/table/event/event-insert.txt";
        String updateRes = "kafka/debezium-bson/table/event/event-update.txt";
        String deleteRes = "kafka/debezium-bson/table/event/event-delete.txt";
        URL url;
        try {
            url = DebeziumBsonRecordParserTest.class.getClassLoader().getResource(insertRes);
            Files.readAllLines(Paths.get(url.toURI())).stream()
                    .filter(this::isRecordLine)
                    .forEach(e -> insertList.add(e));

            url = DebeziumBsonRecordParserTest.class.getClassLoader().getResource(updateRes);
            Files.readAllLines(Paths.get(url.toURI())).stream()
                    .filter(this::isRecordLine)
                    .forEach(e -> updateList.add(e));

            url = DebeziumBsonRecordParserTest.class.getClassLoader().getResource(deleteRes);
            Files.readAllLines(Paths.get(url.toURI())).stream()
                    .filter(this::isRecordLine)
                    .forEach(e -> deleteList.add(e));

        } catch (Exception e) {
            log.error("Fail to init debezium-json cases", e);
        }
    }

    @Test
    public void extractInsertRecord() throws Exception {
        DebeziumBsonRecordParser parser =
                new DebeziumBsonRecordParser(TypeMapping.defaultMapping(), Collections.emptyList());
        for (String json : insertList) {
            // 将json解析为JsonNode对象
            JsonNode rootNode = objMapper.readValue(json, JsonNode.class);
            CdcSourceRecord cdcRecord = new CdcSourceRecord(rootNode);
            Schema schema = parser.buildSchema(cdcRecord);
            Assert.assertEquals(schema.primaryKeys(), Arrays.asList("_id"));

            List<RichCdcMultiplexRecord> records = parser.extractRecords();
            Assert.assertEquals(records.size(), 1);

            CdcRecord result = records.get(0).toRichCdcRecord().toCdcRecord();
            Assert.assertEquals(result.kind(), RowKind.INSERT);
            Assert.assertEquals(beforeEvent, result.data());

            String dbName = parser.getDatabaseName();
            Assert.assertEquals(dbName, "bigdata_test");

            String tableName = parser.getTableName();
            Assert.assertEquals(tableName, "sync_test_table");

            MessageQueueCdcTimestampExtractor extractor = new MessageQueueCdcTimestampExtractor();
            Assert.assertTrue(extractor.extractTimestamp(cdcRecord) > 0);
        }
    }

    @Test
    public void extractUpdateRecord() throws Exception {
        DebeziumBsonRecordParser parser =
                new DebeziumBsonRecordParser(TypeMapping.defaultMapping(), Collections.emptyList());
        for (String json : updateList) {
            // 将json解析为JsonNode对象
            JsonNode jsonNode = objMapper.readValue(json, JsonNode.class);
            CdcSourceRecord cdcRecord = new CdcSourceRecord(jsonNode);
            Schema schema = parser.buildSchema(cdcRecord);
            Assert.assertEquals(schema.primaryKeys(), Arrays.asList("_id"));

            List<RichCdcMultiplexRecord> records = parser.extractRecords();
            Assert.assertEquals(records.size(), 2);

            CdcRecord updateBefore = records.get(0).toRichCdcRecord().toCdcRecord();
            Assert.assertEquals(updateBefore.kind(), RowKind.DELETE);
            Assert.assertEquals(beforeEvent, updateBefore.data());

            CdcRecord updateAfter = records.get(1).toRichCdcRecord().toCdcRecord();
            Assert.assertEquals(updateAfter.kind(), RowKind.INSERT);
            Assert.assertEquals(afterEvent, updateAfter.data());

            String dbName = parser.getDatabaseName();
            Assert.assertEquals(dbName, "bigdata_test");

            String tableName = parser.getTableName();
            Assert.assertEquals(tableName, "sync_test_table");

            MessageQueueCdcTimestampExtractor extractor = new MessageQueueCdcTimestampExtractor();
            Assert.assertTrue(extractor.extractTimestamp(cdcRecord) > 0);
        }
    }

    @Test
    public void extractDeleteRecord() throws Exception {
        DebeziumBsonRecordParser parser =
                new DebeziumBsonRecordParser(TypeMapping.defaultMapping(), Collections.emptyList());
        for (String json : deleteList) {
            // 将json解析为JsonNode对象
            JsonNode jsonNode = objMapper.readValue(json, JsonNode.class);
            CdcSourceRecord cdcRecord = new CdcSourceRecord(jsonNode);
            Schema schema = parser.buildSchema(cdcRecord);
            Assert.assertEquals(schema.primaryKeys(), Arrays.asList("_id"));

            List<RichCdcMultiplexRecord> records = parser.extractRecords();
            Assert.assertEquals(records.size(), 1);

            CdcRecord result = records.get(0).toRichCdcRecord().toCdcRecord();
            Assert.assertEquals(result.kind(), RowKind.DELETE);
            Assert.assertEquals(afterEvent, result.data());

            String dbName = parser.getDatabaseName();
            Assert.assertEquals(dbName, "bigdata_test");

            String tableName = parser.getTableName();
            Assert.assertEquals(tableName, "sync_test_table");

            MessageQueueCdcTimestampExtractor extractor = new MessageQueueCdcTimestampExtractor();
            Assert.assertTrue(extractor.extractTimestamp(cdcRecord) > 0);
        }
    }
}
