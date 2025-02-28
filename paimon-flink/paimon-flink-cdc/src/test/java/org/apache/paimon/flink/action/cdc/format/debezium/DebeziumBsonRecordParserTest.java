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
import org.apache.paimon.flink.action.cdc.format.DataFormat;
import org.apache.paimon.flink.action.cdc.watermark.MessageQueueCdcTimestampExtractor;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.TextNode;

import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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
public class DebeziumBsonRecordParserTest {

    private static final Logger log = LoggerFactory.getLogger(DebeziumBsonRecordParserTest.class);
    private static List<CdcSourceRecord> insertList = new ArrayList<>();
    private static List<CdcSourceRecord> updateList = new ArrayList<>();
    private static List<CdcSourceRecord> deleteList = new ArrayList<>();

    private static ArrayList<CdcSourceRecord> bsonRecords = new ArrayList<>();
    private static ArrayList<CdcSourceRecord> jsonRecords = new ArrayList<>();

    private static Map<String, String> keyEvent = new HashMap<>();

    private static KafkaDeserializationSchema<CdcSourceRecord> kafkaDeserializationSchema = null;

    private static Map<String, String> beforeEvent = new HashMap<>();

    private static Map<String, String> afterEvent = new HashMap<>();

    @BeforeAll
    public static void beforeAll() throws Exception {
        DataFormat dataFormat = new DebeziumBsonDataFormatFactory().create();
        kafkaDeserializationSchema = dataFormat.createKafkaDeserializer(null);

        keyEvent.put("_id", "67ab25755c0d5ac87eb8c632");

        beforeEvent.put("_id", "67ab25755c0d5ac87eb8c632");
        beforeEvent.put("created_at", "1736207571013");
        beforeEvent.put("created_by", "peter");
        beforeEvent.put("tags", "[\"pending\"]");
        beforeEvent.put("updated_at", "1739455297970");

        afterEvent.put("_id", "67ab25755c0d5ac87eb8c632");
        afterEvent.put("created_at", "1736207571013");
        afterEvent.put("created_by", "peter");
        afterEvent.put("tags", "[\"succeed\"]");
        afterEvent.put("updated_at", "1739455397970");

        String insertRes = "kafka/debezium-bson/table/event/event-insert.txt";
        String updateRes = "kafka/debezium-bson/table/event/event-update.txt";
        String deleteRes = "kafka/debezium-bson/table/event/event-delete.txt";
        String bsonPth = "kafka/debezium-bson/table/event/event-bson.txt";
        String jsonPath = "kafka/debezium-bson/table/event/event-json.txt";

        parseCdcSourceRecords(insertRes, insertList);

        parseCdcSourceRecords(updateRes, updateList);

        parseCdcSourceRecords(deleteRes, deleteList);

        parseCdcSourceRecords(bsonPth, bsonRecords);

        parseCdcSourceRecords(jsonPath, jsonRecords);
    }

    @AfterAll
    public static void afterAll() {
        insertList.clear();
        updateList.clear();
        deleteList.clear();
        bsonRecords.clear();
        jsonRecords.clear();
    }

    private static void parseCdcSourceRecords(String resourcePath, List<CdcSourceRecord> records)
            throws Exception {
        URL url = DebeziumBsonRecordParserTest.class.getClassLoader().getResource(resourcePath);
        List<String> line = Files.readAllLines(Paths.get(url.toURI()));
        String key = null;
        for (String json : line) {
            if (StringUtils.isNullOrWhitespaceOnly(json) || !json.startsWith("{")) {
                continue;
            }
            if (key == null) {
                key = json;
            } else {
                // test kafka deserialization
                records.add(deserializeKafkaSchema(key, json));
                key = null;
            }
        }
    }

    @Test
    public void extractInsertRecord() throws Exception {
        DebeziumBsonRecordParser parser =
                new DebeziumBsonRecordParser(TypeMapping.defaultMapping(), Collections.emptyList());
        Assertions.assertFalse(insertList.isEmpty());
        for (CdcSourceRecord cdcRecord : insertList) {
            Schema schema = parser.buildSchema(cdcRecord);
            Assertions.assertEquals(schema.primaryKeys(), Arrays.asList("_id"));

            List<RichCdcMultiplexRecord> records = parser.extractRecords();
            Assertions.assertEquals(records.size(), 1);

            CdcRecord result = records.get(0).toRichCdcRecord().toCdcRecord();
            Assertions.assertEquals(result.kind(), RowKind.INSERT);
            Assertions.assertEquals(beforeEvent, result.data());

            String dbName = parser.getDatabaseName();
            Assertions.assertEquals(dbName, "bigdata_test");

            String tableName = parser.getTableName();
            Assertions.assertEquals(tableName, "sync_test_table");

            MessageQueueCdcTimestampExtractor extractor = new MessageQueueCdcTimestampExtractor();
            Assertions.assertTrue(extractor.extractTimestamp(cdcRecord) > 0);
        }
    }

    @Test
    public void extractUpdateRecord() throws Exception {
        DebeziumBsonRecordParser parser =
                new DebeziumBsonRecordParser(TypeMapping.defaultMapping(), Collections.emptyList());
        Assertions.assertFalse(updateList.isEmpty());
        for (CdcSourceRecord cdcRecord : updateList) {
            Schema schema = parser.buildSchema(cdcRecord);
            Assertions.assertEquals(schema.primaryKeys(), Arrays.asList("_id"));

            List<RichCdcMultiplexRecord> records = parser.extractRecords();
            Assertions.assertEquals(records.size(), 2);

            CdcRecord updateBefore = records.get(0).toRichCdcRecord().toCdcRecord();
            Assertions.assertEquals(updateBefore.kind(), RowKind.DELETE);
            if (parser.checkBeforeExists()) {
                Assertions.assertEquals(beforeEvent, updateBefore.data());
            } else {
                Assertions.assertEquals(keyEvent, updateBefore.data());
            }

            CdcRecord updateAfter = records.get(1).toRichCdcRecord().toCdcRecord();
            Assertions.assertEquals(updateAfter.kind(), RowKind.INSERT);
            Assertions.assertEquals(afterEvent, updateAfter.data());

            String dbName = parser.getDatabaseName();
            Assertions.assertEquals(dbName, "bigdata_test");

            String tableName = parser.getTableName();
            Assertions.assertEquals(tableName, "sync_test_table");

            MessageQueueCdcTimestampExtractor extractor = new MessageQueueCdcTimestampExtractor();
            Assertions.assertTrue(extractor.extractTimestamp(cdcRecord) > 0);
        }
    }

    @Test
    public void extractDeleteRecord() throws Exception {
        DebeziumBsonRecordParser parser =
                new DebeziumBsonRecordParser(TypeMapping.defaultMapping(), Collections.emptyList());
        Assertions.assertFalse(deleteList.isEmpty());
        for (CdcSourceRecord cdcRecord : deleteList) {
            Schema schema = parser.buildSchema(cdcRecord);
            Assertions.assertEquals(schema.primaryKeys(), Arrays.asList("_id"));

            List<RichCdcMultiplexRecord> records = parser.extractRecords();
            Assertions.assertEquals(records.size(), 1);

            CdcRecord result = records.get(0).toRichCdcRecord().toCdcRecord();
            Assertions.assertEquals(result.kind(), RowKind.DELETE);
            if (parser.checkBeforeExists()) {
                Assertions.assertEquals(beforeEvent, result.data());
            } else {
                Assertions.assertEquals(keyEvent, result.data());
            }

            String dbName = parser.getDatabaseName();
            Assertions.assertEquals(dbName, "bigdata_test");

            String tableName = parser.getTableName();
            Assertions.assertEquals(tableName, "sync_test_table");

            MessageQueueCdcTimestampExtractor extractor = new MessageQueueCdcTimestampExtractor();
            Assertions.assertTrue(extractor.extractTimestamp(cdcRecord) > 0);
        }
    }

    @Test
    public void bsonConvertJsonTest() throws Exception {
        DebeziumBsonRecordParser parser =
                new DebeziumBsonRecordParser(TypeMapping.defaultMapping(), Collections.emptyList());

        Assertions.assertFalse(jsonRecords.isEmpty());
        for (int i = 0; i < jsonRecords.size(); i++) {
            CdcSourceRecord bsonRecord = bsonRecords.get(i);
            CdcSourceRecord jsonRecord = jsonRecords.get(i);

            JsonNode bsonTextNode =
                    new TextNode(JsonSerdeUtil.writeValueAsString(bsonRecord.getValue()));
            Map<String, String> resultMap = parser.extractRowData(bsonTextNode, RowType.builder());

            ObjectNode expectNode = (ObjectNode) jsonRecord.getValue();

            expectNode
                    .fields()
                    .forEachRemaining(
                            entry -> {
                                String key = entry.getKey();
                                String expectValue = null;
                                if (!JsonSerdeUtil.isNull(entry.getValue())) {
                                    expectValue = entry.getValue().asText();
                                }
                                Assertions.assertEquals(expectValue, resultMap.get(key));
                            });
        }
    }

    private static CdcSourceRecord deserializeKafkaSchema(String key, String value)
            throws Exception {
        return kafkaDeserializationSchema.deserialize(
                new ConsumerRecord<>("topic", 0, 0, key.getBytes(), value.getBytes()));
    }
}
