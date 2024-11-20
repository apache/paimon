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

package org.apache.paimon.flink.action.cdc.format.aliyun;

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.kafka.KafkaActionITCaseBase;
import org.apache.paimon.flink.action.cdc.watermark.CdcTimestampExtractorFactory;
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
import java.util.List;

/** Test for AliyunJsonRecordParser. */
public class AliyunJsonRecordParserTest extends KafkaActionITCaseBase {

    private static final Logger log = LoggerFactory.getLogger(AliyunJsonRecordParserTest.class);
    private static List<String> insertList = new ArrayList<>();
    private static List<String> updateList = new ArrayList<>();
    private static List<String> deleteList = new ArrayList<>();

    private static ObjectMapper objMapper = new ObjectMapper();

    @Before
    public void setup() {
        String insertRes = "kafka/aliyun/table/event/event-insert.txt";
        String updateRes = "kafka/aliyun/table/event/event-update-in-one.txt";
        String deleteRes = "kafka/aliyun/table/event/event-delete.txt";
        URL url;
        try {
            url = AliyunJsonRecordParserTest.class.getClassLoader().getResource(insertRes);
            Files.readAllLines(Paths.get(url.toURI())).stream()
                    .filter(this::isRecordLine)
                    .forEach(e -> insertList.add(e));

            url = AliyunJsonRecordParserTest.class.getClassLoader().getResource(updateRes);
            Files.readAllLines(Paths.get(url.toURI())).stream()
                    .filter(this::isRecordLine)
                    .forEach(e -> updateList.add(e));

            url = AliyunJsonRecordParserTest.class.getClassLoader().getResource(deleteRes);
            Files.readAllLines(Paths.get(url.toURI())).stream()
                    .filter(this::isRecordLine)
                    .forEach(e -> deleteList.add(e));

        } catch (Exception e) {
            log.error("Fail to init aliyun-json cases", e);
        }
    }

    @Test
    public void extractInsertRecord() throws Exception {
        AliyunRecordParser parser =
                new AliyunRecordParser(TypeMapping.defaultMapping(), Collections.emptyList());
        for (String json : insertList) {
            // 将json解析为JsonNode对象
            JsonNode rootNode = objMapper.readValue(json, JsonNode.class);
            CdcSourceRecord cdcRecord = new CdcSourceRecord(rootNode);
            Schema schema = parser.buildSchema(cdcRecord);
            Assert.assertEquals(schema.primaryKeys(), Arrays.asList("id"));

            List<RichCdcMultiplexRecord> records = parser.extractRecords();
            Assert.assertEquals(records.size(), 1);

            CdcRecord result = records.get(0).toRichCdcRecord().toCdcRecord();
            Assert.assertEquals(result.kind(), RowKind.INSERT);

            String dbName = parser.getDatabaseName();
            Assert.assertEquals(dbName, "bigdata_test");

            String tableName = parser.getTableName();
            Assert.assertEquals(tableName, "sync_test_table");

            CdcTimestampExtractorFactory.MessageQueueCdcTimestampExtractor extractor =
                    new CdcTimestampExtractorFactory.MessageQueueCdcTimestampExtractor();
            Assert.assertTrue(extractor.extractTimestamp(cdcRecord) > 0);
        }
    }

    @Test
    public void extractUpdateRecord() throws Exception {
        AliyunRecordParser parser =
                new AliyunRecordParser(TypeMapping.defaultMapping(), Collections.emptyList());
        for (String json : updateList) {
            // 将json解析为JsonNode对象
            JsonNode jsonNode = objMapper.readValue(json, JsonNode.class);
            CdcSourceRecord cdcRecord = new CdcSourceRecord(jsonNode);
            Schema schema = parser.buildSchema(cdcRecord);
            Assert.assertEquals(schema.primaryKeys(), Arrays.asList("id"));

            List<RichCdcMultiplexRecord> records = parser.extractRecords();
            Assert.assertEquals(records.size(), 1);

            CdcRecord result = records.get(0).toRichCdcRecord().toCdcRecord();
            Assert.assertEquals(result.kind(), RowKind.UPDATE_AFTER);

            String dbName = parser.getDatabaseName();
            Assert.assertEquals(dbName, "bigdata_test");

            String tableName = parser.getTableName();
            Assert.assertEquals(tableName, "sync_test_table");

            CdcTimestampExtractorFactory.MessageQueueCdcTimestampExtractor extractor =
                    new CdcTimestampExtractorFactory.MessageQueueCdcTimestampExtractor();
            Assert.assertTrue(extractor.extractTimestamp(cdcRecord) > 0);
        }
    }

    @Test
    public void extractDeleteRecord() throws Exception {
        AliyunRecordParser parser =
                new AliyunRecordParser(TypeMapping.defaultMapping(), Collections.emptyList());
        for (String json : deleteList) {
            // 将json解析为JsonNode对象
            JsonNode jsonNode = objMapper.readValue(json, JsonNode.class);
            CdcSourceRecord cdcRecord = new CdcSourceRecord(jsonNode);
            Schema schema = parser.buildSchema(cdcRecord);
            Assert.assertEquals(schema.primaryKeys(), Arrays.asList("id"));

            List<RichCdcMultiplexRecord> records = parser.extractRecords();
            Assert.assertEquals(records.size(), 1);

            CdcRecord result = records.get(0).toRichCdcRecord().toCdcRecord();
            Assert.assertEquals(result.kind(), RowKind.DELETE);

            String dbName = parser.getDatabaseName();
            Assert.assertEquals(dbName, "bigdata_test");

            String tableName = parser.getTableName();
            Assert.assertEquals(tableName, "sync_test_table");

            CdcTimestampExtractorFactory.MessageQueueCdcTimestampExtractor extractor =
                    new CdcTimestampExtractorFactory.MessageQueueCdcTimestampExtractor();
            Assert.assertTrue(extractor.extractTimestamp(cdcRecord) > 0);
        }
    }
}
