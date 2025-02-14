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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.ComputedColumnUtils;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.kafka.KafkaActionITCaseBase;
import org.apache.paimon.flink.action.cdc.watermark.MessageQueueCdcTimestampExtractor;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.BinaryStringUtils;

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
import java.util.Map;

/** Test for AliyunJsonRecordParser. */
public class AliyunJsonRecordParserTest extends KafkaActionITCaseBase {

    private static final Logger log = LoggerFactory.getLogger(AliyunJsonRecordParserTest.class);
    private static List<String> insertList = new ArrayList<>();
    private static List<String> updateList = new ArrayList<>();
    private static List<String> deleteList = new ArrayList<>();
    private static List<ComputedColumn> computedColumns = new ArrayList<>();

    private static ObjectMapper objMapper = new ObjectMapper();

    String dateTimeRegex = "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}";

    @Before
    public void setup() {
        String insertRes = "kafka/aliyun/table/event/event-insert.txt";
        String updateRes = "kafka/aliyun/table/event/event-update-in-one.txt";
        String deleteRes = "kafka/aliyun/table/event/event-delete.txt";

        String[] computedColumnArgs = {
            "etl_create_time=create_time()", "etl_update_time=update_time()"
        };

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

            computedColumns =
                    ComputedColumnUtils.buildComputedColumns(
                            Arrays.asList(computedColumnArgs), Collections.emptyList());

        } catch (Exception e) {
            log.error("Fail to init aliyun-json cases", e);
        }
    }

    @Test
    public void extractInsertRecord() throws Exception {

        AliyunRecordParser parser =
                new AliyunRecordParser(TypeMapping.defaultMapping(), computedColumns);
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

            MessageQueueCdcTimestampExtractor extractor = new MessageQueueCdcTimestampExtractor();
            Assert.assertTrue(extractor.extractTimestamp(cdcRecord) > 0);

            Map<String, String> data = records.get(0).toRichCdcRecord().toCdcRecord().data();
            String createTime = data.get("etl_create_time");
            String updateTime = data.get("etl_update_time");

            // Mock the real timestamp string which retrieved from store and convert through paimon
            // Timestamp
            createTime =
                    BinaryStringUtils.toTimestamp(BinaryString.fromString(createTime), 6)
                            .toString();
            updateTime =
                    BinaryStringUtils.toTimestamp(BinaryString.fromString(updateTime), 6)
                            .toString();

            Assert.assertTrue(createTime.matches(dateTimeRegex));
            Assert.assertTrue(updateTime.matches(dateTimeRegex));

            log.info("createTime: {}, updateTime: {}", createTime, updateTime);
        }
    }

    @Test
    public void extractUpdateRecord() throws Exception {
        AliyunRecordParser parser =
                new AliyunRecordParser(TypeMapping.defaultMapping(), computedColumns);
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

            MessageQueueCdcTimestampExtractor extractor = new MessageQueueCdcTimestampExtractor();
            Assert.assertTrue(extractor.extractTimestamp(cdcRecord) > 0);

            Map<String, String> data = records.get(0).toRichCdcRecord().toCdcRecord().data();
            String createTime = data.get("etl_create_time");
            String updateTime = data.get("etl_update_time");
            Assert.assertNull(createTime);

            updateTime =
                    BinaryStringUtils.toTimestamp(BinaryString.fromString(updateTime), 6)
                            .toString();

            Assert.assertTrue(updateTime.matches(dateTimeRegex));
        }
    }

    @Test
    public void extractDeleteRecord() throws Exception {
        AliyunRecordParser parser =
                new AliyunRecordParser(TypeMapping.defaultMapping(), computedColumns);
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

            MessageQueueCdcTimestampExtractor extractor = new MessageQueueCdcTimestampExtractor();
            Assert.assertTrue(extractor.extractTimestamp(cdcRecord) > 0);

            Map<String, String> data = records.get(0).toRichCdcRecord().toCdcRecord().data();
            String createTime = data.get("etl_create_time");
            String updateTime = data.get("etl_update_time");
            Assert.assertNull(createTime);

            updateTime =
                    BinaryStringUtils.toTimestamp(BinaryString.fromString(updateTime), 6)
                            .toString();

            Assert.assertTrue(updateTime.matches(dateTimeRegex));
        }
    }
}
