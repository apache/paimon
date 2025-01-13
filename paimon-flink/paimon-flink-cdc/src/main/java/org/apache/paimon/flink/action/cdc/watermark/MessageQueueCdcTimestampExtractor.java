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

package org.apache.paimon.flink.action.cdc.watermark;

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/** Timestamp extractor for Kafka/Pulsar sources in CDC applications. */
public class MessageQueueCdcTimestampExtractor implements CdcTimestampExtractor {

    private static final long serialVersionUID = 1L;

    @Override
    public long extractTimestamp(CdcSourceRecord cdcSourceRecord) throws JsonProcessingException {
        JsonNode record = (JsonNode) cdcSourceRecord.getValue();
        if (JsonSerdeUtil.isNodeExists(record, "mysqlType")) {
            // Canal json
            return JsonSerdeUtil.extractValue(record, Long.class, "ts");
        } else if (JsonSerdeUtil.isNodeExists(record, "pos")) {
            // Ogg json
            String dateTimeString = JsonSerdeUtil.extractValue(record, String.class, "op_ts");
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
            LocalDateTime dateTime = LocalDateTime.parse(dateTimeString, formatter);
            return dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        } else if (JsonSerdeUtil.isNodeExists(record, "xid")) {
            // Maxwell json
            return JsonSerdeUtil.extractValue(record, Long.class, "ts") * 1000;
        } else if (JsonSerdeUtil.isNodeExists(record, "payload", "source", "connector")) {
            // Dbz json
            return JsonSerdeUtil.extractValue(record, Long.class, "payload", "ts_ms");
        } else if (JsonSerdeUtil.isNodeExists(record, "source", "connector")) {
            // Dbz json
            return JsonSerdeUtil.extractValue(record, Long.class, "ts_ms");
        } else if (JsonSerdeUtil.isNodeExists(record, "payload", "timestamp")) {
            // Aliyun json
            return JsonSerdeUtil.extractValue(
                    record, Long.class, "payload", "timestamp", "systemTime");
        }
        throw new RuntimeException(
                String.format(
                        "Failed to extract timestamp: The JSON format of the message queue is unsupported. Record details: %s",
                        record));
    }
}
