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
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.ververica.cdc.connectors.mongodb.source.MongoDBSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.pulsar.source.PulsarSource;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/** Factory for creating CDC timestamp extractors based on different source types. */
public class CdcTimestampExtractorFactory implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Map<Class<?>, Supplier<CdcTimestampExtractor>> extractorMap =
            new HashMap<>();

    static {
        extractorMap.put(MongoDBSource.class, MongoDBCdcTimestampExtractor::new);
        extractorMap.put(MySqlSource.class, MysqlCdcTimestampExtractor::new);
        extractorMap.put(PulsarSource.class, MessageQueueCdcTimestampExtractor::new);
        extractorMap.put(KafkaSource.class, MessageQueueCdcTimestampExtractor::new);
    }

    public static CdcTimestampExtractor createExtractor(Object source) {
        Supplier<CdcTimestampExtractor> extractorSupplier = extractorMap.get(source.getClass());
        if (extractorSupplier != null) {
            return extractorSupplier.get();
        }
        throw new IllegalArgumentException(
                "Unsupported source type: " + source.getClass().getName());
    }

    /** Timestamp extractor for MongoDB sources in CDC applications. */
    public static class MongoDBCdcTimestampExtractor extends CdcDebeziumTimestampExtractor {

        private static final long serialVersionUID = 1L;

        @Override
        public long extractTimestamp(CdcSourceRecord record) throws JsonProcessingException {
            // If the record is a schema-change event return Long.MIN_VALUE as result.
            return JsonSerdeUtil.extractValueOrDefault(
                    record.getJsonNode(), Long.class, Long.MIN_VALUE, "ts_ms");
        }
    }

    /** Timestamp extractor for Kafka/Pulsar sources in CDC applications. */
    public static class MessageQueueCdcTimestampExtractor implements CdcTimestampExtractor {

        private static final long serialVersionUID = 1L;

        @Override
        public long extractTimestamp(CdcSourceRecord cdcSourceRecord)
                throws JsonProcessingException {
            JsonNode record = cdcSourceRecord.getJsonNode();
            if (JsonSerdeUtil.isNodeExists(record, "mysqlType")) {
                // Canal json
                return JsonSerdeUtil.extractValue(record, Long.class, "ts");
            } else if (JsonSerdeUtil.isNodeExists(record, "pos")) {
                // Ogg json
                String dateTimeString = JsonSerdeUtil.extractValue(record, String.class, "op_ts");
                DateTimeFormatter formatter =
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
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
            }
            throw new RuntimeException(
                    String.format(
                            "Failed to extract timestamp: The JSON format of the message queue is unsupported. Record details: %s",
                            record));
        }
    }

    /** Timestamp extractor for MySQL sources in CDC applications. */
    public static class MysqlCdcTimestampExtractor extends CdcDebeziumTimestampExtractor {

        @Override
        public long extractTimestamp(CdcSourceRecord record) throws JsonProcessingException {
            return JsonSerdeUtil.extractValueOrDefault(
                    record.getJsonNode(), Long.class, Long.MIN_VALUE, "payload", "ts_ms");
        }
    }

    /** Timestamp extractor for Cdc debezium deserialization. */
    public abstract static class CdcDebeziumTimestampExtractor implements CdcTimestampExtractor {

        protected final ObjectMapper objectMapper = new ObjectMapper();

        public CdcDebeziumTimestampExtractor() {
            objectMapper
                    .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        }
    }

    /** Interface defining the contract for CDC timestamp extraction. */
    public interface CdcTimestampExtractor extends Serializable {

        long extractTimestamp(CdcSourceRecord record) throws JsonProcessingException;
    }
}
