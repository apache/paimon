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

package org.apache.paimon.flink.action.cdc.kafka;

import org.apache.paimon.flink.action.cdc.mysql.MySqlTypeUtils;
import org.apache.paimon.types.DataType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

/** Utility class to load canal kafka schema. */
public class KafkaSchema {

    private static final int MAX_RETRY = 100;

    private final String databaseName;
    private final String tableName;
    private final Map<String, DataType> fields;
    private final List<String> primaryKeys;

    public KafkaSchema(
            String databaseName,
            String tableName,
            Map<String, DataType> fields,
            List<String> primaryKeys) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.fields = fields;
        this.primaryKeys = primaryKeys;
    }

    public String tableName() {
        return tableName;
    }

    public String databaseName() {
        return databaseName;
    }

    public Map<String, DataType> fields() {
        return fields;
    }

    public List<String> primaryKeys() {
        return primaryKeys;
    }

    private static KafkaConsumer<String, String> getKafkaEarliestConsumer(
            Configuration kafkaConfig) {
        Properties props = new Properties();
        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaConfig.get(KafkaConnectorOptions.PROPS_BOOTSTRAP_SERVERS));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(props);
    }

    private static Boolean extractIsDDL(JsonNode record) {
        return Boolean.valueOf(extractJsonNode(record, "isDdl"));
    }

    private static String extractJsonNode(JsonNode record, String key) {
        return record != null && record.get(key) != null ? record.get(key).asText() : null;
    }

    private static KafkaSchema parseCanalJson(String record, ObjectMapper objectMapper)
            throws JsonProcessingException {
        String databaseName;
        String tableName;
        final Map<String, DataType> fields = new LinkedHashMap<>();
        final List<String> primaryKeys = new ArrayList<>();
        JsonNode root = objectMapper.readValue(record, JsonNode.class);
        if (!extractIsDDL(root)) {
            JsonNode mysqlType = root.get("mysqlType");
            Iterator<String> iterator = mysqlType.fieldNames();
            while (iterator.hasNext()) {
                String fieldName = iterator.next();
                String fieldType = mysqlType.get(fieldName).asText();
                String type = MySqlTypeUtils.getShortType(fieldType);
                int precision = MySqlTypeUtils.getPrecision(fieldType);
                int scale = MySqlTypeUtils.getScale(fieldType);
                fields.put(fieldName, MySqlTypeUtils.toDataType(type, precision, scale));
            }
            ArrayNode pkNames = (ArrayNode) root.get("pkNames");
            for (int i = 0; i < pkNames.size(); i++) {
                primaryKeys.add(pkNames.get(i).asText());
            }
            databaseName = extractJsonNode(root, "database");
            tableName = extractJsonNode(root, "table");
            return new KafkaSchema(databaseName, tableName, fields, primaryKeys);
        }
        return null;
    }

    public static KafkaSchema getKafkaSchema(Configuration kafkaConfig, String topic)
            throws Exception {
        KafkaConsumer<String, String> consumer = getKafkaEarliestConsumer(kafkaConfig);

        consumer.subscribe(Collections.singletonList(topic));
        KafkaSchema kafkaSchema;
        int retry = 0;
        ObjectMapper objectMapper = new ObjectMapper();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                try {
                    String format = kafkaConfig.get(KafkaConnectorOptions.VALUE_FORMAT);
                    if ("canal-json".equals(format)) {
                        kafkaSchema = parseCanalJson(record.value(), objectMapper);
                        if (kafkaSchema != null) {
                            return kafkaSchema;
                        }
                    } else {
                        throw new UnsupportedOperationException(
                                "This format: " + format + " is not support.");
                    }
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
            if (retry == MAX_RETRY) {
                throw new Exception("Could not get metadata from server,topic :" + topic);
            }
            Thread.sleep(100);
            retry++;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KafkaSchema)) {
            return false;
        }
        KafkaSchema that = (KafkaSchema) o;
        return databaseName.equals(that.databaseName)
                && tableName.equals(that.tableName)
                && fields.equals(that.fields)
                && primaryKeys.equals(that.primaryKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName, fields, primaryKeys);
    }
}
