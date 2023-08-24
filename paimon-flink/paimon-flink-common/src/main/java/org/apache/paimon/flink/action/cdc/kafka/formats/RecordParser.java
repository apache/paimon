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

package org.apache.paimon.flink.action.cdc.kafka.formats;

import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.action.cdc.kafka.KafkaSchema;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Provides a base implementation for parsing messages of various formats into {@link
 * RichCdcMultiplexRecord} objects.
 *
 * <p>This abstract class defines common functionalities and fields required for parsing messages.
 * Subclasses are expected to provide specific implementations for extracting records, validating
 * message formats, and other format-specific operations.
 */
public abstract class RecordParser implements FlatMapFunction<String, RichCdcMultiplexRecord> {

    protected static final String FIELD_TABLE = "table";
    protected static final String FIELD_DATABASE = "database";
    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    protected final TableNameConverter tableNameConverter;
    protected JsonNode root;
    protected String databaseName;
    protected String tableName;
    protected boolean caseSensitive;

    protected abstract List<RichCdcMultiplexRecord> extractRecords();

    protected abstract void validateFormat();

    protected abstract String extractString(String key);

    public abstract KafkaSchema getKafkaSchema(String record);

    public RecordParser(TableNameConverter tableNameConverter, boolean caseSensitive) {
        this.tableNameConverter = tableNameConverter;
        this.caseSensitive = caseSensitive;
    }

    @Override
    public void flatMap(String value, Collector<RichCdcMultiplexRecord> out) throws Exception {
        root = OBJECT_MAPPER.readValue(value, JsonNode.class);
        validateFormat();

        databaseName = extractString(FIELD_DATABASE);
        tableName = tableNameConverter.convert(extractString(FIELD_TABLE));

        extractRecords().forEach(out::collect);
    }

    protected Map<String, String> keyCaseInsensitive(Map<String, String> origin) {
        Map<String, String> keyCaseInsensitive = new HashMap<>();
        for (Map.Entry<String, String> entry : origin.entrySet()) {
            String fieldName = entry.getKey().toLowerCase();
            checkArgument(
                    !keyCaseInsensitive.containsKey(fieldName),
                    "Duplicate key appears when converting map keys to case-insensitive form. Original map is:\n%s",
                    origin);
            keyCaseInsensitive.put(fieldName, entry.getValue());
        }
        return keyCaseInsensitive;
    }

    protected List<String> extractPrimaryKeys(String primaryKeys) {
        return StreamSupport.stream(root.get(primaryKeys).spliterator(), false)
                .map(pk -> toFieldName(pk.asText()))
                .collect(Collectors.toList());
    }

    protected String toFieldName(String rawName) {
        return StringUtils.caseSensitiveConversion(rawName, caseSensitive);
    }
}
