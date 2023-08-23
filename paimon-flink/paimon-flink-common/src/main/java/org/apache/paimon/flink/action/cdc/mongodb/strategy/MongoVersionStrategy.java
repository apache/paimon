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

package org.apache.paimon.flink.action.cdc.mongodb.strategy;

import org.apache.paimon.flink.action.cdc.mongodb.JsonParserUtils;
import org.apache.paimon.flink.action.cdc.mongodb.ModeEnum;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.flink.action.cdc.mongodb.MongoDBActionUtils.FIELD_NAME;
import static org.apache.paimon.flink.action.cdc.mongodb.MongoDBActionUtils.PARSER_PATH;
import static org.apache.paimon.flink.action.cdc.mongodb.MongoDBActionUtils.START_MODE;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Processing strategies for different mongodb versions. */
public interface MongoVersionStrategy {

    ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    List<RichCdcMultiplexRecord> extractRecords(JsonNode root) throws JsonProcessingException;

    default List<String> extractPrimaryKeys() {
        List<String> primaryKeys = new ArrayList<>();
        primaryKeys.add("_id");
        return primaryKeys;
    }

    default Map<String, String> extractRow(String record) {
        return JsonParserUtils.extractMap(record);
    }

    default Map<String, String> keyCaseInsensitive(Map<String, String> origin) {
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

    default Map<String, String> getExtractRow(
            JsonNode jsonNode,
            LinkedHashMap<String, DataType> paimonFieldTypes,
            boolean caseSensitive,
            Configuration mongodbConfig)
            throws JsonProcessingException {
        ModeEnum mode = ModeEnum.valueOf(mongodbConfig.getString(START_MODE).toUpperCase());
        ObjectNode objectNode = (ObjectNode) OBJECT_MAPPER.readTree(jsonNode.asText());
        JsonNode document = objectNode.set("_id", objectNode.get("_id").get("$oid"));
        switch (mode) {
            case SPECIFIED:
                Map<String, String> specifiedRow =
                        getSpecifiedRow(
                                document.toString(),
                                mongodbConfig.getString(PARSER_PATH),
                                mongodbConfig.getString(FIELD_NAME),
                                paimonFieldTypes);
                return caseSensitive ? specifiedRow : keyCaseInsensitive(specifiedRow);
            case DYNAMIC:
                Map<String, String> dynamicRow =
                        getDynamicRow(document.toString(), paimonFieldTypes);
                return caseSensitive ? dynamicRow : keyCaseInsensitive(dynamicRow);
            default:
                throw new RuntimeException();
        }
    }

    default Map<String, String> getDynamicRow(
            String evaluate, LinkedHashMap<String, DataType> paimonFieldTypes) {
        Map<String, String> linkedHashMap = JsonParserUtils.extractMap(evaluate);
        Set<String> keySet = linkedHashMap.keySet();
        String[] columns = keySet.toArray(new String[0]);
        for (String column : columns) {
            paimonFieldTypes.put(column, DataTypes.STRING());
        }
        return extractRow(evaluate);
    }

    static Map<String, String> getSpecifiedRow(
            String record,
            String parsePath,
            String fileName,
            LinkedHashMap<String, DataType> paimonFieldTypes) {
        Map<String, String> resultMap = new HashMap<>();
        String[] columnNames = fileName.split(",");
        String[] parseNames = parsePath.split(",");
        for (int i = 0; i < parseNames.length; i++) {
            paimonFieldTypes.put(columnNames[i], DataTypes.STRING());
            String evaluate = JsonParserUtils.evaluate(record, "$." + parseNames[i]);
            if (evaluate == null) {
                resultMap.put(columnNames[i], "{}");
            } else {
                resultMap.put(columnNames[i], evaluate);
            }
        }
        return resultMap;
    }
}
