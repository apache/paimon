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

import org.apache.paimon.flink.action.cdc.mongodb.SchemaAcquisitionMode;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import com.jayway.jsonpath.JsonPath;
import org.apache.flink.configuration.Configuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.mapKeyCaseConvert;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.recordKeyDuplicateErrMsg;
import static org.apache.paimon.flink.action.cdc.mongodb.MongoDBActionUtils.FIELD_NAME;
import static org.apache.paimon.flink.action.cdc.mongodb.MongoDBActionUtils.PARSER_PATH;
import static org.apache.paimon.flink.action.cdc.mongodb.MongoDBActionUtils.START_MODE;

/** Interface for processing strategies tailored for different MongoDB versions. */
public interface MongoVersionStrategy {

    ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Extracts records from the provided JsonNode.
     *
     * @param root The root JsonNode containing the MongoDB record.
     * @return A list of RichCdcMultiplexRecord extracted from the root node.
     * @throws JsonProcessingException If there's an error during JSON processing.
     */
    List<RichCdcMultiplexRecord> extractRecords(JsonNode root) throws JsonProcessingException;

    /**
     * Extracts primary keys from the MongoDB record.
     *
     * @return A list of primary keys.
     */
    default List<String> extractPrimaryKeys() {
        return Collections.singletonList("_id");
    }

    default Map<String, String> extractRow(String record) {
        return JsonSerdeUtil.parseJsonMap(record, String.class);
    }

    /**
     * Determines the extraction mode and retrieves the row accordingly.
     *
     * @param jsonNode The JsonNode representing the MongoDB document.
     * @param paimonFieldTypes A map to store the field types.
     * @param caseSensitive Flag indicating if the extraction should be case-sensitive.
     * @param mongodbConfig Configuration for the MongoDB connection.
     * @return A map representing the extracted row.
     * @throws JsonProcessingException If there's an error during JSON processing.
     */
    default Map<String, String> getExtractRow(
            JsonNode jsonNode,
            LinkedHashMap<String, DataType> paimonFieldTypes,
            boolean caseSensitive,
            Configuration mongodbConfig)
            throws JsonProcessingException {
        SchemaAcquisitionMode mode =
                SchemaAcquisitionMode.valueOf(mongodbConfig.getString(START_MODE).toUpperCase());
        ObjectNode objectNode = (ObjectNode) OBJECT_MAPPER.readTree(jsonNode.asText());
        JsonNode document = objectNode.set("_id", objectNode.get("_id").get("$oid"));
        Map<String, String> row;
        switch (mode) {
            case SPECIFIED:
                row =
                        parseFieldsFromJsonRecord(
                                document.toString(),
                                mongodbConfig.getString(PARSER_PATH),
                                mongodbConfig.getString(FIELD_NAME),
                                paimonFieldTypes);
                break;
            case DYNAMIC:
                row = parseAndTypeJsonRow(document.toString(), paimonFieldTypes, caseSensitive);
                break;
            default:
                throw new RuntimeException("Unsupported extraction mode: " + mode);
        }
        return mapKeyCaseConvert(row, caseSensitive, recordKeyDuplicateErrMsg(row));
    }

    /**
     * Parses a JSON string into a map and updates the data type mapping for each key.
     *
     * @param evaluate The JSON string to be parsed.
     * @param paimonFieldTypes A map to store the data types of the keys.
     * @return A map containing the parsed key-value pairs from the JSON string.
     */
    default Map<String, String> parseAndTypeJsonRow(
            String evaluate,
            LinkedHashMap<String, DataType> paimonFieldTypes,
            boolean caseSensitive) {
        Map<String, String> parsedMap = JsonSerdeUtil.parseJsonMap(evaluate, String.class);
        for (String column : parsedMap.keySet()) {
            paimonFieldTypes.put(caseSensitive ? column : column.toLowerCase(), DataTypes.STRING());
        }
        return extractRow(evaluate);
    }

    /**
     * Parses specified fields from a JSON record.
     *
     * @param record The JSON record to be parsed.
     * @param fieldPaths The paths of the fields to be parsed from the JSON record.
     * @param fieldNames The names of the fields to be returned in the result map.
     * @param paimonFieldTypes A map to store the data types of the fields.
     * @return A map containing the parsed fields and their values.
     */
    static Map<String, String> parseFieldsFromJsonRecord(
            String record,
            String fieldPaths,
            String fieldNames,
            LinkedHashMap<String, DataType> paimonFieldTypes) {
        Map<String, String> resultMap = new HashMap<>();
        String[] columnNames = fieldNames.split(",");
        String[] parseNames = fieldPaths.split(",");

        for (int i = 0; i < parseNames.length; i++) {
            paimonFieldTypes.put(columnNames[i], DataTypes.STRING());
            String evaluate = JsonPath.read(record, parseNames[i]);
            resultMap.put(columnNames[i], Optional.ofNullable(evaluate).orElse("{}"));
        }
        return resultMap;
    }
}
