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
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import com.jayway.jsonpath.JsonPath;
import org.apache.flink.configuration.Configuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.flink.action.cdc.mongodb.MongoDBActionUtils.DEFAULT_ID_GENERATION;
import static org.apache.paimon.flink.action.cdc.mongodb.MongoDBActionUtils.FIELD_NAME;
import static org.apache.paimon.flink.action.cdc.mongodb.MongoDBActionUtils.PARSER_PATH;
import static org.apache.paimon.flink.action.cdc.mongodb.MongoDBActionUtils.START_MODE;

/** Interface for processing strategies tailored for different MongoDB versions. */
public interface MongoVersionStrategy {

    String ID_FIELD = "_id";
    String OID_FIELD = "$oid";

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
        return Collections.singletonList(ID_FIELD);
    }

    /**
     * Determines the extraction mode and retrieves the row accordingly.
     *
     * @param jsonNode The JsonNode representing the MongoDB document.
     * @param rowTypeBuilder row type builder.
     * @param mongodbConfig Configuration for the MongoDB connection.
     * @return A map representing the extracted row.
     * @throws JsonProcessingException If there's an error during JSON processing.
     */
    default Map<String, String> getExtractRow(
            JsonNode jsonNode, RowType.Builder rowTypeBuilder, Configuration mongodbConfig)
            throws JsonProcessingException {
        SchemaAcquisitionMode mode =
                SchemaAcquisitionMode.valueOf(mongodbConfig.getString(START_MODE).toUpperCase());
        ObjectNode objectNode =
                JsonSerdeUtil.asSpecificNodeType(jsonNode.asText(), ObjectNode.class);
        JsonNode idNode = objectNode.get(ID_FIELD);
        if (idNode == null) {
            throw new IllegalArgumentException(
                    "The provided MongoDB JSON document does not contain an _id field.");
        }
        JsonNode document =
                mongodbConfig.getBoolean(DEFAULT_ID_GENERATION)
                        ? objectNode.set(
                                ID_FIELD,
                                idNode.get(OID_FIELD) == null ? idNode : idNode.get(OID_FIELD))
                        : objectNode;
        switch (mode) {
            case SPECIFIED:
                return parseFieldsFromJsonRecord(
                        document.toString(),
                        mongodbConfig.getString(PARSER_PATH),
                        mongodbConfig.getString(FIELD_NAME),
                        rowTypeBuilder);
            case DYNAMIC:
                return parseAndTypeJsonRow(document.toString(), rowTypeBuilder);
            default:
                throw new RuntimeException("Unsupported extraction mode: " + mode);
        }
    }

    /** Parses and types a JSON row based on the given parameters. */
    default Map<String, String> parseAndTypeJsonRow(
            String evaluate, RowType.Builder rowTypeBuilder) {
        Map<String, String> parsedRow = JsonSerdeUtil.parseJsonMap(evaluate, String.class);
        return processParsedData(parsedRow, rowTypeBuilder);
    }

    /** Parses fields from a JSON record based on the given parameters. */
    static Map<String, String> parseFieldsFromJsonRecord(
            String record, String fieldPaths, String fieldNames, RowType.Builder rowTypeBuilder) {
        String[] columnNames = fieldNames.split(",");
        String[] parseNames = fieldPaths.split(",");
        Map<String, String> parsedRow = new HashMap<>();

        for (int i = 0; i < parseNames.length; i++) {
            String evaluate = JsonPath.read(record, parseNames[i]);
            parsedRow.put(columnNames[i], Optional.ofNullable(evaluate).orElse("{}"));
        }

        return processParsedData(parsedRow, rowTypeBuilder);
    }

    /** Processes the parsed data to generate the result map and update field types. */
    static Map<String, String> processParsedData(
            Map<String, String> parsedRow, RowType.Builder rowTypeBuilder) {
        int initialCapacity = parsedRow.size();
        Map<String, String> resultMap = new HashMap<>(initialCapacity);

        parsedRow.forEach(
                (column, value) -> {
                    rowTypeBuilder.field(column, DataTypes.STRING());
                    resultMap.put(column, value);
                });
        return resultMap;
    }
}
