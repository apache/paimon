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

import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation class for extracting records from MongoDB versions greater than 4.x and less than
 * 6.x.
 */
public class Mongo4VersionStrategy implements MongoVersionStrategy {

    private static final String FIELD_TYPE = "operationType";
    private static final String FIELD_DATA = "fullDocument";
    private static final String OP_UPDATE = "update";
    private static final String OP_INSERT = "insert";
    private static final String OP_REPLACE = "replace";

    private final String databaseName;
    private final String collection;
    private final boolean caseSensitive;
    private final Configuration mongodbConfig;
    private final List<ComputedColumn> computedColumns;

    public Mongo4VersionStrategy(
            String databaseName,
            String collection,
            boolean caseSensitive,
            List<ComputedColumn> computedColumns,
            Configuration mongodbConfig) {
        this.databaseName = databaseName;
        this.collection = collection;
        this.caseSensitive = caseSensitive;
        this.mongodbConfig = mongodbConfig;
        this.computedColumns = computedColumns;
    }

    /**
     * Extracts records from the provided JsonNode based on the MongoDB version strategy.
     *
     * @param root The root JsonNode containing the MongoDB record.
     * @return A list of RichCdcMultiplexRecord extracted from the root node.
     * @throws JsonProcessingException If there's an error during JSON processing.
     */
    @Override
    public List<RichCdcMultiplexRecord> extractRecords(JsonNode root)
            throws JsonProcessingException {
        String op = root.get(FIELD_TYPE).asText();
        JsonNode fullDocument = root.get(FIELD_DATA);
        return handleOperation(op, fullDocument);
    }

    /**
     * Handles the MongoDB operation type and processes the document accordingly.
     *
     * @param op The operation type (e.g., insert, update, replace).
     * @param fullDocument The JsonNode representing the full MongoDB document.
     * @return A list of RichCdcMultiplexRecord based on the operation type.
     * @throws JsonProcessingException If there's an error during JSON processing.
     */
    private List<RichCdcMultiplexRecord> handleOperation(String op, JsonNode fullDocument)
            throws JsonProcessingException {
        List<RichCdcMultiplexRecord> records = new ArrayList<>();
        LinkedHashMap<String, DataType> paimonFieldTypes = new LinkedHashMap<>();

        switch (op) {
            case OP_INSERT:
                records.add(handleInsert(fullDocument, paimonFieldTypes));
                break;
            case OP_REPLACE:
            case OP_UPDATE:
                records.add(handleUpdateOrReplace(fullDocument, paimonFieldTypes));
                break;
            default:
                throw new UnsupportedOperationException("Unknown record type: " + op);
        }
        return records;
    }

    /**
     * Processes the insert operation and constructs a RichCdcMultiplexRecord.
     *
     * @param fullDocument The JsonNode representing the full MongoDB document for insertion.
     * @param paimonFieldTypes A map to store the field types.
     * @return A RichCdcMultiplexRecord representing the insert operation.
     * @throws JsonProcessingException If there's an error during JSON processing.
     */
    private RichCdcMultiplexRecord handleInsert(
            JsonNode fullDocument, LinkedHashMap<String, DataType> paimonFieldTypes)
            throws JsonProcessingException {
        Map<String, String> insert =
                getExtractRow(
                        fullDocument,
                        paimonFieldTypes,
                        caseSensitive,
                        computedColumns,
                        mongodbConfig);
        return new RichCdcMultiplexRecord(
                databaseName,
                collection,
                paimonFieldTypes,
                extractPrimaryKeys(),
                new CdcRecord(RowKind.INSERT, insert));
    }

    /**
     * Processes the update or replace operation and constructs a RichCdcMultiplexRecord.
     *
     * @param fullDocument The JsonNode representing the full MongoDB document for update/replace.
     * @param paimonFieldTypes A map to store the field types.
     * @return A RichCdcMultiplexRecord representing the update or replace operation.
     * @throws JsonProcessingException If there's an error during JSON processing.
     */
    private RichCdcMultiplexRecord handleUpdateOrReplace(
            JsonNode fullDocument, LinkedHashMap<String, DataType> paimonFieldTypes)
            throws JsonProcessingException {
        Map<String, String> after =
                getExtractRow(
                        fullDocument,
                        paimonFieldTypes,
                        caseSensitive,
                        computedColumns,
                        mongodbConfig);
        return new RichCdcMultiplexRecord(
                databaseName,
                collection,
                paimonFieldTypes,
                extractPrimaryKeys(),
                new CdcRecord(RowKind.UPDATE_AFTER, after));
    }
}
