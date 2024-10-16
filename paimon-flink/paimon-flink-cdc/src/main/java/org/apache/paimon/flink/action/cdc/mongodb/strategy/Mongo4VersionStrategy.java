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

import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implementation class for extracting records from MongoDB versions greater than 4.x and less than
 * 6.x.
 */
public class Mongo4VersionStrategy implements MongoVersionStrategy {

    private static final String FIELD_TYPE = "operationType";
    private static final String FIELD_DATA = "fullDocument";
    private static final String FIELD_KEY = "documentKey";
    private static final String OP_UPDATE = "update";
    private static final String OP_INSERT = "insert";
    private static final String OP_REPLACE = "replace";
    private static final String OP_DELETE = "delete";
    private final String databaseName;
    private final String collection;
    private final Configuration mongodbConfig;

    public Mongo4VersionStrategy(
            String databaseName, String collection, Configuration mongodbConfig) {
        this.databaseName = databaseName;
        this.collection = collection;
        this.mongodbConfig = mongodbConfig;
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
        JsonNode documentKey = root.get(FIELD_KEY);
        return handleOperation(op, fullDocument, documentKey);
    }

    /**
     * Handles the MongoDB operation type and processes the document accordingly.
     *
     * @param op The operation type (e.g., insert, update, replace).
     * @param fullDocument The JsonNode representing the full MongoDB document.
     * @return A list of RichCdcMultiplexRecord based on the operation type.
     * @throws JsonProcessingException If there's an error during JSON processing.
     */
    private List<RichCdcMultiplexRecord> handleOperation(
            String op, JsonNode fullDocument, JsonNode documentKey) throws JsonProcessingException {
        List<RichCdcMultiplexRecord> records = new ArrayList<>();

        switch (op) {
            case OP_INSERT:
                records.add(processRecord(fullDocument, RowKind.INSERT));
                break;
            case OP_REPLACE:
            case OP_UPDATE:
                // Before version 6.0 of MongoDB, it was not possible to obtain 'Update Before'
                // information. Therefore, data is first deleted using the primary key '_id', and
                // then inserted.
                records.add(processRecord(documentKey, RowKind.DELETE));
                records.add(processRecord(fullDocument, RowKind.INSERT));
                break;
            case OP_DELETE:
                records.add(processRecord(documentKey, RowKind.DELETE));
                break;
            default:
                throw new UnsupportedOperationException("Unknown record type: " + op);
        }
        return records;
    }

    /**
     * Processes a JSON record based on the specified parameters and returns a
     * RichCdcMultiplexRecord object.
     *
     * @param fullDocument the JSON node containing the full document to be processed.
     * @param rowKind the kind of row to be processed (e.g., insert, update, delete).
     * @throws JsonProcessingException if there is an error in processing the JSON document.
     * @return a RichCdcMultiplexRecord object that contains the processed record information.
     */
    private RichCdcMultiplexRecord processRecord(JsonNode fullDocument, RowKind rowKind)
            throws JsonProcessingException {
        RowType.Builder rowTypeBuilder = RowType.builder();
        Map<String, String> record = getExtractRow(fullDocument, rowTypeBuilder, mongodbConfig);
        List<DataField> fields = rowTypeBuilder.build().getFields();

        return new RichCdcMultiplexRecord(
                databaseName,
                collection,
                fields,
                extractPrimaryKeys(),
                new CdcRecord(rowKind, record));
    }
}
