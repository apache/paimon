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
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** extract record implementation class 6.x>Mongodb Version>4.x. */
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

    public Mongo4VersionStrategy(
            String databaseName,
            String collection,
            boolean caseSensitive,
            Configuration mongodbConfig) {
        this.databaseName = databaseName;
        this.collection = collection;
        this.caseSensitive = caseSensitive;
        this.mongodbConfig = mongodbConfig;
    }

    @Override
    public List<RichCdcMultiplexRecord> extractRecords(JsonNode root)
            throws JsonProcessingException {
        List<RichCdcMultiplexRecord> records = new ArrayList<>();
        LinkedHashMap<String, DataType> paimonFieldTypes = new LinkedHashMap<>();

        String op = root.get(FIELD_TYPE).asText();
        JsonNode fullDocument = root.get(FIELD_DATA);
        // extract row kind and field values
        switch (op) {
            case OP_INSERT:
                Map<String, String> insert =
                        getExtractRow(fullDocument, paimonFieldTypes, caseSensitive, mongodbConfig);
                records.add(
                        new RichCdcMultiplexRecord(
                                databaseName,
                                collection,
                                paimonFieldTypes,
                                extractPrimaryKeys(),
                                new CdcRecord(RowKind.INSERT, insert)));
                break;
            case OP_REPLACE:
            case OP_UPDATE:
                Map<String, String> after =
                        getExtractRow(fullDocument, paimonFieldTypes, caseSensitive, mongodbConfig);
                records.add(
                        new RichCdcMultiplexRecord(
                                databaseName,
                                collection,
                                paimonFieldTypes,
                                extractPrimaryKeys(),
                                new CdcRecord(RowKind.UPDATE_AFTER, after)));
                break;
            default:
                throw new UnsupportedOperationException("Unknown record type: " + op);
        }
        return records;
    }
}
