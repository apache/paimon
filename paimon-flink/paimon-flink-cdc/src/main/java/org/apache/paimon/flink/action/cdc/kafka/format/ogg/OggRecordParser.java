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

package org.apache.paimon.flink.action.cdc.kafka.format.ogg;

import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.kafka.format.RecordParser;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.RowKind;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * The {@code OggRecordParser} class extends the abstract {@link RecordParser} and is responsible
 * for parsing records from the Oracle GoldenGate (OGG) JSON format. Oracle GoldenGate is a software
 * application used for real-time data integration and replication in heterogeneous IT environments.
 * This parser extracts relevant information from the OGG JSON records and transforms them into a
 * list of {@link RichCdcMultiplexRecord} objects.
 *
 * <p>The class handles three types of database operations, represented by "U" for UPDATE, "I" for
 * INSERT, and "D" for DELETE. It then generates corresponding {@link RichCdcMultiplexRecord}
 * objects to represent these changes in the state of the database.
 *
 * <p>Validation is performed to ensure that the JSON records contain all the necessary fields (such
 * as table, operation type, and primary keys). The class also supports Kafka schema extraction,
 * providing a way to understand the structure of the incoming records and their corresponding field
 * types.
 */
public class OggRecordParser extends RecordParser {

    private static final String FIELD_BEFORE = "before";
    private static final String FIELD_TYPE = "op_type";
    private static final String OP_UPDATE = "U";
    private static final String OP_INSERT = "I";
    private static final String OP_DELETE = "D";

    public OggRecordParser(
            boolean caseSensitive, TypeMapping typeMapping, List<ComputedColumn> computedColumns) {
        super(caseSensitive, typeMapping, computedColumns);
    }

    @Override
    public List<RichCdcMultiplexRecord> extractRecords() {
        List<RichCdcMultiplexRecord> records = new ArrayList<>();
        String operation = extractStringFromRootJson(FIELD_TYPE);
        switch (operation) {
            case OP_UPDATE:
                processRecord(root.get(FIELD_BEFORE), RowKind.DELETE, records);
                processRecord(root.get(fieldData), RowKind.INSERT, records);
                break;
            case OP_INSERT:
                processRecord(root.get(fieldData), RowKind.INSERT, records);
                break;
            case OP_DELETE:
                processRecord(root.get(FIELD_BEFORE), RowKind.DELETE, records);
                break;
            default:
                throw new UnsupportedOperationException("Unknown record operation: " + operation);
        }
        return records;
    }

    @Override
    protected void validateFormat() {
        String errorMessageTemplate =
                "Didn't find '%s' node in json. Please make sure your topic's format is correct.";

        checkNotNull(root.get(FIELD_TABLE), errorMessageTemplate, FIELD_TABLE);
        checkNotNull(root.get(FIELD_TYPE), errorMessageTemplate, FIELD_TYPE);
        checkNotNull(root.get(fieldPrimaryKeys), errorMessageTemplate, fieldPrimaryKeys);

        String fieldType = root.get(FIELD_TYPE).asText();

        switch (fieldType) {
            case OP_UPDATE:
                checkNotNull(root.get(fieldData), errorMessageTemplate, fieldData);
                checkNotNull(root.get(FIELD_BEFORE), errorMessageTemplate, FIELD_BEFORE);
                break;
            case OP_INSERT:
                checkNotNull(root.get(fieldData), errorMessageTemplate, fieldData);
                break;
            case OP_DELETE:
                checkNotNull(root.get(FIELD_BEFORE), errorMessageTemplate, FIELD_BEFORE);
                break;
        }
    }

    @Override
    protected String extractStringFromRootJson(String key) {
        if (key.equals(FIELD_TABLE)) {
            extractDatabaseAndTableNames();
            return tableName;
        } else if (key.equals(FIELD_DATABASE)) {
            extractDatabaseAndTableNames();
            return databaseName;
        }
        return root.get(key) != null ? root.get(key).asText() : null;
    }

    @Override
    protected void setPrimaryField() {
        fieldPrimaryKeys = "primary_keys";
    }

    @Override
    protected void setDataField() {
        fieldData = "after";
    }

    private void extractDatabaseAndTableNames() {
        JsonNode tableNode = root.get(FIELD_TABLE);
        if (tableNode != null) {
            String[] dbt = tableNode.asText().split("\\.", 2);
            if (dbt.length == 2) {
                databaseName = dbt[0];
                tableName = dbt[1];
            }
        }
    }
}
