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

package org.apache.paimon.flink.action.cdc.format.debezium;

import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.format.RecordParser;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.RowKind;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.shaded.guava30.com.google.common.base.Preconditions.checkArgument;
import static org.apache.paimon.utils.JsonSerdeUtil.isNull;

/**
 * The {@code DebeziumExcludeJsonRecordParser} class extends the abstract {@link RecordParser} and
 * is designed to parse records from Debezium's JSON change data capture (CDC) format. Debezium is a
 * CDC solution for MySQL databases that captures row-level changes to database tables and outputs
 * them in JSON format. This parser extracts relevant information from the Debezium-JSON format and
 * converts it into a list of {@link RichCdcMultiplexRecord} objects.
 *
 * <p>The class supports various database operations such as INSERT, UPDATE, DELETE, and READ
 * (snapshot reads), and creates corresponding {@link RichCdcMultiplexRecord} objects to represent
 * these changes.
 *
 * <p>Validation is performed to ensure that the JSON records contain all necessary fields,
 * including the 'before' and 'after' states for UPDATE operations, and the class also supports
 * schema extraction for the Kafka topic. Debezium's specific fields such as 'source', 'op' for
 * operation type, and primary key field names are used to construct the details of each record
 * event.
 */
public class DebeziumSchemaExcludeJsonRecordParser extends RecordParser {

    private static final String FIELD_BEFORE = "before";
    private static final String FIELD_AFTER = "after";
    private static final String FIELD_SOURCE = "source";
    private static final String FIELD_PRIMARY = "pkNames";
    private static final String FIELD_DB = "db";
    private static final String FIELD_TYPE = "op";
    private static final String OP_INSERT = "c";
    private static final String OP_UPDATE = "u";
    private static final String OP_DELETE = "d";
    private static final String OP_READE = "r";

    public DebeziumSchemaExcludeJsonRecordParser(
            boolean caseSensitive, TypeMapping typeMapping, List<ComputedColumn> computedColumns) {
        super(caseSensitive, typeMapping, computedColumns);
    }

    @Override
    public List<RichCdcMultiplexRecord> extractRecords() {
        String operation = extractStringFromRootJson(FIELD_TYPE);
        List<RichCdcMultiplexRecord> records = new ArrayList<>();
        switch (operation) {
            case OP_INSERT:
            case OP_READE:
                processRecord(root.get(dataField()), RowKind.INSERT, records);
                break;
            case OP_UPDATE:
                processRecord(
                        mergeOldRecord(root.get(dataField()), root.get(FIELD_BEFORE)),
                        RowKind.DELETE,
                        records);
                processRecord(root.get(dataField()), RowKind.INSERT, records);
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
        checkArgument(
                !isNull(root.get(FIELD_SOURCE).get(FIELD_TABLE)),
                errorMessageTemplate,
                FIELD_TABLE);
        checkArgument(
                !isNull(root.get(FIELD_SOURCE).get(FIELD_DB)),
                errorMessageTemplate,
                FIELD_DATABASE);
        checkArgument(!isNull(root.get(FIELD_TYPE)), errorMessageTemplate, FIELD_TYPE);
        String operation = root.get(FIELD_TYPE).asText();
        switch (operation) {
            case OP_INSERT:
            case OP_READE:
                checkArgument(!isNull(root.get(dataField())), errorMessageTemplate, dataField());
                break;
            case OP_UPDATE:
            case OP_DELETE:
                checkArgument(!isNull(root.get(FIELD_BEFORE)), errorMessageTemplate, FIELD_BEFORE);
                break;
            default:
                throw new IllegalArgumentException("Unsupported operation type: " + operation);
        }
        checkArgument(!isNull(root.get(primaryField())), errorMessageTemplate, primaryField());
    }

    @Override
    protected String primaryField() {
        return FIELD_PRIMARY;
    }

    @Override
    protected String dataField() {
        return FIELD_AFTER;
    }

    @Override
    protected String extractStringFromRootJson(String key) {
        if (key.equals(FIELD_TABLE)) {
            tableName = root.get(FIELD_SOURCE).get(FIELD_TABLE).asText();
            return tableName;
        } else if (key.equals(FIELD_DATABASE)) {
            databaseName = root.get(FIELD_SOURCE).get(FIELD_DB).asText();
            return databaseName;
        }
        return root.get(key) != null ? root.get(key).asText() : null;
    }
}
