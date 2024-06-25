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

package org.apache.paimon.flink.action.cdc.format.maxwell;

import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.format.AbstractJsonRecordParser;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.RowKind;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

/**
 * The {@code MaxwellRecordParser} class extends the abstract {@link AbstractJsonRecordParser} and
 * is designed to parse records from Maxwell's JSON change data capture (CDC) format. Maxwell is a
 * CDC solution for MySQL databases that captures row-level changes to database tables and outputs
 * them in JSON format. This parser extracts relevant information from the Maxwell-JSON format and
 * converts it into a list of {@link RichCdcMultiplexRecord} objects.
 *
 * <p>The class supports various database operations such as INSERT, UPDATE, and DELETE, and creates
 * corresponding {@link RichCdcMultiplexRecord} objects to represent these changes.
 *
 * <p>Validation is performed to ensure that the JSON records contain all necessary fields, and the
 * class also supports schema extraction for the Kafka topic.
 */
public class MaxwellRecordParser extends AbstractJsonRecordParser {

    private static final String FIELD_OLD = "old";
    private static final String FIELD_TYPE = "type";
    private static final String OP_INSERT = "insert";
    private static final String OP_UPDATE = "update";
    private static final String OP_DELETE = "delete";

    public MaxwellRecordParser(TypeMapping typeMapping, List<ComputedColumn> computedColumns) {
        super(typeMapping, computedColumns);
    }

    @Override
    public List<RichCdcMultiplexRecord> extractRecords() {
        String operation = getAndCheck(FIELD_TYPE).asText();
        JsonNode data = getAndCheck(dataField());
        List<RichCdcMultiplexRecord> records = new ArrayList<>();
        switch (operation) {
            case OP_INSERT:
                processRecord(data, RowKind.INSERT, records);
                break;
            case OP_UPDATE:
                JsonNode old = getAndCheck(FIELD_OLD, FIELD_TYPE, operation);
                processRecord(mergeOldRecord(data, old), RowKind.DELETE, records);
                processRecord(data, RowKind.INSERT, records);
                break;
            case OP_DELETE:
                processRecord(data, RowKind.DELETE, records);
                break;
            default:
                throw new UnsupportedOperationException("Unknown record operation: " + operation);
        }
        return records;
    }

    @Override
    protected String primaryField() {
        return "primary_key_columns";
    }

    @Override
    protected String dataField() {
        return "data";
    }

    @Override
    protected String format() {
        return "maxwell-json";
    }
}
