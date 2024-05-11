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

package org.apache.paimon.flink.action.cdc.format.ogg;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.format.JsonRecordParser;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.RowKind;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.JsonSerdeUtil.isNull;

/**
 * The {@code OggRecordParser} class extends the abstract {@link JsonRecordParser} and is
 * responsible for parsing records from the Oracle GoldenGate (OGG) JSON format. Oracle GoldenGate
 * is a software application used for real-time data integration and replication in heterogeneous IT
 * environments. This parser extracts relevant information from the OGG JSON records and transforms
 * them into a list of {@link RichCdcMultiplexRecord} objects.
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
public class OggRecordParser extends JsonRecordParser {

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
        String operation = getAndCheck(FIELD_TYPE).asText();
        switch (operation) {
            case OP_UPDATE:
                processRecord(getBefore(operation), RowKind.DELETE, records);
                processRecord(getData(), RowKind.INSERT, records);
                break;
            case OP_INSERT:
                processRecord(getData(), RowKind.INSERT, records);
                break;
            case OP_DELETE:
                processRecord(getBefore(operation), RowKind.DELETE, records);
                break;
            default:
                throw new UnsupportedOperationException("Unknown record operation: " + operation);
        }
        return records;
    }

    private JsonNode getData() {
        return getAndCheck(dataField());
    }

    private JsonNode getBefore(String op) {
        return getAndCheck(FIELD_BEFORE, FIELD_TYPE, op);
    }

    @Override
    protected String primaryField() {
        return "primary_keys";
    }

    @Override
    protected String dataField() {
        return "after";
    }

    @Nullable
    @Override
    protected String getTableName() {
        Identifier id = getTableId();
        return id == null ? null : id.getObjectName();
    }

    @Nullable
    @Override
    protected String getDatabaseName() {
        Identifier id = getTableId();
        return id == null ? null : id.getDatabaseName();
    }

    @Nullable
    private Identifier getTableId() {
        JsonNode node = root.get(FIELD_TABLE);
        if (isNull(node)) {
            return null;
        }

        return Identifier.fromString(node.asText());
    }

    @Override
    protected String format() {
        return "ogg-json";
    }
}
