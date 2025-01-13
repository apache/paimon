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

package org.apache.paimon.flink.action.cdc.format.dms;

import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.format.AbstractJsonRecordParser;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.Pair;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * The {@code DMSRecordParser} class extends the abstract {@link AbstractJsonRecordParser} and is
 * designed to parse records from AWS DMS's JSON change data capture (CDC) format. AWS DMS is a CDC
 * solution for RDMS that captures row-level changes to database tables and outputs them in JSON
 * format. This parser extracts relevant information from the DMS-JSON format and converts it into a
 * list of {@link RichCdcMultiplexRecord} objects.
 *
 * <p>The class supports various database operations such as INSERT, UPDATE, and DELETE, and creates
 * corresponding {@link RichCdcMultiplexRecord} objects to represent these changes.
 *
 * <p>Validation is performed to ensure that the JSON records contain all necessary fields, and the
 * class also supports schema extraction for the Kafka topic.
 */
public class DMSRecordParser extends AbstractJsonRecordParser {

    private static final String FIELD_DATA = "data";
    private static final String FIELD_METADATA = "metadata";
    private static final String FIELD_TYPE = "record-type";
    private static final String FIELD_OP = "operation";
    private static final String FIELD_DATABASE = "schema-name";
    private static final String FIELD_TABLE = "table-name";

    private static final String OP_LOAD = "load";
    private static final String OP_INSERT = "insert";
    private static final String OP_UPDATE = "update";
    private static final String OP_DELETE = "delete";

    private static final String BEFORE_PREFIX = "BI_";

    public DMSRecordParser(TypeMapping typeMapping, List<ComputedColumn> computedColumns) {
        super(typeMapping, computedColumns);
    }

    @Override
    protected @Nullable String getTableName() {
        JsonNode metaNode = getAndCheck(FIELD_METADATA);
        return metaNode.get(FIELD_TABLE).asText();
    }

    @Override
    protected List<RichCdcMultiplexRecord> extractRecords() {
        if (isDDL()) {
            return Collections.emptyList();
        }

        JsonNode dataNode = getAndCheck(dataField());
        String operation = getAndCheck(FIELD_METADATA).get(FIELD_OP).asText();
        List<RichCdcMultiplexRecord> records = new ArrayList<>();

        switch (operation) {
            case OP_LOAD:
            case OP_INSERT:
                processRecord(dataNode, RowKind.INSERT, records);
                break;
            case OP_UPDATE:
                Pair<JsonNode, JsonNode> dataAndBeforeNodes = splitBeforeAndData(dataNode);
                processRecord(dataAndBeforeNodes.getRight(), RowKind.DELETE, records);
                processRecord(dataAndBeforeNodes.getLeft(), RowKind.INSERT, records);
                break;
            case OP_DELETE:
                processRecord(dataNode, RowKind.DELETE, records);
                break;
            default:
                throw new UnsupportedOperationException("Unknown record operation: " + operation);
        }

        return records;
    }

    @Override
    protected @Nullable String getDatabaseName() {
        JsonNode metaNode = getAndCheck(FIELD_METADATA);
        return metaNode.get(FIELD_DATABASE).asText();
    }

    @Override
    protected String primaryField() {
        return null;
    }

    @Override
    protected String dataField() {
        return FIELD_DATA;
    }

    @Override
    protected String format() {
        return "aws-dms-json";
    }

    @Override
    protected boolean isDDL() {
        String recordType = getAndCheck(FIELD_METADATA).get(FIELD_TYPE).asText();
        return !"data".equals(recordType);
    }

    private Pair<JsonNode, JsonNode> splitBeforeAndData(JsonNode dataNode) {
        JsonNode newDataNode = dataNode.deepCopy();
        JsonNode beforeDataNode = dataNode.deepCopy();

        Iterator<Map.Entry<String, JsonNode>> newDataFields = newDataNode.fields();
        while (newDataFields.hasNext()) {
            Map.Entry<String, JsonNode> next = newDataFields.next();
            if (next.getKey().startsWith(BEFORE_PREFIX)) {
                newDataFields.remove();
            }
        }

        Iterator<Map.Entry<String, JsonNode>> beforeDataFields = beforeDataNode.fields();
        while (beforeDataFields.hasNext()) {
            Map.Entry<String, JsonNode> next = beforeDataFields.next();
            if (next.getKey().startsWith(BEFORE_PREFIX)) {
                String key = next.getKey().replaceFirst(BEFORE_PREFIX, "");
                ((ObjectNode) beforeDataNode).set(key, next.getValue());
                beforeDataFields.remove();
            }
        }

        return Pair.of(newDataNode, beforeDataNode);
    }
}
