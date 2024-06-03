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

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.format.AbstractRecordParser;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import io.debezium.data.geometry.Geometry;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.FIELD_AFTER;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.FIELD_BEFORE;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.FIELD_DB;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.FIELD_SOURCE;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.FIELD_TYPE;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.OP_DELETE;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.OP_INSERT;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.OP_MESSAGE;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.OP_READE;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.OP_TRUNCATE;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.OP_UPDATE;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.avroToPaimonDataType;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Implementation of {@link AbstractRecordParser} for parsing messages in the Debezium avro format.
 *
 * <p>This parser handles records in the Debezium avro format and extracts relevant information to
 * produce {@link RichCdcMultiplexRecord} objects.
 */
public class DebeziumAvroRecordParser extends AbstractRecordParser {
    private static final Logger LOG = LoggerFactory.getLogger(DebeziumAvroRecordParser.class);

    private static final Schema NULL_AVRO_SCHEMA = Schema.create(Schema.Type.NULL);

    private GenericRecord keyRecord;
    private GenericRecord valueRecord;

    public DebeziumAvroRecordParser(
            boolean caseSensitive, TypeMapping typeMapping, List<ComputedColumn> computedColumns) {
        super(caseSensitive, typeMapping, computedColumns);
    }

    @Override
    protected void setRoot(CdcSourceRecord record) {
        keyRecord = (GenericRecord) record.getKey();
        valueRecord = (GenericRecord) record.getValue();
    }

    @Override
    protected List<RichCdcMultiplexRecord> extractRecords() {
        String operation = getAndCheck(FIELD_TYPE).toString();
        List<RichCdcMultiplexRecord> records = new ArrayList<>();
        switch (operation) {
            case OP_READE:
            case OP_INSERT:
                processRecord((GenericRecord) getAndCheck(FIELD_AFTER), RowKind.INSERT, records);
                break;
            case OP_UPDATE:
                processRecord((GenericRecord) getAndCheck(FIELD_BEFORE), RowKind.DELETE, records);
                processRecord((GenericRecord) getAndCheck(FIELD_AFTER), RowKind.INSERT, records);
                break;
            case OP_DELETE:
                processRecord((GenericRecord) getAndCheck(FIELD_BEFORE), RowKind.DELETE, records);
                break;
            case OP_TRUNCATE:
            case OP_MESSAGE:
                LOG.info("Skip record operation: {}", operation);
                break;
            default:
                throw new UnsupportedOperationException("Unknown record operation: " + operation);
        }

        return records;
    }

    private void processRecord(
            GenericRecord record, RowKind rowKind, List<RichCdcMultiplexRecord> records) {
        RowType.Builder rowTypeBuilder = RowType.builder();
        Map<String, String> rowData = this.extractRowData(record, rowTypeBuilder);
        records.add(createRecord(rowKind, rowData, rowTypeBuilder.build().getFields()));
    }

    @Override
    protected List<String> extractPrimaryKeys() {
        if (keyRecord == null) {
            return Collections.emptyList();
        }
        Schema keySchema = sanitizedSchema(keyRecord.getSchema());
        return keySchema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
    }

    private Map<String, String> extractRowData(
            GenericRecord record, RowType.Builder rowTypeBuilder) {
        Schema payloadSchema = sanitizedSchema(record.getSchema());

        LinkedHashMap<String, String> resultMap = new LinkedHashMap<>();
        for (Schema.Field field : payloadSchema.getFields()) {
            Schema schema = sanitizedSchema(field.schema());
            String fieldName = field.name();
            String rawValue = Objects.toString(record.get(fieldName), null);
            String transformed =
                    DebeziumSchemaUtils.transformRawValue(
                            rawValue,
                            schema.getLogicalType().getName(),
                            schema.getFullName(),
                            typeMapping,
                            () ->
                                    (ByteBuffer)
                                            ((GenericRecord) record.get(fieldName))
                                                    .get(Geometry.WKB_FIELD),
                            ZoneOffset.UTC);
            resultMap.put(fieldName, transformed);
            rowTypeBuilder.field(fieldName, avroToPaimonDataType(schema));
        }

        evalComputedColumns(resultMap, rowTypeBuilder);
        return resultMap;
    }

    @Override
    protected String format() {
        return "debezium-avro";
    }

    private Schema sanitizedSchema(Schema schema) {
        if (schema.getType() == Schema.Type.UNION
                && schema.getTypes().size() == 2
                && schema.getTypes().contains(NULL_AVRO_SCHEMA)) {
            for (Schema memberSchema : schema.getTypes()) {
                if (!memberSchema.equals(NULL_AVRO_SCHEMA)) {
                    return memberSchema;
                }
            }
        }
        return schema;
    }

    @Nullable
    @Override
    protected String getTableName() {
        return getFromSourceField(FIELD_TABLE);
    }

    @Nullable
    @Override
    protected String getDatabaseName() {
        return getFromSourceField(FIELD_DB);
    }

    @Nullable
    private String getFromSourceField(String key) {
        GenericRecord source = (GenericRecord) valueRecord.get(FIELD_SOURCE);
        if (Objects.isNull(source)) {
            return null;
        }
        return (String) source.get(key);
    }

    protected Object getAndCheck(String key) {
        Object node = valueRecord.get(key);
        checkNotNull(node, key);
        return node;
    }
}
