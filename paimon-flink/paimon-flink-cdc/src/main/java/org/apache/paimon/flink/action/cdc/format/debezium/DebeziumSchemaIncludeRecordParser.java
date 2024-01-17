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
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.List;

import static io.debezium.pipeline.signal.SchemaChanges.FIELD_SCHEMA;

/**
 * The {@code DebeziumSchemaIncludeRecordParser} class extends the {@link RecordParser} to parse
 * Debezium's change data capture (CDC) records, specifically focusing on parsing records that
 * include the complete Debezium schema. This inclusion of the schema allows for a more detailed and
 * accurate interpretation of the CDC data.
 */
public class DebeziumSchemaIncludeRecordParser extends DebeziumRecordParser {

    private JsonNode schema;

    public DebeziumSchemaIncludeRecordParser(
            boolean caseSensitive, TypeMapping typeMapping, List<ComputedColumn> computedColumns) {
        super(caseSensitive, typeMapping, computedColumns);
    }

    @Override
    protected LinkedHashMap<String, DataType> fillDefaultTypes(JsonNode record) {
        CdcDebeziumSchema cdcDebeziumSchema;
        try {
            cdcDebeziumSchema = JsonSerdeUtil.fromJson(schema.toString(), CdcDebeziumSchema.class);
        } catch (UncheckedIOException e) {
            throw new RuntimeException(e);
        }
        LinkedHashMap<String, DataType> fieldTypes = new LinkedHashMap<>();
        CdcDebeziumSchema field = cdcDebeziumSchema.fields().get(1);
        for (CdcDebeziumSchema f : field.fields()) {
            DataType dataType = toDataType(f.type());
            fieldTypes.put(f.field(), dataType);
        }
        return fieldTypes;
    }

    public static DataType toDataType(String type) {
        switch (type) {
            case "int32":
                return DataTypes.INT();
            case "int64":
                return DataTypes.BIGINT();
            case "string":
                return DataTypes.STRING();
            case "float32":
            case "float64":
                return DataTypes.FLOAT();
            case "double":
                return DataTypes.DOUBLE();
            case "bytes":
                return DataTypes.BYTES();
            default:
                throw new UnsupportedOperationException(
                        String.format("Don't support type '%s' yet.", type));
        }
    }

    @Override
    protected void setRoot(String record) {
        JsonNode node = JsonSerdeUtil.fromJson(record, JsonNode.class);
        root = node.get(FIELD_PAYLOAD);
        schema = node.get(FIELD_SCHEMA);
    }

    @Override
    protected String format() {
        return "debezium_json_schema_include";
    }
}
