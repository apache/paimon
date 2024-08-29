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

package org.apache.paimon.format.avro;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.List;
import java.util.Map;

/** Converts an Avro schema into Paimon's type information. */
public class AvroSchemaConverter {

    private AvroSchemaConverter() {
        // private
    }

    /**
     * Converts Paimon {@link DataType} (can be nested) into an Avro schema.
     *
     * <p>Use "org.apache.paimon.avro.generated.record" as the type name.
     *
     * @param schema the schema type, usually it should be the top level record type, e.g. not a
     *     nested type
     * @return Avro's {@link Schema} matching this logical type.
     */
    public static Schema convertToSchema(DataType schema, Map<String, String> rowNameMapping) {
        return convertToSchema(schema, "org.apache.paimon.avro.generated.record", rowNameMapping);
    }

    /**
     * Converts Paimon {@link DataType} (can be nested) into an Avro schema.
     *
     * <p>The "{rowName}_" is used as the nested row type name prefix in order to generate the right
     * schema. Nested record type that only differs with type name is still compatible.
     *
     * @param dataType logical type
     * @param rowName the record name
     * @return Avro's {@link Schema} matching this logical type.
     */
    public static Schema convertToSchema(
            DataType dataType, String rowName, Map<String, String> rowNameMapping) {
        if (rowNameMapping.containsKey(rowName)) {
            rowName = rowNameMapping.get(rowName);
        }

        int precision;
        boolean nullable = dataType.isNullable();
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                Schema bool = SchemaBuilder.builder().booleanType();
                return nullable ? nullableSchema(bool) : bool;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                Schema integer = SchemaBuilder.builder().intType();
                return nullable ? nullableSchema(integer) : integer;
            case BIGINT:
                Schema bigint = SchemaBuilder.builder().longType();
                return nullable ? nullableSchema(bigint) : bigint;
            case FLOAT:
                Schema f = SchemaBuilder.builder().floatType();
                return nullable ? nullableSchema(f) : f;
            case DOUBLE:
                Schema d = SchemaBuilder.builder().doubleType();
                return nullable ? nullableSchema(d) : d;
            case CHAR:
            case VARCHAR:
                Schema str = SchemaBuilder.builder().stringType();
                return nullable ? nullableSchema(str) : str;
            case BINARY:
            case VARBINARY:
                Schema binary = SchemaBuilder.builder().bytesType();
                return nullable ? nullableSchema(binary) : binary;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                // use long to represents Timestamp
                final TimestampType timestampType = (TimestampType) dataType;
                precision = timestampType.getPrecision();
                org.apache.avro.LogicalType avroLogicalType;
                if (precision <= 3) {
                    avroLogicalType = LogicalTypes.timestampMillis();
                } else if (precision <= 6) {
                    avroLogicalType = LogicalTypes.timestampMicros();
                } else {
                    throw new IllegalArgumentException(
                            "Avro does not support TIMESTAMP type "
                                    + "with precision: "
                                    + precision
                                    + ", it only supports precision less than 6.");
                }
                Schema timestamp = avroLogicalType.addToSchema(SchemaBuilder.builder().longType());
                return nullable ? nullableSchema(timestamp) : timestamp;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final LocalZonedTimestampType localTimestampType =
                        (LocalZonedTimestampType) dataType;
                precision = localTimestampType.getPrecision();
                org.apache.avro.LogicalType localTimestampLogicalType;
                if (precision <= 3) {
                    localTimestampLogicalType = LogicalTypes.localTimestampMillis();
                } else if (precision <= 6) {
                    localTimestampLogicalType = LogicalTypes.localTimestampMicros();
                } else {
                    throw new IllegalArgumentException(
                            "Avro does not support TIMESTAMP type "
                                    + "with precision: "
                                    + precision
                                    + ", it only supports precision less than 6.");
                }
                Schema localTimestampSchema =
                        localTimestampLogicalType.addToSchema(SchemaBuilder.builder().longType());
                return nullable ? nullableSchema(localTimestampSchema) : localTimestampSchema;
            case DATE:
                // use int to represents Date
                Schema date = LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType());
                return nullable ? nullableSchema(date) : date;
            case TIME_WITHOUT_TIME_ZONE:
                precision = ((TimeType) dataType).getPrecision();
                if (precision > 3) {
                    throw new IllegalArgumentException(
                            "Avro does not support TIME type with precision: "
                                    + precision
                                    + ", it only supports precision less than 3.");
                }
                // use int to represents Time, we only support millisecond when deserialization
                Schema time =
                        LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder().intType());
                return nullable ? nullableSchema(time) : time;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) dataType;
                // store BigDecimal as byte[]
                Schema decimal =
                        LogicalTypes.decimal(decimalType.getPrecision(), decimalType.getScale())
                                .addToSchema(SchemaBuilder.builder().bytesType());
                return nullable ? nullableSchema(decimal) : decimal;
            case ROW:
                RowType rowType = (RowType) dataType;
                List<String> fieldNames = rowType.getFieldNames();
                // we have to make sure the record name is different in a Schema
                SchemaBuilder.FieldAssembler<Schema> builder =
                        SchemaBuilder.builder().record(rowName).fields();
                for (int i = 0; i < rowType.getFieldCount(); i++) {
                    String fieldName = fieldNames.get(i);
                    DataType fieldType = rowType.getTypeAt(i);
                    SchemaBuilder.GenericDefault<Schema> fieldBuilder =
                            builder.name(fieldName)
                                    .type(
                                            convertToSchema(
                                                    fieldType,
                                                    rowName + "_" + fieldName,
                                                    rowNameMapping));

                    if (fieldType.isNullable()) {
                        builder = fieldBuilder.withDefault(null);
                    } else {
                        builder = fieldBuilder.noDefault();
                    }
                }
                Schema record = builder.endRecord();
                return nullable ? nullableSchema(record) : record;
            case MULTISET:
            case MAP:
                DataType keyType = extractKeyTypeToAvroMap(dataType);
                DataType valueType = extractValueTypeToAvroMap(dataType);
                Schema map;
                if (isArrayMap(dataType)) {
                    // Avro only natively support map with string key.
                    // To represent a map with non-string key, we use an array containing several
                    // rows. The first field of a row is the key, and the second field is the value.
                    SchemaBuilder.GenericDefault<Schema> kvBuilder =
                            SchemaBuilder.builder()
                                    .record(rowName)
                                    .fields()
                                    .name("key")
                                    .type(
                                            convertToSchema(
                                                    keyType, rowName + "_key", rowNameMapping))
                                    .noDefault()
                                    .name("value")
                                    .type(
                                            convertToSchema(
                                                    valueType, rowName + "_value", rowNameMapping));
                    SchemaBuilder.FieldAssembler<Schema> assembler =
                            valueType.isNullable()
                                    ? kvBuilder.withDefault(null)
                                    : kvBuilder.noDefault();
                    map = SchemaBuilder.builder().array().items(assembler.endRecord());
                    map = LogicalMap.get().addToSchema(map);
                } else {
                    map =
                            SchemaBuilder.builder()
                                    .map()
                                    .values(convertToSchema(valueType, rowName, rowNameMapping));
                }
                return nullable ? nullableSchema(map) : map;
            case ARRAY:
                ArrayType arrayType = (ArrayType) dataType;
                Schema array =
                        SchemaBuilder.builder()
                                .array()
                                .items(
                                        convertToSchema(
                                                arrayType.getElementType(),
                                                rowName,
                                                rowNameMapping));
                return nullable ? nullableSchema(array) : array;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported to derive Schema for type: " + dataType);
        }
    }

    public static boolean isArrayMap(DataType type) {
        DataType keyType = extractKeyTypeToAvroMap(type);
        return keyType.getTypeRoot() != DataTypeRoot.VARCHAR
                && keyType.getTypeRoot() != DataTypeRoot.CHAR;
    }

    public static DataType extractKeyTypeToAvroMap(DataType type) {
        if (type instanceof MapType) {
            MapType mapType = (MapType) type;
            return mapType.getKeyType();
        } else {
            MultisetType multisetType = (MultisetType) type;
            return multisetType.getElementType();
        }
    }

    public static DataType extractValueTypeToAvroMap(DataType type) {
        if (type instanceof MapType) {
            MapType mapType = (MapType) type;
            return mapType.getValueType();
        } else {
            return new IntType();
        }
    }

    /** Returns schema with nullable true. */
    private static Schema nullableSchema(Schema schema) {
        return schema.isNullable()
                ? schema
                : Schema.createUnion(SchemaBuilder.builder().nullType(), schema);
    }
}
