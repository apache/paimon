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

package org.apache.paimon.format.parquet;

import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.Pair;

import org.apache.parquet.schema.ConversionPatterns;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

/** Schema converter converts Parquet schema to and from Paimon internal types. */
public class ParquetSchemaConverter {

    static final String PAIMON_SCHEMA = "paimon_schema";

    static final String MAP_REPEATED_NAME = "key_value";
    static final String MAP_KEY_NAME = "key";
    static final String MAP_VALUE_NAME = "value";
    static final String LIST_REPEATED_NAME = "list";
    static final String LIST_ELEMENT_NAME = "element";

    /** Convert paimon {@link RowType} to parquet {@link MessageType}. */
    public static MessageType convertToParquetMessageType(RowType rowType) {
        return new MessageType(PAIMON_SCHEMA, convertToParquetTypes(rowType));
    }

    private static Type[] convertToParquetTypes(RowType rowType) {
        return rowType.getFields().stream()
                .map(ParquetSchemaConverter::convertToParquetType)
                .toArray(Type[]::new);
    }

    /** Convert paimon {@link DataField} to parquet {@link Type}. */
    public static Type convertToParquetType(DataField field) {
        return convertToParquetType(field.name(), field.type(), field.id(), 0);
    }

    private static Type convertToParquetType(String name, DataType type, int fieldId, int depth) {
        Type.Repetition repetition =
                type.isNullable() ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED;
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
                        .as(LogicalTypeAnnotation.stringType())
                        .named(name)
                        .withId(fieldId);
            case BOOLEAN:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, repetition)
                        .named(name)
                        .withId(fieldId);
            case BINARY:
            case VARBINARY:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
                        .named(name)
                        .withId(fieldId);
            case DECIMAL:
                int precision = ((DecimalType) type).getPrecision();
                int scale = ((DecimalType) type).getScale();
                if (is32BitDecimal(precision)) {
                    return Types.primitive(INT32, repetition)
                            .as(LogicalTypeAnnotation.decimalType(scale, precision))
                            .named(name)
                            .withId(fieldId);
                } else if (is64BitDecimal(precision)) {
                    return Types.primitive(INT64, repetition)
                            .as(LogicalTypeAnnotation.decimalType(scale, precision))
                            .named(name)
                            .withId(fieldId);
                } else {
                    return Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition)
                            .as(LogicalTypeAnnotation.decimalType(scale, precision))
                            .length(computeMinBytesForDecimalPrecision(precision))
                            .named(name)
                            .withId(fieldId);
                }
            case TINYINT:
                return Types.primitive(INT32, repetition)
                        .as(LogicalTypeAnnotation.intType(8, true))
                        .named(name)
                        .withId(fieldId);
            case SMALLINT:
                return Types.primitive(INT32, repetition)
                        .as(LogicalTypeAnnotation.intType(16, true))
                        .named(name)
                        .withId(fieldId);
            case INTEGER:
                return Types.primitive(INT32, repetition).named(name).withId(fieldId);
            case BIGINT:
                return Types.primitive(INT64, repetition).named(name).withId(fieldId);
            case FLOAT:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, repetition)
                        .named(name)
                        .withId(fieldId);
            case DOUBLE:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, repetition)
                        .named(name)
                        .withId(fieldId);
            case DATE:
                return Types.primitive(INT32, repetition)
                        .as(LogicalTypeAnnotation.dateType())
                        .named(name)
                        .withId(fieldId);
            case TIME_WITHOUT_TIME_ZONE:
                return Types.primitive(INT32, repetition)
                        .as(
                                LogicalTypeAnnotation.timeType(
                                        true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                        .named(name)
                        .withId(fieldId);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) type;
                return createTimestampWithLogicalType(
                                name, timestampType.getPrecision(), repetition, false)
                        .withId(fieldId);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType localZonedTimestampType = (LocalZonedTimestampType) type;
                return createTimestampWithLogicalType(
                                name, localZonedTimestampType.getPrecision(), repetition, true)
                        .withId(fieldId);
            case ARRAY:
                ArrayType arrayType = (ArrayType) type;
                Type elementParquetType =
                        convertToParquetType(
                                        LIST_ELEMENT_NAME,
                                        arrayType.getElementType(),
                                        fieldId,
                                        depth + 1)
                                .withId(SpecialFields.getArrayElementFieldId(fieldId, depth + 1));
                return ConversionPatterns.listOfElements(repetition, name, elementParquetType)
                        .withId(fieldId);
            case MAP:
                MapType mapType = (MapType) type;
                DataType keyType = mapType.getKeyType();
                if (keyType.isNullable()) {
                    // key is nullable, but Parquet does not support nullable keys, so we configure
                    // it as not nullable
                    keyType = keyType.copy(false);
                }
                Type mapKeyParquetType =
                        convertToParquetType(MAP_KEY_NAME, keyType, fieldId, depth + 1)
                                .withId(SpecialFields.getMapKeyFieldId(fieldId, depth + 1));
                Type mapValueParquetType =
                        convertToParquetType(
                                        MAP_VALUE_NAME, mapType.getValueType(), fieldId, depth + 1)
                                .withId(SpecialFields.getMapValueFieldId(fieldId, depth + 1));
                return ConversionPatterns.mapType(
                                repetition,
                                name,
                                MAP_REPEATED_NAME,
                                mapKeyParquetType,
                                mapValueParquetType)
                        .withId(fieldId);
            case MULTISET:
                MultisetType multisetType = (MultisetType) type;
                DataType elementType = multisetType.getElementType();
                if (elementType.isNullable()) {
                    // element type is nullable, but Parquet does not support nullable map keys,
                    // so we configure it as not nullable
                    elementType = elementType.copy(false);
                }
                Type multisetKeyParquetType =
                        convertToParquetType(MAP_KEY_NAME, elementType, fieldId, depth + 1)
                                .withId(SpecialFields.getMapKeyFieldId(fieldId, depth + 1));
                Type multisetValueParquetType =
                        convertToParquetType(MAP_VALUE_NAME, new IntType(false), fieldId, depth + 1)
                                .withId(SpecialFields.getMapValueFieldId(fieldId, depth + 1));
                return ConversionPatterns.mapType(
                                repetition,
                                name,
                                MAP_REPEATED_NAME,
                                multisetKeyParquetType,
                                multisetValueParquetType)
                        .withId(fieldId);
            case ROW:
                RowType rowType = (RowType) type;
                return new GroupType(repetition, name, convertToParquetTypes(rowType))
                        .withId(fieldId);
            case VARIANT:
                return Types.buildGroup(repetition)
                        .addField(
                                Types.primitive(
                                                PrimitiveType.PrimitiveTypeName.BINARY,
                                                Type.Repetition.REQUIRED)
                                        .named(Variant.VALUE))
                        .addField(
                                Types.primitive(
                                                PrimitiveType.PrimitiveTypeName.BINARY,
                                                Type.Repetition.REQUIRED)
                                        .named(Variant.METADATA))
                        .named(name);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private static Type createTimestampWithLogicalType(
            String name, int precision, Type.Repetition repetition, boolean isAdjustToUTC) {
        if (precision <= 3) {
            return Types.primitive(INT64, repetition)
                    .as(
                            LogicalTypeAnnotation.timestampType(
                                    isAdjustToUTC, LogicalTypeAnnotation.TimeUnit.MILLIS))
                    .named(name);
        } else if (precision > 6) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.INT96, repetition).named(name);
        } else {
            return Types.primitive(INT64, repetition)
                    .as(
                            LogicalTypeAnnotation.timestampType(
                                    isAdjustToUTC, LogicalTypeAnnotation.TimeUnit.MICROS))
                    .named(name);
        }
    }

    public static int computeMinBytesForDecimalPrecision(int precision) {
        int numBytes = 1;
        while (Math.pow(2.0, 8 * numBytes - 1) < Math.pow(10.0, precision)) {
            numBytes += 1;
        }
        return numBytes;
    }

    // From Decimal.Utils
    public static boolean is32BitDecimal(int precision) {
        return precision <= 9;
    }

    public static boolean is64BitDecimal(int precision) {
        return precision <= 18 && precision > 9;
    }

    /** Convert parquet {@link MessageType} to paimon {@link RowType}. */
    public static RowType convertToPaimonRowType(MessageType messageType) {
        List<DataField> dataFields =
                messageType.asGroupType().getFields().stream()
                        .map(ParquetSchemaConverter::convertToPaimonField)
                        .collect(Collectors.toList());
        return new RowType(dataFields);
    }

    /** Convert parquet {@link Type} to paimon {@link DataField} to. */
    public static DataField convertToPaimonField(Type parquetType) {
        LogicalTypeAnnotation logicalType = parquetType.getLogicalTypeAnnotation();
        DataType paimonDataType;

        if (parquetType.isPrimitive()) {
            switch (parquetType.asPrimitiveType().getPrimitiveTypeName()) {
                case BINARY:
                    if (logicalType instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
                        paimonDataType = DataTypes.STRING();
                    } else {
                        paimonDataType = DataTypes.BYTES();
                    }
                    break;
                case BOOLEAN:
                    paimonDataType = DataTypes.BOOLEAN();
                    break;
                case FLOAT:
                    paimonDataType = DataTypes.FLOAT();
                    break;
                case DOUBLE:
                    paimonDataType = DataTypes.DOUBLE();
                    break;
                case INT32:
                    if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                        LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType =
                                (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalType;
                        paimonDataType =
                                new DecimalType(decimalType.getPrecision(), decimalType.getScale());
                    } else if (logicalType
                            instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
                        LogicalTypeAnnotation.IntLogicalTypeAnnotation intType =
                                (LogicalTypeAnnotation.IntLogicalTypeAnnotation) logicalType;
                        int bitWidth = intType.getBitWidth();
                        if (bitWidth == 8) {
                            paimonDataType = DataTypes.TINYINT();
                        } else if (bitWidth == 16) {
                            paimonDataType = DataTypes.SMALLINT();
                        } else {
                            paimonDataType = DataTypes.INT();
                        }
                    } else if (logicalType
                            instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
                        paimonDataType = DataTypes.DATE();
                    } else if (logicalType
                            instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation) {
                        paimonDataType = DataTypes.TIME();
                    } else {
                        paimonDataType = DataTypes.INT();
                    }
                    break;
                case INT64:
                    if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                        LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType =
                                (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalType;
                        paimonDataType =
                                new DecimalType(decimalType.getPrecision(), decimalType.getScale());
                    } else if (logicalType
                            instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
                        LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampType =
                                (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalType;
                        int precision =
                                timestampType
                                                .getUnit()
                                                .equals(LogicalTypeAnnotation.TimeUnit.MILLIS)
                                        ? 3
                                        : 6;
                        paimonDataType =
                                timestampType.isAdjustedToUTC()
                                        ? new LocalZonedTimestampType(precision)
                                        : new TimestampType(precision);
                    } else {
                        paimonDataType = DataTypes.BIGINT();
                    }
                    break;
                case INT96:
                    paimonDataType = new TimestampType(9);
                    break;
                case FIXED_LEN_BYTE_ARRAY:
                    LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType =
                            (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalType;
                    paimonDataType =
                            new DecimalType(decimalType.getPrecision(), decimalType.getScale());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported type: " + parquetType);
            }
            if (parquetType.getRepetition().equals(Type.Repetition.REQUIRED)) {
                paimonDataType = paimonDataType.notNull();
            }
            return new DataField(
                    parquetType.getId().intValue(), parquetType.getName(), paimonDataType);
        } else {
            GroupType groupType = parquetType.asGroupType();
            if (logicalType instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
                paimonDataType =
                        new ArrayType(
                                convertToPaimonField(parquetListElementType(groupType)).type());
            } else if (logicalType instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation) {
                Pair<Type, Type> keyValueType = parquetMapKeyValueType(groupType);
                paimonDataType =
                        new MapType(
                                // Since parquet does not support nullable key, when converting
                                // back to Paimon, set as nullable by default.
                                convertToPaimonField(keyValueType.getLeft()).type().nullable(),
                                convertToPaimonField(keyValueType.getRight()).type());
            } else {
                paimonDataType =
                        new RowType(
                                groupType.getFields().stream()
                                        .map(ParquetSchemaConverter::convertToPaimonField)
                                        .collect(Collectors.toList()));
            }
        }

        if (parquetType.getRepetition().equals(Type.Repetition.REQUIRED)) {
            paimonDataType = paimonDataType.notNull();
        }

        return new DataField(parquetType.getId().intValue(), parquetType.getName(), paimonDataType);
    }

    public static Type parquetListElementType(GroupType listType) {
        return listType.getType(LIST_REPEATED_NAME).asGroupType().getType(LIST_ELEMENT_NAME);
    }

    public static Pair<Type, Type> parquetMapKeyValueType(GroupType mapType) {
        GroupType keyValue = mapType.getType(MAP_REPEATED_NAME).asGroupType();
        return Pair.of(keyValue.getType(MAP_KEY_NAME), keyValue.getType(MAP_VALUE_NAME));
    }
}
