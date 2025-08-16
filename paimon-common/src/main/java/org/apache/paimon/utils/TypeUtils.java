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

package org.apache.paimon.utils;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarCharType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static org.apache.paimon.types.DataTypeChecks.getNestedTypes;
import static org.apache.paimon.types.DataTypeFamily.BINARY_STRING;
import static org.apache.paimon.types.DataTypeFamily.CHARACTER_STRING;
import static org.apache.paimon.utils.DateTimeUtils.timestampWithLocalZoneToTimestamp;

/** Type related helper functions. */
public class TypeUtils {
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(TypeUtils.class);

    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final DateTimeFormatter FORMATTER_0 =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter FORMATTER_1 =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S");
    private static final DateTimeFormatter FORMATTER_2 =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SS");
    private static final DateTimeFormatter FORMATTER_3 =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final DateTimeFormatter FORMATTER_6 =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
    private static final DateTimeFormatter FORMATTER_9 =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");

    public static RowType concat(RowType left, RowType right) {
        RowType.Builder builder = RowType.builder();
        List<DataField> fields = new ArrayList<>(left.getFields());
        fields.addAll(right.getFields());
        fields.forEach(
                dataField ->
                        builder.field(dataField.name(), dataField.type(), dataField.description()));
        return builder.build();
    }

    public static RowType project(RowType inputType, int[] mapping) {
        List<DataField> fields = inputType.getFields();
        return new RowType(
                Arrays.stream(mapping).mapToObj(fields::get).collect(Collectors.toList()));
    }

    public static RowType project(RowType inputType, List<String> names) {
        List<DataField> fields = inputType.getFields();
        List<String> fieldNames = fields.stream().map(DataField::name).collect(Collectors.toList());
        return new RowType(
                names.stream()
                        .map(k -> fields.get(fieldNames.indexOf(k)))
                        .collect(Collectors.toList()));
    }

    public static Object castFromString(String s, DataType type) {
        return castFromStringInternal(s, type, false);
    }

    public static Object castFromCdcValueString(String s, DataType type) {
        return castFromStringInternal(s, type, true);
    }

    public static Object castFromStringInternal(String s, DataType type, boolean isCdcValue) {
        BinaryString str = BinaryString.fromString(s);
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                int stringLength = DataTypeChecks.getLength(type);
                if (stringLength != VarCharType.MAX_LENGTH && str.numChars() > stringLength) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Length of type %s is %d, but casting result has a length of %d",
                                    type, stringLength, s.length()));
                }
                return str;
            case BOOLEAN:
                return BinaryStringUtils.toBoolean(str);
            case BINARY:
                return isCdcValue
                        ? Base64.getDecoder().decode(s)
                        : s.getBytes(StandardCharsets.UTF_8);
            case VARBINARY:
                int binaryLength = DataTypeChecks.getLength(type);
                byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
                if (bytes.length > binaryLength) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Length of type %s is %d, but casting result has a length of %d",
                                    type, binaryLength, bytes.length));
                }
                return bytes;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                return Decimal.fromBigDecimal(
                        new BigDecimal(s), decimalType.getPrecision(), decimalType.getScale());
            case TINYINT:
                return Byte.valueOf(s);
            case SMALLINT:
                return Short.valueOf(s);
            case INTEGER:
                return Integer.valueOf(s);
            case BIGINT:
                return Long.valueOf(s);
            case FLOAT:
                double d = Double.parseDouble(s);
                if (d == ((float) d)) {
                    return (float) d;
                } else {
                    // Compatible canal-cdc
                    Float f = Float.valueOf(s);
                    String floatStr = f.toString();
                    if (s.contains(".") && !s.contains("E")) {
                        int decimal = s.length() - s.indexOf(".") - 1;
                        floatStr = String.format("%." + decimal + "f", f);
                    }
                    if (!floatStr.equals(s)) {
                        throw new NumberFormatException(
                                s + " cannot be cast to float due to precision loss");
                    } else {
                        return f;
                    }
                }
            case DOUBLE:
                return Double.valueOf(s);
            case DATE:
                return BinaryStringUtils.toDate(str);
            case TIME_WITHOUT_TIME_ZONE:
                return BinaryStringUtils.toTime(str);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) type;
                return BinaryStringUtils.toTimestamp(str, timestampType.getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType localZonedTimestampType = (LocalZonedTimestampType) type;
                return BinaryStringUtils.toTimestamp(
                        str, localZonedTimestampType.getPrecision(), TimeZone.getDefault());
            case ARRAY:
                ArrayType arrayType = (ArrayType) type;
                DataType elementType = arrayType.getElementType();
                try {
                    JsonNode arrayNode = OBJECT_MAPPER.readTree(s);
                    List<Object> resultList = new ArrayList<>();
                    for (JsonNode elementNode : arrayNode) {
                        if (!elementNode.isNull()) {
                            String elementJson;
                            if (elementNode.isTextual()) {
                                elementJson = elementNode.asText();
                            } else {
                                elementJson = elementNode.toString();
                            }
                            Object elementObject =
                                    castFromStringInternal(elementJson, elementType, isCdcValue);
                            resultList.add(elementObject);
                        } else {
                            resultList.add(null);
                        }
                    }
                    return new GenericArray(resultList.toArray());
                } catch (JsonProcessingException e) {
                    LOG.info(
                            String.format(
                                    "Failed to parse ARRAY for type %s with value %s", type, s),
                            e);
                    // try existing code flow
                    if (elementType instanceof VarCharType) {
                        if (s.startsWith("[")) {
                            s = s.substring(1);
                        }
                        if (s.endsWith("]")) {
                            s = s.substring(0, s.length() - 1);
                        }
                        String[] ss = s.split(",");
                        BinaryString[] binaryStrings = new BinaryString[ss.length];
                        for (int i = 0; i < ss.length; i++) {
                            binaryStrings[i] = BinaryString.fromString(ss[i]);
                        }
                        return new GenericArray(binaryStrings);
                    } else {
                        throw new UnsupportedOperationException("Unsupported type " + type);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(
                            String.format("Failed to parse Json String %s", s), e);
                }
            case MAP:
                MapType mapType = (MapType) type;
                DataType keyType = mapType.getKeyType();
                DataType valueType = mapType.getValueType();
                try {
                    JsonNode mapNode = OBJECT_MAPPER.readTree(s);
                    Map<Object, Object> resultMap = new HashMap<>();
                    mapNode.fields()
                            .forEachRemaining(
                                    entry -> {
                                        Object key =
                                                castFromStringInternal(
                                                        entry.getKey(), keyType, isCdcValue);
                                        Object value = null;
                                        if (!entry.getValue().isNull()) {
                                            if (entry.getValue().isTextual()) {
                                                value =
                                                        castFromStringInternal(
                                                                entry.getValue().asText(),
                                                                valueType,
                                                                isCdcValue);
                                            } else {
                                                value =
                                                        castFromStringInternal(
                                                                entry.getValue().toString(),
                                                                valueType,
                                                                isCdcValue);
                                            }
                                        }
                                        resultMap.put(key, value);
                                    });
                    return new GenericMap(resultMap);
                } catch (JsonProcessingException e) {
                    LOG.info(
                            String.format("Failed to parse MAP for type %s with value %s", type, s),
                            e);
                    return new GenericMap(Collections.emptyMap());
                } catch (Exception e) {
                    throw new RuntimeException(
                            String.format("Failed to parse Json String %s", s), e);
                }
            case ROW:
                RowType rowType = (RowType) type;
                try {
                    JsonNode rowNode = OBJECT_MAPPER.readTree(s);
                    GenericRow genericRow =
                            new GenericRow(
                                    rowType.getFields()
                                            .size()); // TODO: What about RowKind? always +I?
                    for (int pos = 0; pos < rowType.getFields().size(); ++pos) {
                        DataField field = rowType.getFields().get(pos);
                        JsonNode fieldNode = rowNode.get(field.name());
                        if (fieldNode != null && !fieldNode.isNull()) {
                            String fieldJson;
                            if (fieldNode.isTextual()) {
                                fieldJson = fieldNode.asText();
                            } else {
                                fieldJson = fieldNode.toString();
                            }
                            Object fieldObject =
                                    castFromStringInternal(fieldJson, field.type(), isCdcValue);
                            genericRow.setField(pos, fieldObject);
                        } else {
                            genericRow.setField(pos, null); // Handle null fields
                        }
                    }
                    return genericRow;
                } catch (JsonProcessingException e) {
                    LOG.info(
                            String.format(
                                    "Failed to parse ROW for type  %s  with value  %s", type, s),
                            e);
                    return new GenericRow(0);
                } catch (Exception e) {
                    throw new RuntimeException(
                            String.format("Failed to parse Json String %s", s), e);
                }
            default:
                throw new UnsupportedOperationException("Unsupported type " + type);
        }
    }

    public static boolean isPrimitive(DataType type) {
        return isPrimitive(type.getTypeRoot());
    }

    public static boolean isPrimitive(DataTypeRoot root) {
        switch (root) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return true;
            default:
                return false;
        }
    }

    /**
     * Can the two types operate with each other. Such as: 1.CodeGen: equal, cast, assignment.
     * 2.Join keys.
     */
    public static boolean isInteroperable(DataType t1, DataType t2) {
        if (t1.getTypeRoot().getFamilies().contains(CHARACTER_STRING)
                && t2.getTypeRoot().getFamilies().contains(CHARACTER_STRING)) {
            return true;
        }
        if (t1.getTypeRoot().getFamilies().contains(BINARY_STRING)
                && t2.getTypeRoot().getFamilies().contains(BINARY_STRING)) {
            return true;
        }

        if (t1.getTypeRoot() != t2.getTypeRoot()) {
            return false;
        }

        switch (t1.getTypeRoot()) {
            case ARRAY:
            case MAP:
            case MULTISET:
            case ROW:
                List<DataType> children1 = getNestedTypes(t1);
                List<DataType> children2 = getNestedTypes(t2);
                if (children1.size() != children2.size()) {
                    return false;
                }
                for (int i = 0; i < children1.size(); i++) {
                    if (!isInteroperable(children1.get(i), children2.get(i))) {
                        return false;
                    }
                }
                return true;
            default:
                return t1.copy(true).equals(t2.copy(true));
        }
    }

    public static boolean isBasicType(Object obj) {
        Class<?> clazz = obj.getClass();
        return clazz.isPrimitive() || isWrapperType(clazz) || clazz.equals(String.class);
    }

    public static String castToString(Object value, DataType dataType)
            throws JsonProcessingException {
        if (value == null) {
            return null;
        }
        DataTypeRoot typeRoot = dataType.getTypeRoot();
        switch (typeRoot) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return value.toString();
            case DECIMAL:
                return ((Decimal) value).toBigDecimal().toString();
            case CHAR:
            case VARCHAR:
                return ((BinaryString) value).toString();
            case BINARY:
            case VARBINARY:
                return Base64.getEncoder().encodeToString((byte[]) value);
            case DATE:
                LocalDate date = LocalDate.ofEpochDay((int) value);
                return date.format(DATE_FORMATTER);
            case TIME_WITHOUT_TIME_ZONE:
                LocalTime time = LocalTime.ofNanoOfDay((int) value * 1_000_000L);
                return time.format(TIME_FORMATTER);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return convertTsToString(value, dataType);
            case ARRAY:
            case MAP:
            case ROW:
                return JsonSerdeUtil.writeValueAsString(convertValue(value, dataType));
            default:
                return value.toString();
        }
    }

    public static Object convertValue(Object value, DataType dataType) {
        if (value == null) {
            return null;
        }
        DataTypeRoot typeRoot = dataType.getTypeRoot();
        switch (typeRoot) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return value;
            case CHAR:
            case VARCHAR:
                return ((BinaryString) value).toString();
            case BINARY:
            case VARBINARY:
                return Base64.getEncoder().encodeToString((byte[]) value);
            case DECIMAL:
                return ((Decimal) value).toBigDecimal().toString();
            case DATE:
                int daysSinceEpoch = (Integer) value;
                return LocalDate.ofEpochDay(daysSinceEpoch).toString();
            case TIME_WITHOUT_TIME_ZONE:
                int millisOfDay = (Integer) value;
                return LocalTime.ofNanoOfDay(millisOfDay * 1_000_000L).toString();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return convertTsToString(value, dataType);
            case ARRAY:
                return convertArray((InternalArray) value, (ArrayType) dataType);
            case MAP:
                return convertMap((InternalMap) value, (MapType) dataType);
            case ROW:
                return convertRow((InternalRow) value, (RowType) dataType);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + dataType);
        }
    }

    private static String convertTsToString(Object value, DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) dataType;
                Timestamp timestamp = (Timestamp) value;
                return formatTimeStampWithPrecision(
                        timestamp.toLocalDateTime(), timestampType.getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType localZonedTimestampType =
                        (LocalZonedTimestampType) dataType;
                timestamp = (Timestamp) value;
                timestamp = timestampWithLocalZoneToTimestamp(timestamp, TimeZone.getDefault());
                return formatTimeStampWithPrecision(
                        timestamp.toLocalDateTime(), localZonedTimestampType.getPrecision());
            default:
                throw new UnsupportedOperationException("Unsupported type: " + dataType);
        }
    }

    private static String formatTimeStampWithPrecision(LocalDateTime dateTime, int precision) {
        switch (precision) {
            case 0:
                return dateTime.format(FORMATTER_0);
            case 1:
                return dateTime.format(FORMATTER_1);
            case 2:
                return dateTime.format(FORMATTER_2);
            case 3:
                return dateTime.format(FORMATTER_3);
            case 6:
                return dateTime.format(FORMATTER_6);
            case 9:
                return dateTime.format(FORMATTER_9);
            default:
                return dateTime.format(FORMATTER_0);
        }
    }

    public static Map<String, Object> convertRow(InternalRow row, RowType rowType) {
        List<DataField> fields = rowType.getFields();
        Map<String, Object> result = new LinkedHashMap<>();

        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            Object value = InternalRowUtils.get(row, i, field.type());
            result.put(field.name(), convertValue(value, field.type()));
        }

        return result;
    }

    private static List<Object> convertArray(InternalArray array, ArrayType arrayType) {
        DataType elementType = arrayType.getElementType();
        List<Object> result = new ArrayList<>();

        for (int i = 0; i < array.size(); i++) {
            Object element = InternalRowUtils.get(array, i, elementType);
            result.add(convertValue(element, elementType));
        }

        return result;
    }

    private static Map<String, Object> convertMap(InternalMap map, MapType mapType) {
        DataType keyType = mapType.getKeyType();
        DataType valueType = mapType.getValueType();
        Map<String, Object> result = new LinkedHashMap<>();

        InternalArray keyArray = map.keyArray();
        InternalArray valueArray = map.valueArray();

        for (int i = 0; i < map.size(); i++) {
            Object key = InternalRowUtils.get(keyArray, i, keyType);
            Object value = InternalRowUtils.get(valueArray, i, valueType);

            String keyStr = convertToString(key, keyType);
            Object convertedValue = convertValue(value, valueType);
            result.put(keyStr, convertedValue);
        }

        return result;
    }

    private static String convertToString(Object value, DataType dataType) {
        if (value == null) {
            return null;
        }

        DataTypeRoot typeRoot = dataType.getTypeRoot();
        switch (typeRoot) {
            case CHAR:
            case VARCHAR:
                return ((BinaryString) value).toString();
            default:
                return value.toString();
        }
    }

    private static boolean isWrapperType(Class<?> clazz) {
        return clazz.equals(Boolean.class)
                || clazz.equals(Character.class)
                || clazz.equals(Byte.class)
                || clazz.equals(Short.class)
                || clazz.equals(Integer.class)
                || clazz.equals(Long.class)
                || clazz.equals(Float.class)
                || clazz.equals(Double.class);
    }
}
