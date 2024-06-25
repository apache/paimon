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
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarCharType;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static org.apache.paimon.types.DataTypeChecks.getNestedTypes;
import static org.apache.paimon.types.DataTypeFamily.BINARY_STRING;
import static org.apache.paimon.types.DataTypeFamily.CHARACTER_STRING;

/** Type related helper functions. */
public class TypeUtils {

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
                    if (f.toString().length() != s.length()) {
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
