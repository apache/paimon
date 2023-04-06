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
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;

import java.math.BigDecimal;
import java.time.DateTimeException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.types.DataTypeChecks.getNestedTypes;
import static org.apache.paimon.types.DataTypeFamily.BINARY_STRING;
import static org.apache.paimon.types.DataTypeFamily.CHARACTER_STRING;

/** Type related helper functions. */
public class TypeUtils {

    private static final List<BinaryString> TRUE_STRINGS =
            Stream.of("t", "true", "y", "yes", "1")
                    .map(BinaryString::fromString)
                    .collect(Collectors.toList());

    private static final List<BinaryString> FALSE_STRINGS =
            Stream.of("f", "false", "n", "no", "0")
                    .map(BinaryString::fromString)
                    .collect(Collectors.toList());

    public static RowType project(RowType inputType, int[] mapping) {
        List<DataField> fields = inputType.getFields();
        return new RowType(
                Arrays.stream(mapping).mapToObj(fields::get).collect(Collectors.toList()));
    }

    public static Object castFromString(String s, DataType type) {
        BinaryString str = BinaryString.fromString(s);
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                int stringLength = DataTypeChecks.getLength(type);
                if (s.length() > stringLength) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Length of type %s is %d, but casting result has a length of %d",
                                    type, stringLength, s.length()));
                }
                return str;
            case BOOLEAN:
                return toBoolean(str);
            case BINARY:
            case VARBINARY:
                int binaryLength = DataTypeChecks.getLength(type);
                byte[] bytes = s.getBytes();
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
                    throw new NumberFormatException(
                            s + " cannot be cast to float due to precision loss");
                }
            case DOUBLE:
                return Double.valueOf(s);
            case DATE:
                return toDate(str);
            case TIME_WITHOUT_TIME_ZONE:
                return toTime(str);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) type;
                return toTimestamp(str, timestampType.getPrecision());
            default:
                throw new UnsupportedOperationException("Unsupported type " + type);
        }
    }

    public static int timestampPrecision(DataType type) {
        if (type instanceof TimestampType) {
            return ((TimestampType) type).getPrecision();
        } else if (type instanceof LocalZonedTimestampType) {
            return ((LocalZonedTimestampType) type).getPrecision();
        }

        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    /** Parse a {@link BinaryString} to boolean. */
    public static boolean toBoolean(BinaryString str) {
        BinaryString lowerCase = str.toLowerCase();
        if (TRUE_STRINGS.contains(lowerCase)) {
            return true;
        }
        if (FALSE_STRINGS.contains(lowerCase)) {
            return false;
        }
        throw new RuntimeException("Cannot parse '" + str + "' as BOOLEAN.");
    }

    public static int toDate(BinaryString input) throws DateTimeException {
        Integer date = DateTimeUtils.parseDate(input.toString());
        if (date == null) {
            throw new DateTimeException("For input string: '" + input + "'.");
        }

        return date;
    }

    public static int toTime(BinaryString input) throws DateTimeException {
        Integer date = DateTimeUtils.parseTime(input.toString());
        if (date == null) {
            throw new DateTimeException("For input string: '" + input + "'.");
        }

        return date;
    }

    /** Used by {@code CAST(x as TIMESTAMP)}. */
    public static Timestamp toTimestamp(BinaryString input, int precision)
            throws DateTimeException {
        return DateTimeUtils.parseTimestampData(input.toString(), precision);
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
}
