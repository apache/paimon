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

package org.apache.flink.table.store.utils;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import java.math.BigDecimal;
import java.time.DateTimeException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.types.logical.LogicalTypeFamily.BINARY_STRING;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.CHARACTER_STRING;

/** Type related helper functions. */
public class TypeUtils {

    private static final List<BinaryStringData> TRUE_STRINGS =
            Stream.of("t", "true", "y", "yes", "1")
                    .map(BinaryStringData::fromString)
                    .peek(BinaryStringData::ensureMaterialized)
                    .collect(Collectors.toList());

    private static final List<BinaryStringData> FALSE_STRINGS =
            Stream.of("f", "false", "n", "no", "0")
                    .map(BinaryStringData::fromString)
                    .peek(BinaryStringData::ensureMaterialized)
                    .collect(Collectors.toList());

    public static RowType project(RowType inputType, int[] mapping) {
        List<RowType.RowField> fields = inputType.getFields();
        return new RowType(
                Arrays.stream(mapping).mapToObj(fields::get).collect(Collectors.toList()));
    }

    public static Object castFromString(String s, LogicalType type) {
        BinaryStringData str = BinaryStringData.fromString(s);
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return str;
            case BOOLEAN:
                return toBoolean(str);
            case BINARY:
            case VARBINARY:
                // this implementation does not match the new behavior of StringToBinaryCastRule,
                // change this if needed
                return s.getBytes();
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                return DecimalData.fromBigDecimal(
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
                return Float.valueOf(s);
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

    public static int timestampPrecision(LogicalType type) {
        if (type instanceof TimestampType) {
            return ((TimestampType) type).getPrecision();
        } else if (type instanceof LocalZonedTimestampType) {
            return ((LocalZonedTimestampType) type).getPrecision();
        }

        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    /** Parse a {@link StringData} to boolean. */
    public static boolean toBoolean(BinaryStringData str) throws TableException {
        BinaryStringData lowerCase = str.toLowerCase();
        if (TRUE_STRINGS.contains(lowerCase)) {
            return true;
        }
        if (FALSE_STRINGS.contains(lowerCase)) {
            return false;
        }
        throw new TableException("Cannot parse '" + str + "' as BOOLEAN.");
    }

    public static int toDate(BinaryStringData input) throws DateTimeException {
        Integer date = DateTimeUtils.parseDate(input.toString());
        if (date == null) {
            throw new DateTimeException("For input string: '" + input + "'.");
        }

        return date;
    }

    public static int toTime(BinaryStringData input) throws DateTimeException {
        Integer date = DateTimeUtils.parseTime(input.toString());
        if (date == null) {
            throw new DateTimeException("For input string: '" + input + "'.");
        }

        return date;
    }

    /** Used by {@code CAST(x as TIMESTAMP)}. */
    public static TimestampData toTimestamp(BinaryStringData input, int precision)
            throws DateTimeException {
        return DateTimeUtils.parseTimestampData(input.toString(), precision);
    }

    public static boolean isPrimitive(LogicalType type) {
        return isPrimitive(type.getTypeRoot());
    }

    public static boolean isPrimitive(LogicalTypeRoot root) {
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
    public static boolean isInteroperable(LogicalType t1, LogicalType t2) {
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
                List<LogicalType> children1 = t1.getChildren();
                List<LogicalType> children2 = t2.getChildren();
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
