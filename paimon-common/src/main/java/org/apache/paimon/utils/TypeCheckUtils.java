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

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;

import static org.apache.paimon.types.DataTypeRoot.ARRAY;
import static org.apache.paimon.types.DataTypeRoot.BIGINT;
import static org.apache.paimon.types.DataTypeRoot.BOOLEAN;
import static org.apache.paimon.types.DataTypeRoot.DECIMAL;
import static org.apache.paimon.types.DataTypeRoot.INTEGER;
import static org.apache.paimon.types.DataTypeRoot.MAP;
import static org.apache.paimon.types.DataTypeRoot.MULTISET;
import static org.apache.paimon.types.DataTypeRoot.ROW;
import static org.apache.paimon.types.DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
import static org.apache.paimon.types.DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.apache.paimon.types.DataTypeRoot.VARIANT;

/** Utils for type. */
public class TypeCheckUtils {

    public static boolean isNumeric(DataType type) {
        return type.getTypeRoot().getFamilies().contains(DataTypeFamily.NUMERIC);
    }

    public static boolean isTemporal(DataType type) {
        return isTimePoint(type);
    }

    public static boolean isTimePoint(DataType type) {
        return type.getTypeRoot().getFamilies().contains(DataTypeFamily.DATETIME);
    }

    public static boolean isCharacterString(DataType type) {
        return type.getTypeRoot().getFamilies().contains(DataTypeFamily.CHARACTER_STRING);
    }

    public static boolean isBinaryString(DataType type) {
        return type.getTypeRoot().getFamilies().contains(DataTypeFamily.BINARY_STRING);
    }

    public static boolean isTimestamp(DataType type) {
        return type.getTypeRoot() == TIMESTAMP_WITHOUT_TIME_ZONE;
    }

    public static boolean isTimestampWithLocalZone(DataType type) {
        return type.getTypeRoot() == TIMESTAMP_WITH_LOCAL_TIME_ZONE;
    }

    public static boolean isBoolean(DataType type) {
        return type.getTypeRoot() == BOOLEAN;
    }

    public static boolean isDecimal(DataType type) {
        return type.getTypeRoot() == DECIMAL;
    }

    public static boolean isInteger(DataType type) {
        return type.getTypeRoot() == INTEGER;
    }

    public static boolean isLong(DataType type) {
        return type.getTypeRoot() == BIGINT;
    }

    public static boolean isArray(DataType type) {
        return type.getTypeRoot() == ARRAY;
    }

    public static boolean isMap(DataType type) {
        return type.getTypeRoot() == MAP;
    }

    public static boolean isMultiset(DataType type) {
        return type.getTypeRoot() == MULTISET;
    }

    public static boolean isRow(DataType type) {
        return type.getTypeRoot() == ROW;
    }

    public static boolean isVariant(DataType type) {
        return type.getTypeRoot() == VARIANT;
    }

    public static boolean isComparable(DataType type) {
        return !isMap(type)
                && !isMultiset(type)
                && !isRow(type)
                && !isArray(type)
                && !isVariant(type);
    }

    public static boolean isMutable(DataType type) {
        // ordered by type root definition
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR: // the internal representation of String is BinaryString which is mutable
            case ARRAY:
            case MULTISET:
            case MAP:
            case ROW:
                return true;
            default:
                return false;
        }
    }

    public static boolean isReference(DataType type) {
        // ordered by type root definition
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return false;
            default:
                return true;
        }
    }
}
