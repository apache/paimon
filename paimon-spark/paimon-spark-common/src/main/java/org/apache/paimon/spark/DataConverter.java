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

package org.apache.paimon.spark;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.spark.data.SparkArrayData;
import org.apache.paimon.spark.data.SparkInternalRow;
import org.apache.paimon.spark.util.shim.TypeUtils;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;

import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

/** DataConverter. */
public class DataConverter {

    public static Object fromPaimon(Object o, DataType type) {
        if (o == null) {
            return null;
        }
        switch (type.getTypeRoot()) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return fromPaimon((Timestamp) o);
            case CHAR:
            case VARCHAR:
                return fromPaimon((BinaryString) o);
            case DECIMAL:
                return fromPaimon((org.apache.paimon.data.Decimal) o);
            case ARRAY:
                return fromPaimon((InternalArray) o, (ArrayType) type);
            case MAP:
            case MULTISET:
                return fromPaimon((InternalMap) o, type);
            case ROW:
                return fromPaimon((InternalRow) o, (RowType) type);
            default:
                return o;
        }
    }

    public static UTF8String fromPaimon(BinaryString string) {
        return UTF8String.fromBytes(string.toBytes());
    }

    public static Decimal fromPaimon(org.apache.paimon.data.Decimal decimal) {
        return Decimal.apply(decimal.toBigDecimal());
    }

    public static org.apache.spark.sql.catalyst.InternalRow fromPaimon(
            InternalRow row, RowType rowType) {
        return SparkInternalRow.create(rowType).replace(row);
    }

    public static long fromPaimon(Timestamp timestamp) {
        if (TypeUtils.treatPaimonTimestampTypeAsSparkTimestampType()) {
            return DateTimeUtils.fromJavaTimestamp(timestamp.toSQLTimestamp());
        } else {
            return timestamp.toMicros();
        }
    }

    public static ArrayData fromPaimon(InternalArray array, ArrayType arrayType) {
        return fromPaimonArrayElementType(array, arrayType.getElementType());
    }

    private static ArrayData fromPaimonArrayElementType(InternalArray array, DataType elementType) {
        return SparkArrayData.create(elementType).replace(array);
    }

    public static MapData fromPaimon(InternalMap map, DataType mapType) {
        DataType keyType;
        DataType valueType;
        if (mapType instanceof MapType) {
            keyType = ((MapType) mapType).getKeyType();
            valueType = ((MapType) mapType).getValueType();
        } else if (mapType instanceof MultisetType) {
            keyType = ((MultisetType) mapType).getElementType();
            valueType = new IntType();
        } else {
            throw new UnsupportedOperationException("Unsupported type: " + mapType);
        }

        return new ArrayBasedMapData(
                fromPaimonArrayElementType(map.keyArray(), keyType),
                fromPaimonArrayElementType(map.valueArray(), valueType));
    }
}
