/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.hive.objectinspector;

import org.apache.flink.table.store.hive.HiveTypeUtils;
import org.apache.flink.table.store.types.ArrayType;
import org.apache.flink.table.store.types.CharType;
import org.apache.flink.table.store.types.DataType;
import org.apache.flink.table.store.types.DecimalType;
import org.apache.flink.table.store.types.MapType;
import org.apache.flink.table.store.types.VarCharType;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

/** Factory to create {@link ObjectInspector}s according to the given {@link DataType}. */
public class TableStoreObjectInspectorFactory {

    public static ObjectInspector create(DataType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case BINARY:
            case VARBINARY:
                return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                        (PrimitiveTypeInfo) HiveTypeUtils.logicalTypeToTypeInfo(logicalType));
            case DECIMAL:
                DecimalType decimalType = (DecimalType) logicalType;
                return new TableStoreDecimalObjectInspector(
                        decimalType.getPrecision(), decimalType.getScale());
            case CHAR:
                CharType charType = (CharType) logicalType;
                return new TableStoreCharObjectInspector(charType.getLength());
            case VARCHAR:
                VarCharType varCharType = (VarCharType) logicalType;
                if (varCharType.getLength() == VarCharType.MAX_LENGTH) {
                    return new TableStoreStringObjectInspector();
                } else {
                    return new TableStoreVarcharObjectInspector(varCharType.getLength());
                }
            case DATE:
                return new TableStoreDateObjectInspector();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return new TableStoreTimestampObjectInspector();
            case ARRAY:
                ArrayType arrayType = (ArrayType) logicalType;
                return new TableStoreListObjectInspector(arrayType.getElementType());
            case MAP:
                MapType mapType = (MapType) logicalType;
                return new TableStoreMapObjectInspector(
                        mapType.getKeyType(), mapType.getValueType());
            default:
                throw new UnsupportedOperationException(
                        "Unsupported logical type " + logicalType.asSQLString());
        }
    }
}
