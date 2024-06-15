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

package org.apache.paimon.hive.objectinspector;

import org.apache.paimon.hive.HiveTypeUtils;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.VarCharType;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Factory to create {@link ObjectInspector}s according to the given {@link DataType}. */
public class PaimonObjectInspectorFactory {

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
                        (PrimitiveTypeInfo) HiveTypeUtils.toTypeInfo(logicalType));
            case DECIMAL:
                DecimalType decimalType = (DecimalType) logicalType;
                return new PaimonDecimalObjectInspector(
                        decimalType.getPrecision(), decimalType.getScale());
            case CHAR:
                CharType charType = (CharType) logicalType;
                return new PaimonCharObjectInspector(charType.getLength());
            case VARCHAR:
                VarCharType varCharType = (VarCharType) logicalType;
                if (varCharType.getLength() == VarCharType.MAX_LENGTH) {
                    return new PaimonStringObjectInspector();
                } else {
                    return new PaimonVarcharObjectInspector(varCharType.getLength());
                }
            case DATE:
                return new PaimonDateObjectInspector();
            case TIME_WITHOUT_TIME_ZONE:
                TimeType timeType = (TimeType) logicalType;
                checkArgument(timeType.getPrecision() <= 3, "TIME type precision must be <= 3.");
                return new PaimonTimeObjectInspector();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return new PaimonTimestampObjectInspector();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new PaimonTimestampLocalTZObjectInspector();
            case ARRAY:
                ArrayType arrayType = (ArrayType) logicalType;
                return new PaimonListObjectInspector(arrayType.getElementType());
            case MAP:
                MapType mapType = (MapType) logicalType;
                return new PaimonMapObjectInspector(mapType.getKeyType(), mapType.getValueType());
            case ROW:
                List<String> fieldComments =
                        ((RowType) logicalType)
                                .getFields().stream()
                                        .map(DataField::description)
                                        .collect(Collectors.toList());
                List<DataType> fieldTypes = ((RowType) logicalType).getFieldTypes();
                List<String> fieldNames = ((RowType) logicalType).getFieldNames();
                return new PaimonInternalRowObjectInspector(fieldNames, fieldTypes, fieldComments);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported logical type " + logicalType.asSQLString());
        }
    }
}
