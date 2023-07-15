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

package org.apache.paimon.hive;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.types.VarBinaryType.MAX_LENGTH;

/** Utils for converting types related classes between Paimon and Hive. */
public class HiveTypeUtils {

    /**
     * Convert paimon data type {@link DataType} to hive data type {@link TypeInfo}.
     *
     * @param logicalType paimon data type.
     * @return hive type info.
     */
    public static TypeInfo logicalTypeToTypeInfo(DataType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                return TypeInfoFactory.booleanTypeInfo;
            case TINYINT:
                return TypeInfoFactory.byteTypeInfo;
            case SMALLINT:
                return TypeInfoFactory.shortTypeInfo;
            case INTEGER:
                return TypeInfoFactory.intTypeInfo;
            case BIGINT:
                return TypeInfoFactory.longTypeInfo;
            case FLOAT:
                return TypeInfoFactory.floatTypeInfo;
            case DOUBLE:
                return TypeInfoFactory.doubleTypeInfo;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) logicalType;
                return TypeInfoFactory.getDecimalTypeInfo(
                        decimalType.getPrecision(), decimalType.getScale());
            case CHAR:
                CharType charType = (CharType) logicalType;
                if (charType.getLength() > HiveChar.MAX_CHAR_LENGTH) {
                    return TypeInfoFactory.stringTypeInfo;
                } else {
                    return TypeInfoFactory.getCharTypeInfo(charType.getLength());
                }
            case VARCHAR:
                VarCharType varCharType = (VarCharType) logicalType;
                if (varCharType.getLength() > HiveVarchar.MAX_VARCHAR_LENGTH) {
                    return TypeInfoFactory.stringTypeInfo;
                } else {
                    return TypeInfoFactory.getVarcharTypeInfo(varCharType.getLength());
                }
            case BINARY:
            case VARBINARY:
                return TypeInfoFactory.binaryTypeInfo;
            case DATE:
                return TypeInfoFactory.dateTypeInfo;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TypeInfoFactory.timestampTypeInfo;
            case ARRAY:
                ArrayType arrayType = (ArrayType) logicalType;
                return TypeInfoFactory.getListTypeInfo(
                        logicalTypeToTypeInfo(arrayType.getElementType()));
            case MAP:
                MapType mapType = (MapType) logicalType;
                return TypeInfoFactory.getMapTypeInfo(
                        logicalTypeToTypeInfo(mapType.getKeyType()),
                        logicalTypeToTypeInfo(mapType.getValueType()));

            case ROW:
                RowType rowType = (RowType) logicalType;
                List<String> fieldNames =
                        rowType.getFields().stream()
                                .map(DataField::name)
                                .collect(Collectors.toList());
                List<TypeInfo> typeInfos =
                        rowType.getFields().stream()
                                .map(DataField::type)
                                .map(HiveTypeUtils::logicalTypeToTypeInfo)
                                .collect(Collectors.toList());
                return TypeInfoFactory.getStructTypeInfo(fieldNames, typeInfos);

            default:
                throw new UnsupportedOperationException(
                        "Unsupported logical type " + logicalType.asSQLString());
        }
    }

    /**
     * Convert hive data type {@link TypeInfo} to paimon data type {@link DataType}.
     *
     * @param type hive type string
     * @return paimon data type
     */
    public static DataType typeInfoToLogicalType(String type) {
        TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(type);
        return typeInfoToLogicalType(typeInfo);
    }

    /**
     * Convert hive data type {@link TypeInfo} to paimon data type {@link DataType}.
     *
     * @param typeInfo hive type info
     * @return paimon data type
     */
    public static DataType typeInfoToLogicalType(TypeInfo typeInfo) {
        if (TypeInfoFactory.booleanTypeInfo.equals(typeInfo)) {
            return DataTypes.BOOLEAN();
        } else if (TypeInfoFactory.byteTypeInfo.equals(typeInfo)) {
            return DataTypes.TINYINT();
        } else if (TypeInfoFactory.shortTypeInfo.equals(typeInfo)) {
            return DataTypes.SMALLINT();
        } else if (TypeInfoFactory.intTypeInfo.equals(typeInfo)) {
            return DataTypes.INT();
        } else if (TypeInfoFactory.longTypeInfo.equals(typeInfo)) {
            return DataTypes.BIGINT();
        } else if (TypeInfoFactory.floatTypeInfo.equals(typeInfo)) {
            return DataTypes.FLOAT();
        } else if (TypeInfoFactory.doubleTypeInfo.equals(typeInfo)) {
            return DataTypes.DOUBLE();
        } else if (typeInfo instanceof DecimalTypeInfo) {
            DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
            return DataTypes.DECIMAL(decimalTypeInfo.getPrecision(), decimalTypeInfo.getScale());
        } else if (typeInfo instanceof CharTypeInfo) {
            return DataTypes.CHAR(((CharTypeInfo) typeInfo).getLength());
        } else if (typeInfo instanceof VarcharTypeInfo) {
            return DataTypes.VARCHAR(((VarcharTypeInfo) typeInfo).getLength());
        } else if (TypeInfoFactory.stringTypeInfo.equals(typeInfo)) {
            return DataTypes.VARCHAR(VarCharType.MAX_LENGTH);
        } else if (TypeInfoFactory.binaryTypeInfo.equals(typeInfo)) {
            return DataTypes.VARBINARY(MAX_LENGTH);
        } else if (TypeInfoFactory.dateTypeInfo.equals(typeInfo)) {
            return DataTypes.DATE();
        } else if (TypeInfoFactory.timestampTypeInfo.equals(typeInfo)) {
            return DataTypes.TIMESTAMP_MILLIS();
        } else if (typeInfo instanceof ListTypeInfo) {
            ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
            return DataTypes.ARRAY(typeInfoToLogicalType(listTypeInfo.getListElementTypeInfo()));
        } else if (typeInfo instanceof MapTypeInfo) {
            MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
            return DataTypes.MAP(
                    typeInfoToLogicalType(mapTypeInfo.getMapKeyTypeInfo()),
                    typeInfoToLogicalType(mapTypeInfo.getMapValueTypeInfo()));
        } else if (typeInfo instanceof StructTypeInfo) {
            StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
            ArrayList<String> fieldNames = structTypeInfo.getAllStructFieldNames();
            ArrayList<TypeInfo> typeInfos = structTypeInfo.getAllStructFieldTypeInfos();

            int highestFieldId = -1;
            DataField[] dataFields = new DataField[fieldNames.size()];
            for (int i = 0; i < fieldNames.size(); i++) {
                dataFields[i] =
                        new DataField(
                                ++highestFieldId,
                                fieldNames.get(i),
                                typeInfoToLogicalType(typeInfos.get(i)),
                                "");
            }

            return DataTypes.ROW(dataFields);
        }

        throw new UnsupportedOperationException("Unsupported hive type " + typeInfo.getTypeName());
    }
}
