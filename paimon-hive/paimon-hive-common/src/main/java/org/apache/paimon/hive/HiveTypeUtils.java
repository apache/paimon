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

package org.apache.paimon.hive;

import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeDefaultVisitor;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.types.VariantType;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
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
import java.util.Arrays;
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
    public static TypeInfo toTypeInfo(DataType logicalType) {
        return logicalType.accept(PaimonToHiveTypeVisitor.INSTANCE);
    }

    /**
     * Convert hive data type {@link TypeInfo} to paimon data type {@link DataType}.
     *
     * @param type hive type string
     * @return paimon data type
     */
    public static DataType toPaimonType(String type) {
        TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(type);
        return toPaimonType(typeInfo);
    }

    /**
     * Convert hive data type {@link TypeInfo} to paimon data type {@link DataType}.
     *
     * @param typeInfo hive type info
     * @return paimon data type
     */
    public static DataType toPaimonType(TypeInfo typeInfo) {
        return HiveToPaimonTypeVisitor.visit(typeInfo);
    }

    private static class PaimonToHiveTypeVisitor extends DataTypeDefaultVisitor<TypeInfo> {

        private static final PaimonToHiveTypeVisitor INSTANCE = new PaimonToHiveTypeVisitor();

        @Override
        public TypeInfo visit(BooleanType booleanType) {
            return TypeInfoFactory.booleanTypeInfo;
        }

        @Override
        public TypeInfo visit(TinyIntType tinyIntType) {
            return TypeInfoFactory.byteTypeInfo;
        }

        @Override
        public TypeInfo visit(SmallIntType smallIntType) {
            return TypeInfoFactory.shortTypeInfo;
        }

        @Override
        public TypeInfo visit(IntType intType) {
            return TypeInfoFactory.intTypeInfo;
        }

        @Override
        public TypeInfo visit(BigIntType bigIntType) {
            return TypeInfoFactory.longTypeInfo;
        }

        @Override
        public TypeInfo visit(FloatType floatType) {
            return TypeInfoFactory.floatTypeInfo;
        }

        @Override
        public TypeInfo visit(DoubleType doubleType) {
            return TypeInfoFactory.doubleTypeInfo;
        }

        @Override
        public TypeInfo visit(DecimalType decimalType) {
            return TypeInfoFactory.getDecimalTypeInfo(
                    decimalType.getPrecision(), decimalType.getScale());
        }

        @Override
        public TypeInfo visit(CharType charType) {
            if (charType.getLength() > HiveChar.MAX_CHAR_LENGTH) {
                return TypeInfoFactory.stringTypeInfo;
            } else {
                return TypeInfoFactory.getCharTypeInfo(charType.getLength());
            }
        }

        @Override
        public TypeInfo visit(VarCharType varCharType) {
            if (varCharType.getLength() > HiveVarchar.MAX_VARCHAR_LENGTH) {
                return TypeInfoFactory.stringTypeInfo;
            } else {
                return TypeInfoFactory.getVarcharTypeInfo(varCharType.getLength());
            }
        }

        @Override
        public TypeInfo visit(BinaryType binaryType) {
            return TypeInfoFactory.binaryTypeInfo;
        }

        @Override
        public TypeInfo visit(VarBinaryType varBinaryType) {
            return TypeInfoFactory.binaryTypeInfo;
        }

        @Override
        public TypeInfo visit(DateType dateType) {
            return TypeInfoFactory.dateTypeInfo;
        }

        @Override
        public TypeInfo visit(TimeType timeType) {
            return TypeInfoFactory.stringTypeInfo;
        }

        @Override
        public TypeInfo visit(TimestampType timestampType) {
            return TypeInfoFactory.timestampTypeInfo;
        }

        @Override
        public TypeInfo visit(LocalZonedTimestampType localZonedTimestampType) {
            return LocalZonedTimestampTypeUtils.hiveLocalZonedTimestampType();
        }

        @Override
        public TypeInfo visit(ArrayType arrayType) {
            DataType elementType = arrayType.getElementType();
            return TypeInfoFactory.getListTypeInfo(elementType.accept(this));
        }

        @Override
        public TypeInfo visit(MultisetType multisetType) {
            return TypeInfoFactory.getMapTypeInfo(
                    multisetType.getElementType().accept(this), TypeInfoFactory.intTypeInfo);
        }

        @Override
        public TypeInfo visit(MapType mapType) {
            return TypeInfoFactory.getMapTypeInfo(
                    mapType.getKeyType().accept(this), mapType.getValueType().accept(this));
        }

        @Override
        public TypeInfo visit(RowType rowType) {
            List<String> fieldNames =
                    rowType.getFields().stream().map(DataField::name).collect(Collectors.toList());
            List<TypeInfo> typeInfos =
                    rowType.getFields().stream()
                            .map(DataField::type)
                            .map(type -> type.accept(this))
                            .collect(Collectors.toList());
            return TypeInfoFactory.getStructTypeInfo(fieldNames, typeInfos);
        }

        @Override
        public TypeInfo visit(VariantType variantType) {
            List<String> fieldNames = Arrays.asList(Variant.VALUE, Variant.METADATA);
            List<TypeInfo> typeInfos =
                    Arrays.asList(TypeInfoFactory.binaryTypeInfo, TypeInfoFactory.binaryTypeInfo);
            return TypeInfoFactory.getStructTypeInfo(fieldNames, typeInfos);
        }

        @Override
        protected TypeInfo defaultMethod(org.apache.paimon.types.DataType dataType) {
            throw new UnsupportedOperationException("Unsupported type: " + dataType);
        }
    }

    private static class HiveToPaimonTypeVisitor {

        static DataType visit(TypeInfo type) {
            return visit(type, new HiveToPaimonTypeVisitor());
        }

        static DataType visit(TypeInfo type, HiveToPaimonTypeVisitor visitor) {
            if (type instanceof StructTypeInfo) {
                StructTypeInfo structTypeInfo = (StructTypeInfo) type;
                ArrayList<String> fieldNames = structTypeInfo.getAllStructFieldNames();
                ArrayList<TypeInfo> typeInfos = structTypeInfo.getAllStructFieldTypeInfos();
                RowType.Builder builder = RowType.builder();
                for (int i = 0; i < fieldNames.size(); i++) {
                    builder.field(fieldNames.get(i), visit(typeInfos.get(i), visitor));
                }
                return builder.build();
            } else if (type instanceof MapTypeInfo) {
                MapTypeInfo mapTypeInfo = (MapTypeInfo) type;
                return DataTypes.MAP(
                        visit(mapTypeInfo.getMapKeyTypeInfo(), visitor),
                        visit(mapTypeInfo.getMapValueTypeInfo(), visitor));
            } else if (type instanceof ListTypeInfo) {
                ListTypeInfo listTypeInfo = (ListTypeInfo) type;
                return DataTypes.ARRAY(visit(listTypeInfo.getListElementTypeInfo(), visitor));
            } else {
                return visitor.atomic(type);
            }
        }

        public DataType atomic(TypeInfo atomic) {
            if (LocalZonedTimestampTypeUtils.isHiveLocalZonedTimestampType(atomic)) {
                return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE();
            }

            if (TypeInfoFactory.booleanTypeInfo.equals(atomic)) {
                return DataTypes.BOOLEAN();
            } else if (TypeInfoFactory.byteTypeInfo.equals(atomic)) {
                return DataTypes.TINYINT();
            } else if (TypeInfoFactory.shortTypeInfo.equals(atomic)) {
                return DataTypes.SMALLINT();
            } else if (TypeInfoFactory.intTypeInfo.equals(atomic)) {
                return DataTypes.INT();
            } else if (TypeInfoFactory.longTypeInfo.equals(atomic)) {
                return DataTypes.BIGINT();
            } else if (TypeInfoFactory.floatTypeInfo.equals(atomic)) {
                return DataTypes.FLOAT();
            } else if (TypeInfoFactory.doubleTypeInfo.equals(atomic)) {
                return DataTypes.DOUBLE();
            } else if (atomic instanceof DecimalTypeInfo) {
                DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) atomic;
                return DataTypes.DECIMAL(
                        decimalTypeInfo.getPrecision(), decimalTypeInfo.getScale());
            } else if (atomic instanceof CharTypeInfo) {
                return DataTypes.CHAR(((CharTypeInfo) atomic).getLength());
            } else if (atomic instanceof VarcharTypeInfo) {
                return DataTypes.VARCHAR(((VarcharTypeInfo) atomic).getLength());
            } else if (TypeInfoFactory.stringTypeInfo.equals(atomic)) {
                return DataTypes.VARCHAR(VarCharType.MAX_LENGTH);
            } else if (TypeInfoFactory.binaryTypeInfo.equals(atomic)) {
                return DataTypes.VARBINARY(MAX_LENGTH);
            } else if (TypeInfoFactory.dateTypeInfo.equals(atomic)) {
                return DataTypes.DATE();
            } else if (TypeInfoFactory.timestampTypeInfo.equals(atomic)) {
                return DataTypes.TIMESTAMP_MILLIS();
            }

            throw new UnsupportedOperationException(
                    "Not a supported type: " + atomic.getTypeName());
        }
    }
}
