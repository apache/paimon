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

package org.apache.paimon.presto;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import java.util.Objects;

import static java.lang.String.format;

/** Presto type from Paimon Type. */
public class PrestoTypeUtils {

    private PrestoTypeUtils() {}

    public static Type toPrestoType(DataType paimonType, TypeManager typeManager) {
        if (paimonType instanceof CharType) {
            return com.facebook.presto.common.type.CharType.createCharType(
                    Math.min(
                            com.facebook.presto.common.type.CharType.MAX_LENGTH,
                            ((CharType) paimonType).getLength()));
        } else if (paimonType instanceof VarCharType) {
            return VarcharType.createUnboundedVarcharType();
        } else if (paimonType instanceof BooleanType) {
            return com.facebook.presto.common.type.BooleanType.BOOLEAN;
        } else if (paimonType instanceof BinaryType) {
            return VarbinaryType.VARBINARY;
        } else if (paimonType instanceof VarBinaryType) {
            return VarbinaryType.VARBINARY;
        } else if (paimonType instanceof DecimalType) {
            return com.facebook.presto.common.type.DecimalType.createDecimalType(
                    ((DecimalType) paimonType).getPrecision(),
                    ((DecimalType) paimonType).getScale());
        } else if (paimonType instanceof TinyIntType) {
            return TinyintType.TINYINT;
        } else if (paimonType instanceof SmallIntType) {
            return SmallintType.SMALLINT;
        } else if (paimonType instanceof IntType) {
            return IntegerType.INTEGER;
        } else if (paimonType instanceof BigIntType) {
            return BigintType.BIGINT;
        } else if (paimonType instanceof FloatType) {
            return RealType.REAL;
        } else if (paimonType instanceof DoubleType) {
            return com.facebook.presto.common.type.DoubleType.DOUBLE;
        } else if (paimonType instanceof DateType) {
            return com.facebook.presto.common.type.DateType.DATE;
        } else if (paimonType instanceof TimeType) {
            return com.facebook.presto.common.type.TimeType.TIME;
        } else if (paimonType instanceof TimestampType) {
            return com.facebook.presto.common.type.TimestampType.TIMESTAMP;
        } else if (paimonType instanceof LocalZonedTimestampType) {
            return TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
        } else if (paimonType instanceof ArrayType) {
            DataType elementType = ((ArrayType) paimonType).getElementType();
            return new com.facebook.presto.common.type.ArrayType(
                    Objects.requireNonNull(toPrestoType(elementType, typeManager)));
        } else if (paimonType instanceof MapType) {
            MapType paimonMapType = (MapType) paimonType;
            TypeSignature keyType =
                    Objects.requireNonNull(toPrestoType(paimonMapType.getKeyType(), typeManager))
                            .getTypeSignature();
            TypeSignature valueType =
                    Objects.requireNonNull(toPrestoType(paimonMapType.getValueType(), typeManager))
                            .getTypeSignature();
            return typeManager.getParameterizedType(
                    StandardTypes.MAP,
                    ImmutableList.of(
                            TypeSignatureParameter.of(keyType),
                            TypeSignatureParameter.of(valueType)));
        } else {
            throw new UnsupportedOperationException(
                    format("Cannot convert from Paimon type '%s' to Presto type", paimonType));
        }
    }

    public static DataType toPaimonType(Type prestoType) {
        if (prestoType instanceof com.facebook.presto.common.type.CharType) {
            return DataTypes.CHAR(
                    ((com.facebook.presto.common.type.CharType) prestoType).getLength());
        } else if (prestoType instanceof VarcharType) {
            return DataTypes.VARCHAR(
                    Math.min(Integer.MAX_VALUE, ((VarcharType) prestoType).getLength()));
        } else if (prestoType instanceof com.facebook.presto.common.type.BooleanType) {
            return DataTypes.BOOLEAN();
        } else if (prestoType instanceof com.facebook.presto.common.type.DecimalType) {
            return DataTypes.DECIMAL(
                    ((com.facebook.presto.common.type.DecimalType) prestoType).getPrecision(),
                    ((com.facebook.presto.common.type.DecimalType) prestoType).getScale());
        } else if (prestoType instanceof TinyintType) {
            return DataTypes.TINYINT();
        } else if (prestoType instanceof SmallintType) {
            return DataTypes.SMALLINT();
        } else if (prestoType instanceof IntegerType) {
            return DataTypes.INT();
        } else if (prestoType instanceof BigintType) {
            return DataTypes.BIGINT();
        } else if (prestoType instanceof RealType) {
            return DataTypes.FLOAT();
        } else if (prestoType instanceof com.facebook.presto.common.type.DoubleType) {
            return DataTypes.DOUBLE();
        } else if (prestoType instanceof com.facebook.presto.common.type.DateType) {
            return DataTypes.DATE();
        } else if (prestoType instanceof com.facebook.presto.common.type.TimeType) {
            return new TimeType();
        } else if (prestoType instanceof com.facebook.presto.common.type.TimestampType) {
            return DataTypes.TIMESTAMP();
        } else if (prestoType instanceof TimestampWithTimeZoneType) {
            return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE();
        } else if (prestoType instanceof com.facebook.presto.common.type.ArrayType) {
            return DataTypes.ARRAY(toPaimonType(prestoType));
        } else if (prestoType instanceof com.facebook.presto.common.type.MapType) {
            return DataTypes.MAP(toPaimonType(prestoType), toPaimonType(prestoType));
        } else {
            throw new UnsupportedOperationException(
                    format("Cannot convert from Presto type '%s' to Paimon type", prestoType));
        }
    }
}
