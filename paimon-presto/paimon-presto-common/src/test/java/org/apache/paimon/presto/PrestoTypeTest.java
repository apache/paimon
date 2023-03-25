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

package org.apache.paimon.presto;

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.TimeType;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.Objects;

import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PrestoTypeUtils}. */
public class PrestoTypeTest {

    @Test
    public void testToPrestoType() {
        Type charType =
                PrestoTypeUtils.toPrestoType(DataTypes.CHAR(1), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(charType).getDisplayName()).isEqualTo("char(1)");

        Type varCharType =
                PrestoTypeUtils.toPrestoType(
                        DataTypes.VARCHAR(10), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(varCharType).getDisplayName()).isEqualTo("varchar");

        Type booleanType =
                PrestoTypeUtils.toPrestoType(
                        DataTypes.BOOLEAN(), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(booleanType).getDisplayName()).isEqualTo("boolean");

        Type binaryType =
                PrestoTypeUtils.toPrestoType(
                        DataTypes.BINARY(10), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(binaryType).getDisplayName()).isEqualTo("varbinary");

        Type varBinaryType =
                PrestoTypeUtils.toPrestoType(
                        DataTypes.VARBINARY(10), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(varBinaryType).getDisplayName()).isEqualTo("varbinary");

        assertThat(
                        PrestoTypeUtils.toPrestoType(
                                        DataTypes.DECIMAL(38, 0),
                                        createTestFunctionAndTypeManager())
                                .getDisplayName())
                .isEqualTo("decimal(38,0)");

        org.apache.paimon.types.DecimalType decimal = DataTypes.DECIMAL(2, 2);
        assertThat(
                        PrestoTypeUtils.toPrestoType(decimal, createTestFunctionAndTypeManager())
                                .getDisplayName())
                .isEqualTo("decimal(2,2)");

        Type tinyIntType =
                PrestoTypeUtils.toPrestoType(
                        DataTypes.TINYINT(), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(tinyIntType).getDisplayName()).isEqualTo("tinyint");

        Type smallIntType =
                PrestoTypeUtils.toPrestoType(
                        DataTypes.SMALLINT(), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(smallIntType).getDisplayName()).isEqualTo("smallint");

        Type intType =
                PrestoTypeUtils.toPrestoType(DataTypes.INT(), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(intType).getDisplayName()).isEqualTo("integer");

        Type bigIntType =
                PrestoTypeUtils.toPrestoType(
                        DataTypes.BIGINT(), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(bigIntType).getDisplayName()).isEqualTo("bigint");

        Type doubleType =
                PrestoTypeUtils.toPrestoType(
                        DataTypes.DOUBLE(), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(doubleType).getDisplayName()).isEqualTo("double");

        Type dateType =
                PrestoTypeUtils.toPrestoType(DataTypes.DATE(), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(dateType).getDisplayName()).isEqualTo("date");

        Type timeType =
                PrestoTypeUtils.toPrestoType(new TimeType(), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(timeType).getDisplayName()).isEqualTo("time");

        Type timestampType =
                PrestoTypeUtils.toPrestoType(
                        DataTypes.TIMESTAMP(), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(timestampType).getDisplayName()).isEqualTo("timestamp");

        Type localZonedTimestampType =
                PrestoTypeUtils.toPrestoType(
                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(),
                        createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(localZonedTimestampType).getDisplayName())
                .isEqualTo("timestamp with time zone");

        Type mapType =
                PrestoTypeUtils.toPrestoType(
                        DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING()),
                        createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(mapType).getDisplayName())
                .isEqualTo("map(bigint, varchar)");
    }

    @Test
    public void testToPaimonType() {
        DataType charType = PrestoTypeUtils.toPaimonType(CharType.createCharType(1));
        assertThat(charType.asSQLString()).isEqualTo("CHAR(1)");

        DataType varCharType =
                PrestoTypeUtils.toPaimonType(VarcharType.createUnboundedVarcharType());
        assertThat(varCharType.asSQLString()).isEqualTo("STRING");

        DataType booleanType = PrestoTypeUtils.toPaimonType(BooleanType.BOOLEAN);
        assertThat(booleanType.asSQLString()).isEqualTo("BOOLEAN");

        DataType decimalType = PrestoTypeUtils.toPaimonType(DecimalType.createDecimalType());
        assertThat(decimalType.asSQLString()).isEqualTo("DECIMAL(38, 0)");

        DataType tinyintType = PrestoTypeUtils.toPaimonType(TinyintType.TINYINT);
        assertThat(tinyintType.asSQLString()).isEqualTo("TINYINT");

        DataType smallintType = PrestoTypeUtils.toPaimonType(SmallintType.SMALLINT);
        assertThat(smallintType.asSQLString()).isEqualTo("SMALLINT");

        DataType intType = PrestoTypeUtils.toPaimonType(IntegerType.INTEGER);
        assertThat(intType.asSQLString()).isEqualTo("INT");

        DataType bigintType = PrestoTypeUtils.toPaimonType(BigintType.BIGINT);
        assertThat(bigintType.asSQLString()).isEqualTo("BIGINT");

        DataType floatType = PrestoTypeUtils.toPaimonType(RealType.REAL);
        assertThat(floatType.asSQLString()).isEqualTo("FLOAT");

        DataType doubleType = PrestoTypeUtils.toPaimonType(DoubleType.DOUBLE);
        assertThat(doubleType.asSQLString()).isEqualTo("DOUBLE");

        DataType dateType = PrestoTypeUtils.toPaimonType(DateType.DATE);
        assertThat(dateType.asSQLString()).isEqualTo("DATE");

        DataType timeType =
                PrestoTypeUtils.toPaimonType(com.facebook.presto.common.type.TimeType.TIME);
        assertThat(timeType.asSQLString()).isEqualTo("TIME(0)");

        DataType timestampType = PrestoTypeUtils.toPaimonType(TimestampType.TIMESTAMP);
        assertThat(timestampType.asSQLString()).isEqualTo("TIMESTAMP(6)");

        DataType timestampWithTimeZoneType =
                PrestoTypeUtils.toPaimonType(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE);
        assertThat(timestampWithTimeZoneType.asSQLString())
                .isEqualTo("TIMESTAMP(6) WITH LOCAL TIME ZONE");

        DataType arrayType = PrestoTypeUtils.toPaimonType(new ArrayType(IntegerType.INTEGER));
        assertThat(arrayType.asSQLString()).isEqualTo("ARRAY<INT>");

        FunctionAndTypeManager testFunctionAndTypeManager = createTestFunctionAndTypeManager();
        Type parameterizedType =
                testFunctionAndTypeManager.getParameterizedType(
                        StandardTypes.MAP,
                        ImmutableList.of(
                                TypeSignatureParameter.of(BigintType.BIGINT.getTypeSignature()),
                                TypeSignatureParameter.of(
                                        VarcharType.createUnboundedVarcharType()
                                                .getTypeSignature())));
        DataType mapType = PrestoTypeUtils.toPaimonType(parameterizedType);
        assertThat(mapType.asSQLString()).isEqualTo("MAP<BIGINT, STRING>");
    }
}
