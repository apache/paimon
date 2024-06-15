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

import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.VarCharType;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link HiveTypeUtils}. */
public class HiveTypeUtilsTest {

    @Test
    public void testLogicalTypeToTypeInfo() {
        TypeInfo boolTypeInfo = HiveTypeUtils.toTypeInfo(DataTypes.BOOLEAN());
        assertThat(boolTypeInfo.getTypeName()).isEqualTo("boolean");

        TypeInfo tinyintTypeInfo = HiveTypeUtils.toTypeInfo(DataTypes.TINYINT());
        assertThat(tinyintTypeInfo.getTypeName()).isEqualTo("tinyint");

        TypeInfo smallintTypeInfo = HiveTypeUtils.toTypeInfo(DataTypes.SMALLINT());
        assertThat(smallintTypeInfo.getTypeName()).isEqualTo("smallint");

        TypeInfo intTypeInfo = HiveTypeUtils.toTypeInfo(DataTypes.INT());
        assertThat(intTypeInfo.getTypeName()).isEqualTo("int");

        TypeInfo bigintTypeInfo = HiveTypeUtils.toTypeInfo(DataTypes.BIGINT());
        assertThat(bigintTypeInfo.getTypeName()).isEqualTo("bigint");

        TypeInfo floatTypeInfo = HiveTypeUtils.toTypeInfo(DataTypes.FLOAT());
        assertThat(floatTypeInfo.getTypeName()).isEqualTo("float");

        TypeInfo doubleTypeInfo = HiveTypeUtils.toTypeInfo(DataTypes.DOUBLE());
        assertThat(doubleTypeInfo.getTypeName()).isEqualTo("double");

        TypeInfo decimalTypeInfo = HiveTypeUtils.toTypeInfo(DataTypes.DECIMAL(38, 0));
        assertThat(decimalTypeInfo.getTypeName()).isEqualTo("decimal(38,0)");

        TypeInfo decimal1TypeInfo = HiveTypeUtils.toTypeInfo(DataTypes.DECIMAL(2, 2));
        assertThat(decimal1TypeInfo.getTypeName()).isEqualTo("decimal(2,2)");

        TypeInfo charTypeInfo = HiveTypeUtils.toTypeInfo(DataTypes.CHAR(1));
        assertThat(charTypeInfo.getTypeName()).isEqualTo("char(1)");

        TypeInfo maxLengthCharType = HiveTypeUtils.toTypeInfo(DataTypes.CHAR(CharType.MAX_LENGTH));
        assertThat(maxLengthCharType.getTypeName()).isEqualTo("string");

        TypeInfo varcharTypeInfo = HiveTypeUtils.toTypeInfo(DataTypes.VARCHAR(10));
        assertThat(varcharTypeInfo.getTypeName()).isEqualTo("varchar(10)");

        TypeInfo maxLengthVarcharType =
                HiveTypeUtils.toTypeInfo(DataTypes.VARCHAR(VarCharType.MAX_LENGTH));
        assertThat(maxLengthVarcharType.getTypeName()).isEqualTo("string");

        TypeInfo binaryTypeInfo = HiveTypeUtils.toTypeInfo(DataTypes.BINARY(10));
        assertThat(binaryTypeInfo.getTypeName()).isEqualTo("binary");

        TypeInfo varbinaryTypeInfo = HiveTypeUtils.toTypeInfo(DataTypes.VARBINARY(10));
        assertThat(varbinaryTypeInfo.getTypeName()).isEqualTo("binary");

        TypeInfo dateTypeInfo = HiveTypeUtils.toTypeInfo(DataTypes.DATE());
        assertThat(dateTypeInfo.getTypeName()).isEqualTo("date");

        TypeInfo timestampTypeInfo = HiveTypeUtils.toTypeInfo(DataTypes.TIMESTAMP());
        assertThat(timestampTypeInfo.getTypeName()).isEqualTo("timestamp");

        TypeInfo arrayTypeInfo = HiveTypeUtils.toTypeInfo(DataTypes.ARRAY(DataTypes.INT()));
        assertThat(arrayTypeInfo.getTypeName()).isEqualTo("array<int>");

        TypeInfo mapTypeInfo =
                HiveTypeUtils.toTypeInfo(DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING()));
        assertThat(mapTypeInfo.getTypeName()).isEqualTo("map<bigint,string>");

        TypeInfo rowTypeInfo =
                HiveTypeUtils.toTypeInfo(
                        DataTypes.ROW(
                                new DataField(0, "id", new IntType()),
                                new DataField(1, "name", new VarCharType(Integer.MAX_VALUE))));
        assertThat(rowTypeInfo.getTypeName()).isEqualTo("struct<id:int,name:string>");

        TypeInfo timestampWithLocalZoneTypeInfo =
                HiveTypeUtils.toTypeInfo(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
        assertThat(timestampWithLocalZoneTypeInfo.getTypeName()).isEqualTo("timestamp");
    }
}
