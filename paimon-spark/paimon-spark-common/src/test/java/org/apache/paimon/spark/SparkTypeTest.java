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

import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.spark.SparkTypeUtils.fromPaimonRowType;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SparkTypeUtils}. */
public class SparkTypeTest {

    public static final RowType ALL_TYPES =
            RowType.builder(
                            true,
                            new AtomicInteger(
                                    1)) // posX and posY have field id 0 and 1, here we start from 2
                    .field("id", DataTypes.INT().notNull())
                    .field("name", DataTypes.STRING()) /* optional by default */
                    .field("char", DataTypes.CHAR(10))
                    .field("varchar", DataTypes.VARCHAR(10))
                    .field("salary", DataTypes.DOUBLE().notNull())
                    .field(
                            "locations",
                            DataTypes.MAP(
                                    DataTypes.STRING().notNull(),
                                    DataTypes.ROW(
                                            DataTypes.FIELD(
                                                    0,
                                                    "posX",
                                                    DataTypes.DOUBLE().notNull(),
                                                    "X field"),
                                            DataTypes.FIELD(
                                                    1,
                                                    "posY",
                                                    DataTypes.DOUBLE().notNull(),
                                                    "Y field"))))
                    .field("strArray", DataTypes.ARRAY(DataTypes.STRING()).nullable())
                    .field("intArray", DataTypes.ARRAY(DataTypes.INT()).nullable())
                    .field("boolean", DataTypes.BOOLEAN().nullable())
                    .field("tinyint", DataTypes.TINYINT())
                    .field("smallint", DataTypes.SMALLINT())
                    .field("bigint", DataTypes.BIGINT())
                    .field("bytes", DataTypes.BYTES())
                    .field("timestamp", DataTypes.TIMESTAMP())
                    .field("date", DataTypes.DATE())
                    .field("decimal", DataTypes.DECIMAL(2, 2))
                    .field("decimal2", DataTypes.DECIMAL(38, 2))
                    .field("decimal3", DataTypes.DECIMAL(10, 1))
                    .build();

    @Test
    public void testAllTypes() {
        String nestedRowMapType =
                "StructField(locations,MapType("
                        + "StringType,"
                        + "StructType(StructField(posX,DoubleType,true),StructField(posY,DoubleType,true)),true),true)";
        String expected =
                "StructType("
                        + "StructField(id,IntegerType,true),"
                        + "StructField(name,StringType,true),"
                        + "StructField(char,CharType(10),true),"
                        + "StructField(varchar,VarcharType(10),true),"
                        + "StructField(salary,DoubleType,true),"
                        + nestedRowMapType
                        + ","
                        + "StructField(strArray,ArrayType(StringType,true),true),"
                        + "StructField(intArray,ArrayType(IntegerType,true),true),"
                        + "StructField(boolean,BooleanType,true),"
                        + "StructField(tinyint,ByteType,true),"
                        + "StructField(smallint,ShortType,true),"
                        + "StructField(bigint,LongType,true),"
                        + "StructField(bytes,BinaryType,true),"
                        + "StructField(timestamp,TimestampType,true),"
                        + "StructField(date,DateType,true),"
                        + "StructField(decimal,DecimalType(2,2),true),"
                        + "StructField(decimal2,DecimalType(38,2),true),"
                        + "StructField(decimal3,DecimalType(10,1),true))";

        StructType sparkType = fromPaimonRowType(ALL_TYPES);
        assertThat(sparkType.toString().replace(", ", ",")).isEqualTo(expected);

        // Ignore the assertion below, since we force to make all the fields nullable.
        // assertThat(toPaimonType(sparkType)).isEqualTo(ALL_TYPES);
    }
}
