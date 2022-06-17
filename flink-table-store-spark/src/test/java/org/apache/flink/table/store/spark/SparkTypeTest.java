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

package org.apache.flink.table.store.spark;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SparkTypeUtils}. */
public class SparkTypeTest {

    public static final RowType ALL_TYPES =
            (RowType)
                    TableSchema.builder()
                            .field("id", DataTypes.INT().notNull())
                            .field("name", DataTypes.STRING()) /* optional by default */
                            .field("salary", DataTypes.DOUBLE().notNull())
                            .field(
                                    "locations",
                                    DataTypes.MAP(
                                            DataTypes.STRING(),
                                            DataTypes.ROW(
                                                    DataTypes.FIELD(
                                                            "posX",
                                                            DataTypes.DOUBLE().notNull(),
                                                            "X field"),
                                                    DataTypes.FIELD(
                                                            "posY",
                                                            DataTypes.DOUBLE().notNull(),
                                                            "Y field"))))
                            .field("strArray", DataTypes.ARRAY(DataTypes.STRING()).nullable())
                            .field("intArray", DataTypes.ARRAY(DataTypes.INT()).nullable())
                            .field("char", DataTypes.CHAR(10).notNull())
                            .field("varchar", DataTypes.VARCHAR(10).notNull())
                            .field("boolean", DataTypes.BOOLEAN().nullable())
                            .field("tinyint", DataTypes.TINYINT())
                            .field("smallint", DataTypes.SMALLINT())
                            .field("bigint", DataTypes.BIGINT())
                            .field("varbinary", DataTypes.VARBINARY(10))
                            .field("binary", DataTypes.BINARY(10))
                            .field("timestampWithoutZone", DataTypes.TIMESTAMP())
                            .field("timestampWithZone", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE())
                            .field("date", DataTypes.DATE())
                            .field("decimal", DataTypes.DECIMAL(2, 2))
                            .field("decimal2", DataTypes.DECIMAL(38, 2))
                            .field("decimal3", DataTypes.DECIMAL(10, 1))
                            .field("multiset", DataTypes.MULTISET(DataTypes.STRING().notNull()))
                            .build()
                            .toRowDataType()
                            .getLogicalType();

    @Test
    public void testAllTypes() {
        String nestedRowMapType =
                "StructField(locations,MapType("
                        + "StringType,"
                        + "StructType(StructField(posX,DoubleType,false), StructField(posY,DoubleType,false)),true),true)";
        String expected =
                "StructType("
                        + "StructField(id,IntegerType,false), "
                        + "StructField(name,StringType,true), "
                        + "StructField(salary,DoubleType,false), "
                        + nestedRowMapType
                        + ", "
                        + "StructField(strArray,ArrayType(StringType,true),true), "
                        + "StructField(intArray,ArrayType(IntegerType,true),true), "
                        + "StructField(char,StringType,false), "
                        + "StructField(varchar,StringType,false), "
                        + "StructField(boolean,BooleanType,true), "
                        + "StructField(tinyint,ByteType,true), "
                        + "StructField(smallint,ShortType,true), "
                        + "StructField(bigint,LongType,true), "
                        + "StructField(varbinary,BinaryType,true), "
                        + "StructField(binary,BinaryType,true), "
                        + "StructField(timestampWithoutZone,TimestampType,true), "
                        + "StructField(timestampWithZone,TimestampType,true), "
                        + "StructField(date,DateType,true), "
                        + "StructField(decimal,DecimalType(2,2),true), "
                        + "StructField(decimal2,DecimalType(38,2),true), "
                        + "StructField(decimal3,DecimalType(10,1),true), "
                        + "StructField(multiset,MapType(StringType,IntegerType,false),true))";

        assertThat(SparkTypeUtils.fromFlinkRowType(ALL_TYPES).toString()).isEqualTo(expected);
    }
}
