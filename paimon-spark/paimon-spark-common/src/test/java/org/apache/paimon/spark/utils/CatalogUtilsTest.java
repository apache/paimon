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

package org.apache.paimon.spark.utils;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.VarCharType;

import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;

import static org.apache.spark.sql.types.DataTypes.createArrayType;
import static org.apache.spark.sql.types.DataTypes.createMapType;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CatalogUtils}. */
public class CatalogUtilsTest {
    @Test
    void paimonType2SparkType() {
        assertThat(CatalogUtils.paimonType2SparkType(new DecimalType(2, 1)))
                .isEqualTo(DataTypes.createDecimalType(2, 1));
        assertThat(CatalogUtils.paimonType2SparkType(new ArrayType(new IntType())))
                .isEqualTo(createArrayType(org.apache.spark.sql.types.DataTypes.IntegerType));
        assertThat(CatalogUtils.paimonType2SparkType(new MapType(new IntType(), new VarCharType())))
                .isEqualTo(
                        createMapType(
                                org.apache.spark.sql.types.DataTypes.IntegerType,
                                DataTypes.StringType));
    }

    @Test
    void paimonType2JavaType() {
        assertThat(CatalogUtils.paimonType2JavaType(new DecimalType(2, 1)))
                .isEqualTo("java.math.BigDecimal");
        assertThat(CatalogUtils.paimonType2JavaType(new ArrayType(new IntType())))
                .isEqualTo("java.lang.Integer[]");
        assertThat(CatalogUtils.paimonType2JavaType(new MapType(new IntType(), new VarCharType())))
                .isEqualTo("java.util.Map<java.lang.Integer,java.lang.String>");
    }
}
