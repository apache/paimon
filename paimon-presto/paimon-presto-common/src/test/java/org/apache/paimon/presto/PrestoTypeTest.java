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

import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.TimeType;

import com.facebook.presto.common.type.Type;
import org.junit.jupiter.api.Test;

import java.util.Objects;

import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PrestoTypeUtils}. */
public class PrestoTypeTest {

    @Test
    public void testCharType() {
        Type type =
                PrestoTypeUtils.toPrestoType(DataTypes.CHAR(1), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(type).getDisplayName()).isEqualTo("char(1)");
    }

    @Test
    public void testVarCharType() {
        Type type =
                PrestoTypeUtils.toPrestoType(
                        DataTypes.VARCHAR(10), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(type).getDisplayName()).isEqualTo("varchar");
    }

    @Test
    public void testBooleanType() {
        Type type =
                PrestoTypeUtils.toPrestoType(
                        DataTypes.BOOLEAN(), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(type).getDisplayName()).isEqualTo("boolean");
    }

    @Test
    public void testBinaryType() {
        Type type =
                PrestoTypeUtils.toPrestoType(
                        DataTypes.BINARY(10), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(type).getDisplayName()).isEqualTo("varbinary");
    }

    @Test
    public void testVarBinaryType() {
        Type type =
                PrestoTypeUtils.toPrestoType(
                        DataTypes.VARBINARY(10), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(type).getDisplayName()).isEqualTo("varbinary");
    }

    @Test
    public void testVarDecimalType() {
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
    }

    @Test
    public void testTinyIntType() {
        Type type =
                PrestoTypeUtils.toPrestoType(
                        DataTypes.TINYINT(), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(type).getDisplayName()).isEqualTo("tinyint");
    }

    @Test
    public void testSmallIntType() {
        Type type =
                PrestoTypeUtils.toPrestoType(
                        DataTypes.SMALLINT(), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(type).getDisplayName()).isEqualTo("smallint");
    }

    @Test
    public void testIntType() {
        Type type =
                PrestoTypeUtils.toPrestoType(DataTypes.INT(), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(type).getDisplayName()).isEqualTo("integer");
    }

    @Test
    public void testBigIntType() {
        Type type =
                PrestoTypeUtils.toPrestoType(
                        DataTypes.BIGINT(), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(type).getDisplayName()).isEqualTo("bigint");
    }

    @Test
    public void testDoubleType() {
        Type type =
                PrestoTypeUtils.toPrestoType(
                        DataTypes.DOUBLE(), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(type).getDisplayName()).isEqualTo("double");
    }

    @Test
    public void testDateType() {
        Type type =
                PrestoTypeUtils.toPrestoType(DataTypes.DATE(), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(type).getDisplayName()).isEqualTo("date");
    }

    @Test
    public void testTimeType() {
        Type type =
                PrestoTypeUtils.toPrestoType(new TimeType(), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(type).getDisplayName()).isEqualTo("time");
    }

    @Test
    public void testTimestampType() {
        Type type =
                PrestoTypeUtils.toPrestoType(
                        DataTypes.TIMESTAMP(), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(type).getDisplayName()).isEqualTo("timestamp");
    }

    @Test
    public void testLocalZonedTimestampType() {
        Type type =
                PrestoTypeUtils.toPrestoType(
                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(),
                        createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(type).getDisplayName())
                .isEqualTo("timestamp with time zone");
    }

    @Test
    public void testMapType() {
        Type type =
                PrestoTypeUtils.toPrestoType(
                        DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING()),
                        createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(type).getDisplayName()).isEqualTo("map(bigint, varchar)");
    }
}
