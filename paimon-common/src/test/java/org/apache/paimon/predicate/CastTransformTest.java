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

package org.apache.paimon.predicate;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class CastTransformTest {

    private static Stream<Arguments> nullableCasts() {
        return Stream.of(
                Arguments.of(DataTypes.INT(), DataTypes.BIGINT()),
                Arguments.of(DataTypes.INT(), DataTypes.STRING()),
                Arguments.of(DataTypes.BOOLEAN(), DataTypes.STRING()),
                Arguments.of(DataTypes.STRING(), DataTypes.INT()),
                Arguments.of(DataTypes.STRING(), DataTypes.DATE()),
                Arguments.of(DataTypes.TIMESTAMP(6), DataTypes.STRING()),
                Arguments.of(DataTypes.DECIMAL(8, 2), DataTypes.DECIMAL(10, 3)));
    }

    @ParameterizedTest
    @MethodSource("nullableCasts")
    void testNullPropagation(DataType source, DataType target) throws Exception {
        CastTransform transform = new CastTransform(new FieldRef(0, "f", source), target);
        GenericRow row = GenericRow.of((Object) null);
        assertThat(transform.transform(row)).isNull();
        assertThat(
                        CastTransform.tryCreate(new FieldRef(0, "f", source), target)
                                .get()
                                .transform(row))
                .isNull();
        Transform javaCopy =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(transform), getClass().getClassLoader());
        assertThat(javaCopy.transform(row)).isNull();
        Transform jsonCopy =
                JsonSerdeUtil.fromJson(JsonSerdeUtil.toJson(transform), Transform.class);
        assertThat(jsonCopy.transform(row)).isNull();
        Transform projected =
                transform.copyWithNewInputs(
                        Collections.singletonList(new FieldRef(1, "f", source)));
        assertThat(projected.transform(GenericRow.of(1, null))).isNull();
    }

    @Test
    void testNonNullCastsUnchanged() {
        assertThat(
                        new CastTransform(new FieldRef(0, "f", DataTypes.INT()), DataTypes.BIGINT())
                                .transform(GenericRow.of(Integer.MAX_VALUE)))
                .isEqualTo((long) Integer.MAX_VALUE);
        assertThat(
                        new CastTransform(new FieldRef(0, "f", DataTypes.INT()), DataTypes.STRING())
                                .transform(GenericRow.of(-123)))
                .isEqualTo(BinaryString.fromString("-123"));
        assertThat(
                        new CastTransform(new FieldRef(0, "f", DataTypes.STRING()), DataTypes.INT())
                                .transform(GenericRow.of(BinaryString.fromString("123"))))
                .isEqualTo(123);
    }
}
