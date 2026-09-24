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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests exact predicate binding by name, including transforms and missing operands. */
class PredicateRemapperTest {

    private static final RowType TYPE =
            RowType.of(
                    new DataField(17, "a", DataTypes.STRING()),
                    new DataField(41, "b", DataTypes.STRING()),
                    new DataField(71, "c", DataTypes.INT()));

    @Test
    void testCompoundPredicateOnReorderedFields() {
        PredicateBuilder builder = new PredicateBuilder(TYPE);
        Predicate predicate =
                PredicateBuilder.and(
                        builder.equal(0, BinaryString.fromString("left")),
                        PredicateBuilder.or(
                                builder.equal(1, BinaryString.fromString("right")),
                                builder.greaterThan(2, 1)));
        Predicate remapped = PredicateRemapper.remap(predicate, TYPE.project("b", "c", "a"));
        assertThat(
                        remapped.test(
                                GenericRow.of(
                                        BinaryString.fromString("right"),
                                        0,
                                        BinaryString.fromString("left"))))
                .isTrue();
        assertThat(
                        remapped.test(
                                GenericRow.of(
                                        BinaryString.fromString("other"),
                                        2,
                                        BinaryString.fromString("left"))))
                .isTrue();
        assertThat(
                        remapped.test(
                                GenericRow.of(
                                        BinaryString.fromString("other"),
                                        0,
                                        BinaryString.fromString("left"))))
                .isFalse();
        assertThat(
                        remapped.test(
                                GenericRow.of(
                                        BinaryString.fromString("right"),
                                        2,
                                        BinaryString.fromString("wrong"))))
                .isFalse();
        // Binding must not mutate the original predicate or its positional references.
        assertThat(
                        predicate.test(
                                GenericRow.of(
                                        BinaryString.fromString("left"),
                                        BinaryString.fromString("right"),
                                        0)))
                .isTrue();
    }

    @Test
    void testFieldIdsAndLiteralTransformInputs() {
        Transform transform =
                new ConcatWsTransform(
                        Arrays.asList(
                                BinaryString.fromString("-"),
                                new FieldRef(17, "a", DataTypes.STRING()),
                                new FieldRef(41, "b", DataTypes.STRING())));
        RowType readType = TYPE.project("b", "a");
        GenericRow row =
                GenericRow.of(BinaryString.fromString("right"), BinaryString.fromString("left"));
        assertThat(PredicateRemapper.remap(transform, readType).transform(row))
                .isEqualTo(BinaryString.fromString("left-right"));
        Predicate predicate =
                new PredicateBuilder(TYPE).equal(transform, BinaryString.fromString("left-right"));
        assertThat(PredicateRemapper.remap(predicate, readType).test(row)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testMissingConjunctOrDisjunctIsRejected(boolean and) {
        PredicateBuilder builder = new PredicateBuilder(TYPE);
        Predicate a = builder.isNotNull(0);
        Predicate b = builder.isNotNull(1);
        Predicate predicate = and ? PredicateBuilder.and(a, b) : PredicateBuilder.or(a, b);
        assertThatThrownBy(() -> PredicateRemapper.remap(predicate, TYPE.project("a")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot resolve field 'b'");
    }

    @Test
    void testNullPredicateOnReorderedFields() {
        Predicate predicate = new PredicateBuilder(TYPE).isNull(0);
        Predicate remapped = PredicateRemapper.remap(predicate, TYPE.project("b", "a"));
        assertThat(remapped.test(GenericRow.of(BinaryString.fromString("b"), null))).isTrue();
        assertThat(remapped.test(GenericRow.of(null, BinaryString.fromString("a")))).isFalse();
    }

    @Test
    void testMalformedCompoundIsRejected() {
        for (String json :
                Arrays.asList(
                        "{\"kind\":\"COMPOUND\",\"function\":\"AND\",\"children\":[]}",
                        "{\"kind\":\"COMPOUND\",\"function\":null,\"children\":[]}",
                        "{\"kind\":\"COMPOUND\",\"function\":\"AND\",\"children\":[null]}")) {
            Predicate predicate = JsonSerdeUtil.fromJson(json, Predicate.class);
            assertThatThrownBy(() -> PredicateRemapper.remap(predicate, TYPE))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
