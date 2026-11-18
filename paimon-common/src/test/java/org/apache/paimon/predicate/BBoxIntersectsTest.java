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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.junit.jupiter.api.Test;

import static org.apache.paimon.geo.WkbTestUtils.line;
import static org.apache.paimon.geo.WkbTestUtils.point;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Envelope predicates preserve their semantics through serialization and full-scan fallback. */
class BBoxIntersectsTest {

    @Test
    void testPredicateAndSerialization() throws Exception {
        Predicate predicate =
                new PredicateBuilder(RowType.of(DataTypes.GEOMETRY()))
                        .bboxIntersects(0, 0, 0, 2, 2);
        Predicate[] predicates = {
            predicate,
            JsonSerdeUtil.fromJson(JsonSerdeUtil.toJson(predicate), Predicate.class),
            InstantiationUtil.deserializeObject(
                    InstantiationUtil.serializeObject(predicate), getClass().getClassLoader())
        };
        for (Predicate value : predicates) {
            assertThat(value.test(GenericRow.of((Object) point(2, 2)))).isTrue();
            assertThat(value.test(GenericRow.of((Object) line(-1, 1, 3, 1)))).isTrue();
            assertThat(value.test(GenericRow.of((Object) point(3, 3)))).isFalse();
            assertThat(value.test(GenericRow.of((Object) point(Double.NaN, Double.NaN)))).isFalse();
            assertThat(value.test(GenericRow.of((Object) null))).isFalse();
            assertThat(value.negate()).isEmpty();
        }
    }

    @Test
    void testRejectsWrongTypesAndInvalidWindows() {
        for (org.apache.paimon.types.DataType type :
                new org.apache.paimon.types.DataType[] {
                    DataTypes.BYTES(), DataTypes.GEOGRAPHY(), DataTypes.INT()
                }) {
            assertThatThrownBy(
                            () ->
                                    new PredicateBuilder(RowType.of(type))
                                            .bboxIntersects(0, 0, 0, 1, 1))
                    .hasMessageContaining("GEOMETRY");
        }
        assertThatThrownBy(
                        () ->
                                new PredicateBuilder(RowType.of(DataTypes.GEOMETRY()))
                                        .bboxIntersects(0, 2, 0, 1, 1))
                .hasMessageContaining("ordered");
    }
}
