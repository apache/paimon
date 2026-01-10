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
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PredicateJsonSerdeTest {

    @Test
    public void testLeafPredicate() {
        PredicateBuilder builder = newBuilder();

        Predicate negatable = builder.equal(0, 1);
        assertThat(negatable).isInstanceOf(LeafPredicate.class);
        assertThat(negatable.negate()).isPresent();
        assertThat(roundTrip(negatable)).isInstanceOf(LeafPredicate.class).isEqualTo(negatable);
        assertThat(roundTrip(negatable).negate()).isEqualTo(negatable.negate());

        Predicate nonNegatable = builder.like(2, BinaryString.fromString("%a%b%"));
        assertThat(nonNegatable).isInstanceOf(LeafPredicate.class);
        assertThat(nonNegatable.negate()).isEmpty();
        Predicate restoredNonNegatable = roundTrip(nonNegatable);
        assertThat(restoredNonNegatable).isInstanceOf(LeafPredicate.class);
        assertThat(restoredNonNegatable.toString()).isEqualTo(nonNegatable.toString());
        assertThat(restoredNonNegatable.negate()).isEmpty();
    }

    @Test
    public void testTransformPredicate() {
        PredicateBuilder builder = newBuilder();

        Transform cast =
                new CastTransform(new FieldRef(0, "f0", new IntType()), DataTypes.BIGINT());
        Transform upper =
                new UpperTransform(
                        Collections.singletonList(new FieldRef(2, "f2", DataTypes.STRING())));
        Transform concat =
                new ConcatTransform(
                        Arrays.asList(
                                new FieldRef(1, "f1", DataTypes.STRING()),
                                BinaryString.fromString("-"),
                                new FieldRef(2, "f2", DataTypes.STRING()),
                                null));
        Transform concatWs =
                new ConcatWsTransform(
                        Arrays.asList(
                                BinaryString.fromString("|"),
                                new FieldRef(1, "f1", DataTypes.STRING()),
                                BinaryString.fromString("X"),
                                null,
                                new FieldRef(2, "f2", DataTypes.STRING())));

        Predicate p1 = builder.greaterThan(cast, 10L);
        Predicate p2 = builder.startsWith(upper, BinaryString.fromString("ABC"));
        Predicate p3 = builder.contains(concat, BinaryString.fromString("m"));
        Predicate p4 = builder.endsWith(concatWs, BinaryString.fromString("z"));
        Predicate p5 = builder.in(upper, manyUpperStringsWithNulls());

        for (Predicate p : Arrays.asList(p1, p2, p3, p4, p5)) {
            assertThat(p)
                    .isInstanceOf(TransformPredicate.class)
                    .isNotInstanceOf(LeafPredicate.class);
            assertThat(p.negate()).isEmpty();
            Predicate restored = roundTrip(p);
            assertThat(restored)
                    .isInstanceOf(TransformPredicate.class)
                    .isNotInstanceOf(LeafPredicate.class)
                    .isEqualTo(p);
            assertThat(restored.negate()).isEmpty();
        }
    }

    @Test
    public void testCompoundPredicate() {
        PredicateBuilder builder = newBuilder();

        Predicate like = builder.like(2, BinaryString.fromString("%a%b%"));
        Predicate compound =
                PredicateBuilder.and(
                        builder.equal(0, 1),
                        builder.in(3, manyInts()),
                        builder.in(3, Collections.emptyList()),
                        like,
                        PredicateBuilder.or(builder.equal(0, 7), builder.isNotNull(2)));

        assertThat(compound).isInstanceOf(CompoundPredicate.class);

        assertThat(compound.negate()).isEmpty();
        Predicate restored = roundTrip(compound);
        assertThat(restored).isInstanceOf(CompoundPredicate.class).isEqualTo(compound);
        assertThat(restored.negate()).isEmpty();
    }

    private static PredicateBuilder newBuilder() {
        return new PredicateBuilder(
                RowType.of(new IntType(), DataTypes.STRING(), DataTypes.STRING(), new IntType()));
    }

    private static List<Object> manyInts() {
        List<Object> ints = new ArrayList<>();
        for (int i = 0; i < 21; i++) {
            ints.add(i);
        }
        return ints;
    }

    private static List<Object> manyUpperStringsWithNulls() {
        List<Object> strings = new ArrayList<>();
        for (int i = 0; i < 21; i++) {
            strings.add(i % 5 == 0 ? null : BinaryString.fromString("S" + i));
        }
        return strings;
    }

    private static Predicate roundTrip(Predicate predicate) {
        String json = JsonSerdeUtil.toFlatJson(predicate);
        return JsonSerdeUtil.fromJson(json, Predicate.class);
    }
}
