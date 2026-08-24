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

package org.apache.paimon.table.format;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.SubstringTransform;
import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.rest.requests.ListPartitionsByFilterRequest;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PartitionNamePatterns}. */
class PartitionNamePatternsTest {

    private static final List<String> ONE_KEY = Collections.singletonList("dt");
    private static final List<String> TWO_KEYS = Arrays.asList("dt", "hour");

    private static PredicateBuilder stringBuilder() {
        return new PredicateBuilder(
                RowType.of(
                        new org.apache.paimon.types.DataType[] {
                            DataTypes.STRING(), DataTypes.STRING()
                        },
                        new String[] {"dt", "hour"}));
    }

    private static PredicateBuilder intBuilder() {
        return new PredicateBuilder(
                RowType.of(
                        new org.apache.paimon.types.DataType[] {DataTypes.INT()},
                        new String[] {"dt"}));
    }

    private static BinaryString s(String v) {
        return BinaryString.fromString(v);
    }

    private static LinkedHashMap<String, String> noPrefix() {
        return new LinkedHashMap<>();
    }

    @Test
    void noFilterKeepsTheExistingEqualityBehaviour() {
        LinkedHashMap<String, String> prefix = new LinkedHashMap<>();
        prefix.put("dt", "20190702");
        assertThat(PartitionNamePatterns.build(TWO_KEYS, prefix, null)).isEqualTo("dt=20190702/%");
        assertThat(PartitionNamePatterns.build(ONE_KEY, prefix, null)).isEqualTo("dt=20190702");
        assertThat(PartitionNamePatterns.build(ONE_KEY, noPrefix(), null)).isNull();
    }

    @Test
    void rangeOnLeadingKeyNarrowsToTheCommonPrefix() {
        PredicateBuilder b = stringBuilder();
        Predicate filter =
                PredicateBuilder.and(
                        b.greaterOrEqual(0, s("2019070100")), b.lessOrEqual(0, s("2019070123")));
        assertThat(PartitionNamePatterns.build(ONE_KEY, noPrefix(), filter))
                .isEqualTo("dt=20190701%");
    }

    @Test
    void supplementaryRangeDoesNotProduceMalformedPatternAcrossJson() throws Exception {
        PredicateBuilder b = stringBuilder();
        Predicate filter = b.between(0, s("😀-a"), s("🙏-z"));

        String pattern = PartitionNamePatterns.build(ONE_KEY, noPrefix(), filter);
        ListPartitionsByFilterRequest request =
                new ListPartitionsByFilterRequest("filter-json", pattern, 1000, null);
        ListPartitionsByFilterRequest roundTripped =
                RESTApi.fromJson(RESTApi.toJson(request), ListPartitionsByFilterRequest.class);

        assertThat(roundTripped.getPartitionNamePattern()).isNull();
    }

    @Test
    void strictBoundsNarrowTheSameWay() {
        PredicateBuilder b = stringBuilder();
        Predicate filter =
                PredicateBuilder.and(
                        b.greaterThan(0, s("2019070100")), b.lessThan(0, s("2019070123")));
        assertThat(PartitionNamePatterns.build(ONE_KEY, noPrefix(), filter))
                .isEqualTo("dt=20190701%");
    }

    @Test
    void rangeUsesAnyNonEmptyCommonPrefix() {
        PredicateBuilder b = stringBuilder();
        Predicate filter =
                PredicateBuilder.and(
                        b.greaterOrEqual(0, s("2019010100")), b.lessOrEqual(0, s("2019123123")));
        // The year is shared, which is still a useful prefix.
        assertThat(PartitionNamePatterns.build(ONE_KEY, noPrefix(), filter)).isEqualTo("dt=2019%");

        Predicate spanning =
                PredicateBuilder.and(
                        b.greaterOrEqual(0, s("2019010100")), b.lessOrEqual(0, s("2020123123")));
        assertThat(PartitionNamePatterns.build(ONE_KEY, noPrefix(), spanning)).isEqualTo("dt=20%");
    }

    @Test
    void oneSidedRangeGivesNoPrefix() {
        PredicateBuilder b = stringBuilder();
        assertThat(
                        PartitionNamePatterns.build(
                                ONE_KEY, noPrefix(), b.greaterOrEqual(0, s("2019070100"))))
                .isNull();
    }

    @Test
    void betweenWithNullBoundaryFallsBack() {
        PredicateBuilder b = stringBuilder();

        assertThat(PartitionNamePatterns.build(ONE_KEY, noPrefix(), b.between(0, null, s("z"))))
                .isNull();
        assertThat(PartitionNamePatterns.build(ONE_KEY, noPrefix(), b.between(0, s("a"), null)))
                .isNull();
    }

    /**
     * The counterexample that forces the string-only guard: on an INT key, {@code dt >= 9 AND dt <=
     * 99} holds for 10, whose partition name {@code dt=10} does not start with the common prefix
     * "9". Deriving a pattern there would silently drop a matching partition.
     */
    @Test
    void numericKeyNeverGetsAPrefix() {
        PredicateBuilder b = intBuilder();
        Predicate filter = PredicateBuilder.and(b.greaterOrEqual(0, 9), b.lessOrEqual(0, 99));
        assertThat(PartitionNamePatterns.build(ONE_KEY, noPrefix(), filter)).isNull();
    }

    @Test
    void substringAnchoredAtOneNarrowsToItsLiteral() {
        Predicate filter = substringEqual(1, 6, "201907");
        assertThat(PartitionNamePatterns.build(ONE_KEY, noPrefix(), filter))
                .isEqualTo("dt=201907%");
    }

    @Test
    void substringNotAnchoredAtOneGivesNoPrefix() {
        // substr(dt, 3, 4) says nothing about the first two characters.
        assertThat(PartitionNamePatterns.build(ONE_KEY, noPrefix(), substringEqual(3, 4, "1907")))
                .isNull();
    }

    @Test
    void valuePrefixExtendsAnEqualityPrefixByExactlyOneKey() {
        LinkedHashMap<String, String> prefix = new LinkedHashMap<>();
        prefix.put("dt", "20190702");
        PredicateBuilder b = stringBuilder();
        Predicate filter =
                PredicateBuilder.and(b.greaterOrEqual(1, s("06")), b.lessOrEqual(1, s("09")));
        assertThat(PartitionNamePatterns.build(TWO_KEYS, prefix, filter))
                .isEqualTo("dt=20190702/hour=0%");
    }

    @Test
    void unrepresentableEqualityPrefixIsNotExtended() {
        LinkedHashMap<String, String> prefix = new LinkedHashMap<>();
        prefix.put("dt", "");
        PredicateBuilder b = stringBuilder();
        Predicate filter =
                PredicateBuilder.and(b.greaterOrEqual(1, s("06")), b.lessOrEqual(1, s("09")));

        assertThat(PartitionNamePatterns.build(TWO_KEYS, prefix, filter)).isNull();
    }

    @Test
    void predicateOnALaterKeyDoesNotLeakIntoTheLeadingPosition() {
        // Constraining `hour` says nothing about `dt`, so the pattern must stay unconstrained.
        PredicateBuilder b = stringBuilder();
        Predicate filter =
                PredicateBuilder.and(b.greaterOrEqual(1, s("06")), b.lessOrEqual(1, s("09")));
        assertThat(PartitionNamePatterns.build(TWO_KEYS, noPrefix(), filter)).isNull();
    }

    @Test
    void orPredicatesAreIgnored() {
        PredicateBuilder b = stringBuilder();
        Predicate filter =
                PredicateBuilder.or(b.equal(0, s("2019070212")), b.equal(0, s("2019070213")));
        // splitAnd yields the OR itself, which is not a leaf: no prefix, and no wrong narrowing.
        assertThat(PartitionNamePatterns.build(ONE_KEY, noPrefix(), filter)).isNull();
    }

    private static Predicate substringEqual(int begin, int length, String literal) {
        FieldRef ref = new FieldRef(0, "dt", DataTypes.STRING());
        SubstringTransform transform = new SubstringTransform(Arrays.asList(ref, begin, length));
        return LeafPredicate.of(transform, Equal.INSTANCE, Collections.singletonList(s(literal)));
    }
}
