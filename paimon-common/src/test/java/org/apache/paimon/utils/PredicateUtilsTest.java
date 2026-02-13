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

package org.apache.paimon.utils;

import org.apache.paimon.predicate.And;
import org.apache.paimon.predicate.Between;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PredicateUtils}. */
public class PredicateUtilsTest {

    @Test
    public void testTryRewriteBetweenPredicateBasic() {
        // Test basic case: AND(a>=1, a<=10, a is not null) should be rewritten to BETWEEN
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate gte = builder.greaterOrEqual(0, 1);
        Predicate lte = builder.lessOrEqual(0, 10);
        Predicate isNotNull = builder.isNotNull(0);

        Predicate andPredicate = PredicateBuilder.and(gte, isNotNull, lte);
        Predicate result = PredicateUtils.tryRewriteBetweenPredicate(andPredicate);

        assertThat(result).isInstanceOf(CompoundPredicate.class);
        CompoundPredicate compoundResult = (CompoundPredicate) result;
        assertThat(compoundResult.function()).isInstanceOf(And.class);
        assertThat(compoundResult.children()).hasSize(2);

        Predicate betweenChild = compoundResult.children().get(1);
        assertThat(betweenChild).isInstanceOf(LeafPredicate.class);
        LeafPredicate betweenLeaf = (LeafPredicate) betweenChild;
        assertThat(betweenLeaf.function()).isInstanceOf(Between.class);
        assertThat(betweenLeaf.literals()).containsExactly(1, 10);

        Predicate notNullChild = compoundResult.children().get(0);
        assertThat(notNullChild).isInstanceOf(LeafPredicate.class);
        assertThat(notNullChild.toString()).contains("IsNotNull");
    }

    @Test
    public void testTryRewriteBetweenPredicateRecursive() {
        // Test recursive case: OR(b>=1, AND(a>=1, a<=10, a is not null)) should rewrite nested AND
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType(), new IntType()));

        Predicate gteB = builder.greaterOrEqual(1, 1);
        Predicate gteA = builder.greaterOrEqual(0, 1);
        Predicate lteA = builder.lessOrEqual(0, 10);
        Predicate isNotNullA = builder.isNotNull(0);
        Predicate andPredicate = PredicateBuilder.and(gteA, isNotNullA, lteA);
        Predicate orPredicate = PredicateBuilder.or(gteB, andPredicate);

        Predicate result = PredicateUtils.tryRewriteBetweenPredicate(orPredicate);

        assertThat(result).isInstanceOf(CompoundPredicate.class);
        CompoundPredicate compoundResult = (CompoundPredicate) result;
        assertThat(compoundResult.function()).isInstanceOf(Or.class);
        assertThat(compoundResult.children()).hasSize(2);

        Predicate secondChild = compoundResult.children().get(1);
        assertThat(secondChild).isInstanceOf(LeafPredicate.class);
        assertThat(secondChild.toString()).contains("GreaterOrEqual");

        Predicate firstChild = compoundResult.children().get(0);
        assertThat(firstChild).isInstanceOf(CompoundPredicate.class);
        CompoundPredicate innerAnd = (CompoundPredicate) firstChild;
        assertThat(innerAnd.function()).isInstanceOf(And.class);
        assertThat(innerAnd.children()).hasSize(2);

        Predicate betweenCandidate = innerAnd.children().get(1);
        assertThat(betweenCandidate).isInstanceOf(LeafPredicate.class);
        LeafPredicate betweenLeaf = (LeafPredicate) betweenCandidate;
        assertThat(betweenLeaf.function()).isInstanceOf(Between.class);
        assertThat(betweenLeaf.literals()).containsExactly(1, 10);
    }

    /**
     * Test this complicated scenario.
     *
     * <pre>{@code
     *             AND
     *           /  |  \
     *         OR  AND a>=1
     *        /|   || \
     *       / |  / |  \
     * a>=1 a<=2 OR AND a>=2
     *          / |  | \
     *         /  |  |  \
     *     a>=1 b<2 b>=1 a<=10
     *
     * }</pre>
     */
    @Test
    public void testAnExtremeComplicatedPredicate() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType(), new IntType()));
        Predicate l3p1 = builder.greaterOrEqual(0, 1);
        Predicate l3p2 = builder.lessThan(1, 2);
        Predicate l3p3 = builder.greaterOrEqual(1, 1);
        Predicate l3p4 = builder.lessOrEqual(0, 10);
        Predicate l2p1 = builder.greaterOrEqual(0, 1);
        Predicate l2p2 = builder.lessOrEqual(1, 2);
        Predicate l2p3 = PredicateBuilder.or(l3p1, l3p2);
        Predicate l2p4 = PredicateBuilder.and(l3p3, l3p4);
        Predicate l2p5 = builder.greaterOrEqual(0, 2);
        Predicate l1p1 = PredicateBuilder.or(l2p1, l2p2);
        Predicate l1p2 = PredicateBuilder.and(l2p3, l2p4, l2p5);
        Predicate l1p3 = builder.greaterOrEqual(0, 1);
        Predicate root = PredicateBuilder.and(l1p1, l1p2, l1p3);

        Predicate result = PredicateUtils.tryRewriteBetweenPredicate(root);
        assertThat(result).isInstanceOf(CompoundPredicate.class);

        CompoundPredicate compoundResult = (CompoundPredicate) result;
        assertThat(compoundResult.function()).isInstanceOf(And.class);

        // directly check the toString
        String resultString = compoundResult.toString();
        assertThat(resultString).contains("Between(f0, [2, 10])");
    }

    @Test
    public void testTryRewriteBetweenPredicateIntersection() {
        // Test intersection case: AND(a>=1, a<=10, a>=2, a<=7) should use intersection (2, 7)
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));

        Predicate gte1 = builder.greaterOrEqual(0, 1);
        Predicate lte10 = builder.lessOrEqual(0, 10);
        Predicate gte2 = builder.greaterOrEqual(0, 2);
        Predicate lte7 = builder.lessOrEqual(0, 7);

        Predicate predicate =
                PredicateBuilder.and(
                        PredicateBuilder.and(gte1, lte10), PredicateBuilder.and(gte2, lte7));
        Predicate result = PredicateUtils.tryRewriteBetweenPredicate(predicate);

        assertThat(result).isInstanceOf(LeafPredicate.class);
        LeafPredicate betweenLeaf = (LeafPredicate) result;
        assertThat(betweenLeaf.function()).isInstanceOf(Between.class);
        assertThat(betweenLeaf.literals()).containsExactly(2, 7);
    }

    @Test
    public void testTryRewriteBetweenPredicateDifferentColumns() {
        // Test different columns case: AND(a>=1, b<=10) should not be rewritten
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType(), new IntType()));

        Predicate gteA = builder.greaterOrEqual(0, 1);
        Predicate lteB = builder.lessOrEqual(1, 10);
        Predicate predicate = PredicateBuilder.and(gteA, lteB);

        Predicate result = PredicateUtils.tryRewriteBetweenPredicate(predicate);

        assertThat(result).isInstanceOf(CompoundPredicate.class);
        CompoundPredicate compoundResult = (CompoundPredicate) result;
        assertThat(compoundResult.function()).isInstanceOf(And.class);
        assertThat(compoundResult.children()).hasSize(2);
        assertThat(compoundResult.children().stream().map(Predicate::toString))
                .containsExactlyInAnyOrderElementsOf(
                        Arrays.asList("GreaterOrEqual(f0, 1)", "LessOrEqual(f1, 10)"));
    }

    @Test
    public void testTryRewriteBetweenPredicateInvalidRange() {
        // Test invalid range case: AND(a>=10, a<=1) should not be rewritten to BETWEEN
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));

        Predicate gte = builder.greaterOrEqual(0, 10);
        Predicate lte = builder.lessOrEqual(0, 1);
        Predicate predicate = PredicateBuilder.and(gte, lte);

        Predicate result = PredicateUtils.tryRewriteBetweenPredicate(predicate);

        assertThat(result).isInstanceOf(CompoundPredicate.class);
        CompoundPredicate compoundResult = (CompoundPredicate) result;
        assertThat(compoundResult.function()).isInstanceOf(And.class);
        assertThat(compoundResult.children()).hasSize(2);
        assertThat(compoundResult.children().stream().map(Predicate::toString))
                .containsExactlyInAnyOrderElementsOf(
                        Arrays.asList("GreaterOrEqual(f0, 10)", "LessOrEqual(f0, 1)"));
    }
}
