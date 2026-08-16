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

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.table.SpecialFields.ROW_ID;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RowIdPredicateVisitor}. */
public class RowIdPredicateVisitorTest {

    private PredicateBuilder builder;
    private RowIdPredicateVisitor visitor;
    private int rowIdIndex;

    @BeforeEach
    public void setUp() {
        RowType rowType =
                RowType.of(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(1, ROW_ID.name(), DataTypes.BIGINT()),
                        new DataField(2, "name", DataTypes.STRING()));
        builder = new PredicateBuilder(rowType);
        rowIdIndex = builder.indexOf(ROW_ID.name());
        visitor = new RowIdPredicateVisitor();
    }

    @Test
    public void testSimplePredicate() {
        // Test Equal
        Predicate equal = builder.equal(rowIdIndex, 5L);
        assertThat(equal.visit(visitor)).contains(Collections.singletonList(new Range(5, 5)));

        // Test In with single value
        Predicate inSingle = builder.in(rowIdIndex, Collections.singletonList(10L));
        assertThat(inSingle.visit(visitor)).contains(Collections.singletonList(new Range(10, 10)));

        // Test In with multiple values (consecutive should merge)
        Predicate inConsecutive = builder.in(rowIdIndex, Arrays.asList(1L, 2L, 3L, 5L, 6L));
        Optional<List<Range>> result = inConsecutive.visit(visitor);
        assertThat(result).isPresent();
        assertThat(result.get()).containsExactly(new Range(1, 3), new Range(5, 6));

        // Test Between
        Predicate between = builder.between(rowIdIndex, 10L, 20L);
        assertThat(between.visit(visitor)).contains(Collections.singletonList(new Range(10, 20)));

        // Test non-ROW_ID field (should return empty)
        Predicate otherField = builder.equal(0, 5);
        assertThat(otherField.visit(visitor)).isEmpty();

        // Test unrecognized predicate (should return empty)
        Predicate unrecognized = builder.greaterThan(0, 10);
        assertThat(unrecognized.visit(visitor)).isEmpty();
    }

    @Test
    public void testCompoundedPredicate() {
        // Test AND intersection
        Predicate equal5 = builder.equal(rowIdIndex, 5L);
        Predicate between1To10 = builder.between(rowIdIndex, 1L, 10L);
        Predicate andIntersect = PredicateBuilder.and(equal5, between1To10);
        assertThat(andIntersect.visit(visitor))
                .contains(Collections.singletonList(new Range(5, 5)));

        // Test AND with no overlap (should return empty list)
        Predicate equal3 = builder.equal(rowIdIndex, 3L);
        Predicate andNoOverlap = PredicateBuilder.and(equal5, equal3);
        Optional<List<Range>> noOverlapResult = andNoOverlap.visit(visitor);
        assertThat(noOverlapResult).isPresent();
        assertThat(noOverlapResult.get()).isEmpty();

        // Test OR union
        Predicate orUnion = PredicateBuilder.or(equal5, equal3);
        Optional<List<Range>> unionResult = orUnion.visit(visitor);
        assertThat(unionResult).isPresent();
        assertThat(unionResult.get()).containsExactly(new Range(3, 3), new Range(5, 5));

        // Test OR with overlapping ranges (should merge)
        Predicate between1To5 = builder.between(rowIdIndex, 1L, 5L);
        Predicate between3To7 = builder.between(rowIdIndex, 3L, 7L);
        Predicate orOverlap = PredicateBuilder.or(between1To5, between3To7);
        assertThat(orOverlap.visit(visitor)).contains(Collections.singletonList(new Range(1, 7)));

        // Test OR with adjacent ranges (should merge)
        Predicate between1To3 = builder.between(rowIdIndex, 1L, 3L);
        Predicate between4To6 = builder.between(rowIdIndex, 4L, 6L);
        Predicate orAdjacent = PredicateBuilder.or(between1To3, between4To6);
        assertThat(orAdjacent.visit(visitor)).contains(Collections.singletonList(new Range(1, 6)));

        // Test OR with mixed fields (should return empty)
        Predicate otherField = builder.equal(0, 5);
        Predicate orMixed = PredicateBuilder.or(equal5, otherField);
        assertThat(orMixed.visit(visitor)).isEmpty();

        // Test complex nested: (1 OR 2) AND (2 OR 3) should result in 2
        Predicate p1 = builder.equal(rowIdIndex, 1L);
        Predicate p2 = builder.equal(rowIdIndex, 2L);
        Predicate p3 = builder.equal(rowIdIndex, 3L);
        Predicate leftOr = PredicateBuilder.or(p1, p2);
        Predicate rightOr = PredicateBuilder.or(p2, p3);
        Predicate complexAnd = PredicateBuilder.and(leftOr, rightOr);
        assertThat(complexAnd.visit(visitor)).contains(Collections.singletonList(new Range(2, 2)));
    }
}
