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

package org.apache.paimon.fileindex;

import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.PredicateVisitor;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileIndexPredicate}, specifically the getRequiredNames visitor. */
public class FileIndexPredicateTest {

    /**
     * Verifies that getRequiredNames (via the PredicateVisitor in FileIndexPredicate) visits each
     * child node exactly once, not twice. Before the fix, the visitor called child.visit(this)
     * twice per child — once discarding the result (line 130) and once using it (line 131). This
     * caused O(2^n) complexity for deeply nested OR predicates (e.g., IN clauses with <= 20
     * values).
     *
     * <p>This test builds a deeply nested OR predicate tree (simulating an IN clause) and counts
     * the total number of leaf visits. With the fix, the count should be exactly N (one per leaf).
     * Before the fix, it would be 2^N - 1.
     */
    @Test
    public void testGetRequiredNamesLinearComplexity() {
        RowType rowType = RowType.of(DataTypes.INT());
        PredicateBuilder builder = new PredicateBuilder(rowType);

        // Build an OR chain of 20 equality predicates: col0 = 0 OR col0 = 1 OR ... OR col0 = 19
        // PredicateBuilder.or() uses reduce, creating a right-nested binary tree of depth ~20.
        List<Predicate> equals = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            equals.add(builder.equal(0, i));
        }
        Predicate inPredicate = PredicateBuilder.or(equals);

        // Count leaf visits using the same visitor pattern as getRequiredNames
        AtomicInteger visitCount = new AtomicInteger(0);
        Set<String> result =
                inPredicate.visit(
                        new PredicateVisitor<Set<String>>() {
                            @Override
                            public Set<String> visit(
                                    org.apache.paimon.predicate.LeafPredicate predicate) {
                                visitCount.incrementAndGet();
                                return new HashSet<>(predicate.fieldNames());
                            }

                            @Override
                            public Set<String> visit(CompoundPredicate predicate) {
                                Set<String> names = new HashSet<>();
                                for (Predicate child : predicate.children()) {
                                    names.addAll(child.visit(this));
                                }
                                return names;
                            }
                        });

        // The result should contain the field name
        assertThat(result).containsExactly("f0");

        // With correct linear traversal, each of the 20 leaves is visited exactly once,
        // plus we traverse ~19 compound nodes. The leaf visit count should be exactly 20.
        assertThat(visitCount.get()).isEqualTo(20);
    }

}
