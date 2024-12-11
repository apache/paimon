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
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FileIndexFilterFallbackPredicateVisitor}. */
public class FileIndexFilterFallbackPredicateVisitorTest {

    @Test
    public void testVisitLeafPredicate() {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", DataTypes.STRING()),
                                new DataField(1, "b", DataTypes.STRING())));
        Set<FieldRef> indexFields = Collections.singleton(new FieldRef(0, "a", DataTypes.STRING()));
        FileIndexFilterFallbackPredicateVisitor visitor =
                new FileIndexFilterFallbackPredicateVisitor(indexFields);

        Predicate p1 = new PredicateBuilder(rowType).equal(0, "a");
        Optional<Predicate> r1 = visitor.visit((LeafPredicate) p1);
        assertThat(r1.isPresent()).isFalse();

        Predicate p2 = new PredicateBuilder(rowType).equal(1, "b");
        Optional<Predicate> r2 = visitor.visit((LeafPredicate) p2);
        assertThat(r2).isPresent();
        assertThat(r2.get()).isEqualTo(p2);
    }

    @Test
    public void testVisitCompoundPredicate() {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", DataTypes.INT()),
                                new DataField(1, "b", DataTypes.STRING()),
                                new DataField(2, "c", DataTypes.INT())));

        Predicate predicate =
                PredicateBuilder.and(
                        new PredicateBuilder(rowType).greaterThan(0, 1),
                        new PredicateBuilder(rowType).equal(1, "b"),
                        new PredicateBuilder(rowType).lessThan(2, 2));

        Set<FieldRef> indexFields =
                new HashSet<>(
                        Arrays.asList(
                                new FieldRef(0, "a", DataTypes.INT()),
                                new FieldRef(2, "c", DataTypes.INT())));
        FileIndexFilterFallbackPredicateVisitor v1 =
                new FileIndexFilterFallbackPredicateVisitor(indexFields);
        Optional<Predicate> r1 = v1.visit((CompoundPredicate) predicate);
        assertThat(r1).isPresent();
        assertThat(r1.get()).isEqualTo(new PredicateBuilder(rowType).equal(1, "b"));

        FileIndexFilterFallbackPredicateVisitor v2 =
                new FileIndexFilterFallbackPredicateVisitor(
                        new HashSet<>(
                                Collections.singletonList(new FieldRef(0, "a", DataTypes.INT()))));
        Optional<Predicate> r2 = v2.visit((CompoundPredicate) predicate);
        assertThat(r2).isPresent();
        assertThat(r2.get())
                .isEqualTo(
                        PredicateBuilder.and(
                                new PredicateBuilder(rowType).equal(1, "b"),
                                new PredicateBuilder(rowType).lessThan(2, 2)));

        FileIndexFilterFallbackPredicateVisitor v3 =
                new FileIndexFilterFallbackPredicateVisitor(
                        new HashSet<>(
                                Arrays.asList(
                                        new FieldRef(0, "a", DataTypes.INT()),
                                        new FieldRef(1, "b", DataTypes.STRING()),
                                        new FieldRef(2, "c", DataTypes.INT()))));
        Optional<Predicate> r3 = v3.visit((CompoundPredicate) predicate);
        assertThat(r3.isPresent()).isFalse();
    }
}
