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

package org.apache.paimon.globalindex;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.UpperTransform;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ScalarIndexedFieldsVisitor}. */
public class ScalarIndexedFieldsVisitorTest {

    private static final RowType ROW_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "id", DataTypes.INT()),
                            new DataField(1, "name", DataTypes.STRING()),
                            new DataField(2, "score", DataTypes.INT())));

    private static final PredicateBuilder BUILDER = new PredicateBuilder(ROW_TYPE);

    private static final ScalarIndexedFieldsVisitor VISITOR =
            new ScalarIndexedFieldsVisitor(new HashSet<>(Arrays.asList("id", "score")));

    @Test
    public void testSupportedPredicates() {
        assertThat(BUILDER.equal(0, 10).visit(VISITOR)).isTrue();
        assertThat(BUILDER.in(0, Arrays.asList(1, 2, 3)).visit(VISITOR)).isTrue();
        assertThat(BUILDER.between(2, 90, 100).visit(VISITOR)).isTrue();
        assertThat(BUILDER.isNull(0).visit(VISITOR)).isTrue();
    }

    @Test
    public void testCompoundPredicates() {
        Predicate andPredicate =
                PredicateBuilder.and(BUILDER.equal(0, 10), BUILDER.between(2, 90, 100));
        Predicate orPredicate =
                PredicateBuilder.or(
                        BUILDER.in(0, Arrays.asList(1, 2, 3)),
                        BUILDER.isNull(2));

        assertThat(andPredicate.visit(VISITOR)).isTrue();
        assertThat(orPredicate.visit(VISITOR)).isTrue();
    }

    @Test
    public void testUnsupportedPredicates() {
        assertThat(BUILDER.greaterThan(0, 10).visit(VISITOR)).isFalse();
        assertThat(BUILDER.equal(1, BinaryString.fromString("name_10")).visit(VISITOR)).isFalse();
    }

    @Test
    public void testNonFieldPredicate() {
        LeafPredicate upperNameEquals =
                LeafPredicate.of(
                        new UpperTransform(
                                Collections.singletonList(
                                        new FieldRef(1, "name", DataTypes.STRING()))),
                        Equal.INSTANCE,
                        Collections.singletonList(BinaryString.fromString("NAME_10")));

        assertThat(upperNameEquals.visit(VISITOR)).isFalse();
    }
}
