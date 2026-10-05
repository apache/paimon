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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.CompositeKeySerializer;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.globalindex.SortedIndexFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Typed interval planning and conservative manifest metadata pruning. */
class CompositeBTreePredicateTest {

    private final List<DataField> fields =
            Arrays.asList(
                    new DataField(10, "category", DataTypes.STRING()),
                    new DataField(20, "item_number", DataTypes.INT()));
    private final PredicateBuilder builder = new PredicateBuilder(new RowType(fields));
    private final BTreeGlobalIndexerFactory factory = new BTreeGlobalIndexerFactory();
    private final KeySerializer serializer = new CompositeKeySerializer(new RowType(fields));

    @Test
    void testMatchesInIndexOrderRegardlessOfPredicateOrder() {
        Predicate predicate =
                PredicateBuilder.and(
                        builder.equal(1, -3), builder.equal(0, BinaryString.fromString("a")));
        assertThat(
                        CompositeBTreePredicate.plan(fields, predicate)
                                .get()
                                .intervals()
                                .get(0)
                                .pointKey())
                .isEqualTo(GenericRow.of(BinaryString.fromString("a"), -3));
        assertThat(CompositeBTreePredicate.plan(fields, predicate).get().isEmpty()).isFalse();
        assertThat(
                        CompositeBTreePredicate.plan(
                                        Arrays.asList(fields.get(1), fields.get(0)), predicate)
                                .get()
                                .intervals()
                                .get(0)
                                .pointKey())
                .isEqualTo(GenericRow.of(-3, BinaryString.fromString("a")));
    }

    @Test
    void testRequiresLeadingConstraintAndMatchingTypes() {
        assertThat(CompositeBTreePredicate.plan(fields, builder.equal(1, 7))).isEmpty();
        assertThat(
                        CompositeBTreePredicate.plan(
                                fields,
                                PredicateBuilder.or(
                                        builder.equal(0, BinaryString.fromString("a")),
                                        builder.equal(1, 7))))
                .isEmpty();
        assertThat(
                        CompositeBTreePredicate.plan(
                                        fields,
                                        PredicateBuilder.and(
                                                builder.equal(0, BinaryString.fromString("a")),
                                                builder.greaterThan(1, 7)))
                                .get()
                                .boundColumns())
                .isEqualTo(2);
        PredicateBuilder incompatible =
                new PredicateBuilder(
                        new RowType(
                                Arrays.asList(
                                        new DataField(10, "category", DataTypes.INT()),
                                        fields.get(1))));
        assertThat(
                        CompositeBTreePredicate.plan(
                                fields,
                                PredicateBuilder.and(
                                        incompatible.equal(0, 1), incompatible.equal(1, 7))))
                .isEmpty();
    }

    @Test
    void testPrunesUsingTypedTupleEndpointsInclusively() {
        GlobalIndexIOMeta first = file("first", "a", -3, "a", 7);
        GlobalIndexIOMeta second = file("second", "a", 10, "b", 2);
        GlobalIndexIOMeta third = file("third", "b", 3, "c", 4);
        List<GlobalIndexIOMeta> files = Arrays.asList(first, second, third);
        assertThat(factory.selectFiles(fields, equal("a", -3), files)).containsExactly(first);
        assertThat(factory.selectFiles(fields, equal("a", 7), files)).containsExactly(first);
        assertThat(factory.selectFiles(fields, equal("a", 10), files)).containsExactly(second);
        assertThat(factory.selectFiles(fields, equal("b", 2), files)).containsExactly(second);
        assertThat(factory.selectFiles(fields, equal("b", 3), files)).containsExactly(third);
        assertThat(factory.selectFiles(fields, equal("a", 8), files)).isEmpty();
        assertThat(factory.selectFiles(fields, equal("z", 7), files)).isEmpty();
    }

    @Test
    void testContradictionsAndSqlNullEqualityPruneAllFiles() {
        List<GlobalIndexIOMeta> files = Collections.singletonList(file("first", "a", -3, "a", 7));
        for (Predicate predicate :
                Arrays.asList(
                        PredicateBuilder.and(equal("a", 7), builder.equal(1, 8)),
                        PredicateBuilder.and(
                                equal("a", 7), builder.equal(0, BinaryString.fromString("b"))),
                        PredicateBuilder.and(builder.equal(0, null), builder.equal(1, 7)),
                        PredicateBuilder.and(
                                builder.equal(0, BinaryString.fromString("a")),
                                builder.equal(1, null)))) {
            assertThat(CompositeBTreePredicate.plan(fields, predicate).get().isEmpty()).isTrue();
            assertThat(factory.selectFiles(fields, predicate, files)).isEmpty();
        }
        Predicate redundant = PredicateBuilder.and(equal("a", 7), builder.equal(1, 7));
        assertThat(CompositeBTreePredicate.plan(fields, redundant).get().isEmpty()).isFalse();
        assertThat(factory.selectFiles(fields, redundant, files)).containsExactlyElementsOf(files);
    }

    @Test
    void testRetainsFilesWhenLeadingConstraintOrMetadataIsUnavailable() {
        GlobalIndexIOMeta file = file("first", "a", -3, "a", 7);
        List<GlobalIndexIOMeta> files = Collections.singletonList(file);
        assertThat(factory.selectFiles(fields, builder.equal(1, 100), files)).isSameAs(files);
        assertThat(
                        factory.selectFiles(
                                fields,
                                PredicateBuilder.and(
                                        builder.equal(0, BinaryString.fromString("a")),
                                        builder.greaterThan(1, 100)),
                                files))
                .isEmpty();
        List<GlobalIndexIOMeta> unknown =
                Arrays.asList(file, new GlobalIndexIOMeta(new Path("unknown"), 1, null));
        assertThat(factory.selectFiles(fields, equal("z", 7), unknown)).isSameAs(unknown);
        assertThat(
                        factory.selectFiles(
                                fields,
                                PredicateBuilder.and(equal("a", 7), builder.equal(1, 8)),
                                unknown))
                .isEmpty();
    }

    @Test
    void testPrefixAndRangePruneTypedEndpoints() {
        GlobalIndexIOMeta first = file("first", "a", -3, "a", 7);
        GlobalIndexIOMeta second = file("second", "a", 10, "b", 2);
        GlobalIndexIOMeta third = file("third", "b", 3, "c", 4);
        List<GlobalIndexIOMeta> files = Arrays.asList(first, second, third);
        Predicate prefix = builder.equal(0, BinaryString.fromString("a"));
        assertThat(factory.selectFiles(fields, prefix, files)).containsExactly(first, second);
        assertThat(
                        factory.selectFiles(
                                fields,
                                PredicateBuilder.and(prefix, builder.greaterThan(1, 7)),
                                files))
                .containsExactly(second);
        assertThat(
                        factory.selectFiles(
                                fields,
                                PredicateBuilder.and(prefix, builder.greaterOrEqual(1, 7)),
                                files))
                .containsExactly(first, second);
        assertThat(
                        factory.selectFiles(
                                fields,
                                PredicateBuilder.and(prefix, builder.lessThan(1, -3)),
                                files))
                .isEmpty();
        assertThat(
                        factory.selectFiles(
                                fields,
                                PredicateBuilder.and(prefix, builder.lessOrEqual(1, -3)),
                                files))
                .containsExactly(first);
    }

    private Predicate equal(String category, int number) {
        return PredicateBuilder.and(
                builder.equal(1, number), builder.equal(0, BinaryString.fromString(category)));
    }

    @Test
    void testDiscreteDomainsIntersectDeduplicateAndBoundExpansion() {
        List<Object> categories = new ArrayList<>();
        List<Object> numbers = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            categories.add(BinaryString.fromString("category-" + i));
            numbers.add(i);
        }
        Predicate first = builder.in(0, categories);
        Predicate max = PredicateBuilder.and(first, builder.in(1, numbers));
        assertThat(CompositeBTreePredicate.plan(fields, max).get().intervals()).hasSize(256);
        numbers.add(null);
        numbers.add(7);
        assertThat(
                        CompositeBTreePredicate.plan(
                                        fields, PredicateBuilder.and(first, builder.in(1, numbers)))
                                .get()
                                .intervals())
                .hasSize(256);
        numbers.add(16);
        Predicate overflow = PredicateBuilder.and(first, builder.in(1, numbers));
        assertThat(CompositeBTreePredicate.plan(fields, overflow)).isEmpty();
        assertThat(
                        CompositeBTreePredicate.plan(
                                        fields, PredicateBuilder.and(overflow, builder.equal(1, 7)))
                                .get()
                                .intervals())
                .hasSize(16);
        assertThat(
                        CompositeBTreePredicate.plan(
                                        fields,
                                        PredicateBuilder.and(
                                                overflow, builder.in(1, Arrays.asList(7, 8))))
                                .get()
                                .intervals())
                .hasSize(32);
        assertThat(
                        CompositeBTreePredicate.plan(
                                        fields,
                                        PredicateBuilder.and(
                                                first,
                                                builder.in(
                                                        0,
                                                        Collections.singletonList(
                                                                BinaryString.fromString(
                                                                        "absent")))))
                                .get()
                                .isEmpty())
                .isTrue();
        List<DataField> nonNull =
                Arrays.asList(
                        new DataField(10, "category", DataTypes.STRING().notNull()), fields.get(1));
        assertThat(CompositeBTreePredicate.plan(nonNull, builder.isNull(0)).get().isEmpty())
                .isTrue();
    }

    @Test
    void testIntervalFileUnionCountsEachFileOnce() {
        GlobalIndexIOMeta first = file("first", "a", -3, "a", 7);
        GlobalIndexIOMeta second = file("second", "a", 10, "b", 2);
        GlobalIndexIOMeta third = file("third", "b", 3, "c", 4);
        Predicate query =
                PredicateBuilder.and(
                        builder.in(
                                0,
                                Arrays.asList(
                                        BinaryString.fromString("b"),
                                        BinaryString.fromString("a"),
                                        BinaryString.fromString("a"))),
                        builder.between(1, 0, 2));
        CompositeBTreePredicate.Plan plan = CompositeBTreePredicate.plan(fields, query).get();
        List<GlobalIndexIOMeta> selected = plan.selectFiles(Arrays.asList(first, second, third));
        assertThat(selected).containsExactly(first, second);
        assertThat(plan.canScan(selected, 2)).isTrue();
        assertThat(plan.canScan(selected, 1)).isFalse();
    }

    private GlobalIndexIOMeta file(
            String name, String first, int firstNumber, String last, int lastNumber) {
        byte[] metadata =
                new SortedIndexFileMeta(
                                serializer.serialize(
                                        GenericRow.of(BinaryString.fromString(first), firstNumber)),
                                serializer.serialize(
                                        GenericRow.of(BinaryString.fromString(last), lastNumber)),
                                false)
                        .serialize();
        return new GlobalIndexIOMeta(new Path(name), 1, metadata);
    }
}
