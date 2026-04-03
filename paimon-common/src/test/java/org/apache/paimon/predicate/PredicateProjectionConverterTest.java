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

import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PredicateProjectionConverter}. */
public class PredicateProjectionConverterTest {

    @Test
    public void testLeafPredicateProjection() {
        // Table: (pt, a, b), project to partition (pt)
        RowType tableType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "pt", DataTypes.INT()),
                        DataTypes.FIELD(1, "a", DataTypes.INT()),
                        DataTypes.FIELD(2, "b", DataTypes.INT()));
        PredicateBuilder builder = new PredicateBuilder(tableType);

        // pt = 1 => should project to pt = 1 (index 0)
        Predicate leaf = builder.equal(0, 1);
        PredicateProjectionConverter converter =
                PredicateProjectionConverter.fromProjection(new int[] {0});

        Optional<Predicate> result = leaf.visit(converter);
        assertThat(result).isPresent();

        // Verify the projected predicate
        RowType partitionType = DataTypes.ROW(DataTypes.FIELD(0, "pt", DataTypes.INT()));
        PredicateBuilder partitionBuilder = new PredicateBuilder(partitionType);
        Predicate expected = partitionBuilder.equal(0, 1);
        assertThat(result.get()).isEqualTo(expected);
    }

    @Test
    public void testLeafPredicateNotProjectable() {
        // Table: (pt, a, b), project to partition (pt)
        RowType tableType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "pt", DataTypes.INT()),
                        DataTypes.FIELD(1, "a", DataTypes.INT()),
                        DataTypes.FIELD(2, "b", DataTypes.INT()));
        PredicateBuilder builder = new PredicateBuilder(tableType);

        // a = 5 => cannot project (field 'a' not in projection)
        Predicate leaf = builder.equal(1, 5);
        PredicateProjectionConverter converter =
                PredicateProjectionConverter.fromProjection(new int[] {0});

        Optional<Predicate> result = leaf.visit(converter);
        assertThat(result).isEmpty();
    }

    @Test
    public void testAndWithPartialProjection() {
        // Table: (pt, a, b), project to partition (pt)
        // Predicate: (pt = 1 AND a = 5) => should project to (pt = 1)
        // This tests the inclusive behavior: we drop non-projectable children from AND
        RowType tableType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "pt", DataTypes.INT()),
                        DataTypes.FIELD(1, "a", DataTypes.INT()),
                        DataTypes.FIELD(2, "b", DataTypes.INT()));
        PredicateBuilder builder = new PredicateBuilder(tableType);

        // (pt = 1 AND a = 5)
        Predicate and = PredicateBuilder.and(builder.equal(0, 1), builder.equal(1, 5));
        PredicateProjectionConverter converter =
                PredicateProjectionConverter.fromProjection(new int[] {0});

        Optional<Predicate> result = and.visit(converter);
        assertThat(result).isPresent();

        // Should project to (pt = 1) - dropping the non-projectable 'a = 5'
        RowType partitionType = DataTypes.ROW(DataTypes.FIELD(0, "pt", DataTypes.INT()));
        PredicateBuilder partitionBuilder = new PredicateBuilder(partitionType);
        Predicate expected = partitionBuilder.equal(0, 1);
        assertThat(result.get()).isEqualTo(expected);
    }

    @Test
    public void testAndWithAllNonProjectable() {
        // Table: (pt, a, b), project to partition (pt)
        // Predicate: (a = 5 AND b = 10) => should return empty (no projectable children)
        RowType tableType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "pt", DataTypes.INT()),
                        DataTypes.FIELD(1, "a", DataTypes.INT()),
                        DataTypes.FIELD(2, "b", DataTypes.INT()));
        PredicateBuilder builder = new PredicateBuilder(tableType);

        // (a = 5 AND b = 10)
        Predicate and = PredicateBuilder.and(builder.equal(1, 5), builder.equal(2, 10));
        PredicateProjectionConverter converter =
                PredicateProjectionConverter.fromProjection(new int[] {0});

        Optional<Predicate> result = and.visit(converter);
        assertThat(result).isEmpty();
    }

    @Test
    public void testOrWithAllProjectable() {
        // Table: (pt, a, b), project to partition (pt)
        // Predicate: (pt = 1 OR pt = 2) => should project to (pt = 1 OR pt = 2)
        RowType tableType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "pt", DataTypes.INT()),
                        DataTypes.FIELD(1, "a", DataTypes.INT()),
                        DataTypes.FIELD(2, "b", DataTypes.INT()));
        PredicateBuilder builder = new PredicateBuilder(tableType);

        // (pt = 1 OR pt = 2)
        Predicate or = PredicateBuilder.or(builder.equal(0, 1), builder.equal(0, 2));
        PredicateProjectionConverter converter =
                PredicateProjectionConverter.fromProjection(new int[] {0});

        Optional<Predicate> result = or.visit(converter);
        assertThat(result).isPresent();

        // Verify the structure is preserved
        assertThat(result.get()).isInstanceOf(CompoundPredicate.class);
        CompoundPredicate compound = (CompoundPredicate) result.get();
        assertThat(compound.function()).isInstanceOf(Or.class);
        assertThat(compound.children()).hasSize(2);
    }

    @Test
    public void testOrWithPartialProjection() {
        // Table: (pt, a, b), project to partition (pt)
        // Predicate: (pt = 1 OR a = 5) => should return empty (OR requires all children
        // projectable)
        RowType tableType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "pt", DataTypes.INT()),
                        DataTypes.FIELD(1, "a", DataTypes.INT()),
                        DataTypes.FIELD(2, "b", DataTypes.INT()));
        PredicateBuilder builder = new PredicateBuilder(tableType);

        // (pt = 1 OR a = 5)
        Predicate or = PredicateBuilder.or(builder.equal(0, 1), builder.equal(1, 5));
        PredicateProjectionConverter converter =
                PredicateProjectionConverter.fromProjection(new int[] {0});

        Optional<Predicate> result = or.visit(converter);
        assertThat(result).isEmpty();
    }

    @Test
    public void testFromMapping() {
        // Test fromMapping factory method
        // fieldIdxMapping[originalIndex] = projectedIndex
        RowType tableType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "pt", DataTypes.INT()),
                        DataTypes.FIELD(1, "a", DataTypes.INT()),
                        DataTypes.FIELD(2, "b", DataTypes.INT()));
        PredicateBuilder builder = new PredicateBuilder(tableType);

        // Mapping: pt(0) -> 0, a(1) -> -1 (not projected), b(2) -> -1 (not projected)
        int[] fieldIdxMapping = new int[] {0, -1, -1};
        PredicateProjectionConverter converter =
                PredicateProjectionConverter.fromMapping(fieldIdxMapping);

        // pt = 1 should be projectable
        Predicate leaf = builder.equal(0, 1);
        Optional<Predicate> result = leaf.visit(converter);
        assertThat(result).isPresent();

        // a = 5 should not be projectable
        Predicate leaf2 = builder.equal(1, 5);
        Optional<Predicate> result2 = leaf2.visit(converter);
        assertThat(result2).isEmpty();
    }

    @Test
    public void testNestedAndOr() {
        // Table: (pt1, pt2, a), project to partition (pt1, pt2)
        // Predicate: ((pt1 = 1 AND a = 5) OR (pt1 = 2 AND a = 8)) AND pt2 > 0
        // Should project to: ((pt1 = 1) OR (pt1 = 2)) AND pt2 > 0
        RowType tableType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "pt1", DataTypes.INT()),
                        DataTypes.FIELD(1, "pt2", DataTypes.INT()),
                        DataTypes.FIELD(2, "a", DataTypes.INT()));
        PredicateBuilder builder = new PredicateBuilder(tableType);

        // (pt1 = 1 AND a = 5) OR (pt1 = 2 AND a = 8)
        Predicate orPart =
                PredicateBuilder.or(
                        PredicateBuilder.and(builder.equal(0, 1), builder.equal(2, 5)),
                        PredicateBuilder.and(builder.equal(0, 2), builder.equal(2, 8)));

        // ((pt1 = 1 AND a = 5) OR (pt1 = 2 AND a = 8)) AND pt2 > 0
        Predicate full = PredicateBuilder.and(orPart, builder.greaterThan(1, 0));

        // Project to (pt1, pt2)
        PredicateProjectionConverter converter =
                PredicateProjectionConverter.fromProjection(new int[] {0, 1});

        Optional<Predicate> result = full.visit(converter);
        assertThat(result).isPresent();

        // The result should be: ((pt1 = 1) OR (pt1 = 2)) AND pt2 > 0
        assertThat(result.get()).isInstanceOf(CompoundPredicate.class);
        CompoundPredicate and = (CompoundPredicate) result.get();
        assertThat(and.function()).isInstanceOf(And.class);
        assertThat(and.children()).hasSize(2);
    }

    @Test
    public void testSingleChildAndOptimization() {
        // When AND has only one projectable child, return that child directly
        RowType tableType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "pt", DataTypes.INT()),
                        DataTypes.FIELD(1, "a", DataTypes.INT()),
                        DataTypes.FIELD(2, "b", DataTypes.INT()));
        PredicateBuilder builder = new PredicateBuilder(tableType);

        // (pt = 1 AND a = 5 AND b = 10) => should project to just (pt = 1)
        Predicate and =
                PredicateBuilder.and(
                        builder.equal(0, 1), builder.equal(1, 5), builder.equal(2, 10));
        PredicateProjectionConverter converter =
                PredicateProjectionConverter.fromProjection(new int[] {0});

        Optional<Predicate> result = and.visit(converter);
        assertThat(result).isPresent();

        // Should return a single LeafPredicate, not a CompoundPredicate
        assertThat(result.get()).isInstanceOf(LeafPredicate.class);
    }
}
