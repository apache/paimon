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

package org.apache.paimon.operation;

import org.apache.paimon.operation.DataEvolutionReadPlanner.DataEvolutionReadPlan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DataEvolutionReadPlanner} (pure, no-IO layout planning). */
class DataEvolutionReadPlannerTest {

    // read type: id INT, nest ROW<a INT, b STRING>
    private static final RowType READ_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "id", DataTypes.INT()),
                            new DataField(
                                    1,
                                    "nest",
                                    DataTypes.ROW(
                                            new DataField(2, "a", DataTypes.INT()),
                                            new DataField(3, "b", DataTypes.STRING())))));

    private static RowType nest(DataField... subFields) {
        return new RowType(
                Collections.singletonList(new DataField(1, "nest", DataTypes.ROW(subFields))));
    }

    @Test
    void testStructSplitAcrossFilesIsComposed() {
        // bunch0 (latest) provides nest.a; bunch1 provides id + nest.b
        RowType avail0 = nest(new DataField(2, "a", DataTypes.INT()));
        RowType avail1 =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.INT()),
                                new DataField(
                                        1,
                                        "nest",
                                        DataTypes.ROW(new DataField(3, "b", DataTypes.STRING())))));

        DataEvolutionReadPlan plan =
                new DataEvolutionReadPlanner(READ_TYPE, Arrays.asList(avail0, avail1)).plan();

        // id is taken whole from bunch1
        assertThat(plan.nested[0]).isNull();
        assertThat(plan.rowOffsets[0]).isEqualTo(1);
        // nest is composed across files (a from bunch0, b from bunch1)
        assertThat(plan.rowOffsets[1]).isEqualTo(-1);
        assertThat(plan.nested[1]).isNotNull();
        // each bunch only physically reads what it provides
        assertThat(plan.bunchReadFields.get(0)).hasSize(1); // nest<a>
        assertThat(plan.bunchReadFields.get(1)).hasSize(2); // id, nest<b>
    }

    @Test
    void testStructWholeFromSingleFile() {
        // bunch0 provides the whole nest; bunch1 provides id
        RowType avail0 =
                nest(
                        new DataField(2, "a", DataTypes.INT()),
                        new DataField(3, "b", DataTypes.STRING()));
        RowType avail1 =
                new RowType(Collections.singletonList(new DataField(0, "id", DataTypes.INT())));

        DataEvolutionReadPlan plan =
                new DataEvolutionReadPlanner(READ_TYPE, Arrays.asList(avail0, avail1)).plan();

        // nest is taken whole from bunch0, not composed
        assertThat(plan.nested[1]).isNull();
        assertThat(plan.rowOffsets[1]).isEqualTo(0);
        assertThat(plan.rowOffsets[0]).isEqualTo(1);
    }

    @Test
    void testSubFieldAbsentEverywhereStaysNullWhenNullable() {
        // only nest.a is provided anywhere; nest.b (nullable) is absent
        RowType avail0 = nest(new DataField(2, "a", DataTypes.INT()));
        RowType avail1 =
                new RowType(Collections.singletonList(new DataField(0, "id", DataTypes.INT())));

        DataEvolutionReadPlan plan =
                new DataEvolutionReadPlanner(READ_TYPE, Arrays.asList(avail0, avail1)).plan();

        // nest still composed (only a present), no exception since b is nullable
        assertThat(plan.nested[1]).isNotNull();
        assertThat(plan.bunchReadFields.get(0)).hasSize(1);
    }

    @Test
    void testDeeperThanOneLevelSplitThrows() {
        // read type: nest ROW<sub ROW<x INT, y INT>>; x and y provided by different files
        RowType readType =
                new RowType(
                        Collections.singletonList(
                                new DataField(
                                        1,
                                        "nest",
                                        DataTypes.ROW(
                                                new DataField(
                                                        4,
                                                        "sub",
                                                        DataTypes.ROW(
                                                                new DataField(
                                                                        5, "x", DataTypes.INT()),
                                                                new DataField(
                                                                        6,
                                                                        "y",
                                                                        DataTypes.INT())))))));
        RowType avail0 =
                new RowType(
                        Collections.singletonList(
                                new DataField(
                                        1,
                                        "nest",
                                        DataTypes.ROW(
                                                new DataField(
                                                        4,
                                                        "sub",
                                                        DataTypes.ROW(
                                                                new DataField(
                                                                        5,
                                                                        "x",
                                                                        DataTypes.INT())))))));
        RowType avail1 =
                new RowType(
                        Collections.singletonList(
                                new DataField(
                                        1,
                                        "nest",
                                        DataTypes.ROW(
                                                new DataField(
                                                        4,
                                                        "sub",
                                                        DataTypes.ROW(
                                                                new DataField(
                                                                        6,
                                                                        "y",
                                                                        DataTypes.INT())))))));

        assertThatThrownBy(
                        () ->
                                new DataEvolutionReadPlanner(
                                                readType, Arrays.asList(avail0, avail1))
                                        .plan())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testDeepAddColumnIsNullFilledInsteadOfRejected() {
        // read type: id INT, payload ROW<inner ROW<x INT, y INT>>
        // "y" was added later by ALTER TABLE ADD COLUMN payload.inner.y, so no file has it yet.
        RowType readType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.INT()),
                                new DataField(
                                        1,
                                        "payload",
                                        DataTypes.ROW(
                                                new DataField(
                                                        2,
                                                        "inner",
                                                        DataTypes.ROW(
                                                                new DataField(
                                                                        3, "x", DataTypes.INT()),
                                                                new DataField(
                                                                        4,
                                                                        "y",
                                                                        DataTypes.INT())))))));
        // bunch0 (latest): an unrelated partial update touching only the top-level "id"
        RowType avail0 =
                new RowType(Collections.singletonList(new DataField(0, "id", DataTypes.INT())));
        // bunch1: the original file, written before the deep ADD COLUMN
        RowType avail1 =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.INT()),
                                new DataField(
                                        1,
                                        "payload",
                                        DataTypes.ROW(
                                                new DataField(
                                                        2,
                                                        "inner",
                                                        DataTypes.ROW(
                                                                new DataField(
                                                                        3,
                                                                        "x",
                                                                        DataTypes.INT())))))));

        DataEvolutionReadPlan plan =
                new DataEvolutionReadPlanner(readType, Arrays.asList(avail0, avail1)).plan();

        // id comes whole from the latest partial file
        assertThat(plan.rowOffsets[0]).isEqualTo(0);
        // payload.inner is entirely provided by bunch1; the missing leaf "y" must be null-filled
        // by schema evolution rather than rejected as an unsupported deep split.
        assertThat(plan.bunchReadFields.get(1)).anySatisfy(f -> assertThat(f.id()).isEqualTo(1));
    }

    @Test
    void testProjectedAddedLeafUsesParentAsReaderAnchor() {
        // Only payload.y is projected. The old file predates y, but its payload field still has to
        // be read so schema evolution can null-fill y and the union reader keeps row cardinality.
        RowType readType =
                new RowType(
                        Collections.singletonList(
                                new DataField(
                                        1,
                                        "payload",
                                        new RowType(
                                                false,
                                                Collections.singletonList(
                                                        new DataField(3, "y", DataTypes.INT()))))));
        RowType unrelatedUpdate =
                new RowType(Collections.singletonList(new DataField(0, "id", DataTypes.INT())));
        RowType oldFile =
                new RowType(
                        Collections.singletonList(
                                new DataField(
                                        1,
                                        "payload",
                                        new RowType(
                                                false,
                                                Collections.singletonList(
                                                        new DataField(2, "x", DataTypes.INT()))))));

        DataEvolutionReadPlan plan =
                new DataEvolutionReadPlanner(readType, Arrays.asList(unrelatedUpdate, oldFile))
                        .plan();

        assertThat(plan.rowOffsets[0]).isEqualTo(-1);
        assertThat(plan.nested[0]).isNotNull();
        assertThat(plan.bunchReadFields.get(0)).isEmpty();
        assertThat(plan.bunchReadFields.get(1)).containsExactly(readType.getFields().get(0));
    }

    @Test
    void testProjectedAddedLeafUsesAllWinningSiblingProvidersAsAnchors() {
        RowType readType =
                rowType(
                        new DataField(
                                1, "payload", rowType(new DataField(5, "added", DataTypes.INT()))));
        RowType latestX =
                rowType(
                        new DataField(
                                1, "payload", rowType(new DataField(2, "x", DataTypes.INT()))));
        RowType latestZ =
                rowType(
                        new DataField(
                                1, "payload", rowType(new DataField(4, "z", DataTypes.INT()))));
        RowType staleX =
                rowType(
                        new DataField(
                                1, "payload", rowType(new DataField(2, "x", DataTypes.INT()))));

        DataEvolutionReadPlan plan =
                new DataEvolutionReadPlanner(readType, Arrays.asList(latestX, latestZ, staleX))
                        .plan();

        assertThat(plan.rowOffsets[0]).isEqualTo(-1);
        assertThat(plan.nested[0]).isNotNull();
        assertThat(plan.bunchReadFields.get(0)).containsExactly(readType.getFields().get(0));
        assertThat(plan.bunchReadFields.get(1)).containsExactly(readType.getFields().get(0));
        assertThat(plan.bunchReadFields.get(2)).isEmpty();
    }

    @Test
    void testProjectedExistingLeafUsesAllWinningSiblingProvidersAsAnchors() {
        RowType readType =
                rowType(
                        new DataField(
                                1, "payload", rowType(new DataField(2, "x", DataTypes.INT()))));
        RowType latestX =
                rowType(
                        new DataField(
                                1, "payload", rowType(new DataField(2, "x", DataTypes.INT()))));
        RowType latestZ =
                rowType(
                        new DataField(
                                1, "payload", rowType(new DataField(4, "z", DataTypes.INT()))));

        DataEvolutionReadPlan plan =
                new DataEvolutionReadPlanner(readType, Arrays.asList(latestX, latestZ)).plan();

        assertThat(plan.rowOffsets[0]).isEqualTo(-1);
        assertThat(plan.nested[0]).isNotNull();
        assertThat(plan.bunchReadFields.get(0)).containsExactly(readType.getFields().get(0));
        assertThat(plan.bunchReadFields.get(1)).containsExactly(readType.getFields().get(0));
    }

    @Test
    void testProjectedDeepAddedLeafUsesSiblingUnderSameParent() {
        DataField projectedSub =
                new DataField(2, "sub", rowType(new DataField(4, "added", DataTypes.INT())));
        RowType readType = rowType(new DataField(1, "payload", rowType(projectedSub)));
        RowType existingSub =
                rowType(
                        new DataField(
                                1,
                                "payload",
                                rowType(
                                        new DataField(
                                                2,
                                                "sub",
                                                rowType(
                                                        new DataField(
                                                                3,
                                                                "existing",
                                                                DataTypes.INT()))))));
        RowType otherSibling =
                rowType(
                        new DataField(
                                1,
                                "payload",
                                rowType(new DataField(5, "other", DataTypes.STRING()))));

        DataEvolutionReadPlan plan =
                new DataEvolutionReadPlanner(readType, Arrays.asList(existingSub, otherSibling))
                        .plan();

        assertThat(plan.rowOffsets[0]).isEqualTo(-1);
        assertThat(plan.nested[0]).isNotNull();
        RowType subProviderReadType = (RowType) plan.bunchReadFields.get(0).get(0).type();
        assertThat(subProviderReadType.getFields()).containsExactly(projectedSub);
        assertThat(plan.bunchReadFields.get(1)).containsExactly(readType.getFields().get(0));
    }

    private static RowType rowType(DataField field) {
        return new RowType(Collections.singletonList(field));
    }
}
