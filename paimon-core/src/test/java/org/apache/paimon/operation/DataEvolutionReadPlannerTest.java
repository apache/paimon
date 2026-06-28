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
}
