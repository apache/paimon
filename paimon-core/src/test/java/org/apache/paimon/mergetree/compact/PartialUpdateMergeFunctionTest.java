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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Projection;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link PartialUpdateMergeFunction}. */
public class PartialUpdateMergeFunctionTest {

    private long sequence = 0;

    @Test
    public void testUpdateNonNull() {
        Options options = new Options();
        RowType rowType =
                RowType.of(
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT());
        MergeFunction<KeyValue> func =
                PartialUpdateMergeFunction.factory(options, rowType).create();
        func.reset();
        add(func, 1, 1, 1, 1, 1, 1, 1);
        add(func, 1, 2, 2, 2, 2, 2, null);
        validate(func, 1, 2, 2, 2, 2, 2, 1);
    }

    @Test
    public void testSequenceGroup() {
        Options options = new Options();
        options.set("fields.f3.sequence-group", "f1,f2");
        options.set("fields.f6.sequence-group", "f4,f5");
        RowType rowType =
                RowType.of(
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT());
        MergeFunction<KeyValue> func =
                PartialUpdateMergeFunction.factory(options, rowType).create();
        func.reset();
        add(func, 1, 1, 1, 1, 1, 1, 1);
        add(func, 1, 2, 2, 2, 2, 2, null);
        validate(func, 1, 2, 2, 2, 1, 1, 1);
        add(func, 1, 3, 3, 1, 3, 3, 3);
        validate(func, 1, 2, 2, 2, 3, 3, 3);

        // delete
        add(func, RowKind.DELETE, 1, 1, 1, 3, 1, 1, null);
        validate(func, 1, null, null, 3, 3, 3, 3);
        add(func, RowKind.DELETE, 1, 1, 1, 3, 1, 1, 4);
        validate(func, 1, null, null, 3, null, null, 4);
        add(func, 1, 4, 4, 4, 5, 5, 5);
        validate(func, 1, 4, 4, 4, 5, 5, 5);
        add(func, RowKind.DELETE, 1, 1, 1, 6, 1, 1, 6);
        validate(func, 1, null, null, 6, null, null, 6);
    }

    @Test
    public void testSequenceGroupRepeatDefine() {
        Options options = new Options();
        options.set("fields.f3.sequence-group", "f1,f2");
        options.set("fields.f4.sequence-group", "f1,f2");
        RowType rowType =
                RowType.of(
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT());
        assertThatThrownBy(() -> PartialUpdateMergeFunction.factory(options, rowType))
                .hasMessageContaining("is defined repeatedly by multiple groups");
    }

    @Test
    public void testSequenceGroupRepeatDefineNoField() {
        Options options = new Options();
        options.set("fields.f3.sequence-group", "f1,f2,f6");
        options.set("fields.f5.sequence-group", "f4,f6");
        RowType rowType =
                RowType.of(
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT());
        MergeFunction<KeyValue> func =
                PartialUpdateMergeFunction.factory(options, rowType).create();
        func.reset();
        add(func, 1, 1, 1, 1, 1, 1);
        add(func, 1, 2, 2, 2, 2, null);
        validate(func, 1, 2, 2, 2, 1, 1);
        add(func, 1, 3, 3, 1, 3, 3);
        validate(func, 1, 2, 2, 2, 3, 3);

        // delete
        add(func, RowKind.DELETE, 1, 1, 1, 3, 1, null);
        validate(func, 1, null, null, 3, 3, 3);
    }

    @Test
    public void testAdjustProjectionRepeatProject() {
        Options options = new Options();
        options.set("fields.f4.sequence-group", "f1,f3");
        options.set("fields.f5.sequence-group", "f7");
        RowType rowType =
                RowType.of(
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT());
        // the field 'f1' is projected twice
        int[][] projection = new int[][] {{1}, {1}, {3}, {7}};
        MergeFunctionFactory<KeyValue> factory =
                PartialUpdateMergeFunction.factory(options, rowType);
        MergeFunctionFactory.AdjustedProjection adjustedProjection =
                factory.adjustProjection(projection);

        validate(adjustedProjection, new int[] {1, 1, 3, 7, 4, 5}, new int[] {0, 1, 2, 3});

        MergeFunction<KeyValue> func = factory.create(adjustedProjection.pushdownProjection);
        func.reset();
        add(func, 1, 1, 1, 1, 1, 1);
        add(func, 2, 2, 6, 2, 2, 2);
        validate(func, 2, 2, 6, 2, 2, 2);

        // enable field updated by null
        add(func, 3, 3, null, 7, 4, null);
        validate(func, 3, 3, null, 2, 4, 2);
    }

    @Test
    public void testAdjustProjectionSequenceFieldsProject() {
        Options options = new Options();
        options.set("fields.f4.sequence-group", "f1,f3");
        options.set("fields.f5.sequence-group", "f7");
        RowType rowType =
                RowType.of(
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT());
        // the sequence field 'f4' is projected too
        int[][] projection = new int[][] {{1}, {4}, {3}, {7}};
        MergeFunctionFactory<KeyValue> factory =
                PartialUpdateMergeFunction.factory(options, rowType);
        MergeFunctionFactory.AdjustedProjection adjustedProjection =
                factory.adjustProjection(projection);

        validate(adjustedProjection, new int[] {1, 4, 3, 7, 5}, new int[] {0, 1, 2, 3});

        MergeFunction<KeyValue> func = factory.create(adjustedProjection.pushdownProjection);
        func.reset();
        // if sequence field is null, the related fields should not be updated
        add(func, 1, 1, 1, 1, 1);
        add(func, 1, null, 1, 2, 2);
        validate(func, 1, 1, 1, 2, 2);
    }

    @Test
    public void testAdjustProjectionAllFieldsProject() {
        Options options = new Options();
        options.set("fields.f4.sequence-group", "f1,f3");
        options.set("fields.f5.sequence-group", "f7");
        RowType rowType =
                RowType.of(
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT());
        // all fields are projected
        int[][] projection = new int[][] {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}};
        MergeFunctionFactory<KeyValue> factory =
                PartialUpdateMergeFunction.factory(options, rowType);
        MergeFunctionFactory.AdjustedProjection adjustedProjection =
                factory.adjustProjection(projection);

        validate(
                adjustedProjection,
                new int[] {0, 1, 2, 3, 4, 5, 6, 7},
                new int[] {0, 1, 2, 3, 4, 5, 6, 7});

        MergeFunction<KeyValue> func = factory.create(adjustedProjection.pushdownProjection);
        func.reset();
        // 'f6' has no sequence group, it should not be updated by null
        add(func, 1, 1, 1, 1, 1, 1, 1, 1);
        add(func, 4, 2, 4, 2, 2, 0, null, 3);
        validate(func, 4, 2, 4, 2, 2, 1, 1, 1);
    }

    @Test
    public void testAdjustProjectionNonProject() {
        Options options = new Options();
        options.set("fields.f4.sequence-group", "f1,f3");
        options.set("fields.f5.sequence-group", "f7");
        RowType rowType =
                RowType.of(
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT());
        // set the projection = null
        MergeFunctionFactory<KeyValue> factory =
                PartialUpdateMergeFunction.factory(options, rowType);
        MergeFunctionFactory.AdjustedProjection adjustedProjection = factory.adjustProjection(null);

        validate(adjustedProjection, null, null);

        MergeFunction<KeyValue> func = factory.create(adjustedProjection.pushdownProjection);
        func.reset();
        // Setting projection with null is similar with projecting all fields
        add(func, 1, 1, 1, 1, 1, 1, 1, 1);
        add(func, 4, 2, 4, 2, 2, 0, null, 3);
        validate(func, 4, 2, 4, 2, 2, 1, 1, 1);
    }

    @Test
    public void testAdjustProjectionNoSequenceGroup() {
        Options options = new Options();
        RowType rowType =
                RowType.of(
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT());
        int[][] projection = new int[][] {{0}, {1}, {3}, {4}, {7}};
        MergeFunctionFactory<KeyValue> factory =
                PartialUpdateMergeFunction.factory(options, rowType);
        MergeFunctionFactory.AdjustedProjection adjustedProjection =
                factory.adjustProjection(projection);

        validate(adjustedProjection, new int[] {0, 1, 3, 4, 7}, null);

        MergeFunction<KeyValue> func = factory.create(adjustedProjection.pushdownProjection);
        func.reset();
        // Without sequence group, all the fields should not be updated by null
        add(func, 1, 1, 1, 1, 1);
        add(func, 3, 3, null, 3, 3);
        validate(func, 3, 3, 1, 3, 3);
        add(func, 2, 2, 2, 2, 2);
        validate(func, 2, 2, 2, 2, 2);
    }

    @Test
    public void testAdjustProjectionCreateDirectly() {
        Options options = new Options();
        options.set("fields.f4.sequence-group", "f1,f3");
        options.set("fields.f5.sequence-group", "f7");
        RowType rowType =
                RowType.of(
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT());
        int[][] projection = new int[][] {{1}, {7}};
        assertThatThrownBy(
                        () ->
                                PartialUpdateMergeFunction.factory(options, rowType)
                                        .create(projection))
                .hasMessageContaining("Can not find new sequence field for new field.");
    }

    private void add(MergeFunction<KeyValue> function, Integer... f) {
        add(function, RowKind.INSERT, f);
    }

    private void add(MergeFunction<KeyValue> function, RowKind rowKind, Integer... f) {
        function.add(
                new KeyValue().replace(GenericRow.of(1), sequence++, rowKind, GenericRow.of(f)));
    }

    private void validate(MergeFunction<KeyValue> function, Integer... f) {
        assertThat(function.getResult().value()).isEqualTo(GenericRow.of(f));
    }

    private void validate(
            MergeFunctionFactory.AdjustedProjection projection, int[] pushdown, int[] outer) {
        if (projection.pushdownProjection == null) {
            assertThat(pushdown).isNull();
        } else {
            assertThat(pushdown)
                    .containsExactly(
                            Projection.of(projection.pushdownProjection).toTopLevelIndexes());
        }

        if (projection.outerProjection == null) {
            assertThat(outer).isNull();
        } else {
            assertThat(outer)
                    .containsExactly(Projection.of(projection.outerProjection).toTopLevelIndexes());
        }
    }
}
