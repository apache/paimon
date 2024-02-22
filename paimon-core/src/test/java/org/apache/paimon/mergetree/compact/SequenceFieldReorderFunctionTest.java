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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Projection;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SequenceFieldReorderFunction}. */
public class SequenceFieldReorderFunctionTest {

    private long sequence = 0;
    private ReorderFunction<KeyValue> reorderFunction;
    private RowType keyType;
    private RowType valueType;
    private CoreOptions options;

    @BeforeEach
    public void createReorderFunction() {
        Options conf = new Options();
        conf.set(CoreOptions.SEQUENCE_FIELD, "c");
        options = new CoreOptions(conf);

        keyType = RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"PK_a"});
        valueType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT()
                        },
                        new String[] {"a", "b", "c", "d", "e"});
    }

    @Test
    public void testAddAndResult() {
        reorderFunction =
                SequenceFieldReorderFunction.factory(keyType, valueType, options).create();
        reorderFunction.reset();
        add(reorderFunction, RowKind.INSERT, 1, 1, 58, 10, 1);
        add(reorderFunction, RowKind.DELETE, 1, 1, 58, 10, 1);
        add(reorderFunction, RowKind.INSERT, 1, 1, 76, 100, 1);
        add(reorderFunction, RowKind.INSERT, 1, 1, 7, 1, 1);
        add(reorderFunction, RowKind.INSERT, 1, 1, null, 1, 1);
        add(reorderFunction, RowKind.INSERT, 1, 1, 40, 21, 1);
        validate(
                reorderFunction,
                createInternalRowSerializer(valueType),
                createKeyValue(RowKind.INSERT, 1, 1, 7, 1, 1),
                createKeyValue(RowKind.INSERT, 1, 1, 40, 21, 1),
                createKeyValue(RowKind.INSERT, 1, 1, 58, 10, 1),
                createKeyValue(RowKind.DELETE, 1, 1, 58, 10, 1),
                createKeyValue(RowKind.INSERT, 1, 1, null, 1, 1),
                createKeyValue(RowKind.INSERT, 1, 1, 76, 100, 1));
    }

    @Test
    public void testSeqFieldSameValue() {
        reorderFunction =
                SequenceFieldReorderFunction.factory(keyType, valueType, options).create();
        reorderFunction.reset();
        add(reorderFunction, RowKind.INSERT, 1, 1, 1, 10, 1);
        add(reorderFunction, RowKind.DELETE, 1, 1, 1, 10, 1);
        add(reorderFunction, RowKind.INSERT, 1, 1, 1, 100, 1);
        validate(
                reorderFunction,
                createInternalRowSerializer(valueType),
                createKeyValue(RowKind.INSERT, 1, 1, 1, 10, 1),
                createKeyValue(RowKind.DELETE, 1, 1, 1, 10, 1),
                createKeyValue(RowKind.INSERT, 1, 1, 1, 100, 1));
    }

    @Test
    public void testSeqFieldNullValue() {
        reorderFunction =
                SequenceFieldReorderFunction.factory(keyType, valueType, options).create();
        reorderFunction.reset();
        add(reorderFunction, RowKind.INSERT, 1, 1, null, 10, 1);
        add(reorderFunction, RowKind.DELETE, 1, 1, null, 10, 1);
        add(reorderFunction, RowKind.INSERT, 1, 1, null, 100, 1);
        validate(
                reorderFunction,
                createInternalRowSerializer(valueType),
                createKeyValue(RowKind.INSERT, 1, 1, null, 10, 1),
                createKeyValue(RowKind.DELETE, 1, 1, null, 10, 1),
                createKeyValue(RowKind.INSERT, 1, 1, null, 100, 1));
    }

    @Test
    public void testAdjustProjectionRepeatProject() {
        int[][] projection = new int[][] {{1}, {1}, {3}};
        MergeFunctionFactory.AdjustedProjection adjustProjection =
                SequenceFieldReorderFunction.factory(keyType, valueType, options)
                        .adjustProjection(projection);

        validate(adjustProjection, new int[] {1, 1, 3, 2}, new int[] {0, 1, 2});

        reorderFunction =
                SequenceFieldReorderFunction.factory(keyType, valueType, options)
                        .create(adjustProjection.pushdownProjection);
        reorderFunction.reset();
        add(reorderFunction, RowKind.INSERT, 1, 1, 10, 58);
        add(reorderFunction, RowKind.DELETE, 1, 1, 10, 58);
        add(reorderFunction, RowKind.INSERT, 1, 1, 100, 76);
        add(reorderFunction, RowKind.INSERT, 1, 1, 1, 7);
        add(reorderFunction, RowKind.INSERT, 1, 1, 1, null);
        add(reorderFunction, RowKind.INSERT, 1, 1, 21, 40);
        validate(
                reorderFunction,
                createInternalRowSerializer(Projection.of(projection).project(valueType)),
                createKeyValue(RowKind.INSERT, 1, 1, 1, 7),
                createKeyValue(RowKind.INSERT, 1, 1, 21, 40),
                createKeyValue(RowKind.INSERT, 1, 1, 10, 58),
                createKeyValue(RowKind.DELETE, 1, 1, 10, 58),
                createKeyValue(RowKind.INSERT, 1, 1, 1, null),
                createKeyValue(RowKind.INSERT, 1, 1, 100, 76));
    }

    @Test
    public void testAdjustProjectionSequenceFieldsProject() {
        int[][] projection = new int[][] {{1}, {3}, {2}};
        MergeFunctionFactory.AdjustedProjection adjustProjection =
                SequenceFieldReorderFunction.factory(keyType, valueType, options)
                        .adjustProjection(projection);

        validate(adjustProjection, new int[] {1, 3, 2}, new int[] {0, 1, 2});

        reorderFunction =
                SequenceFieldReorderFunction.factory(keyType, valueType, options)
                        .create(adjustProjection.pushdownProjection);
        reorderFunction.reset();
        add(reorderFunction, RowKind.INSERT, 1, 10, 58);
        add(reorderFunction, RowKind.DELETE, 1, 10, 58);
        add(reorderFunction, RowKind.INSERT, 1, 100, 76);
        add(reorderFunction, RowKind.INSERT, 1, 1, 7);
        add(reorderFunction, RowKind.INSERT, 1, 1, null);
        add(reorderFunction, RowKind.INSERT, 1, 21, 40);
        validate(
                reorderFunction,
                createInternalRowSerializer(Projection.of(projection).project(valueType)),
                createKeyValue(RowKind.INSERT, 1, 1, 7),
                createKeyValue(RowKind.INSERT, 1, 21, 40),
                createKeyValue(RowKind.INSERT, 1, 10, 58),
                createKeyValue(RowKind.DELETE, 1, 10, 58),
                createKeyValue(RowKind.INSERT, 1, 1, null),
                createKeyValue(RowKind.INSERT, 1, 100, 76));
    }

    @Test
    public void testAdjustProjectionAllFieldsProject() {
        int[][] projection = new int[][] {{0}, {1}, {2}, {3}, {4}};
        MergeFunctionFactory.AdjustedProjection adjustProjection =
                SequenceFieldReorderFunction.factory(keyType, valueType, options)
                        .adjustProjection(projection);

        validate(adjustProjection, new int[] {0, 1, 2, 3, 4}, new int[] {0, 1, 2, 3, 4});

        reorderFunction =
                SequenceFieldReorderFunction.factory(keyType, valueType, options)
                        .create(adjustProjection.pushdownProjection);
        reorderFunction.reset();
        add(reorderFunction, RowKind.INSERT, 1, 1, 58, 10, 1);
        add(reorderFunction, RowKind.DELETE, 1, 1, 58, 10, 1);
        add(reorderFunction, RowKind.INSERT, 1, 1, 76, 100, 1);
        add(reorderFunction, RowKind.INSERT, 1, 1, 7, 1, 1);
        add(reorderFunction, RowKind.INSERT, 1, 1, null, 1, 1);
        add(reorderFunction, RowKind.INSERT, 1, 1, 40, 21, 1);
        validate(
                reorderFunction,
                createInternalRowSerializer(Projection.of(projection).project(valueType)),
                createKeyValue(RowKind.INSERT, 1, 1, 7, 1, 1),
                createKeyValue(RowKind.INSERT, 1, 1, 40, 21, 1),
                createKeyValue(RowKind.INSERT, 1, 1, 58, 10, 1),
                createKeyValue(RowKind.DELETE, 1, 1, 58, 10, 1),
                createKeyValue(RowKind.INSERT, 1, 1, null, 1, 1),
                createKeyValue(RowKind.INSERT, 1, 1, 76, 100, 1));
    }

    @Test
    public void testAdjustProjectionNonProject() {
        MergeFunctionFactory.AdjustedProjection adjustProjection =
                SequenceFieldReorderFunction.factory(keyType, valueType, options)
                        .adjustProjection(null);

        validate(adjustProjection, null, null);

        reorderFunction =
                SequenceFieldReorderFunction.factory(keyType, valueType, options)
                        .create(adjustProjection.pushdownProjection);
        reorderFunction.reset();
        add(reorderFunction, RowKind.INSERT, 1, 1, 58, 10, 1);
        add(reorderFunction, RowKind.DELETE, 1, 1, 58, 10, 1);
        add(reorderFunction, RowKind.INSERT, 1, 1, 76, 100, 1);
        add(reorderFunction, RowKind.INSERT, 1, 1, 7, 1, 1);
        add(reorderFunction, RowKind.INSERT, 1, 1, null, 1, 1);
        add(reorderFunction, RowKind.INSERT, 1, 1, 40, 21, 1);
        validate(
                reorderFunction,
                createInternalRowSerializer(valueType),
                createKeyValue(RowKind.INSERT, 1, 1, 7, 1, 1),
                createKeyValue(RowKind.INSERT, 1, 1, 40, 21, 1),
                createKeyValue(RowKind.INSERT, 1, 1, 58, 10, 1),
                createKeyValue(RowKind.DELETE, 1, 1, 58, 10, 1),
                createKeyValue(RowKind.INSERT, 1, 1, null, 1, 1),
                createKeyValue(RowKind.INSERT, 1, 1, 76, 100, 1));
    }

    private void validate(
            ReorderFunction<KeyValue> reorderFunction,
            InternalRowSerializer serializer,
            KeyValue... keyValues) {
        List<KeyValue> list = Arrays.asList(keyValues);
        for (int i = 0; i < list.size(); i++) {
            assert equals(reorderFunction.getResult().get(i), list.get(i), serializer);
        }
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

    private boolean equals(KeyValue kv1, KeyValue kv2, InternalRowSerializer serializer) {
        return kv1.valueKind() == kv2.valueKind()
                && serializer
                        .toBinaryRow(kv1.value())
                        .copy()
                        .equals(serializer.toBinaryRow(kv2.value()).copy());
    }

    private KeyValue createKeyValue(RowKind rowKind, Integer... value) {
        return new KeyValue().replaceValueKind(rowKind).replaceValue(GenericRow.of(value));
    }

    private void add(ReorderFunction<KeyValue> reorderFunction, RowKind rowKind, Integer... value) {
        reorderFunction.add(
                new KeyValue()
                        .replace(GenericRow.of(1), sequence++, rowKind, GenericRow.of(value)));
    }

    private InternalRowSerializer createInternalRowSerializer(RowType rowType) {
        return new InternalRowSerializer(rowType);
    }
}
