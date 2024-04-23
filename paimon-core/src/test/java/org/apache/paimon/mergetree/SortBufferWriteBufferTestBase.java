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

package org.apache.paimon.mergetree;

import org.apache.paimon.KeyValue;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.mergetree.compact.FirstRowMergeFunction;
import org.apache.paimon.mergetree.compact.LookupMergeFunction;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.mergetree.compact.MergeFunctionTestUtils;
import org.apache.paimon.mergetree.compact.PartialUpdateMergeFunction;
import org.apache.paimon.mergetree.compact.aggregate.AggregateMergeFunction;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.sort.BinaryInMemorySortBuffer;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ReusingKeyValue;
import org.apache.paimon.utils.ReusingTestData;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static org.apache.paimon.utils.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SortBufferWriteBuffer}. */
public abstract class SortBufferWriteBufferTestBase {

    private static final RecordComparator KEY_COMPARATOR =
            (a, b) -> Integer.compare(a.getInt(0), b.getInt(0));

    protected final SortBufferWriteBuffer table =
            new SortBufferWriteBuffer(
                    new RowType(Collections.singletonList(new DataField(0, "key", new IntType()))),
                    new RowType(
                            Collections.singletonList(new DataField(1, "value", new BigIntType()))),
                    null,
                    new HeapMemorySegmentPool(32 * 1024 * 3L, 32 * 1024),
                    false,
                    MemorySize.MAX_VALUE,
                    128,
                    "lz4",
                    null);

    protected abstract boolean addOnly();

    protected abstract List<ReusingTestData> getExpected(List<ReusingTestData> input);

    protected abstract MergeFunction<KeyValue> createMergeFunction();

    @Test
    public void testAndClear() throws IOException {
        testRandom(100);
        table.clear();
        checkState(
                ((BinaryInMemorySortBuffer) table.buffer()).getBufferSegmentCount() == 0,
                "The sort buffer is not empty");
        testRandom(200);
    }

    @Test
    public void testOverflow() throws IOException {
        int numEof = 0;
        try {
            testRandom(100000);
        } catch (EOFException e) {
            numEof++;
        }
        table.clear();
        try {
            testRandom(100000);
        } catch (EOFException e) {
            numEof++;
        }
        assertThat(numEof).isEqualTo(2);
    }

    private void testRandom(int numRecords) throws IOException {
        List<ReusingTestData> input = ReusingTestData.generateData(numRecords, addOnly());
        runTest(input);
    }

    protected void runTest(List<ReusingTestData> input) throws IOException {
        Queue<ReusingTestData> expected = new LinkedList<>(getExpected(input));
        prepareTable(input);
        table.forEach(
                KEY_COMPARATOR,
                createMergeFunction(),
                null,
                kv -> {
                    assertThat(expected.isEmpty()).isFalse();
                    expected.poll().assertEquals(kv);
                });
        assertThat(expected).isEmpty();
    }

    private void prepareTable(List<ReusingTestData> input) throws IOException {
        ReusingKeyValue reuse = new ReusingKeyValue();
        for (ReusingTestData data : input) {
            KeyValue keyValue = reuse.update(data);
            boolean success =
                    table.put(
                            keyValue.sequenceNumber(),
                            keyValue.valueKind(),
                            keyValue.key(),
                            keyValue.value());
            if (!success) {
                throw new EOFException();
            }
        }
        assertThat(table.size()).isEqualTo(input.size());
    }

    /** Test for {@link SortBufferWriteBuffer} with {@link DeduplicateMergeFunction}. */
    public static class WithDeduplicateMergeFunctionTest extends SortBufferWriteBufferTestBase {

        @Override
        protected boolean addOnly() {
            return false;
        }

        @Override
        protected List<ReusingTestData> getExpected(List<ReusingTestData> input) {
            return MergeFunctionTestUtils.getExpectedForDeduplicate(input);
        }

        @Override
        protected MergeFunction<KeyValue> createMergeFunction() {
            return DeduplicateMergeFunction.factory().create();
        }
    }

    /** Test for {@link SortBufferWriteBuffer} with {@link PartialUpdateMergeFunction}. */
    public static class WithPartialUpdateMergeFunctionTest extends SortBufferWriteBufferTestBase {

        @Override
        protected boolean addOnly() {
            return true;
        }

        @Override
        protected List<ReusingTestData> getExpected(List<ReusingTestData> input) {
            return MergeFunctionTestUtils.getExpectedForPartialUpdate(input);
        }

        @Override
        protected MergeFunction<KeyValue> createMergeFunction() {
            Options options = new Options();
            return PartialUpdateMergeFunction.factory(
                            options, RowType.of(DataTypes.BIGINT()), ImmutableList.of("key"))
                    .create();
        }
    }

    /** Test for {@link SortBufferWriteBuffer} with {@link AggregateMergeFunction}. */
    public static class WithAggMergeFunctionTest extends SortBufferWriteBufferTestBase {

        @Override
        protected boolean addOnly() {
            return false;
        }

        @Override
        protected List<ReusingTestData> getExpected(List<ReusingTestData> input) {
            return MergeFunctionTestUtils.getExpectedForAggSum(input);
        }

        @Override
        protected MergeFunction<KeyValue> createMergeFunction() {
            Options options = new Options();
            options.set("fields.value.aggregate-function", "sum");
            return AggregateMergeFunction.factory(
                            options,
                            Collections.singletonList("value"),
                            Collections.singletonList(DataTypes.BIGINT()),
                            Collections.emptyList())
                    .create();
        }
    }

    /** Test for {@link SortBufferWriteBuffer} with {@link LookupMergeFunction}. */
    public static class WithLookupFunctionTest extends SortBufferWriteBufferTestBase {

        @Override
        protected boolean addOnly() {
            return false;
        }

        @Override
        protected List<ReusingTestData> getExpected(List<ReusingTestData> input) {
            return MergeFunctionTestUtils.getExpectedForAggSum(input);
        }

        @Override
        protected MergeFunction<KeyValue> createMergeFunction() {
            Options options = new Options();
            options.set("fields.value.aggregate-function", "sum");
            MergeFunctionFactory<KeyValue> aggMergeFunction =
                    AggregateMergeFunction.factory(
                            options,
                            Collections.singletonList("value"),
                            Collections.singletonList(DataTypes.BIGINT()),
                            Collections.emptyList());
            return LookupMergeFunction.wrap(
                            aggMergeFunction,
                            RowType.of(DataTypes.INT()),
                            RowType.of(DataTypes.BIGINT()))
                    .create();
        }
    }

    /** Test for {@link SortBufferWriteBuffer} with {@link FirstRowMergeFunction}. */
    public static class WithFirstRowMergeFunctionTest extends SortBufferWriteBufferTestBase {

        @Override
        protected boolean addOnly() {
            return true;
        }

        @Override
        protected List<ReusingTestData> getExpected(List<ReusingTestData> input) {
            return MergeFunctionTestUtils.getExpectedForFirstRow(input);
        }

        @Override
        protected MergeFunction<KeyValue> createMergeFunction() {
            return FirstRowMergeFunction.factory(
                            new RowType(Lists.list(new DataField(0, "f0", new IntType()))),
                            new RowType(Lists.list(new DataField(1, "f1", new BigIntType()))))
                    .create();
        }
    }
}
