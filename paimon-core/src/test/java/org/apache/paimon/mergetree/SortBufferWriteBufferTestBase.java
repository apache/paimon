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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.compression.CompressOptions;
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
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ReusingKeyValue;
import org.apache.paimon.utils.ReusingTestData;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.utils.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SortBufferWriteBuffer}. */
public abstract class SortBufferWriteBufferTestBase {

    private static final RecordComparator KEY_COMPARATOR =
            (a, b) -> Integer.compare(a.getInt(0), b.getInt(0));

    protected final SortBufferWriteBuffer table =
            new SortBufferWriteBuffer(
                    RowType.builder().field("key_f0", DataTypes.INT()).build(),
                    RowType.builder()
                            .field("f0", DataTypes.INT())
                            .field("f1", DataTypes.BIGINT())
                            .build(),
                    null,
                    new HeapMemorySegmentPool(32 * 1024 * 3L, 32 * 1024),
                    false,
                    MemorySize.MAX_VALUE,
                    128,
                    CompressOptions.defaultOptions(),
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

        private final boolean addOnly;

        private WithPartialUpdateMergeFunctionTest() {
            this.addOnly = ThreadLocalRandom.current().nextBoolean();
        }

        @Override
        protected boolean addOnly() {
            return addOnly;
        }

        @Override
        protected List<ReusingTestData> getExpected(List<ReusingTestData> input) {
            return MergeFunctionTestUtils.getExpectedForPartialUpdate(input, addOnly);
        }

        @Override
        protected MergeFunction<KeyValue> createMergeFunction() {
            Options options = new Options();
            options.set(CoreOptions.IGNORE_DELETE, !addOnly);
            return PartialUpdateMergeFunction.factory(
                            options,
                            RowType.of(DataTypes.INT().notNull(), DataTypes.BIGINT()),
                            ImmutableList.of("f0"))
                    .create();
        }
    }

    /** Test for {@link SortBufferWriteBuffer} with {@link AggregateMergeFunction}. */
    public static class WithAggMergeFunctionTest extends SortBufferWriteBufferTestBase {

        private final boolean addOnly;
        private final boolean removeRecordOnDelete;

        private WithAggMergeFunctionTest() {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            this.addOnly = random.nextBoolean();
            this.removeRecordOnDelete = !addOnly && random.nextBoolean();
        }

        @Override
        protected boolean addOnly() {
            return addOnly;
        }

        @Override
        protected List<ReusingTestData> getExpected(List<ReusingTestData> input) {
            return MergeFunctionTestUtils.getExpectedForAggSum(
                    input, addOnly, removeRecordOnDelete);
        }

        @Override
        protected MergeFunction<KeyValue> createMergeFunction() {
            Options options = new Options();
            options.set("fields.f1.aggregate-function", "sum");
            options.set(CoreOptions.AGGREGATION_REMOVE_RECORD_ON_DELETE, removeRecordOnDelete);
            return AggregateMergeFunction.factory(
                            options,
                            Arrays.asList("f0", "f1"),
                            Arrays.asList(DataTypes.INT().notNull(), DataTypes.BIGINT()),
                            Collections.singletonList("f0"))
                    .create();
        }
    }

    /** Test for {@link SortBufferWriteBuffer} with {@link LookupMergeFunction}. */
    public static class WithLookupFunctionTest extends SortBufferWriteBufferTestBase {

        private final boolean addOnly;
        private final boolean removeRecordOnDelete;

        private WithLookupFunctionTest() {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            this.addOnly = random.nextBoolean();
            this.removeRecordOnDelete = !addOnly && random.nextBoolean();
        }

        @Override
        protected boolean addOnly() {
            return addOnly;
        }

        @Override
        protected List<ReusingTestData> getExpected(List<ReusingTestData> input) {
            return MergeFunctionTestUtils.getExpectedForAggSum(
                    input, addOnly, removeRecordOnDelete);
        }

        @Override
        protected MergeFunction<KeyValue> createMergeFunction() {
            Options options = new Options();
            options.set("fields.f1.aggregate-function", "sum");
            options.set(CoreOptions.AGGREGATION_REMOVE_RECORD_ON_DELETE, removeRecordOnDelete);
            MergeFunctionFactory<KeyValue> aggMergeFunction =
                    AggregateMergeFunction.factory(
                            options,
                            Arrays.asList("f0", "f1"),
                            Arrays.asList(DataTypes.INT().notNull(), DataTypes.BIGINT()),
                            Collections.singletonList("f0"));
            return LookupMergeFunction.wrap(aggMergeFunction, null, null, null).create();
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
            return FirstRowMergeFunction.factory(new Options()).create();
        }
    }
}
