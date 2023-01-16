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

package org.apache.flink.table.store.file.mergetree;

import org.apache.flink.table.store.codegen.RecordComparator;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.memory.HeapMemorySegmentPool;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateMergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunctionTestUtils;
import org.apache.flink.table.store.file.mergetree.compact.ValueCountMergeFunction;
import org.apache.flink.table.store.file.sort.BinaryInMemorySortBuffer;
import org.apache.flink.table.store.file.utils.ReusingKeyValue;
import org.apache.flink.table.store.file.utils.ReusingTestData;
import org.apache.flink.table.store.types.BigIntType;
import org.apache.flink.table.store.types.DataField;
import org.apache.flink.table.store.types.IntType;
import org.apache.flink.table.store.types.RowType;

import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkState;
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
                    new HeapMemorySegmentPool(32 * 1024 * 3L, 32 * 1024),
                    false,
                    128,
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
                kv -> expected.poll().assertEquals(kv));
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

    /** Test for {@link SortBufferWriteBuffer} with {@link ValueCountMergeFunction}. */
    public static class WithValueCountMergeFunctionTest extends SortBufferWriteBufferTestBase {

        @Override
        protected boolean addOnly() {
            return true;
        }

        @Override
        protected List<ReusingTestData> getExpected(List<ReusingTestData> input) {
            return MergeFunctionTestUtils.getExpectedForValueCount(input);
        }

        @Override
        protected MergeFunction<KeyValue> createMergeFunction() {
            return ValueCountMergeFunction.factory().create();
        }

        @Test
        public void testCancelingRecords() throws IOException {
            runTest(
                    ReusingTestData.parse(
                            "1, 1, +, 100 | 3, 5, +, -300 | 5, 300, +, 300 | "
                                    + "1, 4, +, -200 | 3, 3, +, 300 | "
                                    + "5, 100, +, -200 | 7, 123, +, -500 | "
                                    + "7, 321, +, 200 | "
                                    + "7, 456, +, 300"));
            table.clear();
            runTest(ReusingTestData.parse("1, 2, +, 100 | 1, 1, +, -100"));
        }
    }
}
