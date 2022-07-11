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

import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.memory.HeapMemorySegmentPool;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateMergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunctionTestUtils;
import org.apache.flink.table.store.file.mergetree.compact.ValueCountMergeFunction;
import org.apache.flink.table.store.file.utils.ReusingKeyValue;
import org.apache.flink.table.store.file.utils.ReusingTestData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SortBufferMemTable}. */
public abstract class SortBufferMemTableTestBase {

    private static final RecordComparator KEY_COMPARATOR =
            (a, b) -> Integer.compare(a.getInt(0), b.getInt(0));

    protected final SortBufferMemTable table =
            new SortBufferMemTable(
                    new RowType(
                            Collections.singletonList(new RowType.RowField("key", new IntType()))),
                    new RowType(
                            Collections.singletonList(
                                    new RowType.RowField("value", new BigIntType()))),
                    new HeapMemorySegmentPool(32 * 1024 * 3L, 32 * 1024),
                    null);

    protected abstract boolean addOnly();

    protected abstract List<ReusingTestData> getExpected(List<ReusingTestData> input);

    protected abstract MergeFunction createMergeFunction();

    @Test
    public void testAndClear() throws IOException {
        testRandom(100);
        table.clear();
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
        List<ReusingTestData> expected = getExpected(input);
        prepareTable(input);
        Iterator<KeyValue> actual = table.mergeIterator(KEY_COMPARATOR, createMergeFunction());

        Random rnd = new Random();
        for (ReusingTestData data : expected) {
            int r = rnd.nextInt(3) + 1;
            for (int i = 0; i < r; i++) {
                // test idempotence of this method
                assertThat(actual.hasNext()).isTrue();
            }
            KeyValue kv = actual.next();
            data.assertEquals(kv);
        }
        // test idempotence of this method
        int r = rnd.nextInt(3) + 1;
        for (int i = 0; i < r; i++) {
            assertThat(actual.hasNext()).isFalse();
            assertThat(actual.next()).isNull();
        }
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

    /** Test for {@link SortBufferMemTable} with {@link DeduplicateMergeFunction}. */
    public static class WithDeduplicateMergeFunctionTest extends SortBufferMemTableTestBase {

        @Override
        protected boolean addOnly() {
            return false;
        }

        @Override
        protected List<ReusingTestData> getExpected(List<ReusingTestData> input) {
            return MergeFunctionTestUtils.getExpectedForDeduplicate(input);
        }

        @Override
        protected MergeFunction createMergeFunction() {
            return new DeduplicateMergeFunction();
        }
    }

    /** Test for {@link SortBufferMemTable} with {@link ValueCountMergeFunction}. */
    public static class WithValueCountMergeFunctionTest extends SortBufferMemTableTestBase {

        @Override
        protected boolean addOnly() {
            return true;
        }

        @Override
        protected List<ReusingTestData> getExpected(List<ReusingTestData> input) {
            return MergeFunctionTestUtils.getExpectedForValueCount(input);
        }

        @Override
        protected MergeFunction createMergeFunction() {
            return new ValueCountMergeFunction();
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
