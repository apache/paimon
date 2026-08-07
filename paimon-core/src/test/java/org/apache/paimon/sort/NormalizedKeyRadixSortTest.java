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

package org.apache.paimon.sort;

import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.MutableObjectIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link NormalizedKeyRadixSort}. */
class NormalizedKeyRadixSortTest {

    @TempDir Path tempDir;

    @Test
    void testFullyDeterminedKey() throws Exception {
        List<TestRecord> records = records(10_000, false, false);
        assertSorted(
                records, new int[] {0}, Comparator.comparingInt(record -> record.key), 32L << 20);
    }

    @Test
    void testPartialKeyAndNulls() throws Exception {
        List<TestRecord> records = records(10_000, true, true);
        assertSorted(records, new int[] {0, 1}, recordComparator(), 32L << 20);
    }

    @Test
    void testSpilledRuns() throws Exception {
        List<TestRecord> records = records(20_000, true, false);
        assertSorted(records, new int[] {0, 1}, recordComparator(), 256L << 10);
    }

    private void assertSorted(
            List<TestRecord> records,
            int[] keyFields,
            Comparator<TestRecord> expectedComparator,
            long memorySize)
            throws Exception {
        List<TestRecord> expected = new ArrayList<>(records);
        expected.sort(expectedComparator);
        Collections.shuffle(records, new Random(42));

        IOManager ioManager = IOManager.create(tempDir.toString());
        BinaryExternalSortBuffer sorter =
                BinaryExternalSortBuffer.createWithRadixSort(
                        ioManager,
                        RowType.of(new IntType(), new VarCharType()),
                        keyFields,
                        memorySize,
                        MemorySegmentPool.DEFAULT_PAGE_SIZE,
                        128,
                        CompressOptions.defaultOptions(),
                        MemorySize.MAX_VALUE);
        try {
            BinaryRow row = new BinaryRow(2);
            BinaryRowWriter writer = new BinaryRowWriter(row);
            for (TestRecord record : records) {
                writer.reset();
                writer.writeInt(0, record.key);
                if (record.value == null) {
                    writer.setNullAt(1);
                } else {
                    writer.writeString(1, BinaryString.fromString(record.value));
                }
                writer.complete();
                sorter.write(row);
            }

            MutableObjectIterator<BinaryRow> iterator = sorter.sortedIterator();
            BinaryRow reuse = new BinaryRow(2);
            for (TestRecord record : expected) {
                reuse = iterator.next(reuse);
                assertThat(reuse.getInt(0)).isEqualTo(record.key);
                assertThat(reuse.isNullAt(1) ? null : reuse.getString(1).toString())
                        .isEqualTo(record.value);
            }
            assertThat(iterator.next(reuse)).isNull();
        } finally {
            sorter.clear();
            ioManager.close();
        }
    }

    private static Comparator<TestRecord> recordComparator() {
        return Comparator.comparingInt((TestRecord record) -> record.key)
                .thenComparing(
                        record -> record.value, Comparator.nullsFirst(Comparator.naturalOrder()));
    }

    private static List<TestRecord> records(int size, boolean repeatedKeys, boolean withNulls) {
        List<TestRecord> records = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            int key = repeatedKeys ? i % 37 : i - size / 2;
            String value = withNulls && i % 17 == 0 ? null : String.format("value-%08d", size - i);
            records.add(new TestRecord(key, value));
        }
        return records;
    }

    private static class TestRecord {
        private final int key;
        private final String value;

        private TestRecord(int key, String value) {
            this.key = key;
            this.value = value;
        }
    }
}
