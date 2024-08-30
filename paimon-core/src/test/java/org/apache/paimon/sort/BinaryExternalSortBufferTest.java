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
import org.apache.paimon.data.serializer.AbstractRowDataSerializer;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.utils.MutableObjectIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BinaryExternalSortBuffer}. */
public class BinaryExternalSortBufferTest {

    private static final int MEMORY_SIZE = 1024 * 1024 * 32;

    @TempDir Path tempDir;

    private IOManager ioManager;
    private MemorySegmentPool memorySegmentPool;
    private int totalPages;
    private BinaryRowSerializer serializer;

    private static String getString(int count) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 8; i++) {
            builder.append(count);
        }
        return builder.toString();
    }

    @BeforeEach
    public void beforeTest() {
        ioManager = IOManager.create(tempDir.toString());
        initMemorySegmentPool(MEMORY_SIZE);
        this.serializer = new BinaryRowSerializer(2);
    }

    @AfterEach
    public void afterTest() throws Exception {
        assertAfterTest();
        this.ioManager.close();
    }

    private void initMemorySegmentPool(long maxMemory) {
        this.memorySegmentPool =
                new HeapMemorySegmentPool(maxMemory, MemorySegmentPool.DEFAULT_PAGE_SIZE);
        this.totalPages = memorySegmentPool.freePages();
    }

    private void assertAfterTest() throws IOException {
        assertThat(memorySegmentPool.freePages()).isEqualTo(totalPages);
        List<File> files =
                Files.walk(tempDir)
                        .map(Path::toFile)
                        .filter(f -> !f.isDirectory())
                        .collect(Collectors.toList());
        assertThat(files).isEmpty();
    }

    @Test
    public void testSortNoSpill() throws Exception {
        int size = 1_000_000;

        MockBinaryRowReader reader = new MockBinaryRowReader(size);

        // there are two sort buffer if sortMemory > 100 * 1024 * 1024.
        initMemorySegmentPool(1024 * 1024 * 101);
        BinaryExternalSortBuffer sorter = createBuffer();
        sorter.write(reader);

        assertThat(sorter.size()).isEqualTo(size);

        MutableObjectIterator<BinaryRow> iterator = sorter.sortedIterator();

        BinaryRow next = serializer.createInstance();
        for (int i = 0; i < size; i++) {
            next = iterator.next(next);
            assertThat(next.getInt(0)).isEqualTo(i);
            assertThat(next.getString(1).toString()).isEqualTo(getString(i));
        }

        sorter.clear();
    }

    @Test
    public void testSort() throws Exception {
        int size = 10_000;

        MockBinaryRowReader reader = new MockBinaryRowReader(size);

        BinaryExternalSortBuffer sorter = createBuffer();
        sorter.write(reader);

        assertThat(sorter.size()).isEqualTo(size);

        assertThat(sorter.getOccupancy()).isGreaterThan(0);

        MutableObjectIterator<BinaryRow> iterator = sorter.sortedIterator();

        BinaryRow next = serializer.createInstance();
        for (int i = 0; i < size; i++) {
            next = iterator.next(next);
            assertThat(next.getInt(0)).isEqualTo(i);
            assertThat(next.getString(1).toString()).isEqualTo(getString(i));
        }

        sorter.clear();
        assertThat(sorter.getOccupancy()).isEqualTo(0);
    }

    @Test
    public void testSortIntStringWithRepeat() throws Exception {
        int size = 10_000;

        BinaryExternalSortBuffer sorter = createBuffer();
        sorter.write(new MockBinaryRowReader(size));
        assertThat(sorter.size()).isEqualTo(size);

        sorter.write(new MockBinaryRowReader(size));
        assertThat(sorter.size()).isEqualTo(size * 2);

        sorter.write(new MockBinaryRowReader(size));
        assertThat(sorter.size()).isEqualTo(size * 3);

        MutableObjectIterator<BinaryRow> iterator = sorter.sortedIterator();

        BinaryRow next = serializer.createInstance();
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < 3; j++) {
                next = iterator.next(next);
                assertThat(next.getInt(0)).isEqualTo(i);
                assertThat(next.getString(1).toString()).isEqualTo(getString(i));
            }
        }

        sorter.clear();
    }

    @Test
    public void testRepeatUsingWhenSpill() throws Exception {
        BinaryExternalSortBuffer sorter = createBuffer();
        innerTestSpilling(sorter);
        assertAfterTest();
        innerTestSpilling(sorter);
    }

    @Test
    public void testSpilling() throws Exception {
        innerTestSpilling(createBuffer());
    }

    private void innerTestSpilling(BinaryExternalSortBuffer sorter) throws Exception {
        int size = 1000_000;

        MockBinaryRowReader reader = new MockBinaryRowReader(size);
        sorter.write(reader);
        assertThat(sorter.size()).isEqualTo(size);

        MutableObjectIterator<BinaryRow> iterator = sorter.sortedIterator();

        BinaryRow next = serializer.createInstance();
        for (int i = 0; i < size; i++) {
            next = iterator.next(next);
            assertThat(next.getInt(0)).isEqualTo(i);
            assertThat(next.getString(1).toString()).isEqualTo(getString(i));
        }

        sorter.clear();
    }

    @Test
    public void testMergeManyTimes() throws Exception {
        int size = 1000_000;

        MockBinaryRowReader reader = new MockBinaryRowReader(size);

        BinaryExternalSortBuffer sorter = createBuffer(8);
        sorter.write(reader);
        assertThat(sorter.size()).isEqualTo(size);

        MutableObjectIterator<BinaryRow> iterator = sorter.sortedIterator();

        BinaryRow next = serializer.createInstance();
        for (int i = 0; i < size; i++) {
            next = iterator.next(next);
            assertThat(next.getInt(0)).isEqualTo(i);
            assertThat(next.getString(1).toString()).isEqualTo(getString(i));
        }

        sorter.clear();
    }

    @Test
    public void testSpillingRandom() throws Exception {
        int size = 1000_000;

        MockBinaryRowReader reader = new MockBinaryRowReader(size);

        BinaryExternalSortBuffer sorter = createBuffer(8);

        List<BinaryRow> data = new ArrayList<>();
        BinaryRow row = serializer.createInstance();
        for (int i = 0; i < size; i++) {
            row = reader.next(row);
            data.add(row.copy());
        }

        Collections.shuffle(data);

        for (int i = 0; i < size; i++) {
            sorter.write(data.get(i));
        }

        MutableObjectIterator<BinaryRow> iterator = sorter.sortedIterator();

        data.sort(Comparator.comparingInt(o -> o.getInt(0)));

        BinaryRow next = serializer.createInstance();
        for (int i = 0; i < size; i++) {
            next = iterator.next(next);
            assertThat(next.getInt(0)).isEqualTo(data.get(i).getInt(0));
            assertThat(next.getString(1).toString()).isEqualTo(data.get(i).getString(1).toString());
        }

        sorter.clear();
    }

    @Test
    public void testSpillingMaxDiskSize() throws Exception {
        BinaryExternalSortBuffer sorter = createBuffer(128, MemorySize.ofKibiBytes(10));
        int size = 1000_000;

        MockBinaryRowReader reader = new MockBinaryRowReader(size);
        sorter.write(reader);

        assertThat(sorter.flushMemory()).isFalse();
        assertThat(sorter.size()).isEqualTo(size);

        MutableObjectIterator<BinaryRow> iterator = sorter.sortedIterator();

        BinaryRow next = serializer.createInstance();
        for (int i = 0; i < size; i++) {
            next = iterator.next(next);
            assertThat(next.getInt(0)).isEqualTo(i);
            assertThat(next.getString(1).toString()).isEqualTo(getString(i));
        }

        sorter.clear();
    }

    private BinaryExternalSortBuffer createBuffer() {
        return createBuffer(128, MemorySize.MAX_VALUE);
    }

    private BinaryExternalSortBuffer createBuffer(int maxNumFileHandles) {
        return createBuffer(maxNumFileHandles, MemorySize.MAX_VALUE);
    }

    private BinaryExternalSortBuffer createBuffer(int maxNumFileHandles, MemorySize diskSize) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        BinaryInMemorySortBuffer inMemorySortBuffer =
                BinaryInMemorySortBuffer.createBuffer(
                        IntNormalizedKeyComputer.INSTANCE,
                        (AbstractRowDataSerializer) serializer,
                        IntRecordComparator.INSTANCE,
                        memorySegmentPool);
        return new BinaryExternalSortBuffer(
                serializer,
                IntRecordComparator.INSTANCE,
                MemorySegmentPool.DEFAULT_PAGE_SIZE,
                inMemorySortBuffer,
                ioManager,
                maxNumFileHandles,
                CompressOptions.defaultOptions(),
                diskSize);
    }

    /** Mock reader for binary row. */
    public static class MockBinaryRowReader implements MutableObjectIterator<BinaryRow> {

        private final int size;
        private final BinaryRow row;
        private final BinaryRowWriter writer;

        private int count;

        public MockBinaryRowReader(int size) {
            this.size = size;
            this.row = new BinaryRow(2);
            this.writer = new BinaryRowWriter(row);
        }

        @Override
        public BinaryRow next(BinaryRow reuse) {
            return next();
        }

        @Override
        public BinaryRow next() {
            if (count >= size) {
                return null;
            }
            writer.reset();
            writer.writeInt(0, count);
            writer.writeString(1, BinaryString.fromString(getString(count)));
            writer.complete();
            count++;
            return row;
        }
    }
}
