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

import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.serializer.AbstractRowDataSerializer;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.sort.BinaryInMemorySortBuffer;
import org.apache.paimon.sort.IntNormalizedKeyComputer;
import org.apache.paimon.sort.IntRecordComparator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test that {@link SortBufferWriteBuffer} (used by {@link MergeTreeWriter}) handles int overflow in
 * numRecords correctly by using isEmpty() instead of size().
 */
public class SortBufferWriteBufferOverflowTest {

    @TempDir Path tempDir;

    private IOManager ioManager;
    private MemorySegmentPool memorySegmentPool;

    @BeforeEach
    public void setUp() {
        ioManager = IOManager.create(tempDir.toString());
        memorySegmentPool = new HeapMemorySegmentPool(32 * 1024 * 3L, 32 * 1024);
    }

    @AfterEach
    public void tearDown() throws Exception {
        ioManager.close();
    }

    private BinaryExternalSortBuffer createSortBuffer() {
        BinaryRowSerializer serializer = new BinaryRowSerializer(1);
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
                memorySegmentPool.pageSize(),
                inMemorySortBuffer,
                ioManager,
                128,
                CompressOptions.defaultOptions(),
                MemorySize.MAX_VALUE);
    }

    private static SortBufferWriteBuffer createWriteBuffer(BinaryExternalSortBuffer buffer)
            throws Exception {
        Field theUnsafeField = Unsafe.class.getDeclaredField("theUnsafe");
        theUnsafeField.setAccessible(true);
        Unsafe unsafe = (Unsafe) theUnsafeField.get(null);
        SortBufferWriteBuffer writeBuffer =
                (SortBufferWriteBuffer) unsafe.allocateInstance(SortBufferWriteBuffer.class);

        Field bufferField = SortBufferWriteBuffer.class.getDeclaredField("buffer");
        bufferField.setAccessible(true);
        bufferField.set(writeBuffer, buffer);
        return writeBuffer;
    }

    private static void setNumRecords(BinaryExternalSortBuffer buffer, long numRecords)
            throws Exception {
        Field numRecordsField = BinaryExternalSortBuffer.class.getDeclaredField("numRecords");
        numRecordsField.setAccessible(true);
        numRecordsField.setLong(buffer, numRecords);
    }

    @Test
    public void testIsEmptyWorksWhenNumRecordsExceedsIntMax() throws Exception {
        BinaryExternalSortBuffer buffer = createSortBuffer();
        SortBufferWriteBuffer writeBuffer = createWriteBuffer(buffer);

        assertThat(writeBuffer.size()).isEqualTo(0);
        assertThat(writeBuffer.isEmpty()).isTrue();

        setNumRecords(buffer, Integer.MAX_VALUE);
        assertThat(writeBuffer.size()).isEqualTo(Integer.MAX_VALUE);
        assertThat(writeBuffer.isEmpty()).isFalse();

        setNumRecords(buffer, (long) Integer.MAX_VALUE + 1);

        assertThat(writeBuffer.isEmpty()).isFalse();
        assertThatThrownBy(writeBuffer::size)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("exceeds Integer.MAX_VALUE");

        writeBuffer.clear();
        assertThat(writeBuffer.isEmpty()).isTrue();
        assertThat(writeBuffer.size()).isEqualTo(0);
    }
}
