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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.sort.SortBuffer;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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

    @BeforeEach
    public void setUp() {
        ioManager = IOManager.create(tempDir.toString());
    }

    @AfterEach
    public void tearDown() throws Exception {
        ioManager.close();
    }

    private SortBufferWriteBuffer createSpillableWriteBuffer() {
        RowType keyType = RowType.builder().field("k", DataTypes.INT()).build();
        RowType valueType = RowType.builder().field("v", DataTypes.INT()).build();
        return new SortBufferWriteBuffer(
                keyType,
                valueType,
                null,
                new HeapMemorySegmentPool(32 * 1024 * 3L, 32 * 1024),
                true,
                MemorySize.MAX_VALUE,
                128,
                CompressOptions.defaultOptions(),
                ioManager);
    }

    private static void setNumRecords(SortBufferWriteBuffer writeBuffer, long numRecords)
            throws Exception {
        Field bufferField = SortBufferWriteBuffer.class.getDeclaredField("buffer");
        bufferField.setAccessible(true);
        SortBuffer buffer = (SortBuffer) bufferField.get(writeBuffer);
        assertThat(buffer).isInstanceOf(BinaryExternalSortBuffer.class);

        Field numRecordsField = BinaryExternalSortBuffer.class.getDeclaredField("numRecords");
        numRecordsField.setAccessible(true);
        numRecordsField.setLong(buffer, numRecords);
    }

    @Test
    public void testIsEmptyWorksWhenNumRecordsExceedsIntMax() throws Exception {
        SortBufferWriteBuffer writeBuffer = createSpillableWriteBuffer();
        writeBuffer.put(1L, RowKind.INSERT, GenericRow.of(1), GenericRow.of(100));

        assertThat(writeBuffer.size()).isEqualTo(1);
        assertThat(writeBuffer.isEmpty()).isFalse();

        setNumRecords(writeBuffer, (long) Integer.MAX_VALUE + 1);

        // isEmpty() should still work without throwing — this is the key behavior
        // that MergeTreeWriter.flushWriteBuffer relies on
        assertThat(writeBuffer.isEmpty()).isFalse();

        // size() should throw
        assertThatThrownBy(writeBuffer::size)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("exceeds Integer.MAX_VALUE");

        writeBuffer.clear();
        assertThat(writeBuffer.isEmpty()).isTrue();
        assertThat(writeBuffer.size()).isEqualTo(0);
    }
}
