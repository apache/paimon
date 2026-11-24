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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link BinaryExternalSortBuffer#sortedIterator()} must honor the {@link MutableObjectIterator}
 * contract: once an iterator returned {@code null}, further polls must also return {@code null}.
 * This is a regression test for a spilled iterator whose {@link BinaryMergeIterator} kept a
 * dangling head stream after it was polled out of the heap, so a later poll passed a {@code null}
 * reuse row into the channel reader and threw a {@link NullPointerException}.
 */
public class BinaryMergeIteratorDoublePollTest {

    private static final int NUM_ROWS = 20000;

    @TempDir Path tempDir;

    @Test
    public void testMultiplePollsAfterEofWhenSpilled() throws Exception {
        IOManager ioManager = IOManager.create(tempDir.toString());
        try {
            // three 64kb pages only (minimum allowed), so 20000 rows must spill
            BinaryExternalSortBuffer buffer = createBuffer(ioManager, 3 * 64 * 1024);
            writeRows(buffer, NUM_ROWS);
            assertThat(spillChannelCount(buffer)).isGreaterThan(0);

            MutableObjectIterator<BinaryRow> iterator = buffer.sortedIterator();
            int count = 0;
            BinaryRow reuse = new BinaryRow(2);
            while (iterator.next(reuse) != null) {
                count++;
            }
            assertThat(count).isEqualTo(NUM_ROWS);

            assertThat(iterator.next(reuse)).isNull();
            assertThat(iterator.next(reuse)).isNull();
        } finally {
            ioManager.close();
        }
    }

    @Test
    public void testMultiplePollsAfterEofWhenNotSpilled() throws Exception {
        IOManager ioManager = IOManager.create(tempDir.toString());
        try {
            BinaryExternalSortBuffer buffer = createBuffer(ioManager, 32 * 1024 * 1024);
            writeRows(buffer, 100);
            assertThat(spillChannelCount(buffer)).isZero();

            MutableObjectIterator<BinaryRow> iterator = buffer.sortedIterator();
            BinaryRow reuse = new BinaryRow(2);
            int count = 0;
            while (iterator.next(reuse) != null) {
                count++;
            }
            assertThat(count).isEqualTo(100);
            assertThat(iterator.next(reuse)).isNull();
            assertThat(iterator.next(reuse)).isNull();
        } finally {
            ioManager.close();
        }
    }

    private static BinaryExternalSortBuffer createBuffer(IOManager ioManager, long maxMemory) {
        BinaryRowSerializer serializer = new BinaryRowSerializer(2);
        MemorySegmentPool pool =
                new HeapMemorySegmentPool(maxMemory, MemorySegmentPool.DEFAULT_PAGE_SIZE);
        @SuppressWarnings({"unchecked", "rawtypes"})
        BinaryInMemorySortBuffer inMemorySortBuffer =
                BinaryInMemorySortBuffer.createBuffer(
                        IntNormalizedKeyComputer.INSTANCE,
                        (AbstractRowDataSerializer) serializer,
                        IntRecordComparator.INSTANCE,
                        pool);
        return new BinaryExternalSortBuffer(
                serializer,
                IntRecordComparator.INSTANCE,
                MemorySegmentPool.DEFAULT_PAGE_SIZE,
                inMemorySortBuffer,
                ioManager,
                128,
                CompressOptions.defaultOptions(),
                MemorySize.MAX_VALUE);
    }

    private static void writeRows(BinaryExternalSortBuffer buffer, int numRows) throws Exception {
        BinaryRow row = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        for (int i = 0; i < numRows; i++) {
            writer.reset();
            writer.writeInt(0, i);
            writer.writeString(1, BinaryString.fromString("postpone-carrier-" + i));
            writer.complete();
            buffer.write(row);
        }
    }

    @SuppressWarnings("unchecked")
    private static int spillChannelCount(BinaryExternalSortBuffer buffer) throws Exception {
        Field field = BinaryExternalSortBuffer.class.getDeclaredField("spillChannelIDs");
        field.setAccessible(true);
        return ((List<Object>) field.get(buffer)).size();
    }
}
