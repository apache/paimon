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

package org.apache.paimon.sst;

import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceOutput;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;

/** Test for {@link BlockIterator}. */
public class BlockIteratorTest {

    private static final int ROW_NUM = 10_000;
    private static final Comparator<MemorySlice> COMPARATOR =
            Comparator.comparingInt(slice -> slice.readInt(0));

    @Test
    public void testAlignedIterator() throws IOException {
        innerTest(true);
    }

    @Test
    public void testUnalignedIterator() throws IOException {
        innerTest(false);
    }

    public void innerTest(boolean aligned) throws IOException {
        MemorySlice data = writeBlock(aligned);
        BlockIterator iterator = BlockReader.create(data, COMPARATOR).iterator();

        // 1. test for normal cases:
        final int step = 3;
        MemorySliceOutput keyOut = new MemorySliceOutput(4);
        MemorySliceOutput valueOut = new MemorySliceOutput(4);
        for (int i = 0; i < ROW_NUM * 2; i += step) {
            keyOut.reset();
            keyOut.writeInt(i);
            iterator.seekTo(keyOut.toSlice());
            int expected = (i + 1) / 2;
            while (iterator.hasNext()) {
                Map.Entry<MemorySlice, MemorySlice> entry = iterator.next();
                MemorySlice key = entry.getKey();
                MemorySlice value = entry.getValue();
                Assertions.assertEquals(expected * 2, key.readInt(0));
                Assertions.assertArrayEquals(
                        constructValue(valueOut, aligned, expected), value.copyBytes());
                expected++;
            }
        }

        // 2. test seek to boundaries

        // 2.1 seek to some key smaller than the first key
        keyOut.reset();
        keyOut.writeInt(-1);
        iterator.seekTo(keyOut.toSlice());
        Assertions.assertTrue(iterator.hasNext());
        Map.Entry<MemorySlice, MemorySlice> entry = iterator.next();
        Assertions.assertEquals(0, entry.getKey().readInt(0));

        // 2.2 seek to some key greater than the last key
        keyOut.reset();
        keyOut.writeInt(ROW_NUM * 2 + 2);
        iterator.seekTo(keyOut.toSlice());
        Assertions.assertFalse(iterator.hasNext());
    }

    private MemorySlice writeBlock(boolean aligned) throws IOException {
        BlockWriter writer = new BlockWriter(ROW_NUM * 14);
        MemorySliceOutput keyOut = new MemorySliceOutput(4);
        MemorySliceOutput valueOut = new MemorySliceOutput(4);
        for (int i = 0; i < ROW_NUM; i++) {
            keyOut.reset();
            keyOut.writeInt(i * 2);
            writer.add(keyOut.toSlice().getHeapMemory(), constructValue(valueOut, aligned, i));
        }
        return writer.finish();
    }

    private byte[] constructValue(MemorySliceOutput valueOut, boolean aligned, int i) {
        valueOut.reset();
        valueOut.writeInt(i * 2);
        if (aligned && i % 2 == 0) {
            valueOut.writeInt(i * 2);
        }
        return valueOut.toSlice().copyBytes();
    }
}
