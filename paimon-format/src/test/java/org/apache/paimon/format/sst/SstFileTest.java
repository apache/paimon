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

package org.apache.paimon.format.sst;

import org.apache.paimon.format.sst.layout.BlockEntry;
import org.apache.paimon.format.sst.layout.SstFileReader;
import org.apache.paimon.format.sst.layout.SstFileWriter;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;
import org.apache.paimon.memory.MemorySliceOutput;
import org.apache.paimon.utils.BloomFilter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Random;
import java.util.UUID;

/** Test for {@link SstFileReader} and {@link SstFileWriter}. */
public class SstFileTest {
    private static final Logger LOG = LoggerFactory.getLogger(SstFileTest.class);

    // 256 records per block
    private static final int BLOCK_SIZE = (10) * 256;
    @TempDir java.nio.file.Path tempPath;

    protected FileIO fileIO;
    protected Path file;
    protected Path parent;

    @BeforeEach
    public void beforeEach() {
        this.fileIO = LocalFileIO.create();
        this.parent = new Path(tempPath.toUri());
        this.file = new Path(new Path(tempPath.toUri()), UUID.randomUUID().toString());
    }

    private void writeData(int recordCount, BloomFilter.Builder bloomFilter) throws Exception {
        try (PositionOutputStream outputStream = fileIO.newOutputStream(file, true);
                SstFileWriter writer =
                        new SstFileWriter(outputStream, BLOCK_SIZE, bloomFilter, null); ) {
            MemorySliceOutput keyOut = new MemorySliceOutput(4);
            MemorySliceOutput valueOut = new MemorySliceOutput(4);
            long start = System.currentTimeMillis();
            for (int i = 0; i < recordCount; i++) {
                keyOut.reset();
                valueOut.reset();
                keyOut.writeInt(i);
                valueOut.writeInt(i);
                writer.put(keyOut.toSlice().getHeapMemory(), valueOut.toSlice().getHeapMemory());
            }
            LOG.info("Write {} data cost {} ms", recordCount, System.currentTimeMillis() - start);
        }
    }

    @Test
    public void testLookup() throws Exception {
        writeData(5000, null);

        long fileSize = fileIO.getFileSize(file);
        try (SeekableInputStream inputStream = fileIO.newInputStream(file);
                SstFileReader reader =
                        new SstFileReader(
                                inputStream,
                                Comparator.comparingInt(slice -> slice.readInt(0)),
                                fileSize,
                                file); ) {
            Random random = new Random();
            MemorySliceOutput keyOut = new MemorySliceOutput(4);

            // 1. lookup random existing keys
            for (int i = 0; i < 100; i++) {
                int key = random.nextInt(5000);
                keyOut.reset();
                keyOut.writeInt(key);
                byte[] queried = reader.lookup(keyOut.toSlice().getHeapMemory());
                Assertions.assertNotNull(queried);
                Assertions.assertEquals(key, MemorySlice.wrap(queried).readInt(0));
            }

            // 2. lookup boundaries
            keyOut.reset();
            keyOut.writeInt(0);
            byte[] queried = reader.lookup(keyOut.toSlice().getHeapMemory());
            Assertions.assertNotNull(queried);
            Assertions.assertEquals(0, MemorySlice.wrap(queried).readInt(0));

            keyOut.reset();
            keyOut.writeInt(511);
            byte[] queried1 = reader.lookup(keyOut.toSlice().getHeapMemory());
            Assertions.assertNotNull(queried1);
            Assertions.assertEquals(511, MemorySlice.wrap(queried1).readInt(0));

            keyOut.reset();
            keyOut.writeInt(4999);
            byte[] queried2 = reader.lookup(keyOut.toSlice().getHeapMemory());
            Assertions.assertNotNull(queried2);
            Assertions.assertEquals(4999, MemorySlice.wrap(queried2).readInt(0));

            // 2. lookup key smaller than first key
            keyOut.reset();
            keyOut.writeInt(-10);
            Assertions.assertNull(reader.lookup(keyOut.toSlice().getHeapMemory()));

            // 3. lookup key greater than last key
            keyOut.reset();
            keyOut.writeInt(10000);
            Assertions.assertNull(reader.lookup(keyOut.toSlice().getHeapMemory()));
        }
    }

    @Test
    public void testFullScan() throws Exception {
        writeData(5000, null);

        long fileSize = fileIO.getFileSize(file);
        try (SeekableInputStream inputStream = fileIO.newInputStream(file);
                SstFileReader reader =
                        new SstFileReader(
                                inputStream,
                                Comparator.comparingInt(slice -> slice.readInt(0)),
                                fileSize,
                                file); ) {
            assertScan(0, reader);
        }
    }

    @Test
    public void testSeekAndScan() throws Exception {
        writeData(5000, null);

        long fileSize = fileIO.getFileSize(file);
        try (SeekableInputStream inputStream = fileIO.newInputStream(file);
                SstFileReader reader =
                        new SstFileReader(
                                inputStream,
                                Comparator.comparingInt(slice -> slice.readInt(0)),
                                fileSize,
                                file); ) {
            MemorySliceOutput keyOut = new MemorySliceOutput(4);
            int startPos;

            // 1. seek to each data block
            for (int start = BLOCK_SIZE / 10 / 2; start < 5000; start += BLOCK_SIZE / 10) {
                keyOut.reset();
                keyOut.writeInt(start);
                startPos = reader.seekTo(keyOut.toSlice().getHeapMemory());
                Assertions.assertEquals(start, startPos);
                assertScan(startPos, reader);
            }

            // 2. seek to boundaries should behave well
            keyOut.reset();
            keyOut.writeInt(0);
            startPos = reader.seekTo(keyOut.toSlice().getHeapMemory());
            Assertions.assertEquals(0, startPos);
            assertScan(startPos, reader);

            keyOut.reset();
            keyOut.writeInt(BLOCK_SIZE / 10 - 1);
            startPos = reader.seekTo(keyOut.toSlice().getHeapMemory());
            Assertions.assertEquals(BLOCK_SIZE / 10 - 1, startPos);
            assertScan(startPos, reader);

            keyOut.reset();
            keyOut.writeInt(4999);
            startPos = reader.seekTo(keyOut.toSlice().getHeapMemory());
            Assertions.assertEquals(4999, startPos);
            assertScan(startPos, reader);

            // 3. seek will reset the iterator
            keyOut.reset();
            keyOut.writeInt(2000);
            startPos = reader.seekTo(keyOut.toSlice().getHeapMemory());
            Assertions.assertEquals(2000, startPos);
            assertScan(startPos, reader);

            // 4. seeking to some key smaller than the first key is equal to full scan
            keyOut.reset();
            keyOut.writeInt(-8);
            startPos = reader.seekTo(keyOut.toSlice().getHeapMemory());
            Assertions.assertEquals(0, startPos);
            assertScan(startPos, reader);

            // 5. seeking to some key greater than the last key will return null immediately
            keyOut.reset();
            keyOut.writeInt(6000);
            startPos = reader.seekTo(keyOut.toSlice().getHeapMemory());
            Assertions.assertEquals(-1, startPos);
            Assertions.assertNull(reader.readBatch());
        }
    }

    @Test
    public void testSeekToPositionAndScan() throws Exception {
        writeData(5000, null);

        long fileSize = fileIO.getFileSize(file);
        try (SeekableInputStream inputStream = fileIO.newInputStream(file);
                SstFileReader reader =
                        new SstFileReader(
                                inputStream,
                                Comparator.comparingInt(slice -> slice.readInt(0)),
                                fileSize,
                                file); ) {

            // 1. seek to each block
            for (int start = BLOCK_SIZE / 10 / 2; start < 5000; start += BLOCK_SIZE / 10) {
                reader.seekTo(start);
                assertScan(start, reader);
            }

            // 2. seek to boundaries
            // zero
            reader.seekTo(0);
            assertScan(0, reader);
            // last position
            reader.seekTo(4999);
            assertScan(4999, reader);
            // first position of an inner block
            reader.seekTo(512);
            assertScan(512, reader);
            // last position of an inner block
            reader.seekTo(767);
            assertScan(767, reader);

            // 3. seek out of boundary
            Assertions.assertThrows(IndexOutOfBoundsException.class, () -> reader.seekTo(-1));
            Assertions.assertThrows(IndexOutOfBoundsException.class, () -> reader.seekTo(5000));
        }
    }

    @Test
    public void testInterleavedLookupAndSeek() throws Exception {
        writeData(5000, null);

        long fileSize = fileIO.getFileSize(file);
        try (SeekableInputStream inputStream = fileIO.newInputStream(file);
                SstFileReader reader =
                        new SstFileReader(
                                inputStream,
                                Comparator.comparingInt(slice -> slice.readInt(0)),
                                fileSize,
                                file); ) {
            MemorySliceOutput keyOut = new MemorySliceOutput(4);
            int startPos;

            // 1. First seek to somewhere
            keyOut.reset();
            keyOut.writeInt(1000);
            startPos = reader.seekTo(keyOut.toSlice().getHeapMemory());

            // 2. Lookup
            keyOut.reset();
            keyOut.writeInt(2000);
            byte[] result = reader.lookup(keyOut.toSlice().getHeapMemory());
            Assertions.assertNotNull(result);
            MemorySliceInput input = MemorySlice.wrap(result).toInput();
            Assertions.assertEquals(2000, input.readInt());

            // 3. Continue to scan
            Assertions.assertEquals(1000, startPos);
        }
    }

    private static void assertScan(int startPosition, SstFileReader reader) throws Exception {
        int count = startPosition;
        Iterator<BlockEntry> iter;
        while ((iter = reader.readBatch()) != null) {
            while (iter.hasNext()) {
                BlockEntry entry = iter.next();
                Assertions.assertEquals(count, entry.getKey().readInt(0));
                Assertions.assertEquals(count, entry.getValue().readInt(0));
                count++;
            }
        }
    }
}
