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

package org.apache.paimon.lookup.sort;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceOutput;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.sst.BlockEntry;
import org.apache.paimon.sst.BlockIterator;
import org.apache.paimon.sst.SstFileReader;
import org.apache.paimon.sst.SstFileWriter;
import org.apache.paimon.testutils.junit.parameterized.ParameterizedTestExtension;
import org.apache.paimon.testutils.junit.parameterized.Parameters;
import org.apache.paimon.utils.BloomFilter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/** Test for {@link SstFileReader} and {@link SstFileWriter}. */
@ExtendWith(ParameterizedTestExtension.class)
public class SortLookupStoreTest {

    private static final Logger LOG = LoggerFactory.getLogger(SortLookupStoreTest.class);

    // 256 records per block
    private static final int BLOCK_SIZE = (10) * 256;
    private static final CacheManager CACHE_MANAGER = new CacheManager(MemorySize.ofMebiBytes(10));
    @TempDir java.nio.file.Path tempPath;

    private final boolean bloomFilterEnabled;
    private final CompressOptions compress;

    private FileIO fileIO;
    private Path file;
    private File localFile;

    public SortLookupStoreTest(List<Object> var) {
        this.bloomFilterEnabled = (Boolean) var.get(0);
        this.compress = new CompressOptions((String) var.get(1), 1);
    }

    @SuppressWarnings("unused")
    @Parameters(name = "enableBf&compress-{0}")
    public static List<List<Object>> getVarSeg() {
        return Arrays.asList(
                Arrays.asList(true, "none"),
                Arrays.asList(false, "none"),
                Arrays.asList(false, "lz4"),
                Arrays.asList(true, "lz4"),
                Arrays.asList(false, "zstd"),
                Arrays.asList(true, "zstd"));
    }

    @BeforeEach
    public void beforeEach() {
        this.fileIO = LocalFileIO.create();
        this.file = new Path(new Path(tempPath.toUri()), UUID.randomUUID().toString());
        this.localFile = new File(file.toUri().getPath());
    }

    @TestTemplate
    public void testLookup() throws Exception {
        writeData(5000, bloomFilterEnabled);
        innerTestLookup();
    }

    private SortLookupStoreReader newReader() throws IOException {
        SeekableInputStream input = LocalFileIO.INSTANCE.newInputStream(file);
        return new SortLookupStoreReader(
                Comparator.comparingInt(slice -> slice.readInt(0)),
                file,
                localFile.length(),
                input,
                CACHE_MANAGER);
    }

    private void innerTestLookup() throws Exception {
        try (SortLookupStoreReader reader = newReader()) {
            Random random = new Random();
            MemorySliceOutput keyOut = new MemorySliceOutput(4);

            // 1. lookup random existing keys
            for (int i = 0; i < 100; i++) {
                int key = random.nextInt(5000 * 2 - 1);
                keyOut.reset();
                keyOut.writeInt(key);
                byte[] queried = reader.lookup(keyOut.toSlice().getHeapMemory());
                if (key % 2 == 0) {
                    Assertions.assertNotNull(queried);
                    Assertions.assertEquals(key, MemorySlice.wrap(queried).readInt(0));
                } else {
                    Assertions.assertNull(queried);
                }
            }

            // 2. lookup boundaries
            keyOut.reset();
            keyOut.writeInt(0);
            byte[] queried = reader.lookup(keyOut.toSlice().getHeapMemory());
            Assertions.assertNotNull(queried);
            Assertions.assertEquals(0, MemorySlice.wrap(queried).readInt(0));

            keyOut.reset();
            keyOut.writeInt(511 * 2);
            byte[] queried1 = reader.lookup(keyOut.toSlice().getHeapMemory());
            Assertions.assertNotNull(queried1);
            Assertions.assertEquals(511 * 2, MemorySlice.wrap(queried1).readInt(0));

            keyOut.reset();
            keyOut.writeInt(4999 * 2);
            byte[] queried2 = reader.lookup(keyOut.toSlice().getHeapMemory());
            Assertions.assertNotNull(queried2);
            Assertions.assertEquals(4999 * 2, MemorySlice.wrap(queried2).readInt(0));

            // 2. lookup key smaller than first key
            for (int i = 0; i < 100; i++) {
                keyOut.reset();
                keyOut.writeInt(-10 - i);
                Assertions.assertNull(reader.lookup(keyOut.toSlice().getHeapMemory()));
            }

            // 3. lookup key greater than last key
            for (int i = 0; i < 100; i++) {
                keyOut.reset();
                keyOut.writeInt(10001 + i);
                Assertions.assertNull(reader.lookup(keyOut.toSlice().getHeapMemory()));
            }
        }
    }

    @TestTemplate
    public void testScan() throws Exception {
        int recordNum = 20000;
        writeData(recordNum, bloomFilterEnabled);

        try (SortLookupStoreReader reader = newReader()) {
            SstFileReader.SstFileIterator fileIterator = reader.createIterator();

            MemorySliceOutput keyOut = new MemorySliceOutput(4);

            // 1. test full scan
            assertScan(0, recordNum - 1, fileIterator);

            // 2. test random seek and scan
            Random random = new Random();
            for (int i = 0; i < 1000; i++) {
                resetIterator(fileIterator, keyOut);
                int key = random.nextInt(recordNum * 2 - 1);
                int targetPosition = (key + 1) / 2;

                keyOut.reset();
                keyOut.writeInt(key);
                fileIterator.seekTo(keyOut.toSlice().getHeapMemory());

                // lookup should not affect reader position
                interleaveLookup(reader, keyOut);

                assertScan(targetPosition, recordNum - 1, fileIterator);
            }

            // 3. test boundaries
            resetIterator(fileIterator, keyOut);
            keyOut.reset();
            keyOut.writeInt(0);
            fileIterator.seekTo(keyOut.toSlice().getHeapMemory());
            assertScan(0, recordNum - 1, fileIterator);

            resetIterator(fileIterator, keyOut);
            keyOut.reset();
            keyOut.writeInt(recordNum * 2 - 2);
            fileIterator.seekTo(keyOut.toSlice().getHeapMemory());
            assertScan(recordNum - 1, recordNum - 1, fileIterator);

            // 4. test out of boundaries
            resetIterator(fileIterator, keyOut);
            keyOut.reset();
            keyOut.writeInt(-10);
            fileIterator.seekTo(keyOut.toSlice().getHeapMemory());
            assertScan(0, recordNum - 1, fileIterator);

            resetIterator(fileIterator, keyOut);
            keyOut.reset();
            keyOut.writeInt(recordNum * 2 + 10);
            fileIterator.seekTo(keyOut.toSlice().getHeapMemory());
            Assertions.assertNull(fileIterator.readBatch());
        }
    }

    private void resetIterator(SstFileReader.SstFileIterator iterator, MemorySliceOutput keyOut)
            throws IOException {
        keyOut.reset();
        keyOut.writeInt(-1);
        iterator.seekTo(keyOut.toSlice().getHeapMemory());
    }

    private void interleaveLookup(SortLookupStoreReader reader, MemorySliceOutput keyOut)
            throws Exception {
        keyOut.reset();
        keyOut.writeInt(0);
        reader.lookup(keyOut.toSlice().getHeapMemory());
    }

    private void writeData(int recordCount, boolean withBloomFilter) throws Exception {
        BloomFilter.Builder bloomFilter = null;
        if (withBloomFilter) {
            bloomFilter = BloomFilter.builder(recordCount, 0.05);
        }
        BlockCompressionFactory compressionFactory = BlockCompressionFactory.create(compress);
        try (PositionOutputStream outputStream = fileIO.newOutputStream(file, true);
                SortLookupStoreWriter writer =
                        new SortLookupStoreWriter(
                                outputStream, BLOCK_SIZE, bloomFilter, compressionFactory); ) {
            MemorySliceOutput keyOut = new MemorySliceOutput(4);
            MemorySliceOutput valueOut = new MemorySliceOutput(4);
            long start = System.currentTimeMillis();
            for (int i = 0; i < recordCount; i++) {
                keyOut.reset();
                valueOut.reset();
                keyOut.writeInt(i * 2);
                valueOut.writeInt(i * 2);
                writer.put(keyOut.toSlice().getHeapMemory(), valueOut.toSlice().getHeapMemory());
            }
            LOG.info("Write {} data cost {} ms", recordCount, System.currentTimeMillis() - start);
        }
    }

    private static void assertScan(
            int startPosition, int lastPosition, SstFileReader.SstFileIterator iterator)
            throws Exception {
        int count = startPosition;
        BlockIterator iter;
        while ((iter = iterator.readBatch()) != null) {
            while (iter.hasNext()) {
                BlockEntry entry = iter.next();
                Assertions.assertEquals(count * 2, entry.getKey().readInt(0));
                Assertions.assertEquals(count * 2, entry.getValue().readInt(0));
                count++;
            }
        }
        Assertions.assertEquals(lastPosition, count - 1);
    }
}
