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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/** Test for {@link SstFileReader} and {@link SstFileWriter}. */
@ExtendWith(ParameterizedTestExtension.class)
public class SstFileTest {
    private static final Logger LOG = LoggerFactory.getLogger(SstFileTest.class);

    // 256 records per block
    private static final int BLOCK_SIZE = (10) * 256;
    private static final CacheManager CACHE_MANAGER = new CacheManager(MemorySize.ofMebiBytes(10));
    @TempDir java.nio.file.Path tempPath;

    private final boolean bloomFilterEnabled;
    private final CompressOptions compress;

    private FileIO fileIO;
    private Path file;
    private Path parent;

    public SstFileTest(List<Object> var) {
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
                Arrays.asList(true, "zstd"),
                Arrays.asList(false, "lzo"),
                Arrays.asList(true, "lzo"));
    }

    @BeforeEach
    public void beforeEach() {
        this.fileIO = LocalFileIO.create();
        this.parent = new Path(tempPath.toUri());
        this.file = new Path(new Path(tempPath.toUri()), UUID.randomUUID().toString());
    }

    @TestTemplate
    public void testLookup() throws Exception {
        writeData(5000, bloomFilterEnabled);
        innerTestLookup();
    }

    @TestTemplate
    public void testEmptyFile() throws Exception {
        writeData(0, false);

        long fileSize = fileIO.getFileSize(file);
        try (SeekableInputStream inputStream = fileIO.newInputStream(file);
                SstFileReader reader =
                        new SstFileReader(
                                Comparator.comparingInt(slice -> slice.readInt(0)),
                                fileSize,
                                file,
                                inputStream,
                                CACHE_MANAGER); ) {
            MemorySliceOutput keyOut = new MemorySliceOutput(4);
            for (int i = -10; i < 10; i++) {
                keyOut.reset();
                keyOut.writeInt(i);
                Assertions.assertNull(reader.lookup(keyOut.toSlice().copyBytes()));
            }

            FileInfo fileInfo = reader.readFileInfo();
            Assertions.assertEquals(0, fileInfo.getAvgKeyLength());
            Assertions.assertEquals(0, fileInfo.getAvgValueLength());
            Assertions.assertEquals(0, fileInfo.getMaxKeyLength());
            Assertions.assertEquals(0, fileInfo.getMinKeyLength());
            Assertions.assertEquals(0, fileInfo.getMaxValueLength());
            Assertions.assertEquals(0, fileInfo.getMinValueLength());
        }
    }

    private void innerTestLookup() throws Exception {
        long fileSize = fileIO.getFileSize(file);
        try (SeekableInputStream inputStream = fileIO.newInputStream(file);
                SstFileReader reader =
                        new SstFileReader(
                                Comparator.comparingInt(slice -> slice.readInt(0)),
                                fileSize,
                                file,
                                inputStream,
                                CACHE_MANAGER); ) {
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
            for (int i = 0; i < 100; i++) {
                keyOut.reset();
                keyOut.writeInt(-10 - i);
                Assertions.assertNull(reader.lookup(keyOut.toSlice().getHeapMemory()));
            }

            // 3. lookup key greater than last key
            for (int i = 0; i < 100; i++) {
                keyOut.reset();
                keyOut.writeInt(10000 + i);
                Assertions.assertNull(reader.lookup(keyOut.toSlice().getHeapMemory()));
            }
        }
    }

    private void writeData(int recordCount, boolean withBloomFilter) throws Exception {
        BloomFilter.Builder bloomFilter = null;
        if (withBloomFilter) {
            bloomFilter = BloomFilter.builder(recordCount, 0.05);
        }
        BlockCompressionFactory compressionFactory = BlockCompressionFactory.create(compress);
        try (PositionOutputStream outputStream = fileIO.newOutputStream(file, true);
                SstFileWriter writer =
                        new SstFileWriter(
                                outputStream, BLOCK_SIZE, bloomFilter, compressionFactory); ) {
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

    @TestTemplate
    public void testAddExtraFileInfo() throws Exception {
        BlockCompressionFactory compressionFactory = BlockCompressionFactory.create(compress);
        try (PositionOutputStream outputStream = fileIO.newOutputStream(file, true);
                SstFileWriter writer =
                        new SstFileWriter(outputStream, BLOCK_SIZE, null, compressionFactory); ) {
            writer.addExtraFileInfo("testKey".getBytes(), "testValue".getBytes());
        }

        long fileSize = fileIO.getFileSize(file);
        try (SeekableInputStream inputStream = fileIO.newInputStream(file);
                SstFileReader reader =
                        new SstFileReader(
                                Comparator.comparingInt(slice -> slice.readInt(0)),
                                fileSize,
                                file,
                                inputStream,
                                CACHE_MANAGER); ) {
            FileInfo fileInfo = reader.readFileInfo();
            // it's an empty file
            Assertions.assertEquals(0, fileInfo.getAvgKeyLength());
            Assertions.assertEquals(0, fileInfo.getAvgValueLength());
            Map<byte[], byte[]> extraValues = fileInfo.getExtras();
            Assertions.assertEquals(1, extraValues.size());
            Assertions.assertTrue(extraValues.containsKey("testKey".getBytes()));
            Assertions.assertArrayEquals(
                    extraValues.get("testKey".getBytes()), "testValue".getBytes());
        }
    }
}
