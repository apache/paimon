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

package org.apache.paimon.utils;

import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.options.MemorySize;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/** Test for {@link FileBasedBloomFilter}. */
public class FileBasedBloomFilterTest {

    @TempDir Path tempDir;

    @ParameterizedTest
    @CsvSource({
        "64,0,64,true",
        "64,0.5,32,true",
        "64,0.5,33,false",
        "64,0.75,32,true",
        "0,0,1,false"
    })
    void testFilterFitsItsActualIndexPool(long capacity, double indexRatio, int size, boolean fits)
            throws Exception {
        byte[] bytes = new byte[size];
        Arrays.fill(bytes, (byte) 0xff);
        AtomicInteger reads = new AtomicInteger();
        try (CacheManager manager = new CacheManager(MemorySize.ofBytes(capacity), indexRatio);
                FileBasedBloomFilter filter =
                        new FileBasedBloomFilter(
                                countingInput(bytes, reads),
                                new org.apache.paimon.fs.Path("pool-boundary"),
                                manager,
                                10,
                                0,
                                size)) {
            Assertions.assertThat(filter.isCacheable()).isEqualTo(fits);
            Assertions.assertThat(filter.testHash(123)).isTrue();
            Assertions.assertThat(filter.testHash(123)).isTrue();
            Assertions.assertThat(reads).hasValue(fits ? 1 : 2);
        }
    }

    @Test
    void testResidentProbeDoesNotReadOrRetainAnEvictedFilter() throws Exception {
        MemorySegment segment = MemorySegment.allocateHeapMemory(64);
        BloomFilter bloom = new BloomFilter(10, segment.size());
        bloom.setMemorySegment(segment, 0);
        bloom.addHash(123);
        Assertions.assertThat(bloom.testHash(124)).isFalse();
        AtomicInteger reads = new AtomicInteger();
        org.apache.paimon.fs.Path path = new org.apache.paimon.fs.Path("resident-filter");
        try (CacheManager manager = new CacheManager(MemorySize.ofKibiBytes(64), 0.5);
                FileBasedBloomFilter filter =
                        new FileBasedBloomFilter(
                                countingInput(segment.getHeapMemory(), reads),
                                path,
                                manager,
                                10,
                                0,
                                segment.size())) {
            Assertions.assertThat(filter.testHashIfPresent(123)).isNull();
            Assertions.assertThat(reads).hasValue(0);
            Assertions.assertThat(filter.testHash(123)).isTrue();
            Assertions.assertThat(filter.testHashIfPresent(123)).isTrue();
            Assertions.assertThat(filter.testHashIfPresent(124)).isFalse();
            Assertions.assertThat(reads).hasValue(1);

            manager.invalidFile(path);
            Assertions.assertThat(filter.testHashIfPresent(123)).isNull();
            Assertions.assertThat(reads).hasValue(1);
            Assertions.assertThat(filter.bloomFilter().getMemorySegment()).isNull();
            Assertions.assertThat(filter.testHash(123)).isTrue();
            Assertions.assertThat(reads).hasValue(2);
        }
    }

    @Test
    public void testProbe() throws IOException {
        MemorySegment segment = MemorySegment.wrap(new byte[1000]);
        BloomFilter bloomFilter = new BloomFilter(100, segment.size());
        bloomFilter.setMemorySegment(segment, 0);
        int[] inputs = CommonTestUtils.generateRandomInts(100);
        Arrays.stream(inputs).forEach(i -> bloomFilter.addHash(Integer.hashCode(i)));
        org.apache.paimon.fs.Path filePath =
                new org.apache.paimon.fs.Path(writeFile(segment.getArray()).getAbsolutePath());
        FileIO fileIO = LocalFileIO.create();

        CacheManager cacheManager = new CacheManager(MemorySize.ofMebiBytes(1), 0.1);
        FileBasedBloomFilter filter =
                new FileBasedBloomFilter(
                        fileIO.newInputStream(filePath), filePath, cacheManager, 100, 0, 1000);

        Arrays.stream(inputs)
                .forEach(i -> Assertions.assertThat(filter.testHash(Integer.hashCode(i))).isTrue());
        filter.close();
        Assertions.assertThat(cacheManager.dataCache().asMap()).isEmpty();
        Assertions.assertThat(cacheManager.indexCache().asMap()).isEmpty();
        Assertions.assertThat(filter.bloomFilter().getMemorySegment()).isNull();
    }

    @Test
    void testSharedFilterReloadsAfterFileInvalidation() throws Exception {
        byte[] bytes = new byte[64];
        Arrays.fill(bytes, (byte) 0xff);
        AtomicInteger reads = new AtomicInteger();
        org.apache.paimon.fs.Path path = new org.apache.paimon.fs.Path("shared-filter");
        try (CacheManager manager = new CacheManager(MemorySize.ofKibiBytes(64), 0.5)) {
            FileBasedBloomFilter first =
                    new FileBasedBloomFilter(
                            countingInput(bytes, reads), path, manager, 10, 0, bytes.length);
            FileBasedBloomFilter second =
                    new FileBasedBloomFilter(
                            countingInput(bytes, reads), path, manager, 10, 0, bytes.length);
            Assertions.assertThat(first.testHash(123)).isTrue();
            Assertions.assertThat(second.testHash(123)).isTrue();
            Assertions.assertThat(reads).hasValue(1);
            manager.invalidFile(path);
            Assertions.assertThat(second.testHash(123)).isTrue();
            Assertions.assertThat(reads).hasValue(2);
            Assertions.assertThat(first.bloomFilter().getMemorySegment()).isNull();
            Assertions.assertThat(second.bloomFilter().getMemorySegment()).isNull();
        }
    }

    private static ByteArraySeekableStream countingInput(byte[] bytes, AtomicInteger reads) {
        return new ByteArraySeekableStream(bytes) {
            @Override
            public int read(byte[] b, int offset, int length) throws IOException {
                reads.incrementAndGet();
                return super.read(b, offset, length);
            }
        };
    }

    private File writeFile(byte[] bytes) throws IOException {
        File file = new File(tempDir.toFile(), UUID.randomUUID().toString());
        if (!file.createNewFile()) {
            throw new IOException("Can not create: " + file);
        }
        Files.write(file.toPath(), bytes, StandardOpenOption.WRITE);
        return file;
    }
}
