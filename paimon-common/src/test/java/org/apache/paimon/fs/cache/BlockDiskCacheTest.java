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

package org.apache.paimon.fs.cache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BlockDiskCache}. */
class BlockDiskCacheTest {

    @TempDir Path tempDir;

    private String cacheDir;

    @BeforeEach
    void setUp() {
        cacheDir = tempDir.resolve("cache").toString();
    }

    @Test
    void testPutAndGet() {
        BlockDiskCache cache = new BlockDiskCache(cacheDir, Long.MAX_VALUE, 64);
        byte[] data = "hello world block".getBytes();
        cache.putBlock("file1.index", 0, data);
        assertThat(cache.getBlock("file1.index", 0)).isEqualTo(data);
    }

    @Test
    void testCacheMiss() {
        BlockDiskCache cache = new BlockDiskCache(cacheDir, Long.MAX_VALUE, 64);
        assertThat(cache.getBlock("nonexistent", 0)).isNull();
    }

    @Test
    void testDifferentKeys() {
        BlockDiskCache cache = new BlockDiskCache(cacheDir, Long.MAX_VALUE, 64);
        cache.putBlock("file1", 0, "block0".getBytes());
        cache.putBlock("file1", 1, "block1".getBytes());
        cache.putBlock("file2", 0, "other0".getBytes());

        assertThat(cache.getBlock("file1", 0)).isEqualTo("block0".getBytes());
        assertThat(cache.getBlock("file1", 1)).isEqualTo("block1".getBytes());
        assertThat(cache.getBlock("file2", 0)).isEqualTo("other0".getBytes());
    }

    @Test
    void testDuplicatePutIsNoop() {
        BlockDiskCache cache = new BlockDiskCache(cacheDir, Long.MAX_VALUE, 64);
        cache.putBlock("file1", 0, "original".getBytes());
        cache.putBlock("file1", 0, "duplicate".getBytes());
        assertThat(cache.getBlock("file1", 0)).isEqualTo("original".getBytes());
    }

    @Test
    void testEviction() {
        BlockDiskCache cache = new BlockDiskCache(cacheDir, 100, 64);
        byte[] block0 = new byte[60];
        byte[] block1 = new byte[60];
        java.util.Arrays.fill(block0, (byte) 'a');
        java.util.Arrays.fill(block1, (byte) 'b');
        cache.putBlock("f", 0, block0);
        cache.putBlock("f", 1, block1);
        // total 120 > 100, at least one block should be evicted
        byte[] remaining0 = cache.getBlock("f", 0);
        byte[] remaining1 = cache.getBlock("f", 1);
        assertThat(remaining0 == null || remaining1 == null).isTrue();
    }

    @Test
    void testScanSizeOnRestart() {
        BlockDiskCache cache1 = new BlockDiskCache(cacheDir, Long.MAX_VALUE, 64);
        byte[] data0 = new byte[100];
        byte[] data1 = new byte[200];
        java.util.Arrays.fill(data0, (byte) 'x');
        java.util.Arrays.fill(data1, (byte) 'y');
        cache1.putBlock("f", 0, data0);
        cache1.putBlock("f", 1, data1);

        BlockDiskCache cache2 = new BlockDiskCache(cacheDir, Long.MAX_VALUE, 64);
        assertThat(cache2.currentSize()).isEqualTo(300);
        assertThat(cache2.getBlock("f", 0)).isEqualTo(data0);
        assertThat(cache2.getBlock("f", 1)).isEqualTo(data1);
    }

    @Test
    void testCacheDirCreated() {
        String deepDir = tempDir.resolve("sub").resolve("deep").toString();
        new BlockDiskCache(deepDir, Long.MAX_VALUE, 64);
        assertThat(new java.io.File(deepDir).isDirectory()).isTrue();
    }

    @Test
    void testConcurrentPutGet() throws InterruptedException {
        BlockDiskCache cache = new BlockDiskCache(cacheDir, Long.MAX_VALUE, 64);
        List<Throwable> errors = new CopyOnWriteArrayList<>();

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            final int idx = i;
            threads.add(
                    new Thread(
                            () -> {
                                try {
                                    byte[] data = new byte[100];
                                    java.util.Arrays.fill(data, (byte) (idx % 256));
                                    cache.putBlock("concurrent", idx, data);
                                } catch (Throwable e) {
                                    errors.add(e);
                                }
                            }));
            threads.add(
                    new Thread(
                            () -> {
                                try {
                                    byte[] result = cache.getBlock("concurrent", idx);
                                    if (result != null) {
                                        byte[] expected = new byte[100];
                                        java.util.Arrays.fill(expected, (byte) (idx % 256));
                                        assertThat(result).isEqualTo(expected);
                                    }
                                } catch (Throwable e) {
                                    errors.add(e);
                                }
                            }));
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
        assertThat(errors).isEmpty();
    }

    @Test
    void testUnlimitedCacheSkipsEviction() {
        BlockDiskCache cache = new BlockDiskCache(cacheDir, Long.MAX_VALUE, 64);
        for (int i = 0; i < 50; i++) {
            byte[] data = new byte[100];
            java.util.Arrays.fill(data, (byte) 'x');
            cache.putBlock("f", i, data);
        }
        for (int i = 0; i < 50; i++) {
            assertThat(cache.getBlock("f", i)).isNotNull();
        }
    }
}
