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

package org.apache.paimon.cache;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.cache.rcache.RCache;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.cache.CachedSeekableInputStreamTest.generateRandomFile;

/** Test for {@link RCache}. */
public class RCacheTest {

    private static @TempDir java.nio.file.Path tempDir;
    private static File file;

    @BeforeAll
    public static void setup() throws Exception {
        file = new File(tempDir.toFile(), "random.bin");
        generateRandomFile(file, 1024 * 1024 * 1024L);
    }

    @Test
    public void testMultiThreadRead() throws Exception {
        Options options = new Options();
        options.set(
                CoreOptions.BLOCK_CACHE_DISK_LOCAL_PATH,
                new File(tempDir.toFile(), UUID.randomUUID().toString()).toString());
        options.set(CoreOptions.BLOCK_CACHE_DISK_SIZE, MemorySize.parse("10gb"));
        RCache cache = RCache.getCache(new BlockCacheConfig(new CoreOptions(options)));

        // build cache
        try (FileInputStream fis = new FileInputStream(file)) {
            long offset = 0;
            while (true) {
                byte[] bytes = new byte[1024 * 1024];
                int length = fis.read(bytes);
                if (length > 0) {
                    cache.write(new CacheKey(file.getName(), offset), bytes);
                    offset += length;
                } else {
                    break;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        ExecutorService service = Executors.newFixedThreadPool(5);

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            futures.add(
                    service.submit(
                            () -> {
                                try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                                    for (int j = 0; j < 1000; j++) {
                                        int block = ThreadLocalRandom.current().nextInt(0, 1024);
                                        long off = ((long) block) << 20;
                                        byte[] bytes = new byte[1024 * 1024];
                                        raf.seek(off);
                                        raf.read(bytes);
                                        Assertions.assertArrayEquals(
                                                cache.read(new CacheKey(file.getName(), off)).get(),
                                                bytes);
                                    }
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }));
        }

        for (Future<?> future : futures) {
            future.get();
        }
        service.shutdown();
    }

    @Test
    public void testMultiThreadWrite() throws Exception {
        Options options = new Options();
        options.set(
                CoreOptions.BLOCK_CACHE_DISK_LOCAL_PATH,
                new File(tempDir.toFile(), UUID.randomUUID().toString()).toString());
        options.set(CoreOptions.BLOCK_CACHE_DISK_SIZE, MemorySize.parse("10gb"));
        RCache cache = RCache.getCache(new BlockCacheConfig(new CoreOptions(options)));
        ExecutorService service = Executors.newFixedThreadPool(5);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            futures.add(
                    service.submit(
                            () -> {
                                try (FileInputStream fis = new FileInputStream(file)) {
                                    long offset = 0;
                                    while (true) {
                                        byte[] bytes = new byte[1024 * 1024];
                                        int length = fis.read(bytes);
                                        if (length > 0) {
                                            cache.write(
                                                    new CacheKey(file.getName(), offset), bytes);
                                            offset += length;
                                        } else {
                                            break;
                                        }
                                    }
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }));
        }

        for (Future<?> future : futures) {
            future.get();
        }
        service.shutdown();
        File cacheFile = cache.getCacheFile();
        try (RandomAccessFile ori = new RandomAccessFile(file, "r");
                RandomAccessFile raf = new RandomAccessFile(cacheFile, "r")) {
            for (Map.Entry<CacheKey, Integer> entry : cache.getCache().asMap().entrySet()) {
                CacheKey key = entry.getKey();
                long blockId = (long) entry.getValue();
                raf.seek(blockId << 20);
                byte[] cachedBytes = new byte[1024 * 1024];
                raf.read(cachedBytes);

                byte[] expected = new byte[1024 * 1024];
                ori.seek(key.getOffset());
                ori.read(expected);

                Assertions.assertArrayEquals(expected, cachedBytes);
            }
        }
    }
}
