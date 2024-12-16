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

package org.apache.paimon.benchmark.cache;

import org.apache.paimon.benchmark.Benchmark;
import org.apache.paimon.io.cache.Cache;
import org.apache.paimon.io.cache.CacheKey;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.options.MemorySize;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Benchmark for measure the performance for cache. */
public class CacheManagerBenchmark {
    @TempDir Path tempDir;

    @Test
    public void testCache() throws Exception {
        Benchmark benchmark =
                new Benchmark("cache-benchmark", 100)
                        .setNumWarmupIters(1)
                        .setOutputPerIteration(true);
        File file1 = new File(tempDir.toFile(), "cache-benchmark1");
        assertThat(file1.createNewFile()).isTrue();
        CacheKey key1 = CacheKey.forPageIndex(new RandomAccessFile(file1, "r"), 0, 0);

        File file2 = new File(tempDir.toFile(), "cache-benchmark2");
        assertThat(file2.createNewFile()).isTrue();
        CacheKey key2 = CacheKey.forPageIndex(new RandomAccessFile(file2, "r"), 0, 0);

        for (Cache.CacheType cacheType : Cache.CacheType.values()) {
            CacheManager cacheManager = new CacheManager(cacheType, MemorySize.ofBytes(10), 0.1);
            benchmark.addCase(
                    String.format("cache-%s", cacheType.toString()),
                    5,
                    () -> {
                        try {
                            final int count = 10;
                            for (int i = 0; i < count; i++) {
                                cacheManager.getPage(
                                        i < count / 2 ? key1 : key2,
                                        key -> {
                                            try {
                                                Thread.sleep(1000);
                                            } catch (InterruptedException e) {
                                                throw new RuntimeException(e);
                                            }
                                            return new byte[6];
                                        },
                                        key -> {});
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
        benchmark.run();
    }
}
