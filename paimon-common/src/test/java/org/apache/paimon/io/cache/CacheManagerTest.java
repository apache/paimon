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

package org.apache.paimon.io.cache;

import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.options.MemorySize;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for cache manager. */
public class CacheManagerTest {
    @TempDir Path tempDir;

    @Test
    @Timeout(60)
    void testCaffeineCache() throws Exception {
        File file1 = new File(tempDir.toFile(), "test.caffeine1");
        assertThat(file1.createNewFile()).isTrue();
        CacheKey key1 = CacheKey.forPageIndex(new RandomAccessFile(file1, "r"), 0, 0);

        File file2 = new File(tempDir.toFile(), "test.caffeine2");
        assertThat(file2.createNewFile()).isTrue();
        CacheKey key2 = CacheKey.forPageIndex(new RandomAccessFile(file2, "r"), 0, 0);

        for (Cache.CacheType cacheType : Cache.CacheType.values()) {
            CacheManager cacheManager = new CacheManager(cacheType, MemorySize.ofBytes(10), 0.1);
            byte[] value = new byte[6];
            Arrays.fill(value, (byte) 1);
            for (int i = 0; i < 10; i++) {
                for (int j = 0; j < 10; j++) {
                    MemorySegment segment =
                            cacheManager.getPage(
                                    j < 5 ? key1 : key2,
                                    key -> {
                                        byte[] result = new byte[6];
                                        Arrays.fill(result, (byte) 1);
                                        return result;
                                    },
                                    key -> {});
                    assertThat(segment.getHeapMemory()).isEqualTo(value);
                }
            }
        }
    }
}
