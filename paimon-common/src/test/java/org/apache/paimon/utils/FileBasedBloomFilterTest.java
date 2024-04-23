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

import org.apache.paimon.io.PageFileInput;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.options.MemorySize;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.UUID;

/** Test for {@link FileBasedBloomFilter}. */
public class FileBasedBloomFilterTest {

    @TempDir Path tempDir;

    @Test
    public void testProbe() throws IOException {
        MemorySegment segment = MemorySegment.wrap(new byte[1000]);
        BloomFilter.Builder builder = new BloomFilter.Builder(segment, 100);
        int[] inputs = CommonTestUtils.generateRandomInts(100);
        Arrays.stream(inputs).forEach(i -> builder.addHash(Integer.hashCode(i)));
        File file = writeFile(segment.getArray());

        CacheManager cacheManager = new CacheManager(MemorySize.ofMebiBytes(1));
        FileBasedBloomFilter filter =
                new FileBasedBloomFilter(
                        PageFileInput.create(file, 1024, null, 0, null),
                        cacheManager,
                        100,
                        0,
                        1000);

        Arrays.stream(inputs)
                .forEach(i -> Assertions.assertThat(filter.testHash(Integer.hashCode(i))).isTrue());
        cacheManager.cache().invalidateAll();
        Assertions.assertThat(filter.bloomFilter().getMemorySegment()).isNull();
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
