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

import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreFactory.Context;
import org.apache.paimon.options.MemorySize;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SortLookupStoreFactory}. */
public class SortLookupStoreFactoryTest {

    @TempDir Path tempDir;

    private File file;

    @BeforeEach
    public void before() throws Exception {
        file = new File(tempDir.toFile(), UUID.randomUUID().toString());
        if (!file.createNewFile()) {
            throw new IOException("Can not create file: " + file);
        }
    }

    @Test
    public void testNormal() throws IOException {
        SortLookupStoreFactory factory =
                new SortLookupStoreFactory(
                        Comparator.naturalOrder(),
                        new CacheManager(MemorySize.ofMebiBytes(1)),
                        1024,
                        "zstd");

        SortLookupStoreWriter writer = factory.createWriter(file, null);
        int valueCount = 10_000;
        for (int i = 0; i < valueCount; i++) {
            byte[] bytes = toBytes(i);
            writer.put(bytes, bytes);
        }
        Context context = writer.close();

        SortLookupStoreReader reader = factory.createReader(file, context);
        for (int i = 0; i < valueCount; i++) {
            byte[] bytes = toBytes(i);
            assertThat(fromBytes(reader.lookup(bytes))).isEqualTo(i);
        }
        reader.close();
    }

    private byte[] toBytes(int i) {
        return String.valueOf(10_000 + i).getBytes(StandardCharsets.UTF_8);
    }

    private int fromBytes(byte[] bytes) {
        return Integer.parseInt(new String(bytes, StandardCharsets.UTF_8)) - 10_000;
    }
}
