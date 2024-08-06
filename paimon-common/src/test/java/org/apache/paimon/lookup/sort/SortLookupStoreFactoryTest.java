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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreFactory.Context;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SortLookupStoreFactory}. */
public class SortLookupStoreFactoryTest {
    private static final int VALUE_COUNT = 10_000_000;
    private static final int QUERY_COUNT = 10_000;

    private final ThreadLocalRandom rnd = ThreadLocalRandom.current();

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
        for (int i = 0; i < VALUE_COUNT; i++) {
            byte[] bytes = toBytes(i);
            writer.put(bytes, bytes);
        }
        Context context = writer.close();

        SortLookupStoreReader reader = factory.createReader(file, context);
        for (int i = 0; i < QUERY_COUNT; i++) {
            int query = rnd.nextInt(VALUE_COUNT);
            byte[] bytes = toBytes(query);
            assertThat(fromBytes(reader.lookup(bytes))).isEqualTo(query);
        }
        reader.close();
    }

    @Test
    public void testIntKey() throws IOException {
        RowCompactedSerializer keySerializer =
                new RowCompactedSerializer(RowType.of(new IntType()));
        GenericRow row = new GenericRow(1);
        SortLookupStoreFactory factory =
                new SortLookupStoreFactory(
                        keySerializer.createSliceComparator(),
                        new CacheManager(MemorySize.ofMebiBytes(1)),
                        64 * 1024,
                        "zstd");
        SortLookupStoreWriter writer = factory.createWriter(file, null);
        for (int i = 0; i < VALUE_COUNT; i++) {
            byte[] bytes = toBytes(keySerializer, row, i);
            writer.put(bytes, toBytes(i));
        }
        Context context = writer.close();

        SortLookupStoreReader reader = factory.createReader(file, context);
        for (int i = 0; i < QUERY_COUNT; i++) {
            int query = rnd.nextInt(VALUE_COUNT);
            byte[] bytes = toBytes(keySerializer, row, query);
            try {
                assertThat(fromBytes(reader.lookup(bytes))).isEqualTo(query);
            } catch (Exception e) {
                System.out.println("query value " + query + " failed");
                throw new RuntimeException(e);
            }
        }
        reader.close();
    }

    private byte[] toBytes(RowCompactedSerializer serializer, GenericRow row, int i) {
        row.setField(0, i);
        return serializer.serializeToBytes(row);
    }

    private byte[] toBytes(int i) {
        return String.valueOf(VALUE_COUNT + i).getBytes(StandardCharsets.UTF_8);
    }

    private int fromBytes(byte[] bytes) {
        return Integer.parseInt(new String(bytes, StandardCharsets.UTF_8)) - VALUE_COUNT;
    }
}
