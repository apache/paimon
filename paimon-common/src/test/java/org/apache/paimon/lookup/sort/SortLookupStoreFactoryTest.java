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

import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreFactory.Context;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.testutils.junit.parameterized.ParameterizedTestExtension;
import org.apache.paimon.testutils.junit.parameterized.Parameters;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BloomFilter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SortLookupStoreFactory}. */
@ExtendWith(ParameterizedTestExtension.class)
public class SortLookupStoreFactoryTest {
    private static final int VALUE_COUNT = 10_000_000;
    private static final int QUERY_COUNT = 10_000;

    private final ThreadLocalRandom rnd = ThreadLocalRandom.current();
    private final boolean bloomFilterEnabled;
    private final CompressOptions compress;

    @TempDir Path tempDir;

    private File file;

    public SortLookupStoreFactoryTest(List<Object> var) {
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
    public void before() throws Exception {
        file = new File(tempDir.toFile(), UUID.randomUUID().toString());
        if (!file.createNewFile()) {
            throw new IOException("Can not create file: " + file);
        }
    }

    @TestTemplate
    public void testNormal() throws IOException {
        SortLookupStoreFactory factory =
                new SortLookupStoreFactory(
                        Comparator.naturalOrder(),
                        new CacheManager(MemorySize.ofMebiBytes(1)),
                        1024,
                        compress);

        SortLookupStoreWriter writer =
                factory.createWriter(file, createBloomFiler(bloomFilterEnabled));
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

        assertThat(reader.lookup(toBytes(VALUE_COUNT + 1000))).isNull();

        reader.close();
    }

    @TestTemplate
    public void testIntKey() throws IOException {
        RowCompactedSerializer keySerializer =
                new RowCompactedSerializer(RowType.of(new IntType()));
        GenericRow row = new GenericRow(1);
        SortLookupStoreFactory factory =
                new SortLookupStoreFactory(
                        keySerializer.createSliceComparator(),
                        new CacheManager(MemorySize.ofMebiBytes(1)),
                        64 * 1024,
                        compress);
        SortLookupStoreWriter writer =
                factory.createWriter(file, createBloomFiler(bloomFilterEnabled));
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

        assertThat(reader.lookup(toBytes(VALUE_COUNT + 1000))).isNull();

        reader.close();
    }

    private BloomFilter.Builder createBloomFiler(boolean enabled) {
        if (!enabled) {
            return null;
        }
        return BloomFilter.builder(100, 0.01);
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
