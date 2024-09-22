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

package org.apache.paimon.lookup.hash;

import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreFactory.Context;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.testutils.junit.parameterized.ParameterizedTestExtension;
import org.apache.paimon.testutils.junit.parameterized.Parameters;
import org.apache.paimon.utils.BloomFilter;
import org.apache.paimon.utils.MathUtils;
import org.apache.paimon.utils.VarLengthIntUtils;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/* This file is based on source code from the PalDB Project (https://github.com/linkedin/PalDB), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Test for {@link HashLookupStoreFactory}. */
@ExtendWith(ParameterizedTestExtension.class)
public class HashLookupStoreFactoryTest {

    @TempDir Path tempDir;

    private final RandomDataGenerator random = new RandomDataGenerator();
    private final int pageSize = 1024;

    private final boolean enableBloomFilter;
    private final CompressOptions compress;

    private File file;
    private HashLookupStoreFactory factory;

    public HashLookupStoreFactoryTest(List<Object> var) {
        this.enableBloomFilter = (Boolean) var.get(0);
        this.compress = new CompressOptions((String) var.get(1), 1);
    }

    @SuppressWarnings("unused")
    @Parameters(name = "enableBf&compress-{0}")
    public static List<List<Object>> getVarSeg() {
        return Arrays.asList(
                Arrays.asList(true, "none"),
                Arrays.asList(false, "none"),
                Arrays.asList(false, "lz4"),
                Arrays.asList(true, "lz4"));
    }

    @BeforeEach
    public void setUp() throws IOException {
        this.factory =
                new HashLookupStoreFactory(
                        new CacheManager(MemorySize.ofMebiBytes(1)), pageSize, 0.75d, compress);
        this.file = new File(tempDir.toFile(), UUID.randomUUID().toString());
        if (!file.createNewFile()) {
            throw new IOException("Can not create file: " + file);
        }
    }

    private BloomFilter.Builder createBloomFiler(boolean enabled) {
        if (!enabled) {
            return null;
        }
        return BloomFilter.builder(100, 0.01);
    }

    private byte[] toBytes(Object o) {
        return toBytes(o.toString());
    }

    private byte[] toBytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    @TestTemplate
    public void testEmpty() throws IOException {
        HashLookupStoreWriter writer =
                factory.createWriter(file, createBloomFiler(enableBloomFilter));
        Context context = writer.close();

        assertThat(file.exists()).isTrue();

        HashLookupStoreReader reader = factory.createReader(file, context);

        assertThat(reader.lookup(toBytes(1))).isNull();

        reader.close();
    }

    @TestTemplate
    public void testOneKey() throws IOException {
        HashLookupStoreWriter writer =
                factory.createWriter(file, createBloomFiler(enableBloomFilter));
        writer.put(toBytes(1), toBytes("foo"));
        Context context = writer.close();

        HashLookupStoreReader reader = factory.createReader(file, context);
        assertThat(reader.lookup(toBytes(1))).isEqualTo(toBytes("foo"));
        reader.close();
    }

    @TestTemplate
    public void testTwoFirstKeyLength() throws IOException {
        int key1 = 1;
        int key2 = 245;

        // Write
        Context context = writeStore(file, new Object[] {key1, key2}, new Object[] {1, 6});

        // Read
        HashLookupStoreReader reader = factory.createReader(file, context);
        assertThat(reader.lookup(toBytes(key1))).isEqualTo(toBytes(1));
        assertThat(reader.lookup((toBytes(key2)))).isEqualTo(toBytes(6));
        assertThat(reader.lookup(toBytes(0))).isNull();
        assertThat(reader.lookup(toBytes(6))).isNull();
        assertThat(reader.lookup(toBytes(244))).isNull();
        assertThat(reader.lookup(toBytes(246))).isNull();
        assertThat(reader.lookup(toBytes(1245))).isNull();
    }

    @TestTemplate
    public void testKeyLengthGap() throws IOException {
        int key1 = 1;
        int key2 = 2450;

        // Write
        Context context = writeStore(file, new Object[] {key1, key2}, new Object[] {1, 6});

        // Read
        HashLookupStoreReader reader = factory.createReader(file, context);
        assertThat(reader.lookup(toBytes(key1))).isEqualTo(toBytes(1));
        assertThat(reader.lookup((toBytes(key2)))).isEqualTo(toBytes(6));
        assertThat(reader.lookup(toBytes(0))).isNull();
        assertThat(reader.lookup(toBytes(6))).isNull();
        assertThat(reader.lookup(toBytes(244))).isNull();
        assertThat(reader.lookup(toBytes(267))).isNull();
        assertThat(reader.lookup(toBytes(2449))).isNull();
        assertThat(reader.lookup(toBytes(2451))).isNull();
        assertThat(reader.lookup(toBytes(2454441))).isNull();
    }

    @TestTemplate
    public void testKeyLengthStartTwo() throws IOException {
        int key1 = 245;
        int key2 = 2450;

        // Write
        Context context = writeStore(file, new Object[] {key1, key2}, new Object[] {1, 6});

        // Read
        HashLookupStoreReader reader = factory.createReader(file, context);
        assertThat(reader.lookup(toBytes(key1))).isEqualTo(toBytes(1));
        assertThat(reader.lookup((toBytes(key2)))).isEqualTo(toBytes(6));
        assertThat(reader.lookup(toBytes(6))).isNull();
        assertThat(reader.lookup(toBytes(244))).isNull();
        assertThat(reader.lookup(toBytes(267))).isNull();
        assertThat(reader.lookup(toBytes(2449))).isNull();
        assertThat(reader.lookup(toBytes(2451))).isNull();
        assertThat(reader.lookup(toBytes(2454441))).isNull();
    }

    @TestTemplate
    public void testDataOnTwoBuffers() throws IOException {
        Object[] keys = new Object[] {1, 2, 3};
        Object[] values =
                new Object[] {
                    generateStringData(100), generateStringData(10000), generateStringData(100)
                };

        int byteSize = toBytes(values[0]).length + toBytes(values[1]).length;

        int pageSize = MathUtils.roundDownToPowerOf2(byteSize - 100);

        factory =
                new HashLookupStoreFactory(
                        new CacheManager(MemorySize.ofMebiBytes(1)), pageSize, 0.75d, compress);

        Context context = writeStore(factory, file, keys, values);

        // Read
        HashLookupStoreReader reader = factory.createReader(file, context);
        for (int i = 0; i < keys.length; i++) {
            assertThat(reader.lookup(toBytes(keys[i]))).isEqualTo(toBytes(values[i]));
        }
    }

    @TestTemplate
    public void testDataSizeOnTwoBuffers() throws IOException {
        Object[] keys = new Object[] {1, 2, 3};
        Object[] values =
                new Object[] {
                    generateStringData(100), generateStringData(10000), generateStringData(100)
                };

        byte[] b1 = toBytes(values[0]);
        byte[] b2 = toBytes(values[1]);
        int byteSize = b1.length + b2.length;
        int sizeSize =
                VarLengthIntUtils.encodeInt(new DataOutputSerializer(4), b1.length)
                        + VarLengthIntUtils.encodeInt(new DataOutputSerializer(4), b2.length);

        int pageSize = MathUtils.roundDownToPowerOf2(byteSize + sizeSize + 3);

        factory =
                new HashLookupStoreFactory(
                        new CacheManager(MemorySize.ofMebiBytes(1)), pageSize, 0.75d, compress);

        Context context = writeStore(file, keys, values);

        HashLookupStoreReader reader = factory.createReader(file, context);
        for (int i = 0; i < keys.length; i++) {
            assertThat(reader.lookup(toBytes(keys[i]))).isEqualTo(toBytes(values[i]));
        }
    }

    @TestTemplate
    public void testReadStringToString() throws IOException {
        testReadKeyToString(generateStringKeys(100));
    }

    @TestTemplate
    public void testReadIntToString() throws IOException {
        testReadKeyToString(generateIntKeys(100));
    }

    @TestTemplate
    public void testReadDoubleToString() throws IOException {
        testReadKeyToString(generateDoubleKeys(100));
    }

    @TestTemplate
    public void testReadLongToString() throws IOException {
        testReadKeyToString(generateLongKeys(100));
    }

    @TestTemplate
    public void testReadStringToInt() throws IOException {
        testReadKeyToInt(generateStringKeys(100));
    }

    @TestTemplate
    public void testReadByteToInt() throws IOException {
        testReadKeyToInt(generateByteKeys(100));
    }

    @TestTemplate
    public void testReadIntToInt() throws IOException {
        testReadKeyToInt(generateIntKeys(100));
    }

    @TestTemplate
    public void testReadCompoundToString() throws IOException {
        testReadKeyToString(generateCompoundKeys(100));
    }

    @TestTemplate
    public void testReadCompoundByteToString() throws IOException {
        testReadKeyToString(new Object[] {generateCompoundByteKey()});
    }

    @TestTemplate
    public void testCacheExpiration() throws IOException {
        int len = 1000;
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        Object[] keys = new Object[len];
        Object[] values = new Object[len];
        for (int i = 0; i < len; i++) {
            keys[i] = rnd.nextInt();
            values[i] = generateStringData(100);
        }

        // Write
        Context context = writeStore(file, keys, values);

        // Read
        factory =
                new HashLookupStoreFactory(
                        new CacheManager(new MemorySize(8096)), pageSize, 0.75d, compress);
        HashLookupStoreReader reader = factory.createReader(file, context);
        for (int i = 0; i < keys.length; i++) {
            assertThat(reader.lookup(toBytes(keys[i]))).isEqualTo(toBytes(values[i]));
        }
    }

    @TestTemplate
    public void testIterate() throws IOException {
        Integer[] keys = generateIntKeys(100);
        String[] values = generateStringData(keys.length, 12);

        // Write
        Context context = writeStore(file, keys, values);

        // Sets
        Set<Integer> keysSet = new HashSet<>(Arrays.asList(keys));
        Set<String> valuesSet = new HashSet<>(Arrays.asList(values));

        // Read
        HashLookupStoreReader reader = factory.createReader(file, context);
        Iterator<Map.Entry<byte[], byte[]>> itr = reader.iterator();
        for (int i = 0; i < keys.length; i++) {
            assertThat(itr.hasNext()).isTrue();
            Map.Entry<byte[], byte[]> entry = itr.next();
            assertThat(entry).isNotNull();
            assertThat(keysSet.remove(Integer.valueOf(new String(entry.getKey())))).isTrue();
            assertThat(valuesSet.remove(new String(entry.getValue()))).isTrue();

            assertThat(reader.lookup(entry.getKey())).isEqualTo(entry.getValue());
        }
        assertThat(itr.hasNext()).isFalse();
        reader.close();

        assertThat(keysSet).isEmpty();
        assertThat(valuesSet).isEmpty();
    }

    // UTILITY

    private void testReadKeyToString(Object[] keys) throws IOException {
        // Write
        Object[] values = generateStringData(keys.length, 10);
        Context context = writeStore(file, keys, values);

        // Read
        HashLookupStoreReader reader = factory.createReader(file, context);

        for (int i = 0; i < keys.length; i++) {
            assertThat(reader.lookup(toBytes(keys[i]))).isEqualTo(toBytes(values[i]));
        }

        reader.close();
    }

    private void testReadKeyToInt(Object[] keys) throws IOException {
        // Write
        Integer[] values = generateIntData(keys.length);
        Context context = writeStore(file, keys, values);

        // Read
        HashLookupStoreReader reader = factory.createReader(file, context);

        for (int i = 0; i < keys.length; i++) {
            assertThat(reader.lookup(toBytes(keys[i]))).isEqualTo(toBytes(values[i]));
        }

        reader.close();
    }

    private Context writeStore(File location, Object[] keys, Object[] values) throws IOException {
        return writeStore(factory, location, keys, values);
    }

    private Context writeStore(
            HashLookupStoreFactory factory, File location, Object[] keys, Object[] values)
            throws IOException {
        HashLookupStoreWriter writer =
                factory.createWriter(location, createBloomFiler(enableBloomFilter));
        for (int i = 0; i < keys.length; i++) {
            writer.put(toBytes(keys[i]), toBytes(values[i]));
        }
        return writer.close();
    }

    private Integer[] generateIntKeys(int count) {
        Integer[] res = new Integer[count];
        for (int i = 0; i < count; i++) {
            res[i] = i;
        }
        return res;
    }

    private String[] generateStringKeys(int count) {
        String[] res = new String[count];
        for (int i = 0; i < count; i++) {
            res[i] = i + "";
        }
        return res;
    }

    private Byte[] generateByteKeys(int count) {
        if (count > 127) {
            throw new RuntimeException("Too large range");
        }
        Byte[] res = new Byte[count];
        for (int i = 0; i < count; i++) {
            res[i] = (byte) i;
        }
        return res;
    }

    private Double[] generateDoubleKeys(int count) {
        Double[] res = new Double[count];
        for (int i = 0; i < count; i++) {
            res[i] = (double) i;
        }
        return res;
    }

    private Long[] generateLongKeys(int count) {
        Long[] res = new Long[count];
        for (int i = 0; i < count; i++) {
            res[i] = (long) i;
        }
        return res;
    }

    private Object[] generateCompoundKeys(int count) {
        Object[] res = new Object[count];
        Random random = new Random(345);
        for (int i = 0; i < count; i++) {
            Object[] k = new Object[] {(byte) random.nextInt(10), i};
            res[i] = k;
        }
        return res;
    }

    private Object[] generateCompoundByteKey() {
        Object[] res = new Object[2];
        res[0] = (byte) 6;
        res[1] = (byte) 0;
        return res;
    }

    private String generateStringData(int letters) {
        return random.nextHexString(letters);
    }

    private String[] generateStringData(int count, int letters) {
        String[] res = new String[count];
        for (int i = 0; i < count; i++) {
            res[i] = random.nextHexString(letters);
        }
        return res;
    }

    private Integer[] generateIntData(int count) {
        Integer[] res = new Integer[count];
        Random random = new Random(count + 34593263544354353L);
        for (int i = 0; i < count; i++) {
            res[i] = random.nextInt(1000000);
        }
        return res;
    }
}
