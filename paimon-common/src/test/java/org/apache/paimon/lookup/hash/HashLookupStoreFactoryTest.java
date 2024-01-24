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

/* This file is based on source code from the PalDB Project (https://github.com/linkedin/PalDB), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

package org.apache.paimon.lookup.hash;

import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.utils.BloomFilter;
import org.apache.paimon.utils.MathUtils;
import org.apache.paimon.utils.VarLengthIntUtils;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link HashLookupStoreFactory}. */
public class HashLookupStoreFactoryTest {

    @TempDir Path tempDir;

    private final RandomDataGenerator random = new RandomDataGenerator();
    private final int pageSize = 1024;

    private File file;
    private HashLookupStoreFactory factory;

    @BeforeEach
    public void setUp() throws IOException {
        this.factory =
                new HashLookupStoreFactory(
                        new CacheManager(MemorySize.ofMebiBytes(1)), pageSize, 0.75d);
        this.file = new File(tempDir.toFile(), UUID.randomUUID().toString());
        if (!file.createNewFile()) {
            throw new IOException("Can not create file: " + file);
        }
    }

    public static Object[] enableBloomFilter() {
        return new Object[] {false, true};
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

    @ParameterizedTest
    @MethodSource(value = "enableBloomFilter")
    public void testEmpty(boolean enableBloomFilter) throws IOException {
        HashLookupStoreWriter writer =
                factory.createWriter(file, createBloomFiler(enableBloomFilter));
        writer.close();

        assertThat(file.exists()).isTrue();

        HashLookupStoreReader reader = factory.createReader(file);

        assertThat(reader.lookup(toBytes(1))).isNull();

        reader.close();
    }

    @ParameterizedTest
    @MethodSource(value = "enableBloomFilter")
    public void testOneKey(boolean enableBloomFilter) throws IOException {
        HashLookupStoreWriter writer =
                factory.createWriter(file, createBloomFiler(enableBloomFilter));
        writer.put(toBytes(1), toBytes("foo"));
        writer.close();

        HashLookupStoreReader reader = factory.createReader(file);
        assertThat(reader.lookup(toBytes(1))).isEqualTo(toBytes("foo"));
        reader.close();
    }

    @ParameterizedTest
    @MethodSource(value = "enableBloomFilter")
    public void testTwoFirstKeyLength(boolean enableBloomFilter) throws IOException {
        int key1 = 1;
        int key2 = 245;

        // Write
        writeStore(enableBloomFilter, file, new Object[] {key1, key2}, new Object[] {1, 6});

        // Read
        HashLookupStoreReader reader = factory.createReader(file);
        assertThat(reader.lookup(toBytes(key1))).isEqualTo(toBytes(1));
        assertThat(reader.lookup((toBytes(key2)))).isEqualTo(toBytes(6));
        assertThat(reader.lookup(toBytes(0))).isNull();
        assertThat(reader.lookup(toBytes(6))).isNull();
        assertThat(reader.lookup(toBytes(244))).isNull();
        assertThat(reader.lookup(toBytes(246))).isNull();
        assertThat(reader.lookup(toBytes(1245))).isNull();
    }

    @ParameterizedTest
    @MethodSource(value = "enableBloomFilter")
    public void testKeyLengthGap(boolean enableBf) throws IOException {
        int key1 = 1;
        int key2 = 2450;

        // Write
        writeStore(enableBf, file, new Object[] {key1, key2}, new Object[] {1, 6});

        // Read
        HashLookupStoreReader reader = factory.createReader(file);
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

    @ParameterizedTest
    @MethodSource(value = "enableBloomFilter")
    public void testKeyLengthStartTwo(boolean enableBf) throws IOException {
        int key1 = 245;
        int key2 = 2450;

        // Write
        writeStore(enableBf, file, new Object[] {key1, key2}, new Object[] {1, 6});

        // Read
        HashLookupStoreReader reader = factory.createReader(file);
        assertThat(reader.lookup(toBytes(key1))).isEqualTo(toBytes(1));
        assertThat(reader.lookup((toBytes(key2)))).isEqualTo(toBytes(6));
        assertThat(reader.lookup(toBytes(6))).isNull();
        assertThat(reader.lookup(toBytes(244))).isNull();
        assertThat(reader.lookup(toBytes(267))).isNull();
        assertThat(reader.lookup(toBytes(2449))).isNull();
        assertThat(reader.lookup(toBytes(2451))).isNull();
        assertThat(reader.lookup(toBytes(2454441))).isNull();
    }

    @ParameterizedTest
    @MethodSource(value = "enableBloomFilter")
    public void testDataOnTwoBuffers(boolean enableBf) throws IOException {
        Object[] keys = new Object[] {1, 2, 3};
        Object[] values =
                new Object[] {
                    generateStringData(100), generateStringData(10000), generateStringData(100)
                };

        int byteSize = toBytes(values[0]).length + toBytes(values[1]).length;

        int pageSize = MathUtils.roundDownToPowerOf2(byteSize - 100);

        factory =
                new HashLookupStoreFactory(
                        new CacheManager(MemorySize.ofMebiBytes(1)), pageSize, 0.75d);

        writeStore(factory, enableBf, file, keys, values);

        // Read
        HashLookupStoreReader reader = factory.createReader(file);
        for (int i = 0; i < keys.length; i++) {
            assertThat(reader.lookup(toBytes(keys[i]))).isEqualTo(toBytes(values[i]));
        }
    }

    @ParameterizedTest
    @MethodSource(value = "enableBloomFilter")
    public void testDataSizeOnTwoBuffers(boolean enableBf) throws IOException {
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
                        new CacheManager(MemorySize.ofMebiBytes(1)), pageSize, 0.75d);

        writeStore(enableBf, file, keys, values);

        HashLookupStoreReader reader = factory.createReader(file);
        for (int i = 0; i < keys.length; i++) {
            assertThat(reader.lookup(toBytes(keys[i]))).isEqualTo(toBytes(values[i]));
        }
    }

    @ParameterizedTest
    @MethodSource(value = "enableBloomFilter")
    public void testReadStringToString(boolean enableBf) throws IOException {
        testReadKeyToString(generateStringKeys(100), enableBf);
    }

    @ParameterizedTest
    @MethodSource(value = "enableBloomFilter")
    public void testReadIntToString(boolean enableBf) throws IOException {
        testReadKeyToString(generateIntKeys(100), enableBf);
    }

    @ParameterizedTest
    @MethodSource(value = "enableBloomFilter")
    public void testReadDoubleToString(boolean enableBf) throws IOException {
        testReadKeyToString(generateDoubleKeys(100), enableBf);
    }

    @ParameterizedTest
    @MethodSource(value = "enableBloomFilter")
    public void testReadLongToString(boolean enableBf) throws IOException {
        testReadKeyToString(generateLongKeys(100), enableBf);
    }

    @ParameterizedTest
    @MethodSource(value = "enableBloomFilter")
    public void testReadStringToInt(boolean enableBf) throws IOException {
        testReadKeyToInt(generateStringKeys(100), enableBf);
    }

    @ParameterizedTest
    @MethodSource(value = "enableBloomFilter")
    public void testReadByteToInt(boolean enableBf) throws IOException {
        testReadKeyToInt(generateByteKeys(100), enableBf);
    }

    @ParameterizedTest
    @MethodSource(value = "enableBloomFilter")
    public void testReadIntToInt(boolean enableBf) throws IOException {
        testReadKeyToInt(generateIntKeys(100), enableBf);
    }

    @ParameterizedTest
    @MethodSource(value = "enableBloomFilter")
    public void testReadCompoundToString(boolean enableBf) throws IOException {
        testReadKeyToString(generateCompoundKeys(100), enableBf);
    }

    @ParameterizedTest
    @MethodSource(value = "enableBloomFilter")
    public void testReadCompoundByteToString(boolean enableBf) throws IOException {
        testReadKeyToString(new Object[] {generateCompoundByteKey()}, enableBf);
    }

    @ParameterizedTest
    @MethodSource(value = "enableBloomFilter")
    public void testCacheExpiration(boolean enableBf) throws IOException {
        int len = 1000;
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        Object[] keys = new Object[len];
        Object[] values = new Object[len];
        for (int i = 0; i < len; i++) {
            keys[i] = rnd.nextInt();
            values[i] = generateStringData(100);
        }

        // Write
        writeStore(enableBf, file, keys, values);

        // Read
        factory =
                new HashLookupStoreFactory(new CacheManager(new MemorySize(8096)), pageSize, 0.75d);
        HashLookupStoreReader reader = factory.createReader(file);
        for (int i = 0; i < keys.length; i++) {
            assertThat(reader.lookup(toBytes(keys[i]))).isEqualTo(toBytes(values[i]));
        }
    }

    @ParameterizedTest
    @MethodSource(value = "enableBloomFilter")
    public void testIterate(boolean enableBf) throws IOException {
        Integer[] keys = generateIntKeys(100);
        String[] values = generateStringData(keys.length, 12);

        // Write
        writeStore(enableBf, file, keys, values);

        // Sets
        Set<Integer> keysSet = new HashSet<>(Arrays.asList(keys));
        Set<String> valuesSet = new HashSet<>(Arrays.asList(values));

        // Read
        HashLookupStoreReader reader = factory.createReader(file);
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

    private void testReadKeyToString(Object[] keys, boolean enableBf) throws IOException {
        // Write
        Object[] values = generateStringData(keys.length, 10);
        writeStore(enableBf, file, keys, values);

        // Read
        HashLookupStoreReader reader = factory.createReader(file);

        for (int i = 0; i < keys.length; i++) {
            assertThat(reader.lookup(toBytes(keys[i]))).isEqualTo(toBytes(values[i]));
        }

        reader.close();
    }

    private void testReadKeyToInt(Object[] keys, boolean enableBf) throws IOException {
        // Write
        Integer[] values = generateIntData(keys.length);
        writeStore(enableBf, file, keys, values);

        // Read
        HashLookupStoreReader reader = factory.createReader(file);

        for (int i = 0; i < keys.length; i++) {
            assertThat(reader.lookup(toBytes(keys[i]))).isEqualTo(toBytes(values[i]));
        }

        reader.close();
    }

    private void writeStore(boolean enableBf, File location, Object[] keys, Object[] values)
            throws IOException {
        writeStore(factory, enableBf, location, keys, values);
    }

    private void writeStore(
            HashLookupStoreFactory factory,
            boolean enableBf,
            File location,
            Object[] keys,
            Object[] values)
            throws IOException {
        HashLookupStoreWriter writer = factory.createWriter(location, createBloomFiler(enableBf));
        for (int i = 0; i < keys.length; i++) {
            writer.put(toBytes(keys[i]), toBytes(values[i]));
        }
        writer.close();
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
