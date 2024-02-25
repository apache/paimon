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

package org.apache.paimon.benchmark;

import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.hash.HashLookupStoreFactory;
import org.apache.paimon.lookup.hash.HashLookupStoreReader;
import org.apache.paimon.lookup.hash.HashLookupStoreWriter;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.utils.BloomFilter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/** Benchmark for measure the bloom filter for lookup. */
public class LookupBloomFilterBenchmark {

    @TempDir Path tempDir;
    ThreadLocalRandom rnd = ThreadLocalRandom.current();

    @Test
    public void testHighMatch() throws Exception {
        innerTest("lookup", generateSequenceInputs(0, 100000), generateRandomInputs(0, 100000));
    }

    @Test
    public void testHalfMatch() throws Exception {
        innerTest("lookup", generateSequenceInputs(0, 100000), generateRandomInputs(50000, 150000));
    }

    @Test
    public void testLowMatch() throws Exception {
        innerTest(
                "lookup", generateSequenceInputs(0, 100000), generateRandomInputs(100000, 200000));
    }

    private byte[][] generateSequenceInputs(int start, int end) {
        int count = end - start;
        byte[][] result = new byte[count][4];
        for (int i = 0; i < count; i++) {
            result[i] = intToByteArray(i);
        }
        return result;
    }

    private byte[][] generateRandomInputs(int start, int end) {
        int count = end - start;
        byte[][] result = new byte[count][4];
        for (int i = 0; i < count; i++) {
            result[i] = intToByteArray(rnd.nextInt(start, end));
        }
        return result;
    }

    public byte[] intToByteArray(int value) {
        return new byte[] {
            (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value
        };
    }

    public void innerTest(String name, byte[][] inputs, byte[][] probe) throws Exception {
        Benchmark benchmark =
                new Benchmark(name, probe.length).setNumWarmupIters(1).setOutputPerIteration(true);

        int[] valueLengths = {0, 500, 1000, 2000};
        for (int valueLength : valueLengths) {
            HashLookupStoreReader reader = writeData(null, inputs, valueLength);

            benchmark.addCase(
                    String.format("bf-disabled-%dB-value", valueLength),
                    5,
                    () -> {
                        try {
                            for (byte[] key : probe) {
                                reader.lookup(key);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });

            HashLookupStoreReader reader2 =
                    writeData(BloomFilter.builder(inputs.length, 0.05), inputs, valueLength);

            benchmark.addCase(
                    String.format("bf-enabled-%dB-value", valueLength),
                    5,
                    () -> {
                        try {
                            for (byte[] key : probe) {
                                reader2.lookup(key);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
        }

        benchmark.run();
    }

    private HashLookupStoreReader writeData(
            BloomFilter.Builder filter, byte[][] inputs, int valueLength) throws IOException {
        byte[] value = new byte[valueLength];
        Arrays.fill(value, (byte) 1);
        HashLookupStoreFactory factory =
                new HashLookupStoreFactory(
                        new CacheManager(MemorySize.ofMebiBytes(10)), 16 * 1024, 0.75, "none");

        File file = new File(tempDir.toFile(), UUID.randomUUID().toString());
        HashLookupStoreWriter writer = factory.createWriter(file, filter);
        for (byte[] input : inputs) {
            writer.put(input, value);
        }
        return factory.createReader(file, writer.close());
    }
}
