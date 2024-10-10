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

package org.apache.paimon.benchmark.lookup;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.lookup.LookupStoreWriter;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BloomFilter;
import org.apache.paimon.utils.Pair;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/** Abstract benchmark class for lookup. */
abstract class AbstractLookupBenchmark {
    protected static final int[] VALUE_LENGTHS = {0, 64, 500, 1000, 2000};
    protected static final List<Integer> RECORD_COUNT_LIST =
            Arrays.asList(100000, 1000000, 5000000, 10000000, 15000000);
    private final ThreadLocalRandom rnd = ThreadLocalRandom.current();
    private final RowCompactedSerializer keySerializer =
            new RowCompactedSerializer(RowType.of(new IntType()));
    private final GenericRow reusedKey = new GenericRow(1);

    protected static List<List<Object>> getCountBloomList() {
        List<List<Object>> countBloomList = new ArrayList<>();
        for (Integer recordCount : RECORD_COUNT_LIST) {
            countBloomList.add(Arrays.asList(recordCount, false));
            countBloomList.add(Arrays.asList(recordCount, true));
        }
        return countBloomList;
    }

    protected byte[][] generateSequenceInputs(int start, int end) {
        int count = end - start;
        byte[][] result = new byte[count][6];
        for (int i = 0; i < count; i++) {
            result[i] = intToByteArray(i);
        }
        return result;
    }

    protected byte[][] generateRandomInputs(int start, int end) {
        return generateRandomInputs(start, end, end - start);
    }

    protected byte[][] generateRandomInputs(int start, int end, int count) {
        byte[][] result = new byte[count][6];
        for (int i = 0; i < count; i++) {
            result[i] = intToByteArray(rnd.nextInt(start, end));
        }
        return result;
    }

    protected byte[] intToByteArray(int value) {
        reusedKey.setField(0, value);
        return keySerializer.serializeToBytes(reusedKey);
    }

    protected Pair<String, LookupStoreFactory.Context> writeData(
            Path tempDir,
            CoreOptions options,
            byte[][] inputs,
            int valueLength,
            boolean sameValue,
            boolean bloomFilterEnabled)
            throws IOException {
        byte[] value1 = new byte[valueLength];
        byte[] value2 = new byte[valueLength];
        Arrays.fill(value1, (byte) 1);
        Arrays.fill(value2, (byte) 2);
        LookupStoreFactory factory =
                LookupStoreFactory.create(
                        options,
                        new CacheManager(MemorySize.ofMebiBytes(10)),
                        keySerializer.createSliceComparator());

        File file = new File(tempDir.toFile(), UUID.randomUUID().toString());
        LookupStoreWriter writer = factory.createWriter(file, createBloomFiler(bloomFilterEnabled));
        int i = 0;
        for (byte[] input : inputs) {
            if (sameValue) {
                writer.put(input, value1);
            } else {
                if (i == 0) {
                    writer.put(input, value1);
                } else {
                    writer.put(input, value2);
                }
                i = (i + 1) % 2;
            }
        }
        LookupStoreFactory.Context context = writer.close();
        return Pair.of(file.getAbsolutePath(), context);
    }

    private BloomFilter.Builder createBloomFiler(boolean enabled) {
        if (!enabled) {
            return null;
        }
        return BloomFilter.builder(100, 0.01);
    }
}
