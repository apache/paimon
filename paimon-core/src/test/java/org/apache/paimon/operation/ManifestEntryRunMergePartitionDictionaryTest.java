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

package org.apache.paimon.operation;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ManifestEntryRunMergePartitionDictionary}. */
class ManifestEntryRunMergePartitionDictionaryTest {

    @Test
    void testComparatorEqualPartitionsKeepDistinctIds() {
        ManifestEntryRunMergePartitionDictionary dictionary =
                new ManifestEntryRunMergePartitionDictionary((left, right) -> 0);
        int first = dictionary.id(partitionBytes(1));
        int second = dictionary.id(partitionBytes(2));

        dictionary.finish();

        assertThat(first).isNotEqualTo(second);
        assertThat(dictionary.partition(first).getInt(0)).isEqualTo(1);
        assertThat(dictionary.partition(second).getInt(0)).isEqualTo(2);
        assertThat(dictionary.rank(first)).isEqualTo(dictionary.rank(second));
    }

    @Test
    void testConcurrentCollectionAndRanks() throws Exception {
        ManifestEntryRunMergePartitionDictionary dictionary =
                new ManifestEntryRunMergePartitionDictionary(
                        (left, right) -> Integer.compare(left.getInt(0), right.getInt(0)));
        Map<Integer, Integer> ids = new ConcurrentHashMap<>();
        ExecutorService executor = Executors.newFixedThreadPool(8);
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int thread = 0; thread < 8; thread++) {
                futures.add(
                        executor.submit(
                                () -> {
                                    for (int value = 31; value >= 0; value--) {
                                        int id = dictionary.id(partitionBytes(value));
                                        Integer previous = ids.putIfAbsent(value, id);
                                        if (previous != null) {
                                            assertThat(id).isEqualTo(previous);
                                        }
                                    }
                                }));
            }
            for (Future<?> future : futures) {
                future.get();
            }
        } finally {
            executor.shutdownNow();
        }

        dictionary.finish();
        for (int value = 0; value < 32; value++) {
            int id = dictionary.id(partitionBytes(value));
            assertThat(dictionary.partition(id).getInt(0)).isEqualTo(value);
            assertThat(dictionary.rank(id)).isEqualTo(value);
        }
    }

    private static byte[] partitionBytes(int value) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, value);
        writer.complete();
        return serializeBinaryRow(row);
    }
}
