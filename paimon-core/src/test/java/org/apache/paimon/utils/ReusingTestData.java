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

import org.apache.paimon.KeyValue;
import org.apache.paimon.types.RowKind;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A simple test data structure which is mainly used for testing if other components handle reuse of
 * {@link KeyValue} correctly. Use along with {@link ReusingKeyValue}.
 */
public class ReusingTestData implements Comparable<ReusingTestData> {

    public final int key;
    public final long sequenceNumber;
    public final RowKind valueKind;
    public final long value;

    public ReusingTestData(int key, long sequenceNumber, RowKind valueKind, long value) {
        this.key = key;
        this.sequenceNumber = sequenceNumber;
        this.valueKind = valueKind;
        this.value = value;
    }

    @Override
    public int compareTo(ReusingTestData that) {
        if (key != that.key) {
            return Integer.compare(key, that.key);
        }
        int result = Long.compare(sequenceNumber, that.sequenceNumber);
        Preconditions.checkArgument(
                result != 0 || this == that,
                "Found two CompactTestData with the same sequenceNumber. This is invalid.");
        return result;
    }

    public void assertEquals(KeyValue kv) {
        assertThat(kv.key().getInt(0)).isEqualTo(key);
        assertThat(kv.sequenceNumber()).isEqualTo(sequenceNumber);
        assertThat(kv.valueKind()).isEqualTo(valueKind);
        assertThat(kv.value().getLong(0)).isEqualTo(value);
    }

    /**
     * String format:
     *
     * <ul>
     *   <li>Use | to split different {@link KeyValue}s.
     *   <li>For a certain key value, the format is <code>
     *       key, sequenceNumber, valueKind (+ or -), value</code>.
     * </ul>
     */
    public static List<ReusingTestData> parse(String s) {
        List<ReusingTestData> result = new ArrayList<>();
        for (String kv : s.split("\\|")) {
            if (kv.trim().isEmpty()) {
                continue;
            }
            String[] split = kv.split(",");
            Preconditions.checkArgument(split.length == 4, "Found invalid data string " + kv);
            result.add(
                    new ReusingTestData(
                            Integer.parseInt(split[0].trim()),
                            Long.parseLong(split[1].trim()),
                            split[2].trim().equals("+") ? RowKind.INSERT : RowKind.DELETE,
                            Long.parseLong(split[3].trim())));
        }
        return result;
    }

    public static List<ReusingTestData> generateData(int numRecords, boolean onlyAdd) {
        List<ReusingTestData> result = new ArrayList<>();
        SequenceNumberGenerator generator = new SequenceNumberGenerator();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < numRecords; i++) {
            result.add(
                    new ReusingTestData(
                            random.nextInt(numRecords),
                            generator.next(),
                            random.nextBoolean() || onlyAdd ? RowKind.INSERT : RowKind.DELETE,
                            getValue(random)));
        }
        return result;
    }

    public static List<ReusingTestData> generateOrderedNoDuplicatedKeys(
            int numRecords, boolean onlyAdd) {
        TreeMap<Integer, ReusingTestData> result = new TreeMap<>();
        SequenceNumberGenerator generator = new SequenceNumberGenerator();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        while (result.size() < numRecords) {
            int key = random.nextInt(numRecords * 3);
            result.put(
                    key,
                    new ReusingTestData(
                            key,
                            generator.next(),
                            random.nextBoolean() || onlyAdd ? RowKind.INSERT : RowKind.DELETE,
                            getValue(random)));
        }
        return new ArrayList<>(result.values());
    }

    private static long getValue(ThreadLocalRandom random) {
        int value = random.nextInt(10) - 5;
        return value == 0 ? getValue(random) : value;
    }

    private static class SequenceNumberGenerator {
        private final Set<Long> usedSequenceNumber;

        private SequenceNumberGenerator() {
            this.usedSequenceNumber = new HashSet<>();
        }

        public long next() {
            long result;
            do {
                result = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
            } while (usedSequenceNumber.contains(result));
            usedSequenceNumber.add(result);
            return result;
        }
    }
}
