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

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.Function;

import static java.util.Comparator.comparingLong;

/** A bin packing implementation for fixed bin number. */
public class FixBinPacking {
    private FixBinPacking() {}

    public static <T> List<List<T>> pack(
            Iterable<T> items, Function<T, Long> weightFunc, int binNumber) {
        // 1. sort items first
        List<T> sorted = new ArrayList<>();
        items.forEach(sorted::add);
        sorted.sort(comparingLong(weightFunc::apply));

        // 2. packing
        PriorityQueue<Bin<T>> bins = new PriorityQueue<>();
        for (T item : sorted) {
            long weight = weightFunc.apply(item);
            Bin<T> bin = bins.size() < binNumber ? new Bin<>() : bins.poll();
            bin.add(item, weight);
            bins.add(bin);
        }

        // 3. output
        List<List<T>> packed = new ArrayList<>();
        bins.forEach(bin -> packed.add(bin.items));
        return packed;
    }

    private static class Bin<T> implements Comparable<Bin<T>> {
        private final List<T> items = new ArrayList<>();
        private long binWeight = 0L;

        void add(T item, long weight) {
            this.binWeight += weight;
            items.add(item);
        }

        @Override
        public int compareTo(Bin<T> other) {
            return Long.compare(binWeight, other.binWeight);
        }
    }
}
