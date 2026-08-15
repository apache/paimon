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
import org.apache.paimon.utils.ByteArrayKey;
import org.apache.paimon.utils.ByteArrayLookupKey;
import org.apache.paimon.utils.SerializationUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.paimon.utils.Preconditions.checkState;

/** Concurrent partition dictionary and ordering used only by manifest run merge. */
final class ManifestEntryRunMergePartitionDictionary {

    private final Comparator<BinaryRow> comparator;
    private final Map<ByteArrayKey, Integer> ids = new ConcurrentHashMap<>();
    private final ThreadLocal<ByteArrayLookupKey> lookup =
            ThreadLocal.withInitial(ByteArrayLookupKey::new);
    private volatile BinaryRow[] partitions = new BinaryRow[16];
    private int partitionCount;
    private int[] ranks;

    ManifestEntryRunMergePartitionDictionary(Comparator<BinaryRow> comparator) {
        this.comparator = comparator;
    }

    int id(byte[] bytes) {
        ByteArrayLookupKey lookupKey = lookup.get();
        lookupKey.reset(bytes);
        try {
            Integer existing = ids.get(lookupKey);
            if (existing != null) {
                return existing;
            }
            synchronized (this) {
                existing = ids.get(lookupKey);
                if (existing != null) {
                    return existing;
                }
                checkState(ranks == null, "Manifest scan found an unknown partition.");
                byte[] canonical = Arrays.copyOf(bytes, bytes.length);
                int id = partitionCount;
                if (id == partitions.length) {
                    partitions = Arrays.copyOf(partitions, partitions.length << 1);
                }
                partitions[id] = SerializationUtils.deserializeBinaryRow(canonical);
                ids.put(new ByteArrayKey(canonical), id);
                partitionCount = id + 1;
                return id;
            }
        } finally {
            lookupKey.clear();
        }
    }

    void finish() {
        List<Integer> order = new ArrayList<>(partitionCount);
        for (int id = 0; id < partitionCount; id++) {
            order.add(id);
        }
        order.sort((left, right) -> compareIds(left, right));
        ranks = new int[partitionCount];
        int rank = 0;
        for (int position = 0; position < order.size(); position++) {
            if (position > 0 && compareIds(order.get(position - 1), order.get(position)) != 0) {
                rank++;
            }
            ranks[order.get(position)] = rank;
        }
    }

    int compareIds(int left, int right) {
        return comparator.compare(partitions[left], partitions[right]);
    }

    int rank(int id) {
        return ranks == null ? 0 : ranks[id];
    }

    BinaryRow partition(int id) {
        return partitions[id];
    }
}
