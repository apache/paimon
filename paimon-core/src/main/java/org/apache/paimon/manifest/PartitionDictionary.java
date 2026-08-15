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

package org.apache.paimon.manifest;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.utils.ByteArrayKey;
import org.apache.paimon.utils.ByteArrayLookupKey;
import org.apache.paimon.utils.SerializationUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Deduplicates serialized partitions and assigns compact integer identifiers. */
public final class PartitionDictionary {

    private final Map<ByteArrayKey, Integer> ids;
    private final @Nullable ByteArrayLookupKey lookup;
    private final @Nullable ThreadLocal<ByteArrayLookupKey> concurrentLookup;
    private final @Nullable Comparator<BinaryRow> comparator;
    private volatile BinaryRow[] partitions = new BinaryRow[16];
    private int partitionCount;
    private @Nullable int[] ranks;

    /** Creates the low-overhead dictionary used by single-threaded manifest rewriting. */
    public PartitionDictionary() {
        this.ids = new HashMap<>();
        this.lookup = new ByteArrayLookupKey();
        this.concurrentLookup = null;
        this.comparator = null;
    }

    /**
     * Creates a dictionary which supports concurrent collection and comparator-compatible ranks.
     */
    public PartitionDictionary(Comparator<BinaryRow> comparator) {
        checkArgument(comparator != null, "Partition comparator cannot be null.");
        this.ids = new ConcurrentHashMap<>();
        this.lookup = null;
        this.concurrentLookup = ThreadLocal.withInitial(ByteArrayLookupKey::new);
        this.comparator = comparator;
    }

    public int id(byte[] bytes) {
        ByteArrayLookupKey lookupKey = concurrentLookup == null ? lookup : concurrentLookup.get();
        checkState(lookupKey != null, "Partition lookup key is unavailable.");
        lookupKey.reset(bytes);
        try {
            Integer existing = ids.get(lookupKey);
            if (existing != null) {
                return existing;
            }
            if (concurrentLookup != null) {
                synchronized (this) {
                    existing = ids.get(lookupKey);
                    if (existing != null) {
                        return existing;
                    }
                    return add(bytes);
                }
            }
            return add(bytes);
        } finally {
            lookupKey.clear();
        }
    }

    private int add(byte[] bytes) {
        checkState(ranks == null, "Manifest scan found a partition after ranks were assigned.");
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

    public void finish() {
        checkState(comparator != null, "Partition dictionary has no comparator.");
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

    public int compareIds(int left, int right) {
        checkState(comparator != null, "Partition dictionary has no comparator.");
        return comparator.compare(partitions[left], partitions[right]);
    }

    public int rank(int id) {
        return ranks == null ? 0 : ranks[id];
    }

    public BinaryRow partition(int id) {
        return partitions[id];
    }
}
