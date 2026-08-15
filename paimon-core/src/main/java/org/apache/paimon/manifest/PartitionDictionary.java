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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/** Deduplicates serialized partitions and assigns compact integer identifiers. */
public final class PartitionDictionary {

    private final Map<ByteArrayKey, Integer> ids = new HashMap<>();
    private final ByteArrayLookupKey lookup = new ByteArrayLookupKey();
    private BinaryRow[] partitions = new BinaryRow[16];
    private int partitionCount;

    public int id(byte[] bytes) {
        lookup.reset(bytes);
        try {
            Integer existing = ids.get(lookup);
            if (existing != null) {
                return existing;
            }
            byte[] canonical = Arrays.copyOf(bytes, bytes.length);
            int id = partitionCount;
            if (id == partitions.length) {
                partitions = Arrays.copyOf(partitions, partitions.length << 1);
            }
            partitions[id] = SerializationUtils.deserializeBinaryRow(canonical);
            ids.put(new ByteArrayKey(canonical), id);
            partitionCount = id + 1;
            return id;
        } finally {
            lookup.clear();
        }
    }

    public BinaryRow partition(int id) {
        return partitions[id];
    }
}
