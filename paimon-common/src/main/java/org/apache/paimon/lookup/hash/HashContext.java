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

import org.apache.paimon.lookup.LookupStoreFactory.Context;

/** Context for {@link HashLookupStoreFactory}. */
public class HashContext implements Context {

    // is bloom filter enabled
    final boolean bloomFilterEnabled;
    // expected entries for bloom filter
    final long bloomFilterExpectedEntries;
    // bytes for bloom filter
    final int bloomFilterBytes;

    // Key count for each key length
    final int[] keyCounts;
    // Slot size for each key length
    final int[] slotSizes;
    // Number of slots for each key length
    final int[] slots;
    // Offset of the index for different key length
    final int[] indexOffsets;
    // Offset of the data for different key length
    final long[] dataOffsets;

    final long uncompressBytes;
    final long[] compressPages;

    public HashContext(
            boolean bloomFilterEnabled,
            long bloomFilterExpectedEntries,
            int bloomFilterBytes,
            int[] keyCounts,
            int[] slotSizes,
            int[] slots,
            int[] indexOffsets,
            long[] dataOffsets,
            long uncompressBytes,
            long[] compressPages) {
        this.bloomFilterEnabled = bloomFilterEnabled;
        this.bloomFilterExpectedEntries = bloomFilterExpectedEntries;
        this.bloomFilterBytes = bloomFilterBytes;
        this.keyCounts = keyCounts;
        this.slotSizes = slotSizes;
        this.slots = slots;
        this.indexOffsets = indexOffsets;
        this.dataOffsets = dataOffsets;
        this.uncompressBytes = uncompressBytes;
        this.compressPages = compressPages;
    }

    public HashContext copy(long uncompressBytes, long[] compressPages) {
        return new HashContext(
                bloomFilterEnabled,
                bloomFilterExpectedEntries,
                bloomFilterBytes,
                keyCounts,
                slotSizes,
                slots,
                indexOffsets,
                dataOffsets,
                uncompressBytes,
                compressPages);
    }
}
