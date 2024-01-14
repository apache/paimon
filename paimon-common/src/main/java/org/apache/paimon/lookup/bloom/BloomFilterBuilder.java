/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.paimon.lookup.bloom;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.utils.Preconditions;

/** Bloom filter based on one memory segment. */
public class BloomFilterBuilder {

    private final MemorySegment buffer;
    private final BloomFilter filter;
    private final long numRecords;

    public BloomFilterBuilder(MemorySegment buffer, long numRecords) {
        this.buffer = buffer;
        this.filter = new BloomFilter(numRecords, buffer.size());
        filter.setBitsLocation(buffer, 0);
        this.numRecords = numRecords;
    }

    public boolean testHash(int hash) {
        return filter.testHash(hash);
    }

    public void addHash(int hash) {
        filter.addHash(hash);
    }

    public MemorySegment getBuffer() {
        return buffer;
    }

    public long getNumRecords() {
        return numRecords;
    }

    public static BloomFilterBuilder bfBuilder(long expectedRow, double fpp) {
        int numBytes = (int) Math.ceil(BloomFilter.optimalNumOfBits(expectedRow, fpp) / 8D);
        Preconditions.checkArgument(
                numBytes > 0,
                "The optimal bits should > 0. expectedRow: %s, fpp: %s",
                expectedRow,
                fpp);
        return new BloomFilterBuilder(MemorySegment.wrap(new byte[numBytes]), expectedRow);
    }

    @VisibleForTesting
    public BloomFilter getFilter() {
        return filter;
    }
}
