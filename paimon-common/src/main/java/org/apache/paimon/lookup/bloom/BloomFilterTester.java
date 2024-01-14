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
import org.apache.paimon.io.cache.CachedRandomInputView;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.utils.Preconditions;

import java.util.function.BiConsumer;

/** Util to apply a built bloom filter . */
public class BloomFilterTester {

    private final BloomFilter filter;
    private final CachedRandomInputView inputView;
    private final int bfIndex;
    private final int bfLength;
    private final MemorySegment[] segments;

    public BloomFilterTester(
            long numRecords, int bfIndex, int bfLength, CachedRandomInputView inputView) {
        Preconditions.checkArgument(numRecords >= 0);
        this.filter = new BloomFilter(numRecords, bfLength);
        this.segments = new MemorySegment[1];
        this.inputView = inputView;
        this.bfIndex = bfIndex;
        this.bfLength = bfLength;
    }

    public boolean testHash(int hash) {
        if (segments[0] == null) {
            MemorySegment segment =
                    inputView.read(
                            bfIndex,
                            bfLength,
                            new BiConsumer<Long, Integer>() {
                                @Override
                                public void accept(Long readOffset, Integer readLength) {
                                    segments[0] = null;
                                }
                            });
            segments[0] = segment;
        }
        filter.setBitsLocation(segments[0], 0);
        return filter.testHash(hash);
    }

    @VisibleForTesting
    public MemorySegment[] getSegments() {
        return segments;
    }
}
