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
import org.apache.paimon.data.Segments;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link Segments} for manifest entries, it builds indexes based on partitions and buckets, which
 * can make subsequent queries very fast based on {@link ManifestEntryFilters}.
 */
public class ManifestEntrySegments implements Segments {

    private final List<RichSegments> segments;
    private final long totalMemorySize;

    private final Map<BinaryRow, Map<Integer, List<RichSegments>>> indexedSegments;

    public ManifestEntrySegments(List<RichSegments> segments) {
        this.segments = segments;
        this.totalMemorySize =
                segments.stream()
                        .map(RichSegments::segments)
                        .mapToLong(Segments::totalMemorySize)
                        .sum();
        this.indexedSegments = new HashMap<>();
        for (RichSegments seg : segments) {
            indexedSegments
                    .computeIfAbsent(seg.partition(), k -> new HashMap<>())
                    .computeIfAbsent(seg.bucket(), k -> new ArrayList<>())
                    .add(seg);
        }
    }

    public List<RichSegments> segments() {
        return segments;
    }

    public Map<BinaryRow, Map<Integer, List<RichSegments>>> indexedSegments() {
        return indexedSegments;
    }

    @Override
    public long totalMemorySize() {
        return totalMemorySize;
    }

    /** Segments with partition and bucket information. */
    public static class RichSegments {

        private final BinaryRow partition;
        private final int bucket;
        private final int totalBucket;
        private final Segments segments;

        public RichSegments(BinaryRow partition, int bucket, int totalBucket, Segments segments) {
            this.partition = partition;
            this.bucket = bucket;
            this.totalBucket = totalBucket;
            this.segments = segments;
        }

        public BinaryRow partition() {
            return partition;
        }

        public int bucket() {
            return bucket;
        }

        public int totalBucket() {
            return totalBucket;
        }

        public Segments segments() {
            return segments;
        }
    }
}
