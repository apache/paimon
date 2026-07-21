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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PartitionEntry#merge(PartitionEntry)}. */
public class PartitionEntryTest {

    private static final BinaryRow PARTITION = BinaryRow.EMPTY_ROW;

    /**
     * Creates a PartitionEntry with the given fileCount, totalBuckets, and creation time.
     * recordCount and fileSizeInBytes are set to fileCount for simplicity.
     */
    private static PartitionEntry entry(long fileCount, int totalBuckets, long creationTime) {
        return new PartitionEntry(
                PARTITION, fileCount, fileCount, fileCount, creationTime, totalBuckets);
    }

    // -------------------------------------------------------------------------
    // Tests for totalBuckets selection based on lastFileCreationTime
    // -------------------------------------------------------------------------

    @Test
    public void testMergeTakesTotalBucketsFromNewerEntry() {
        // Old files (2 buckets, earlier creation time) merged with new files (4 buckets, later).
        // totalBuckets should come from the newer entry.
        PartitionEntry old = entry(3, 2, 1000L);
        PartitionEntry newer = entry(3, 4, 2000L);

        PartitionEntry result = old.merge(newer);
        assertThat(result.totalBuckets()).isEqualTo(4);
        assertThat(result.lastFileCreationTime()).isEqualTo(2000L);
        assertThat(result.fileCount()).isEqualTo(6);
    }

    @Test
    public void testMergeOrderDoesNotAffectTotalBuckets() {
        // Regardless of whether old.merge(newer) or newer.merge(old) is called,
        // the result must always take totalBuckets from the entry with the later creation time.
        PartitionEntry old = entry(3, 2, 1000L);
        PartitionEntry newer = entry(3, 4, 2000L);

        PartitionEntry result1 = old.merge(newer);
        PartitionEntry result2 = newer.merge(old);

        assertThat(result1.totalBuckets()).isEqualTo(4);
        assertThat(result2.totalBuckets()).isEqualTo(4);
        assertThat(result1.lastFileCreationTime()).isEqualTo(2000L);
        assertThat(result2.lastFileCreationTime()).isEqualTo(2000L);
    }

    @Test
    public void testMergeWithDeleteEntryPreservesNewerTotalBuckets() {
        // Simulates the scenario after INSERT OVERWRITE with rescale:
        // - original ADD entries (2 buckets, time=1000) still present in base manifest
        // - DELETE entries for old files (2 buckets, time=1000) in delta manifest
        // - new ADD entries (4 buckets, time=2000) in delta manifest
        //
        // The merged entry should have totalBuckets=4 (from the newest files).
        PartitionEntry originalAdd = entry(3, 2, 1000L); // original ADD (base manifest)
        PartitionEntry deleteOld = entry(-3, 2, 1000L); // DELETE old files (same timestamp)
        PartitionEntry newAdd = entry(3, 4, 2000L); // new ADD after overwrite

        // Simulate concurrent processing in any order (all 6 permutations produce same result)
        PartitionEntry r1 = originalAdd.merge(deleteOld).merge(newAdd);
        PartitionEntry r2 = originalAdd.merge(newAdd).merge(deleteOld);
        PartitionEntry r3 = deleteOld.merge(originalAdd).merge(newAdd);
        PartitionEntry r4 = deleteOld.merge(newAdd).merge(originalAdd);
        PartitionEntry r5 = newAdd.merge(originalAdd).merge(deleteOld);
        PartitionEntry r6 = newAdd.merge(deleteOld).merge(originalAdd);

        for (PartitionEntry r : new PartitionEntry[] {r1, r2, r3, r4, r5, r6}) {
            assertThat(r.totalBuckets())
                    .as("totalBuckets should be 4 regardless of merge order")
                    .isEqualTo(4);
            assertThat(r.fileCount())
                    .as("net fileCount should be 3 (original 3 files remain visible)")
                    .isEqualTo(3);
            assertThat(r.lastFileCreationTime()).isEqualTo(2000L);
        }
    }

    @Test
    public void testMergeWithEqualCreationTimeIsCommutative() {
        // When creation times are equal, merge must be commutative: a.merge(b) == b.merge(a).
        // The tie-break takes the larger totalBuckets so that the parallel, non-deterministic
        // aggregation in readPartitionEntries() always produces the same result regardless of
        // manifest processing order.
        PartitionEntry a = entry(1, 2, 1000L);
        PartitionEntry b = entry(1, 4, 1000L);

        PartitionEntry ab = a.merge(b);
        PartitionEntry ba = b.merge(a);

        assertThat(ab.totalBuckets()).isEqualTo(4); // max(2, 4) = 4
        assertThat(ba.totalBuckets()).isEqualTo(4); // max(4, 2) = 4, commutative
        assertThat(ab.fileCount()).isEqualTo(2);
        assertThat(ba.fileCount()).isEqualTo(2);
    }

    @Test
    public void testMergeAggregatesCountsCorrectly() {
        PartitionEntry a = entry(5, 4, 1000L);
        PartitionEntry b = entry(3, 4, 2000L);

        PartitionEntry result = a.merge(b);
        assertThat(result.fileCount()).isEqualTo(8);
        assertThat(result.recordCount()).isEqualTo(8);
        assertThat(result.fileSizeInBytes()).isEqualTo(8);
        assertThat(result.totalBuckets()).isEqualTo(4);
        assertThat(result.lastFileCreationTime()).isEqualTo(2000L);
    }
}
