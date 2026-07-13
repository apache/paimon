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

package org.apache.paimon.table;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.COMMIT_STRICT_MODE_LAST_SAFE_SNAPSHOT;
import static org.apache.paimon.CoreOptions.WRITE_ONLY;

/** Utils for postpone table. */
public class PostponeUtils {

    public static int computeBucketNumByRowCount(long rowCount, long targetRowNumPerBucket) {
        if (targetRowNumPerBucket <= 0) {
            throw new IllegalArgumentException(
                    "Option 'postpone.target-row-num-per-bucket' must be greater than 0.");
        }

        long bucketNum = rowCount <= 0 ? 1 : (rowCount - 1) / targetRowNumPerBucket + 1;
        if (bucketNum > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Computed postpone bucket number "
                            + bucketNum
                            + " exceeds the maximum integer value (Integer.MAX_VALUE = "
                            + Integer.MAX_VALUE
                            + "). Consider increasing 'postpone.target-row-num-per-bucket' "
                            + "to reduce the bucket count.");
        }
        return (int) bucketNum;
    }

    public static int determineBucketNum(
            BinaryRow partition,
            Map<BinaryRow, Integer> knownNumBuckets,
            Optional<Long> targetRowNumPerBucket,
            Map<BinaryRow, Long> postponeRowCounts,
            int defaultBucketNum) {
        return determineBucketNum(
                partition,
                knownNumBuckets,
                targetRowNumPerBucket.orElse(null),
                postponeRowCounts,
                defaultBucketNum);
    }

    public static int determineBucketNum(
            BinaryRow partition,
            Map<BinaryRow, Integer> knownNumBuckets,
            @Nullable Long targetRowNumPerBucket,
            Map<BinaryRow, Long> postponeRowCounts,
            int defaultBucketNum) {
        Integer knownBucketNum = knownNumBuckets.get(partition);
        if (knownBucketNum != null) {
            return knownBucketNum;
        } else if (targetRowNumPerBucket != null) {
            return computeBucketNumByRowCount(
                    postponeRowCounts.getOrDefault(partition, 0L), targetRowNumPerBucket);
        } else {
            return defaultBucketNum;
        }
    }

    public static Map<BinaryRow, Integer> getKnownNumBuckets(FileStoreTable table) {
        return getKnownNumBuckets(
                table.store().newScan().onlyReadRealBuckets().readSimpleEntries());
    }

    public static Map<BinaryRow, Integer> getKnownNumBuckets(
            FileStoreTable table, long snapshotId) {
        return getKnownNumBuckets(
                table.store()
                        .newScan()
                        .withSnapshot(snapshotId)
                        .onlyReadRealBuckets()
                        .readSimpleEntries());
    }

    private static Map<BinaryRow, Integer> getKnownNumBuckets(
            List<SimpleFileEntry> simpleFileEntries) {
        Map<BinaryRow, Integer> knownNumBuckets = new HashMap<>();
        for (SimpleFileEntry entry : simpleFileEntries) {
            if (entry.totalBuckets() >= 0) {
                Integer oldTotalBuckets =
                        knownNumBuckets.put(entry.partition(), entry.totalBuckets());
                if (oldTotalBuckets != null && oldTotalBuckets != entry.totalBuckets()) {
                    throw new IllegalStateException(
                            "Partition "
                                    + entry.partition()
                                    + " has different totalBuckets "
                                    + oldTotalBuckets
                                    + " and "
                                    + entry.totalBuckets());
                }
            }
        }
        return knownNumBuckets;
    }

    /** Returns real buckets containing active Level-0 files in the specified snapshot. */
    public static List<CompactBucket> getLevel0Buckets(FileStoreTable table, long snapshotId) {
        List<SimpleFileEntry> entries =
                table.store()
                        .newScan()
                        .withSnapshot(snapshotId)
                        .onlyReadRealBuckets()
                        .readSimpleEntries();
        Set<CompactBucket> buckets = new LinkedHashSet<>();
        for (SimpleFileEntry entry : entries) {
            if (entry.bucket() >= 0 && entry.totalBuckets() > 0 && entry.level() == 0) {
                buckets.add(
                        new CompactBucket(entry.partition(), entry.bucket(), entry.totalBuckets()));
            }
        }
        return new ArrayList<>(buckets);
    }

    /** Returns row counts of current active files in the postpone bucket. */
    public static Map<BinaryRow, Long> getPostponeRowCounts(FileStoreTable table) {
        return getPostponeRowCounts(
                table.newSnapshotReader()
                        .withBucket(BucketMode.POSTPONE_BUCKET)
                        .readFileIterator());
    }

    /** Returns row counts of active postpone files in the specified snapshot. */
    public static Map<BinaryRow, Long> getPostponeRowCounts(FileStoreTable table, long snapshotId) {
        return getPostponeRowCounts(
                table.newSnapshotReader()
                        .withSnapshot(snapshotId)
                        .withBucket(BucketMode.POSTPONE_BUCKET)
                        .readFileIterator());
    }

    private static Map<BinaryRow, Long> getPostponeRowCounts(Iterator<ManifestEntry> iterator) {
        Map<BinaryRow, Long> rowCounts = new HashMap<>();
        while (iterator.hasNext()) {
            ManifestEntry entry = iterator.next();
            rowCounts.merge(entry.partition(), entry.file().rowCount(), Long::sum);
        }
        return rowCounts;
    }

    public static FileStoreTable tableForFixBucketWrite(FileStoreTable table) {
        Map<String, String> batchWriteOptions = new HashMap<>();
        batchWriteOptions.put(WRITE_ONLY.key(), "true");
        // It's just used to create merge tree writer for writing files to fixed bucket.
        // The real bucket number is determined at runtime.
        batchWriteOptions.put(BUCKET.key(), "1");
        return table.copy(batchWriteOptions);
    }

    public static FileStoreTable tableForPostponeCompact(
            FileStoreTable table, int numBuckets, long snapshotId) {
        Map<String, String> compactOptions = new HashMap<>();
        compactOptions.put(BUCKET.key(), String.valueOf(numBuckets));
        compactOptions.put(WRITE_ONLY.key(), "false");
        compactOptions.put(COMMIT_STRICT_MODE_LAST_SAFE_SNAPSHOT.key(), String.valueOf(snapshotId));
        return table.copy(compactOptions);
    }

    public static FileStoreTable tableForCommit(FileStoreTable table) {
        return table.copy(
                Collections.singletonMap(BUCKET.key(), String.valueOf(BucketMode.POSTPONE_BUCKET)));
    }

    /** A real bucket which requires background compaction. */
    public static final class CompactBucket implements Serializable {

        private static final long serialVersionUID = 1L;

        private final BinaryRow partition;
        private final int bucket;
        private final int totalBuckets;

        public CompactBucket(BinaryRow partition, int bucket, int totalBuckets) {
            this.partition = partition.copy();
            this.bucket = bucket;
            this.totalBuckets = totalBuckets;
        }

        public BinaryRow partition() {
            return partition;
        }

        public int bucket() {
            return bucket;
        }

        public int totalBuckets() {
            return totalBuckets;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof CompactBucket)) {
                return false;
            }
            CompactBucket that = (CompactBucket) o;
            return bucket == that.bucket
                    && totalBuckets == that.totalBuckets
                    && Objects.equals(partition, that.partition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partition, bucket, totalBuckets);
        }
    }
}
