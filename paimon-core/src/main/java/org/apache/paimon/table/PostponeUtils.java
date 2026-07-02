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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.CoreOptions.BUCKET;
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
        Map<BinaryRow, Integer> knownNumBuckets = new HashMap<>();
        List<SimpleFileEntry> simpleFileEntries =
                table.store().newScan().onlyReadRealBuckets().readSimpleEntries();
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

    /** Returns row counts of current active files in the postpone bucket. */
    public static Map<BinaryRow, Long> getPostponeRowCounts(FileStoreTable table) {
        Map<BinaryRow, Long> rowCounts = new HashMap<>();
        Iterator<ManifestEntry> iterator =
                table.newSnapshotReader().withBucket(BucketMode.POSTPONE_BUCKET).readFileIterator();
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

    public static FileStoreTable tableForCommit(FileStoreTable table) {
        return table.copy(
                Collections.singletonMap(BUCKET.key(), String.valueOf(BucketMode.POSTPONE_BUCKET)));
    }
}
