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

package org.apache.paimon.table.source;

import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Snapshot-scoped scalar-index result addressed by physical data-file row positions. */
public class PrimaryKeySortedIndexResult implements GlobalIndexSplitResult {

    private static final Logger LOG = LoggerFactory.getLogger(PrimaryKeySortedIndexResult.class);
    private static final int MAX_INDEXED_RANGES_PER_FILE = 4096;

    private final long snapshotId;
    private final List<Split> splits;

    PrimaryKeySortedIndexResult(PrimaryKeySortedIndexScan.EvaluatedPlan plan) {
        this.snapshotId = plan.snapshotId();
        List<Split> converted = new ArrayList<>();
        Set<DataSplit> preservedNonRawSplits = Collections.newSetFromMap(new IdentityHashMap<>());
        for (PrimaryKeySortedIndexScan.EvaluatedFile evaluatedFile : plan.files()) {
            PrimaryKeySortedIndexScan.FilePlan file = evaluatedFile.file();
            DataSplit sourceSplit = file.sourceSplit();
            if (!sourceSplit.rawConvertible()) {
                if (preservedNonRawSplits.add(sourceSplit)) {
                    converted.add(sourceSplit);
                }
                continue;
            }

            Optional<GlobalIndexResult> result = evaluatedFile.result();
            if (!result.isPresent()) {
                converted.add(toSingleFileSplit(file));
                continue;
            }

            RoaringNavigableMap64 positions = result.get().results();
            if (positions.isEmpty()) {
                continue;
            }
            List<Range> ranges = ranges(positions, file.dataFile().rowCount());
            if (ranges == null) {
                LOG.warn(
                        "Primary-key sorted index returned an invalid row position for data file "
                                + "{}; falling back to a raw scan for this file.",
                        file.dataFile().fileName());
                converted.add(toSingleFileSplit(file));
            } else {
                converted.add(new IndexedSplit(toSingleFileSplit(file), ranges, null));
            }
        }
        this.splits = Collections.unmodifiableList(converted);
    }

    @Override
    public long snapshotId() {
        return snapshotId;
    }

    @Override
    public List<Split> splits() {
        return splits;
    }

    @Override
    public RoaringNavigableMap64 results() {
        throw new UnsupportedOperationException(
                "Primary-key sorted-index results use physical file positions, not global row ids.");
    }

    private static DataSplit toSingleFileSplit(PrimaryKeySortedIndexScan.FilePlan file) {
        DataSplit source = file.sourceSplit();
        DataSplit.Builder builder =
                DataSplit.builder()
                        .withSnapshot(source.snapshotId())
                        .withPartition(source.partition())
                        .withBucket(source.bucket())
                        .withBucketPath(source.bucketPath())
                        .withTotalBuckets(source.totalBuckets())
                        .withDataFiles(Collections.singletonList(file.dataFile()))
                        .isStreaming(false)
                        .rawConvertible(false);
        if (source.deletionFiles().isPresent()) {
            builder.withDataDeletionFiles(
                    Collections.singletonList(source.deletionFiles().get().get(file.fileIndex())));
        }
        return builder.build();
    }

    private static List<Range> ranges(RoaringNavigableMap64 positions, long rowCount) {
        List<Range> ranges = new ArrayList<>();
        long from = -1;
        long to = -1;
        for (long position : positions) {
            if (position < 0 || position >= rowCount || position > Integer.MAX_VALUE) {
                return null;
            }
            if (from < 0) {
                from = position;
            } else if (position != to + 1) {
                if (ranges.size() >= MAX_INDEXED_RANGES_PER_FILE) {
                    return null;
                }
                ranges.add(new Range(from, to));
                from = position;
            }
            to = position;
        }
        if (ranges.size() >= MAX_INDEXED_RANGES_PER_FILE) {
            return null;
        }
        ranges.add(new Range(from, to));
        return ranges;
    }
}
