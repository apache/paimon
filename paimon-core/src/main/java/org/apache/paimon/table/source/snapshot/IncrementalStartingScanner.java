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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.Snapshot;
import org.apache.paimon.Snapshot.CommitKind;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.PlanImpl;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** {@link StartingScanner} for incremental changes by snapshot. */
public class IncrementalStartingScanner extends AbstractStartingScanner {

    private static final Logger LOG = LoggerFactory.getLogger(IncrementalStartingScanner.class);

    private long endingSnapshotId;

    private ScanMode scanMode;

    public IncrementalStartingScanner(
            SnapshotManager snapshotManager, long start, long end, ScanMode scanMode) {
        super(snapshotManager);
        this.startingSnapshotId = start;
        this.endingSnapshotId = end;
        this.scanMode = scanMode;
    }

    @Override
    public Result scan(SnapshotReader reader) {
        // Check the validity of scan staring snapshotId.
        Optional<Result> checkResult = checkScanSnapshotIdValidity();
        if (checkResult.isPresent()) {
            return checkResult.get();
        }
        Map<SplitInfo, List<DataFileMeta>> grouped = new HashMap<>();
        for (long i = startingSnapshotId + 1; i < endingSnapshotId + 1; i++) {
            List<DataSplit> splits = readSplits(reader, snapshotManager.snapshot(i));
            for (DataSplit split : splits) {
                grouped.computeIfAbsent(
                                new SplitInfo(
                                        split.partition(),
                                        split.bucket(),
                                        // take it for false, because multiple snapshot read may
                                        // need merge for primary key table
                                        false,
                                        split.bucketPath(),
                                        split.deletionFiles().orElse(null)),
                                k -> new ArrayList<>())
                        .addAll(split.dataFiles());
            }
        }

        List<Split> result = new ArrayList<>();
        for (Map.Entry<SplitInfo, List<DataFileMeta>> entry : grouped.entrySet()) {
            BinaryRow partition = entry.getKey().partition;
            int bucket = entry.getKey().bucket;
            boolean rawConvertible = entry.getKey().rawConvertible;
            String bucketPath = entry.getKey().bucketPath;
            List<DeletionFile> deletionFiles = entry.getKey().deletionFiles;
            for (SplitGenerator.SplitGroup splitGroup :
                    reader.splitGenerator().splitForBatch(entry.getValue())) {
                DataSplit.Builder dataSplitBuilder =
                        DataSplit.builder()
                                .withSnapshot(endingSnapshotId)
                                .withPartition(partition)
                                .withBucket(bucket)
                                .withDataFiles(splitGroup.files)
                                .rawConvertible(rawConvertible)
                                .withBucketPath(bucketPath);
                if (deletionFiles != null) {
                    dataSplitBuilder.withDataDeletionFiles(deletionFiles);
                }
                result.add(dataSplitBuilder.build());
            }
        }

        return StartingScanner.fromPlan(new PlanImpl(null, endingSnapshotId, result));
    }

    /**
     * Check the validity of staring snapshotId early.
     *
     * @return If the check passes return empty.
     */
    public Optional<Result> checkScanSnapshotIdValidity() {
        Long earliestSnapshotId = snapshotManager.earliestSnapshotId();
        Long latestSnapshotId = snapshotManager.latestSnapshotId();

        if (earliestSnapshotId == null || latestSnapshotId == null) {
            LOG.warn("There is currently no snapshot. Waiting for snapshot generation.");
            return Optional.of(new NoSnapshot());
        }

        checkArgument(
                startingSnapshotId <= endingSnapshotId,
                "Starting snapshotId %s must less than ending snapshotId %s.",
                startingSnapshotId,
                endingSnapshotId);

        // because of the left open right closed rule of IncrementalStartingScanner that is
        // different from StaticFromStartingScanner, so we should allow starting snapshotId to be
        // equal to the earliestSnapshotId - 1.
        checkArgument(
                startingSnapshotId >= earliestSnapshotId - 1
                        && endingSnapshotId <= latestSnapshotId,
                "The specified scan snapshotId range [%s, %s] is out of available snapshotId range [%s, %s].",
                startingSnapshotId,
                endingSnapshotId,
                earliestSnapshotId,
                latestSnapshotId);

        return Optional.empty();
    }

    private List<DataSplit> readSplits(SnapshotReader reader, Snapshot s) {
        switch (scanMode) {
            case CHANGELOG:
                return readChangeLogSplits(reader, s);
            case DELTA:
                return readDeltaSplits(reader, s);
            default:
                throw new UnsupportedOperationException("Unsupported scan kind: " + scanMode);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private List<DataSplit> readDeltaSplits(SnapshotReader reader, Snapshot s) {
        if (s.commitKind() != CommitKind.APPEND) {
            // ignore COMPACT and OVERWRITE
            return Collections.emptyList();
        }
        return (List) reader.withSnapshot(s).withMode(ScanMode.DELTA).read().splits();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private List<DataSplit> readChangeLogSplits(SnapshotReader reader, Snapshot s) {
        if (s.commitKind() == CommitKind.OVERWRITE) {
            // ignore OVERWRITE
            return Collections.emptyList();
        }
        return (List) reader.withSnapshot(s).withMode(ScanMode.CHANGELOG).read().splits();
    }

    /** Split information to pass. */
    private static class SplitInfo {

        private final BinaryRow partition;
        private final int bucket;
        private final boolean rawConvertible;
        private final String bucketPath;
        @Nullable private final List<DeletionFile> deletionFiles;

        private SplitInfo(
                BinaryRow partition,
                int bucket,
                boolean rawConvertible,
                String bucketPath,
                @Nullable List<DeletionFile> deletionFiles) {
            this.partition = partition;
            this.bucket = bucket;
            this.rawConvertible = rawConvertible;
            this.bucketPath = bucketPath;
            this.deletionFiles = deletionFiles;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(
                    new Object[] {partition, bucket, rawConvertible, bucketPath, deletionFiles});
        }

        @Override
        public boolean equals(Object obj) {

            if (!(obj instanceof SplitInfo)) {
                return false;
            }

            SplitInfo that = (SplitInfo) obj;

            return Objects.equals(partition, that.partition)
                    && bucket == that.bucket
                    && rawConvertible == that.rawConvertible
                    && Objects.equals(bucketPath, that.bucketPath)
                    && Objects.equals(deletionFiles, that.deletionFiles);
        }
    }
}
