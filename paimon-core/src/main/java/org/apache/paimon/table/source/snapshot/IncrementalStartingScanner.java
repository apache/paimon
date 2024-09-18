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
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.operation.ManifestsReader;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.PlanImpl;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.utils.ManifestReadThreadPool;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** {@link StartingScanner} for incremental changes by snapshot. */
public class IncrementalStartingScanner extends AbstractStartingScanner {

    private static final Logger LOG = LoggerFactory.getLogger(IncrementalStartingScanner.class);

    private final long endingSnapshotId;
    private final ScanMode scanMode;

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
        Map<Pair<BinaryRow, Integer>, List<DataFileMeta>> grouped = new ConcurrentHashMap<>();
        ManifestsReader manifestsReader = reader.manifestsReader();

        List<Long> snapshots =
                LongStream.range(startingSnapshotId + 1, endingSnapshotId + 1)
                        .boxed()
                        .collect(Collectors.toList());
        ManifestReadThreadPool.randomlyOnlyExecute(
                id -> {
                    Snapshot snapshot = snapshotManager.snapshot(id);
                    switch (scanMode) {
                        case DELTA:
                            if (snapshot.commitKind() != CommitKind.APPEND) {
                                // ignore COMPACT and OVERWRITE
                                return;
                            }
                            break;
                        case CHANGELOG:
                            if (snapshot.commitKind() == CommitKind.OVERWRITE) {
                                // ignore OVERWRITE
                                return;
                            }
                            break;
                        default:
                            throw new UnsupportedOperationException(
                                    "Unsupported scan mode: " + scanMode);
                    }

                    List<ManifestFileMeta> manifests =
                            manifestsReader.read(snapshot, scanMode).getRight();
                    for (ManifestFileMeta manifest : manifests) {
                        List<ManifestEntry> entries = reader.readManifest(manifest);
                        for (ManifestEntry entry : entries) {
                            checkArgument(
                                    entry.kind() == FileKind.ADD,
                                    "Delta or changelog should only have ADD files.");
                            grouped.compute(
                                    Pair.of(entry.partition(), entry.bucket()),
                                    (key, files) -> {
                                        if (files == null) {
                                            files = new ArrayList<>();
                                        }
                                        files.add(entry.file());
                                        return files;
                                    });
                        }
                    }
                },
                snapshots,
                reader.parallelism());

        List<Split> result = new ArrayList<>();
        for (Map.Entry<Pair<BinaryRow, Integer>, List<DataFileMeta>> entry : grouped.entrySet()) {
            BinaryRow partition = entry.getKey().getLeft();
            int bucket = entry.getKey().getRight();
            String bucketPath = reader.pathFactory().bucketPath(partition, bucket).toString();
            for (SplitGenerator.SplitGroup splitGroup :
                    reader.splitGenerator().splitForBatch(entry.getValue())) {
                DataSplit.Builder dataSplitBuilder =
                        DataSplit.builder()
                                .withSnapshot(endingSnapshotId)
                                .withPartition(partition)
                                .withBucket(bucket)
                                .withDataFiles(splitGroup.files)
                                .rawConvertible(splitGroup.rawConvertible)
                                .withBucketPath(bucketPath);
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
}
