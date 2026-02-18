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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.operation.ManifestsReader;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.PlanImpl;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.paimon.utils.ManifestReadThreadPool.randomlyExecuteSequentialReturn;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Get incremental data by reading delta or changelog files from snapshots between start and end.
 */
public class IncrementalDeltaStartingScanner extends AbstractStartingScanner {

    private static final Logger LOG =
            LoggerFactory.getLogger(IncrementalDeltaStartingScanner.class);

    private final long endingSnapshotId;
    private final ScanMode scanMode;

    public IncrementalDeltaStartingScanner(
            SnapshotManager snapshotManager, long start, long end, ScanMode scanMode) {
        super(snapshotManager);
        this.startingSnapshotId = start;
        this.endingSnapshotId = end;
        this.scanMode = scanMode;
    }

    @Override
    public Result scan(SnapshotReader reader) {
        Map<Pair<BinaryRow, Integer>, List<ManifestEntry>> grouped = new ConcurrentHashMap<>();
        ManifestsReader manifestsReader = reader.manifestsReader();

        List<Long> snapshots =
                LongStream.range(startingSnapshotId + 1, endingSnapshotId + 1)
                        .boxed()
                        .collect(Collectors.toList());

        Iterator<ManifestFileMeta> manifests =
                randomlyExecuteSequentialReturn(
                        id -> {
                            Snapshot snapshot = snapshotManager.snapshot(id);
                            switch (scanMode) {
                                case DELTA:
                                    if (snapshot.commitKind() != Snapshot.CommitKind.APPEND) {
                                        // ignore COMPACT and OVERWRITE
                                        return Collections.emptyList();
                                    }
                                    break;
                                case CHANGELOG:
                                    if (snapshot.commitKind() == Snapshot.CommitKind.OVERWRITE) {
                                        // ignore OVERWRITE
                                        return Collections.emptyList();
                                    }
                                    break;
                                default:
                                    throw new UnsupportedOperationException(
                                            "Unsupported scan mode: " + scanMode);
                            }

                            return manifestsReader.read(snapshot, scanMode).filteredManifests;
                        },
                        snapshots,
                        reader.parallelism());

        Iterator<ManifestEntry> entries =
                randomlyExecuteSequentialReturn(
                        reader::readManifest, Lists.newArrayList(manifests), reader.parallelism());

        while (entries.hasNext()) {
            ManifestEntry entry = entries.next();
            checkArgument(
                    entry.kind() == FileKind.ADD, "Delta or changelog should only have ADD files.");
            grouped.computeIfAbsent(
                            Pair.of(entry.partition(), entry.bucket()), ignore -> new ArrayList<>())
                    .add(entry);
        }

        List<Split> result = new ArrayList<>();
        for (Map.Entry<Pair<BinaryRow, Integer>, List<ManifestEntry>> entry : grouped.entrySet()) {
            BinaryRow partition = entry.getKey().getLeft();
            int bucket = entry.getKey().getRight();
            String bucketPath = reader.pathFactory().bucketPath(partition, bucket).toString();
            for (SplitGenerator.SplitGroup splitGroup :
                    reader.splitGenerator()
                            .splitForBatch(
                                    entry.getValue().stream()
                                            .map(ManifestEntry::file)
                                            .collect(Collectors.toList()))) {
                 List<DataFileMeta> files = splitGroup.files;
                //  Sort the files by creation time from smallest to largest.
                files= files.stream().sorted(Comparator.comparingLong(DataFileMeta::creationTimeEpochMillis))
                        .collect(Collectors.toList());
                DataSplit.Builder dataSplitBuilder =
                        DataSplit.builder()
                                .isStreaming(true)
                                .withSnapshot(endingSnapshotId)
                                .withPartition(partition)
                                .withBucket(bucket)
                                .withTotalBuckets(entry.getValue().get(0).totalBuckets())
                                .withDataFiles(files)
                                .rawConvertible(splitGroup.rawConvertible)
                                .withBucketPath(bucketPath);
                result.add(dataSplitBuilder.build());
            }
        }

        return StartingScanner.fromPlan(new PlanImpl(null, endingSnapshotId, result));
    }

    public static StartingScanner betweenSnapshotIds(
            long startId, long endId, SnapshotManager snapshotManager, ScanMode scanMode) {
        long earliestSnapshotId = snapshotManager.earliestSnapshotId();
        long latestSnapshotId = snapshotManager.latestSnapshotId();

        // because of the left open right closed rule of IncrementalStartingScanner that is
        // different from StaticFromStartingScanner, so we should allow starting snapshotId to be
        // equal to the earliestSnapshotId - 1.
        checkArgument(
                startId >= earliestSnapshotId - 1 && endId <= latestSnapshotId,
                "The specified scan snapshotId range [%s, %s] is out of available snapshotId range [%s, %s].",
                startId,
                endId,
                earliestSnapshotId,
                latestSnapshotId);

        return new IncrementalDeltaStartingScanner(snapshotManager, startId, endId, scanMode);
    }

    public static IncrementalDeltaStartingScanner betweenTimestamps(
            long startTimestamp,
            long endTimestamp,
            SnapshotManager snapshotManager,
            ScanMode scanMode) {
        Snapshot startingSnapshot = snapshotManager.earlierOrEqualTimeMills(startTimestamp);
        Snapshot earliestSnapshot = snapshotManager.earliestSnapshot();
        // if earliestSnapShot.timeMillis() > startTimestamp we should include the earliestSnapShot
        long startId =
                (startingSnapshot == null || earliestSnapshot.timeMillis() > startTimestamp)
                        ? earliestSnapshot.id() - 1
                        : startingSnapshot.id();

        Snapshot endSnapshot = snapshotManager.earlierOrEqualTimeMills(endTimestamp);
        long endId = endSnapshot == null ? snapshotManager.latestSnapshot().id() : endSnapshot.id();

        return new IncrementalDeltaStartingScanner(snapshotManager, startId, endId, scanMode);
    }
}
