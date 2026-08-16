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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.partition.PartitionTimeExtractor;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PartitionModification;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ChainPartitionProjector;
import org.apache.paimon.utils.ChainTableUtils;
import org.apache.paimon.utils.InternalRowPartitionComputer;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * Partition expiration for chain tables.
 *
 * <p>Chain tables store data across snapshot and delta branches. A delta partition depends on its
 * nearest earlier snapshot partition as an anchor for merge-on-read. This class expires partitions
 * in "segments" defined by consecutive snapshot partitions to maintain chain integrity.
 *
 * <p>A segment consists of one snapshot partition and all delta partitions whose time falls between
 * that snapshot and the next snapshot in sorted order. The segment is the atomic unit of
 * expiration: either the entire segment (snapshot + deltas) is expired, or nothing in it is.
 *
 * <p>Algorithm per group:
 *
 * <ol>
 *   <li>List all snapshot branch partitions sorted by chain partition time.
 *   <li>Filter to those before the cutoff ({@code now - expirationTime}).
 *   <li>If fewer than 2 snapshots are before the cutoff, nothing can be expired (the last one must
 *       be kept as anchor).
 *   <li>The most recent snapshot before the cutoff is the anchor (kept). All earlier snapshots form
 *       expirable segments together with their associated delta partitions.
 *   <li>The number of segments expired is limited by {@code maxExpireNum}.
 *   <li>Delta partitions are dropped first, then snapshot partitions, so that {@code
 *       ChainTableCommitPreCallback} validation passes.
 * </ol>
 */
public class ChainTablePartitionExpire implements PartitionExpire {

    private static final Logger LOG = LoggerFactory.getLogger(ChainTablePartitionExpire.class);

    private final Duration expirationTime;
    private final Duration checkInterval;
    private final FileStoreTable snapshotTable;
    private final FileStoreTable deltaTable;
    private final PartitionTimeExtractor timeExtractor;
    private final ChainPartitionProjector projector;
    private final RecordComparator chainPartitionComparator;
    private final InternalRowPartitionComputer partitionComputer;
    private final List<String> partitionKeys;
    private final List<String> chainPartitionKeys;
    private final boolean endInputCheckPartitionExpire;
    private final int maxExpireNum;
    private final int expireBatchSize;
    @Nullable private final PartitionModification snapshotPartitionModification;
    @Nullable private final PartitionModification deltaPartitionModification;
    private LocalDateTime lastCheck;

    public ChainTablePartitionExpire(
            Duration expirationTime,
            Duration checkInterval,
            FileStoreTable snapshotTable,
            FileStoreTable deltaTable,
            CoreOptions options,
            RowType partitionType,
            boolean endInputCheckPartitionExpire,
            int maxExpireNum,
            int expireBatchSize,
            @Nullable PartitionModification snapshotPartitionModification,
            @Nullable PartitionModification deltaPartitionModification) {
        this.expirationTime = expirationTime;
        this.checkInterval = checkInterval;
        this.snapshotTable = snapshotTable;
        this.deltaTable = deltaTable;
        this.partitionKeys = partitionType.getFieldNames();
        this.maxExpireNum = maxExpireNum;
        this.expireBatchSize = expireBatchSize;
        this.snapshotPartitionModification = snapshotPartitionModification;
        this.deltaPartitionModification = deltaPartitionModification;

        List<String> allPartitionKeys = partitionType.getFieldNames();
        this.chainPartitionKeys = ChainTableUtils.chainPartitionKeys(options, allPartitionKeys);
        int chainFieldCount = chainPartitionKeys.size();
        this.projector = new ChainPartitionProjector(partitionType, chainFieldCount);
        this.chainPartitionComparator =
                CodeGenUtils.newRecordComparator(projector.chainPartitionType().getFieldTypes());
        this.timeExtractor =
                new PartitionTimeExtractor(
                        options.partitionTimestampPattern(), options.partitionTimestampFormatter());
        this.partitionComputer =
                new InternalRowPartitionComputer(
                        options.partitionDefaultName(),
                        partitionType,
                        allPartitionKeys.toArray(new String[0]),
                        options.legacyPartitionName());
        this.endInputCheckPartitionExpire = endInputCheckPartitionExpire;

        long rndSeconds = 0;
        long checkIntervalSeconds = checkInterval.toMillis() / 1000;
        if (checkIntervalSeconds > 0) {
            rndSeconds = ThreadLocalRandom.current().nextLong(checkIntervalSeconds);
        }
        this.lastCheck = LocalDateTime.now().minusSeconds(rndSeconds);
    }

    @Override
    public List<Map<String, String>> expire(long commitIdentifier) {
        return expire(LocalDateTime.now(), commitIdentifier);
    }

    @Override
    public boolean isValueExpiration() {
        return true;
    }

    @Override
    public boolean isValueAllExpired(Collection<BinaryRow> partitions) {
        return isValueAllExpired(partitions, LocalDateTime.now());
    }

    @VisibleForTesting
    boolean isValueAllExpired(Collection<BinaryRow> partitions, LocalDateTime now) {
        LocalDateTime expireDateTime = now.minus(expirationTime);
        for (BinaryRow partition : partitions) {
            LocalDateTime partTime = extractPartitionTime(partition);
            if (partTime == null || !expireDateTime.isAfter(partTime)) {
                return false;
            }
        }

        // All partitions are time-wise before cutoff, but chain table retains anchors
        // (the most recent snapshot before cutoff per group) and their segment's deltas.
        // Compute per-group retain boundary: partitions at or after the boundary are retained.
        Map<BinaryRow, LocalDateTime> retainBoundary = computeGroupRetainBoundary(expireDateTime);
        for (BinaryRow partition : partitions) {
            BinaryRow groupKey = projector.extractGroupPartition(partition);
            LocalDateTime boundary = retainBoundary.get(groupKey);
            if (boundary == null) {
                return false;
            }
            LocalDateTime partTime = extractPartitionTime(partition);
            if (partTime != null && !partTime.isBefore(boundary)) {
                return false;
            }
        }
        return true;
    }

    /**
     * For each group that has snapshot partitions, compute the time boundary at or above which
     * partitions are retained (not expired). Returns {@link LocalDateTime#MIN} for groups where
     * fewer than 2 snapshots fall before the cutoff (nothing can be expired). Groups with no
     * snapshot partitions at all (delta-only) are not included in the result; without a snapshot
     * anchor, their earlier deltas may still be required by later delta-only chain reads.
     */
    private Map<BinaryRow, LocalDateTime> computeGroupRetainBoundary(LocalDateTime cutoffTime) {
        List<PartitionEntry> snapshotEntries = snapshotTable.newSnapshotReader().partitionEntries();
        Map<BinaryRow, List<BinaryRow>> groupedSnapshots = groupByGroupKey(snapshotEntries);

        Map<BinaryRow, LocalDateTime> boundaries = new HashMap<>();
        for (Map.Entry<BinaryRow, List<BinaryRow>> entry : groupedSnapshots.entrySet()) {
            BinaryRow groupKey = entry.getKey();
            int countBeforeCutoff = 0;
            LocalDateTime latestBeforeCutoff = null;
            for (BinaryRow snapshot : entry.getValue()) {
                LocalDateTime time = extractPartitionTime(snapshot);
                if (time != null && cutoffTime.isAfter(time)) {
                    countBeforeCutoff++;
                    if (latestBeforeCutoff == null || time.isAfter(latestBeforeCutoff)) {
                        latestBeforeCutoff = time;
                    }
                }
            }
            if (countBeforeCutoff < 2) {
                boundaries.put(groupKey, LocalDateTime.MIN);
            } else {
                boundaries.put(groupKey, latestBeforeCutoff);
            }
        }
        return boundaries;
    }

    @VisibleForTesting
    void setLastCheck(LocalDateTime time) {
        lastCheck = time;
    }

    @VisibleForTesting
    List<Map<String, String>> expire(LocalDateTime now, long commitIdentifier) {
        if (checkInterval.isZero()
                || now.isAfter(lastCheck.plus(checkInterval))
                || (endInputCheckPartitionExpire && Long.MAX_VALUE == commitIdentifier)) {
            List<Map<String, String>> expired = doExpire(now.minus(expirationTime));
            lastCheck = now;
            return expired;
        }
        return null;
    }

    private List<Map<String, String>> doExpire(LocalDateTime cutoffTime) {
        List<PartitionEntry> snapshotPartitions =
                snapshotTable.newSnapshotReader().partitionEntries();
        List<PartitionEntry> deltaPartitions = deltaTable.newSnapshotReader().partitionEntries();

        Map<BinaryRow, List<BinaryRow>> groupedSnapshots = groupByGroupKey(snapshotPartitions);
        Map<BinaryRow, List<BinaryRow>> groupedDeltas = groupByGroupKey(deltaPartitions);

        List<BinaryRow> snapshotPartitionsToExpire = new ArrayList<>();
        List<BinaryRow> deltaPartitionsToExpire = new ArrayList<>();

        for (Map.Entry<BinaryRow, List<BinaryRow>> entry : groupedSnapshots.entrySet()) {
            BinaryRow groupKey = entry.getKey();
            List<BinaryRow> groupSnapshots = entry.getValue();

            groupSnapshots.sort(
                    (a, b) ->
                            chainPartitionComparator.compare(
                                    projector.extractChainPartition(a),
                                    projector.extractChainPartition(b)));

            List<BinaryRow> snapshotsBeforeCutoff = new ArrayList<>();
            for (BinaryRow partition : groupSnapshots) {
                LocalDateTime partTime = extractPartitionTime(partition);
                if (partTime != null && cutoffTime.isAfter(partTime)) {
                    snapshotsBeforeCutoff.add(partition);
                }
            }

            if (snapshotsBeforeCutoff.size() < 2) {
                continue;
            }

            // Anchor = most recent snapshot before cutoff, kept as merge base
            int anchorIndex = snapshotsBeforeCutoff.size() - 1;

            // Expirable snapshots: all before anchor, oldest first.
            // Each forms a segment with its associated deltas.
            int segmentsToExpire = Math.min(anchorIndex, maxExpireNum);

            List<BinaryRow> groupDeltas = groupedDeltas.get(groupKey);

            for (int i = 0; i < segmentsToExpire; i++) {
                BinaryRow segmentSnapshot = snapshotsBeforeCutoff.get(i);
                snapshotPartitionsToExpire.add(segmentSnapshot);

                if (groupDeltas != null) {
                    // Segment boundary: from this snapshot's time up to the next snapshot's time
                    LocalDateTime segmentStart = extractPartitionTime(segmentSnapshot);
                    BinaryRow nextSnapshot = snapshotsBeforeCutoff.get(i + 1);
                    LocalDateTime segmentEnd = extractPartitionTime(nextSnapshot);

                    if (segmentStart != null && segmentEnd != null) {
                        for (BinaryRow deltaPartition : groupDeltas) {
                            LocalDateTime deltaTime = extractPartitionTime(deltaPartition);
                            if (deltaTime != null
                                    && !deltaTime.isBefore(segmentStart)
                                    && deltaTime.isBefore(segmentEnd)) {
                                deltaPartitionsToExpire.add(deltaPartition);
                            }
                        }
                    }
                }
            }

            // Also collect orphan deltas before the earliest expired snapshot
            if (segmentsToExpire > 0 && groupDeltas != null) {
                LocalDateTime firstSnapshotTime =
                        extractPartitionTime(snapshotsBeforeCutoff.get(0));
                if (firstSnapshotTime != null) {
                    for (BinaryRow deltaPartition : groupDeltas) {
                        LocalDateTime deltaTime = extractPartitionTime(deltaPartition);
                        if (deltaTime != null && deltaTime.isBefore(firstSnapshotTime)) {
                            deltaPartitionsToExpire.add(deltaPartition);
                        }
                    }
                }
            }
        }

        if (snapshotPartitionsToExpire.isEmpty() && deltaPartitionsToExpire.isEmpty()) {
            return new ArrayList<>();
        }

        List<Map<String, String>> deltaSpecs = toPartitionSpecs(deltaPartitionsToExpire);
        List<Map<String, String>> snapshotSpecs = toPartitionSpecs(snapshotPartitionsToExpire);
        List<Map<String, String>> allExpired = new ArrayList<>();

        if (!deltaSpecs.isEmpty()) {
            LOG.info("Chain table expire delta partitions: {}", deltaSpecs);
            batchDropPartitions(deltaTable, deltaSpecs, deltaPartitionModification);
            allExpired.addAll(deltaSpecs);
        }

        if (!snapshotSpecs.isEmpty()) {
            LOG.info("Chain table expire snapshot partitions: {}", snapshotSpecs);
            batchDropPartitions(snapshotTable, snapshotSpecs, snapshotPartitionModification);
            allExpired.addAll(snapshotSpecs);
        }

        return allExpired;
    }

    private void batchDropPartitions(
            FileStoreTable table,
            List<Map<String, String>> partitionSpecs,
            @Nullable PartitionModification partitionModification) {
        if (partitionModification != null) {
            try {
                if (expireBatchSize > 0 && expireBatchSize < partitionSpecs.size()) {
                    for (List<Map<String, String>> batch :
                            Lists.partition(partitionSpecs, expireBatchSize)) {
                        partitionModification.dropPartitions(batch);
                        partitionModification.dropPartitions(toDonePartitions(batch));
                    }
                } else {
                    partitionModification.dropPartitions(partitionSpecs);
                    partitionModification.dropPartitions(toDonePartitions(partitionSpecs));
                }
            } catch (Catalog.TableNotExistException e) {
                throw new RuntimeException(e);
            }
        } else {
            if (expireBatchSize > 0 && expireBatchSize < partitionSpecs.size()) {
                for (List<Map<String, String>> batch :
                        Lists.partition(partitionSpecs, expireBatchSize)) {
                    dropPartitions(table, batch);
                }
            } else {
                dropPartitions(table, partitionSpecs);
            }
        }
    }

    private List<Map<String, String>> toDonePartitions(
            List<Map<String, String>> expiredPartitions) {
        List<Map<String, String>> donePartitions = new ArrayList<>(expiredPartitions.size());
        for (Map<String, String> partition : expiredPartitions) {
            LinkedHashMap<String, String> donePartition = new LinkedHashMap<>(partition);
            Map.Entry<String, String> lastEntry = null;
            for (Map.Entry<String, String> entry : donePartition.entrySet()) {
                lastEntry = entry;
            }
            if (lastEntry != null) {
                donePartition.put(lastEntry.getKey(), lastEntry.getValue() + ".done");
                donePartitions.add(donePartition);
            }
        }
        return donePartitions;
    }

    private Map<BinaryRow, List<BinaryRow>> groupByGroupKey(List<PartitionEntry> partitionEntries) {
        Map<BinaryRow, List<BinaryRow>> grouped = new LinkedHashMap<>();
        for (PartitionEntry entry : partitionEntries) {
            BinaryRow fullPartition = entry.partition();
            BinaryRow groupKey = projector.extractGroupPartition(fullPartition);
            grouped.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(fullPartition);
        }
        return grouped;
    }

    private LocalDateTime extractPartitionTime(BinaryRow partition) {
        try {
            LinkedHashMap<String, String> partValues =
                    partitionComputer.generatePartValues(partition);
            List<String> chainValues = new ArrayList<>();
            for (String key : chainPartitionKeys) {
                chainValues.add(partValues.get(key));
            }
            return timeExtractor.extract(chainPartitionKeys, chainValues);
        } catch (Exception e) {
            LOG.warn("Failed to extract partition time from {}", partition, e);
            return null;
        }
    }

    private List<Map<String, String>> toPartitionSpecs(List<BinaryRow> partitions) {
        return partitions.stream()
                .map(
                        p -> {
                            LinkedHashMap<String, String> values =
                                    partitionComputer.generatePartValues(p);
                            Map<String, String> spec = new LinkedHashMap<>();
                            for (String key : partitionKeys) {
                                String value = values.get(key);
                                if (value != null) {
                                    spec.put(key, value);
                                }
                            }
                            return spec;
                        })
                .collect(Collectors.toList());
    }

    private void dropPartitions(FileStoreTable table, List<Map<String, String>> partitionSpecs) {
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.truncatePartitions(partitionSpecs);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to drop partitions from %s: %s.", table.name(), partitionSpecs),
                    e);
        }
    }
}
