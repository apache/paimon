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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateVisitor;
import org.apache.paimon.table.source.ChainSplit;
import org.apache.paimon.table.source.DataFilePlan;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.DataTableStreamScan;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.SnapshotNotExistPlan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.table.source.snapshot.StartingContext;
import org.apache.paimon.utils.ChainPartitionProjector;
import org.apache.paimon.utils.ChainTableUtils;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Streaming scan for chain tables with a two-phase design:
 *
 * <ul>
 *   <li><b>Phase 1 (Starting):</b> Outputs the latest snapshot partition (per group) and delta
 *       partitions that come after it. Older snapshot partitions are excluded as they are
 *       considered outdated. Each primary key appears exactly once under its natural partition.
 *       Unlike batch full scan, anchor-based chain merging is intentionally skipped to keep Phase 1
 *       lightweight — this avoids split explosion in long-running jobs with many partitions.
 *   <li><b>Phase 2 (Incremental):</b> Stream new snapshots from the delta branch only, picking up
 *       from where Phase 1 left off.
 * </ul>
 *
 * <p>Checkpoint state is a single {@code Long} — the delta branch's next snapshot id. On stateful
 * restart, Phase 1 is skipped and incremental streaming resumes from the checkpointed position. On
 * stateless restart (null state), a fresh starting scan is performed.
 */
public class ChainTableStreamScan implements StreamDataTableScan {

    private static final Logger LOG = LoggerFactory.getLogger(ChainTableStreamScan.class);

    private final ChainGroupReadTable chainGroupReadTable;

    /** Phase 1: batch scan used to access snapshot branch data via {@code mainScan}. */
    private final ChainGroupReadTable.ChainTableBatchScan batchScan;

    /** Phase 2: delta-only stream scan. */
    private final DataTableStreamScan deltaStreamScan;

    /** Projector for splitting full partition into group and chain parts. */
    private final ChainPartitionProjector partitionProjector;

    /** Comparator for chain partition keys only. */
    private final RecordComparator chainPartitionComparator;

    /** Partition keys of the table, used to reject partition filters in streaming mode. */
    private final List<String> partitionKeys;

    /**
     * Checkpoint state: the next delta snapshot id to read. Null before Phase 1 completes; non-null
     * once Phase 1 is done or after a stateful restore.
     */
    @Nullable private Long nextDeltaSnapshotId;

    /** Whether the starting plan (Phase 1) has been completed. */
    private boolean startingDone = false;

    /** Predicates and shard for applying to local scans created in {@link #planStarting()}. */
    private final List<Predicate> predicates = new ArrayList<>();

    private int shardIndex = -1;

    private int shardCount = -1;

    /** Maximum number of retries when race condition is detected during position capture. */
    private static final int MAX_RACE_RETRIES = 3;

    public ChainTableStreamScan(ChainGroupReadTable chainGroupReadTable) {
        this.chainGroupReadTable = chainGroupReadTable;
        this.batchScan =
                new ChainGroupReadTable.ChainTableBatchScan(
                        chainGroupReadTable.schema(), chainGroupReadTable);
        this.deltaStreamScan = (DataTableStreamScan) chainGroupReadTable.other().newStreamScan();

        // Initialize partition projector and chain comparator using the established pattern
        // from ChainTableBatchScan.
        List<String> chainKeys =
                ChainTableUtils.chainPartitionKeys(
                        chainGroupReadTable.coreOptions(),
                        chainGroupReadTable.schema().partitionKeys());
        this.partitionProjector =
                new ChainPartitionProjector(
                        chainGroupReadTable.schema().logicalPartitionType(), chainKeys.size());
        this.chainPartitionComparator =
                CodeGenUtils.newRecordComparator(
                        partitionProjector.chainPartitionType().getFieldTypes());
        this.partitionKeys = chainGroupReadTable.schema().partitionKeys();
    }

    @Override
    public StartingContext startingContext() {
        if (!startingDone) {
            return StartingContext.EMPTY;
        }
        return deltaStreamScan.startingContext();
    }

    @Override
    public TableScan.Plan plan() {
        if (!startingDone) {
            return planStarting();
        }

        TableScan.Plan plan = deltaStreamScan.plan();
        // Never return SnapshotNotExistPlan — it would cause the Flink enumerator to
        // set stopTriggerScan=true and permanently stop polling for new data.
        if (plan instanceof SnapshotNotExistPlan) {
            return new DataFilePlan<>(Collections.emptyList());
        }

        // Phase 2 reads from the delta branch only. The delta stream scan already produces
        // DataSplits with the correct branch context and all required metadata (bucket,
        // isStreaming, rawConvertible, deletionFiles). Wrapping them in ChainSplit would drop
        // that metadata, which breaks changelog-producer=input streaming reads because the
        // reader would fall back to LSM merging instead of streaming the changelog rows.
        return plan;
    }

    /**
     * Starting plan: outputs the latest snapshot partition (per group) and delta partitions that
     * come after it. Older snapshot partitions are excluded. Each primary key appears exactly once
     * under its natural partition.
     *
     * <p>Unlike batch full scan, anchor-based chain merging is not performed. This keeps Phase 1
     * lightweight for long-running jobs.
     */
    private TableScan.Plan planStarting() {
        FileStoreTable deltaTable = chainGroupReadTable.other();
        String deltaBranch = deltaTable.coreOptions().branch();
        String snapshotBranch = chainGroupReadTable.wrapped.coreOptions().branch();

        // Capture both delta and snapshot positions with race detection.
        // We capture snapshot, delta, snapshot again. If the first and third snapshot IDs
        // differ, a race occurred and we retry. This prevents data loss from snapshot commits
        // between captures.
        Long snapshotLatestId;
        Long deltaLatestId;
        int attempt = 0;
        while (true) {
            Long snapshotId1 = chainGroupReadTable.wrapped.snapshotManager().latestSnapshotId();
            deltaLatestId = captureDeltaPosition(deltaTable);
            Long snapshotId2 = chainGroupReadTable.wrapped.snapshotManager().latestSnapshotId();

            if (Objects.equals(snapshotId1, snapshotId2)) {
                // No race detected
                snapshotLatestId = snapshotId1;
                LOG.info(
                        "ChainTableStreamScan: captured positions (attempt {}): "
                                + "snapshot={}, delta={}",
                        attempt + 1,
                        snapshotId1,
                        deltaLatestId);
                break;
            }

            // Race detected: snapshot committed between captures
            LOG.warn(
                    "ChainTableStreamScan: race condition detected (attempt {}): "
                            + "snapshot changed from {} to {}",
                    attempt + 1,
                    snapshotId1,
                    snapshotId2);

            attempt++;
            if (attempt >= MAX_RACE_RETRIES) {
                throw new IllegalStateException(
                        "ChainTableStreamScan: failed to capture consistent positions after "
                                + MAX_RACE_RETRIES
                                + " retries due to continuous snapshot commits. "
                                + "This indicates high snapshot commit frequency. "
                                + "The job will fail and rely on Flink failover mechanism to retry.");
            }
        }

        // 1. Read delta branch data at the pinned snapshot, grouped by partition.
        Map<BinaryRow, List<DataSplit>> deltaSplitsByPartition;
        if (deltaLatestId != null) {
            FileStoreTable pinnedDelta =
                    deltaTable.copy(
                            Collections.singletonMap(
                                    CoreOptions.SCAN_SNAPSHOT_ID.key(),
                                    String.valueOf(deltaLatestId)));
            DataTableScan pinnedDeltaScan = pinnedDelta.newScan();
            applyPredicatesAndShard(pinnedDeltaScan);
            deltaSplitsByPartition = groupByPartition(pinnedDeltaScan);
        } else {
            deltaSplitsByPartition = Collections.emptyMap();
        }

        // 2. List snapshot partitions at the pinned snapshot (lightweight — partition metadata
        //    only, no file I/O). Find the latest chain partition per group, then scan only those
        //    partitions for files. This avoids reading file manifests for hundreds of historical
        //    partitions that will be discarded (only the latest per group is kept).
        Map<Object, BinaryRow> latestChainPartitionPerGroup = new HashMap<>();
        FileStoreTable pinnedSnapshot = null;
        if (snapshotLatestId != null) {
            pinnedSnapshot =
                    chainGroupReadTable.wrapped.copy(
                            Collections.singletonMap(
                                    CoreOptions.SCAN_SNAPSHOT_ID.key(),
                                    String.valueOf(snapshotLatestId)));
            DataTableScan partitionListingScan = pinnedSnapshot.newScan();
            for (BinaryRow partition : partitionListingScan.listPartitions()) {
                Object groupKey = toGroupKey(partition);
                BinaryRow existingLatest = latestChainPartitionPerGroup.get(groupKey);
                if (existingLatest == null
                        || chainPartitionComparator.compare(
                                        partitionProjector.extractChainPartition(partition),
                                        partitionProjector.extractChainPartition(existingLatest))
                                > 0) {
                    latestChainPartitionPerGroup.put(groupKey, partition);
                }
            }
        }

        // 3. Scan file splits for latest snapshot partitions only, at the pinned snapshot.
        //    Reuse the pinnedSnapshot from step 2 to avoid redundant copy operations.
        List<BinaryRow> latestPartitions = new ArrayList<>(latestChainPartitionPerGroup.values());
        Map<BinaryRow, List<DataSplit>> snapshotSplitsByPartition;
        if (!latestPartitions.isEmpty() && pinnedSnapshot != null) {
            DataTableScan snapshotScan = pinnedSnapshot.newScan();
            snapshotScan.withPartitionFilter(latestPartitions);
            applyPredicatesAndShard(snapshotScan);
            snapshotSplitsByPartition = groupByPartition(snapshotScan);
        } else {
            snapshotSplitsByPartition = Collections.emptyMap();
        }

        // 4. Build ChainSplits:
        //    - Snapshot partitions are already filtered to latest per group at the pinned snapshot.
        //    - Delta partitions: include partitions with chain key > latest snapshot chain key for
        //      that group, or all partitions if no snapshot exists for that group.
        List<Split> allSplits = new ArrayList<>();

        for (Map.Entry<BinaryRow, List<DataSplit>> entry : snapshotSplitsByPartition.entrySet()) {
            for (DataSplit ds : entry.getValue()) {
                allSplits.add(ChainSplit.from(ds, snapshotBranch));
            }
        }

        for (Map.Entry<BinaryRow, List<DataSplit>> entry : deltaSplitsByPartition.entrySet()) {
            BinaryRow partition = entry.getKey();
            Object groupKey = toGroupKey(partition);
            BinaryRow latestPartition = latestChainPartitionPerGroup.get(groupKey);
            // Include delta partition if:
            // - No snapshot exists for this group, OR
            // - Chain key > latest snapshot chain key
            if (latestPartition == null
                    || chainPartitionComparator.compare(
                                    partitionProjector.extractChainPartition(partition),
                                    partitionProjector.extractChainPartition(latestPartition))
                            > 0) {
                for (DataSplit ds : entry.getValue()) {
                    allSplits.add(ChainSplit.from(ds, deltaBranch));
                }
            }
        }

        LOG.info(
                "ChainTableStreamScan.planStarting [snapshot={}, delta={}]: "
                        + "{} delta partitions, {} snapshot partitions, "
                        + "{} latest snapshot groups, {} total splits",
                snapshotBranch,
                deltaBranch,
                deltaSplitsByPartition.size(),
                snapshotSplitsByPartition.size(),
                latestChainPartitionPerGroup.size(),
                allSplits.size());

        startingDone = true;
        return new DataFilePlan<>(allSplits);
    }

    /**
     * Captures the delta branch's latest snapshot id and positions the Phase 2 stream scan to start
     * from the next snapshot. This makes the Phase 1 / Phase 2 boundary deterministic: Phase 1
     * reads delta data pinned at the returned snapshot id, Phase 2 starts from the snapshot after.
     *
     * @return the latest delta snapshot id, or {@code null} if the delta branch has no snapshots
     */
    @Nullable
    private Long captureDeltaPosition(FileStoreTable deltaTable) {
        SnapshotManager deltaSnapshotManager = deltaTable.snapshotManager();
        Long latestId = deltaSnapshotManager.latestSnapshotId();
        nextDeltaSnapshotId = latestId != null ? latestId + 1 : Snapshot.FIRST_SNAPSHOT_ID;
        LOG.info(
                "ChainTableStreamScan: pinned delta branch '{}' at snapshot {}, "
                        + "nextDeltaSnapshotId={}",
                deltaTable.coreOptions().branch(),
                latestId,
                nextDeltaSnapshotId);
        deltaStreamScan.restore(nextDeltaSnapshotId);
        return latestId;
    }

    /** Plans a scan and groups the resulting splits by partition. */
    private static Map<BinaryRow, List<DataSplit>> groupByPartition(DataTableScan scan) {
        Map<BinaryRow, List<DataSplit>> grouped = new LinkedHashMap<>();
        for (Split s : scan.plan().splits()) {
            DataSplit ds = (DataSplit) s;
            grouped.computeIfAbsent(ds.partition(), k -> new ArrayList<>()).add(ds);
        }
        return grouped;
    }

    /**
     * Extracts a stable group key from a full partition row. When there is no group partition (all
     * fields are chain keys), returns a shared singleton to avoid zero-field {@link BinaryRow}
     * instances that may have inconsistent {@code hashCode}/{@code equals} across different
     * partitions.
     */
    private Object toGroupKey(BinaryRow fullPartition) {
        if (!partitionProjector.hasGroupPartition()) {
            return Collections.emptyList();
        }
        return partitionProjector.extractGroupPartition(fullPartition);
    }

    @Override
    public InnerTableScan withFilter(Predicate predicate) {
        if (predicate == null) {
            return this;
        }
        if (!partitionKeys.isEmpty()) {
            Set<String> referencedFields = PredicateVisitor.collectFieldNames(predicate);
            boolean containsPartitionField =
                    referencedFields.stream().anyMatch(partitionKeys::contains);
            if (containsPartitionField) {
                throw new UnsupportedOperationException(
                        "Partition filter is not supported in chain table streaming read. "
                                + "The chain table streaming scan determines which partitions to read "
                                + "based on the chain-merge logic across snapshot and delta branches. "
                                + "Applying a partition filter would interfere with this logic. "
                                + "If you need to read a specific partition, use batch mode instead.");
            }
        }
        predicates.add(predicate);
        batchScan.withFilter(predicate);
        deltaStreamScan.withFilter(predicate);
        return this;
    }

    @Override
    public InnerTableScan withPartitionFilter(Map<String, String> partitionSpec) {
        throw new UnsupportedOperationException(
                "Partition filter is not supported in chain table streaming read.");
    }

    @Override
    public InnerTableScan withPartitionFilter(List<BinaryRow> partitions) {
        throw new UnsupportedOperationException(
                "Partition filter is not supported in chain table streaming read.");
    }

    @Override
    public InnerTableScan withPartitionFilter(PartitionPredicate partitionPredicate) {
        if (partitionPredicate != null) {
            throw new UnsupportedOperationException(
                    "Partition filter is not supported in chain table streaming read.");
        }
        return this;
    }

    @Override
    public InnerTableScan withPartitionFilter(Predicate predicate) {
        if (predicate != null) {
            throw new UnsupportedOperationException(
                    "Partition filter is not supported in chain table streaming read.");
        }
        return this;
    }

    @Override
    public DataTableScan withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
        shardIndex = indexOfThisSubtask;
        shardCount = numberOfParallelSubtasks;
        batchScan.withShard(indexOfThisSubtask, numberOfParallelSubtasks);
        deltaStreamScan.withShard(indexOfThisSubtask, numberOfParallelSubtasks);
        return this;
    }

    /**
     * Applies all previously set predicates and shard to a newly created scan. Used for the pinned
     * delta scan in {@link #planStarting()}.
     */
    private void applyPredicatesAndShard(DataTableScan scan) {
        for (Predicate p : predicates) {
            scan.withFilter(p);
        }
        if (shardIndex >= 0) {
            scan.withShard(shardIndex, shardCount);
        }
    }

    @Nullable
    @Override
    public Long checkpoint() {
        if (startingDone) {
            return deltaStreamScan.checkpoint();
        }
        return nextDeltaSnapshotId;
    }

    @Nullable
    @Override
    public Long watermark() {
        if (!startingDone) {
            return null;
        }
        return deltaStreamScan.watermark();
    }

    @Override
    public void restore(@Nullable Long nextSnapshotId) {
        this.nextDeltaSnapshotId = nextSnapshotId;
        if (nextSnapshotId != null) {
            startingDone = true;
            deltaStreamScan.restore(nextSnapshotId);
        } else {
            startingDone = false;
        }
    }

    @Override
    public void restore(@Nullable Long nextSnapshotId, boolean scanAllSnapshot) {
        if (scanAllSnapshot) {
            startingDone = false;
            this.nextDeltaSnapshotId = nextSnapshotId;
            // No need to call deltaStreamScan.restore() here — Phase 1 will re-run and
            // captureDeltaPosition() will re-position the delta stream scan.
        } else {
            restore(nextSnapshotId);
        }
    }

    @Override
    public void notifyCheckpointComplete(@Nullable Long nextSnapshot) {
        deltaStreamScan.notifyCheckpointComplete(nextSnapshot);
    }

    @Override
    public List<PartitionEntry> listPartitionEntries() {
        throw new UnsupportedOperationException(
                "List Partition Entries is not supported in Chain Table Stream Scan.");
    }
}
