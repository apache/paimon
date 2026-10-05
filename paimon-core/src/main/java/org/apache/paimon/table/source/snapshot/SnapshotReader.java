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
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.BucketEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.ManifestsReader;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BiFilter;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RowRangeIndex;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Read splits from specified {@link Snapshot} with given configuration. */
public interface SnapshotReader {

    @Nullable
    Integer parallelism();

    SnapshotManager snapshotManager();

    ChangelogManager changelogManager();

    ManifestsReader manifestsReader();

    List<ManifestEntry> readManifest(ManifestFileMeta manifest);

    ConsumerManager consumerManager();

    SplitGenerator splitGenerator();

    FileStorePathFactory pathFactory();

    @Nullable
    IndexFileHandler indexFileHandler();

    SnapshotReader withSnapshot(long snapshotId);

    SnapshotReader withSnapshot(Snapshot snapshot);

    SnapshotReader withFilter(Predicate predicate);

    /**
     * Applies a full read-time filter and an optional scan-pruning filter.
     *
     * <p>{@code predicate} is used to determine whether the reader has a non-partition filter which
     * must still be evaluated at read time. {@code pushdownPredicate} is the only predicate used
     * for scan pruning such as partition, stats, and bucket pruning.
     */
    SnapshotReader withFilter(Predicate predicate, @Nullable Predicate pushdownPredicate);

    SnapshotReader withPartitionFilter(Map<String, String> partitionSpec);

    SnapshotReader withPartitionFilter(Predicate predicate);

    SnapshotReader withPartitionFilter(List<BinaryRow> partitions);

    SnapshotReader withPartitionFilter(PartitionPredicate partitionPredicate);

    SnapshotReader withPartitionsFilter(List<Map<String, String>> partitions);

    SnapshotReader withMode(ScanMode scanMode);

    SnapshotReader withLevel(int level);

    SnapshotReader withLevelFilter(Filter<Integer> levelFilter);

    SnapshotReader withLevelMinMaxFilter(BiFilter<Integer, Integer> minMaxFilter);

    SnapshotReader enableValueFilter();

    SnapshotReader withManifestEntryFilter(Filter<ManifestEntry> filter);

    SnapshotReader withBucket(int bucket);

    SnapshotReader onlyReadRealBuckets();

    SnapshotReader withBucketFilter(Filter<Integer> bucketFilter);

    SnapshotReader withDataFileNameFilter(Filter<String> fileNameFilter);

    SnapshotReader dropStats();

    SnapshotReader keepStats();

    SnapshotReader withShard(int indexOfThisSubtask, int numberOfParallelSubtasks);

    SnapshotReader withMetricRegistry(MetricRegistry registry);

    SnapshotReader withRowRanges(List<Range> rowRanges);

    SnapshotReader withRowRangeIndex(RowRangeIndex rowRangeIndex);

    SnapshotReader withReadType(RowType readType);

    SnapshotReader withLimit(long limit);

    /** Whether the pushed filter still contains non-partition predicates. */
    boolean hasNonPartitionFilter();

    /** Get splits plan from snapshot. */
    Plan read();

    /** Whether this reader can expose fine-grained splits without materializing a complete plan. */
    default boolean supportsFineGrainedSplitPlanning() {
        return false;
    }

    /**
     * Opens a single-pass split plan.
     *
     * <p>The default implementation adapts the fully materialized {@link Plan}. Implementations may
     * instead return fine-grained splits, which preserve the files and rows of the logical plan but
     * deliberately leave connector-specific task grouping to the caller. Callers must close the
     * returned iterator and discard all previously consumed splits if iteration fails.
     */
    default SplitPlan openSplitPlan() {
        return SplitPlan.fromPlan(read());
    }

    /** Get splits plan from file changes. */
    Plan readChanges();

    Plan readIncrementalDiff(Snapshot before);

    /** List partitions. */
    List<BinaryRow> partitions();

    List<PartitionEntry> partitionEntries();

    List<BucketEntry> bucketEntries();

    Iterator<ManifestEntry> readFileIterator();

    /** Result plan of this scan. */
    interface Plan extends TableScan.Plan {

        @Nullable
        Long watermark();

        /**
         * Snapshot id of this plan, return null if the table is empty or the manifest list is
         * specified.
         */
        @Nullable
        Long snapshotId();

        /** Snapshot used to plan the splits, if available. */
        @Nullable
        Snapshot snapshot();

        /** Result splits. */
        List<Split> splits();

        @SuppressWarnings({"unchecked", "rawtypes"})
        default List<DataSplit> dataSplits() {
            return (List) splits();
        }
    }

    /** Metadata and a closeable, single-use iterator for a split plan. */
    final class SplitPlan {
        @Nullable private final Long watermark;
        @Nullable private final Long snapshotId;
        private final boolean fineGrained;
        private final CloseableIterator<Split> splits;

        public SplitPlan(
                @Nullable Long watermark,
                @Nullable Long snapshotId,
                boolean fineGrained,
                CloseableIterator<Split> splits) {
            this.watermark = watermark;
            this.snapshotId = snapshotId;
            this.fineGrained = fineGrained;
            this.splits = splits;
        }

        public static SplitPlan fromPlan(Plan plan) {
            return new SplitPlan(
                    plan.watermark(),
                    plan.snapshotId(),
                    false,
                    CloseableIterator.adapterForIterator(plan.splits().iterator()));
        }

        public static SplitPlan fromTablePlan(TableScan.Plan plan) {
            if (plan instanceof Plan) {
                return fromPlan((Plan) plan);
            }
            return fromTablePlan(plan, null, null);
        }

        public static SplitPlan fromTablePlan(
                TableScan.Plan plan, @Nullable Long watermark, @Nullable Long snapshotId) {
            return new SplitPlan(
                    watermark,
                    snapshotId,
                    false,
                    CloseableIterator.adapterForIterator(plan.splits().iterator()));
        }

        @Nullable
        public Long watermark() {
            return watermark;
        }

        @Nullable
        public Long snapshotId() {
            return snapshotId;
        }

        /** Whether the iterator intentionally leaves task grouping to the caller. */
        public boolean fineGrained() {
            return fineGrained;
        }

        /** Returns the single-use iterator. The caller must close it. */
        public CloseableIterator<Split> splits() {
            return splits;
        }
    }
}
