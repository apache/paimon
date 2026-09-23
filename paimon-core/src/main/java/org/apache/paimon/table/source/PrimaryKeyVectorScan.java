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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Plans complete bucket-local vector inputs from one captured primary-key table snapshot. */
public class PrimaryKeyVectorScan implements VectorScan {

    private final FileStoreTable table;
    private final int vectorFieldId;
    private final String indexType;
    @Nullable private final PartitionPredicate partitionFilter;
    @Nullable private final Predicate filter;
    @Nullable private final Snapshot pinnedSnapshot;

    public PrimaryKeyVectorScan(
            FileStoreTable table,
            int vectorFieldId,
            String indexType,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable Predicate filter) {
        this(table, vectorFieldId, indexType, partitionFilter, filter, null);
    }

    PrimaryKeyVectorScan(
            FileStoreTable table,
            int vectorFieldId,
            String indexType,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable Predicate filter,
            @Nullable Snapshot pinnedSnapshot) {
        this.table = table;
        this.vectorFieldId = vectorFieldId;
        this.indexType = indexType;
        this.partitionFilter = partitionFilter;
        this.filter = filter;
        this.pinnedSnapshot = pinnedSnapshot;
    }

    @Override
    public Plan scan() {
        checkArgument(
                table.coreOptions().primaryKeyVectorIndexEnabled(),
                "Primary-key vector search requires a configured primary-key vector index.");
        checkArgument(
                filter == null
                        || (table.coreOptions().deletionVectorsEnabled()
                                && !table.coreOptions().deletionVectorsMergeOnRead()),
                "Primary-key vector pre-filter requires deletion vectors without merge-on-read.");
        FileStoreTable scanTable = scanTable();
        SnapshotReader snapshotReader = scanTable.newSnapshotReader().keepStats();
        DataTableScan dataScan = scanTable.newScan(ignored -> snapshotReader);
        checkArgument(
                dataScan instanceof PrimaryKeyBatchScan,
                "Primary-key vector search requires a primary-key batch scan.");
        PrimaryKeyBatchScan batchScan = (PrimaryKeyBatchScan) dataScan;
        if (partitionFilter != null) {
            batchScan.withPartitionFilter(partitionFilter);
        }
        if (filter != null) {
            batchScan.withFilter(filter);
        }
        TableScan.Plan tablePlan = batchScan.planWithoutAuth();
        if (!(tablePlan instanceof SnapshotReader.Plan)) {
            checkArgument(
                    tablePlan.splits().isEmpty(),
                    "Primary-key vector search requires a snapshot plan.");
            return new Plan(0, Collections.emptyList());
        }
        SnapshotReader.Plan snapshotPlan = (SnapshotReader.Plan) tablePlan;
        if (snapshotPlan.snapshotId() == null) {
            return new Plan(0, Collections.emptyList());
        }
        Snapshot snapshot =
                pinnedSnapshot == null
                        ? snapshotReader.snapshotManager().snapshot(snapshotPlan.snapshotId())
                        : pinnedSnapshot;
        checkArgument(snapshot != null, "Primary-key vector snapshot does not exist.");
        checkArgument(
                snapshot.id() == snapshotPlan.snapshotId(),
                "Primary-key vector plan snapshot %s does not match pinned snapshot %s.",
                snapshotPlan.snapshotId(),
                snapshot.id());

        IndexFileHandler indexFileHandler = snapshotReader.indexFileHandler();
        checkArgument(indexFileHandler != null, "Primary-key vector index handler is unavailable.");
        List<IndexManifestEntry> vectorIndexEntries =
                indexFileHandler.scan(
                        snapshot,
                        entry ->
                                entry.indexFile().indexType().equals(indexType)
                                        && entry.indexFile().globalIndexMeta() != null
                                        && entry.indexFile().globalIndexMeta().sourceMeta() != null
                                        && entry.indexFile().globalIndexMeta().indexFieldId()
                                                == vectorFieldId
                                        && (partitionFilter == null
                                                || partitionFilter.test(entry.partition())));
        return plan(snapshot.id(), snapshotPlan.splits(), vectorIndexEntries);
    }

    private FileStoreTable scanTable() {
        if (pinnedSnapshot == null) {
            return table;
        }
        return (FileStoreTable)
                table.copy(
                        Collections.singletonMap(
                                CoreOptions.SCAN_SNAPSHOT_ID.key(),
                                String.valueOf(pinnedSnapshot.id())));
    }

    static Plan plan(
            long snapshotId,
            List<? extends Split> dataSplits,
            List<IndexManifestEntry> vectorIndexEntries) {
        Map<Pair<BinaryRow, Integer>, List<IndexFileMeta>> payloads = new LinkedHashMap<>();
        for (IndexManifestEntry entry : vectorIndexEntries) {
            checkArgument(
                    entry.kind() == FileKind.ADD,
                    "Primary-key vector index file %s is not active.",
                    entry.indexFile().fileName());
            checkArgument(
                    entry.indexFile().globalIndexMeta() != null
                            && entry.indexFile().globalIndexMeta().sourceMeta() != null,
                    "Primary-key vector index file %s has no source metadata.",
                    entry.indexFile().fileName());
            Pair<BinaryRow, Integer> key = Pair.of(entry.partition(), entry.bucket());
            payloads.computeIfAbsent(key, ignored -> new ArrayList<>()).add(entry.indexFile());
        }

        Map<Pair<BinaryRow, Integer>, BucketAccumulator> buckets = new LinkedHashMap<>();
        for (Split split : dataSplits) {
            DataSplit dataSplit = unwrapDataSplit(split);
            checkArgument(
                    dataSplit.snapshotId() == snapshotId,
                    "Data split snapshot %s does not match vector scan snapshot %s.",
                    dataSplit.snapshotId(),
                    snapshotId);
            checkArgument(
                    !dataSplit.isStreaming(), "Primary-key vector search requires a batch split.");
            Pair<BinaryRow, Integer> key = Pair.of(dataSplit.partition(), dataSplit.bucket());
            BucketAccumulator accumulator = buckets.get(key);
            if (accumulator == null) {
                accumulator = new BucketAccumulator(dataSplit);
                buckets.put(key, accumulator);
            }
            accumulator.add(split);
        }

        List<BucketVectorSearchSplit> result = new ArrayList<>(buckets.size());
        for (Map.Entry<Pair<BinaryRow, Integer>, BucketAccumulator> entry : buckets.entrySet()) {
            result.add(
                    new BucketVectorSearchSplit(
                            entry.getValue().build(),
                            payloads.getOrDefault(entry.getKey(), Collections.emptyList()),
                            entry.getValue().rowRangesByFile()));
        }
        return new Plan(snapshotId, result);
    }

    private static DataSplit unwrapDataSplit(Split split) {
        if (split instanceof IndexedSplit) {
            return ((IndexedSplit) split).dataSplit();
        }
        checkArgument(
                split instanceof DataSplit,
                "Unsupported primary-key vector source split: %s.",
                split.getClass().getName());
        return (DataSplit) split;
    }

    /** Immutable snapshot vector-search plan. */
    public static class Plan implements VectorScan.Plan {

        private final long snapshotId;
        private final List<VectorSearchSplit> splits;

        private Plan(long snapshotId, List<BucketVectorSearchSplit> splits) {
            this.snapshotId = snapshotId;
            this.splits = Collections.unmodifiableList(new ArrayList<VectorSearchSplit>(splits));
        }

        public long snapshotId() {
            return snapshotId;
        }

        @Override
        public List<VectorSearchSplit> splits() {
            return splits;
        }
    }

    private static class BucketAccumulator {

        private final long snapshotId;
        private final BinaryRow partition;
        private final int bucket;
        private final String bucketPath;
        @Nullable private final Integer totalBuckets;
        private final List<DataFileMeta> dataFiles = new ArrayList<>();
        private final List<DeletionFile> deletionFiles = new ArrayList<>();
        private final Set<String> dataFileNames = new HashSet<>();
        private final Map<String, List<Range>> rowRangesByFile = new LinkedHashMap<>();
        private boolean hasDeletionFile;

        private BucketAccumulator(DataSplit split) {
            this.snapshotId = split.snapshotId();
            this.partition = split.partition();
            this.bucket = split.bucket();
            this.bucketPath = split.bucketPath();
            this.totalBuckets = split.totalBuckets();
        }

        private void add(Split split) {
            DataSplit dataSplit;
            List<Range> rowRanges = null;
            if (split instanceof IndexedSplit) {
                IndexedSplit indexedSplit = (IndexedSplit) split;
                dataSplit = indexedSplit.dataSplit();
                checkArgument(
                        dataSplit.dataFiles().size() == 1,
                        "Primary-key vector pre-filter split must contain one data file.");
                rowRanges = indexedSplit.rowRanges();
            } else {
                dataSplit = unwrapDataSplit(split);
            }
            checkArgument(
                    snapshotId == dataSplit.snapshotId()
                            && partition.equals(dataSplit.partition())
                            && bucket == dataSplit.bucket()
                            && bucketPath.equals(dataSplit.bucketPath()),
                    "Cannot combine data splits from different snapshot buckets.");
            checkArgument(
                    totalBuckets == null
                            ? dataSplit.totalBuckets() == null
                            : totalBuckets.equals(dataSplit.totalBuckets()),
                    "Bucket split total-bucket metadata is inconsistent.");

            List<DeletionFile> splitDeletions = dataSplit.deletionFiles().orElse(null);
            checkArgument(
                    splitDeletions == null || splitDeletions.size() == dataSplit.dataFiles().size(),
                    "Deletion files must align with data files in a bucket split.");
            for (int i = 0; i < dataSplit.dataFiles().size(); i++) {
                DataFileMeta file = dataSplit.dataFiles().get(i);
                checkArgument(
                        dataFileNames.add(file.fileName()),
                        "Data file %s appears more than once in vector bucket planning.",
                        file.fileName());
                dataFiles.add(file);
                DeletionFile deletion = splitDeletions == null ? null : splitDeletions.get(i);
                deletionFiles.add(deletion);
                hasDeletionFile |= deletion != null;
            }
            if (rowRanges != null) {
                rowRangesByFile.put(
                        dataSplit.dataFiles().get(0).fileName(), new ArrayList<>(rowRanges));
            }
        }

        private Map<String, List<Range>> rowRangesByFile() {
            return rowRangesByFile;
        }

        private DataSplit build() {
            DataSplit.Builder builder =
                    DataSplit.builder()
                            .withSnapshot(snapshotId)
                            .withPartition(partition)
                            .withBucket(bucket)
                            .withBucketPath(bucketPath)
                            .withTotalBuckets(totalBuckets)
                            .withDataFiles(dataFiles)
                            .isStreaming(false)
                            .rawConvertible(false);
            if (hasDeletionFile) {
                builder.withDataDeletionFiles(deletionFiles);
            }
            return builder.build();
        }
    }
}
