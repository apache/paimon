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
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexDefinition;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourcePolicy;
import org.apache.paimon.index.pkfulltext.PkFullTextBucketIndexState;
import org.apache.paimon.index.pkfulltext.PkFullTextIndexFile;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Plans compaction-visible full-text inputs from one captured primary-key snapshot. */
public class PrimaryKeyFullTextScan implements FullTextScan {

    private final FileStoreTable table;
    private final PrimaryKeyIndexDefinition definition;
    @Nullable private final PartitionPredicate partitionFilter;
    @Nullable private final Snapshot pinnedSnapshot;

    public PrimaryKeyFullTextScan(
            FileStoreTable table,
            PrimaryKeyIndexDefinition definition,
            @Nullable PartitionPredicate partitionFilter) {
        this(table, definition, partitionFilter, null);
    }

    PrimaryKeyFullTextScan(
            FileStoreTable table,
            PrimaryKeyIndexDefinition definition,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable Snapshot pinnedSnapshot) {
        checkArgument(
                definition.family() == PrimaryKeyIndexDefinition.Family.FULL_TEXT,
                "Primary-key full-text scan requires a full-text index definition.");
        this.table = table;
        this.definition = definition;
        this.partitionFilter = partitionFilter;
        this.pinnedSnapshot = pinnedSnapshot;
    }

    @Override
    public Plan scan() {
        FileStoreTable scanTable = scanTable();
        SnapshotReader snapshotReader = scanTable.newSnapshotReader().keepStats();
        DataTableScan dataScan = scanTable.newScan(ignored -> snapshotReader);
        checkArgument(
                dataScan instanceof PrimaryKeyBatchScan,
                "Primary-key full-text search requires a primary-key batch scan.");
        PrimaryKeyBatchScan batchScan = (PrimaryKeyBatchScan) dataScan;
        if (partitionFilter != null) {
            batchScan.withPartitionFilter(partitionFilter);
        }
        TableScan.Plan tablePlan = batchScan.planWithoutAuth();
        if (!(tablePlan instanceof SnapshotReader.Plan)) {
            checkArgument(
                    tablePlan.splits().isEmpty(),
                    "Primary-key full-text search requires a snapshot plan.");
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
        checkArgument(snapshot != null, "Primary-key full-text snapshot does not exist.");
        checkArgument(
                snapshot.id() == snapshotPlan.snapshotId(),
                "Primary-key full-text plan snapshot %s does not match pinned snapshot %s.",
                snapshotPlan.snapshotId(),
                snapshot.id());

        IndexFileHandler indexFileHandler = snapshotReader.indexFileHandler();
        checkArgument(
                indexFileHandler != null, "Primary-key full-text index handler is unavailable.");
        List<IndexManifestEntry> payloadEntries =
                indexFileHandler.scan(
                        snapshot,
                        entry ->
                                matchesDefinition(entry)
                                        && (partitionFilter == null
                                                || partitionFilter.test(entry.partition())));
        return plan(snapshot.id(), snapshotPlan.splits(), payloadEntries, definition.fieldId());
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

    private boolean matchesDefinition(IndexManifestEntry entry) {
        IndexFileMeta payload = entry.indexFile();
        GlobalIndexMeta globalMeta = payload.globalIndexMeta();
        if (!PkFullTextIndexFile.INDEX_TYPE.equals(payload.indexType())
                || globalMeta == null
                || globalMeta.sourceMeta() == null
                || globalMeta.indexFieldId() != definition.fieldId()) {
            return false;
        }
        return true;
    }

    static Plan plan(
            long snapshotId,
            List<? extends Split> dataSplits,
            List<IndexManifestEntry> payloadEntries,
            int textFieldId) {
        Map<Pair<BinaryRow, Integer>, List<IndexFileMeta>> payloads = new LinkedHashMap<>();
        for (IndexManifestEntry entry : payloadEntries) {
            checkArgument(
                    entry.kind() == FileKind.ADD,
                    "Primary-key full-text index file %s is not active.",
                    entry.indexFile().fileName());
            Pair<BinaryRow, Integer> key = Pair.of(entry.partition(), entry.bucket());
            payloads.computeIfAbsent(key, ignored -> new ArrayList<>()).add(entry.indexFile());
        }

        Map<Pair<BinaryRow, Integer>, BucketAccumulator> buckets = new LinkedHashMap<>();
        for (Split split : dataSplits) {
            DataSplit dataSplit = unwrapDataSplit(split);
            checkArgument(
                    dataSplit.snapshotId() == snapshotId,
                    "Data split snapshot %s does not match full-text scan snapshot %s.",
                    dataSplit.snapshotId(),
                    snapshotId);
            checkArgument(
                    !dataSplit.isStreaming(),
                    "Primary-key full-text search requires a batch split.");
            if (dataSplit.bucket() < 0) {
                continue;
            }
            Pair<BinaryRow, Integer> key = Pair.of(dataSplit.partition(), dataSplit.bucket());
            BucketAccumulator accumulator = buckets.get(key);
            if (accumulator == null) {
                accumulator = new BucketAccumulator(dataSplit);
                buckets.put(key, accumulator);
            }
            accumulator.add(dataSplit);
        }

        List<PrimaryKeyFullTextSearchSplit> result = new ArrayList<>();
        for (Map.Entry<Pair<BinaryRow, Integer>, BucketAccumulator> entry : buckets.entrySet()) {
            BucketAccumulator bucket = entry.getValue();
            if (bucket.isEmpty()) {
                continue;
            }
            PkFullTextBucketIndexState state =
                    PkFullTextBucketIndexState.fromActivePayloads(
                            textFieldId,
                            payloads.getOrDefault(entry.getKey(), Collections.emptyList()));
            Set<String> activeSources = bucket.dataFileNames();
            Map<String, IndexFileMeta> currentPayloads = new LinkedHashMap<>();
            Set<String> covered = new HashSet<>();
            for (Map.Entry<String, IndexFileMeta> payload :
                    state.payloadBySourceFile().entrySet()) {
                if (activeSources.contains(payload.getKey())) {
                    currentPayloads.put(payload.getValue().fileName(), payload.getValue());
                    covered.add(payload.getKey());
                }
            }
            List<String> uncovered = new ArrayList<>();
            for (DataFileMeta dataFile : bucket.dataFiles()) {
                if (!covered.contains(dataFile.fileName())) {
                    uncovered.add(dataFile.fileName());
                }
            }
            result.add(
                    new PrimaryKeyFullTextSearchSplit(
                            bucket.build(), new ArrayList<>(currentPayloads.values()), uncovered));
        }
        return new Plan(snapshotId, result);
    }

    private static DataSplit unwrapDataSplit(Split split) {
        if (split instanceof IndexedSplit) {
            return ((IndexedSplit) split).dataSplit();
        }
        checkArgument(
                split instanceof DataSplit,
                "Unsupported primary-key full-text source split: %s.",
                split.getClass().getName());
        return (DataSplit) split;
    }

    /** Immutable snapshot full-text plan. */
    public static class Plan implements FullTextScan.Plan {

        private final long snapshotId;
        private final List<FullTextSearchSplit> splits;

        private Plan(long snapshotId, List<PrimaryKeyFullTextSearchSplit> splits) {
            this.snapshotId = snapshotId;
            this.splits = Collections.unmodifiableList(new ArrayList<FullTextSearchSplit>(splits));
        }

        public long snapshotId() {
            return snapshotId;
        }

        @Override
        public List<FullTextSearchSplit> splits() {
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
        private boolean hasDeletionMetadata;

        private BucketAccumulator(DataSplit split) {
            this.snapshotId = split.snapshotId();
            this.partition = split.partition();
            this.bucket = split.bucket();
            this.bucketPath = split.bucketPath();
            this.totalBuckets = split.totalBuckets();
        }

        private void add(DataSplit split) {
            checkArgument(
                    snapshotId == split.snapshotId()
                            && partition.equals(split.partition())
                            && bucket == split.bucket()
                            && bucketPath.equals(split.bucketPath()),
                    "Cannot combine data splits from different snapshot buckets.");
            checkArgument(
                    totalBuckets == null
                            ? split.totalBuckets() == null
                            : totalBuckets.equals(split.totalBuckets()),
                    "Bucket split total-bucket metadata is inconsistent.");
            List<DeletionFile> splitDeletions = split.deletionFiles().orElse(null);
            checkArgument(
                    splitDeletions == null || splitDeletions.size() == split.dataFiles().size(),
                    "Deletion files must align with data files in a full-text bucket split.");
            hasDeletionMetadata |= splitDeletions != null;
            for (int i = 0; i < split.dataFiles().size(); i++) {
                DataFileMeta file = split.dataFiles().get(i);
                if (!PrimaryKeyIndexSourcePolicy.shouldRead(file)) {
                    continue;
                }
                checkArgument(
                        dataFileNames.add(file.fileName()),
                        "Data file %s appears more than once in full-text bucket planning.",
                        file.fileName());
                dataFiles.add(file);
                deletionFiles.add(splitDeletions == null ? null : splitDeletions.get(i));
            }
        }

        private boolean isEmpty() {
            return dataFiles.isEmpty();
        }

        private List<DataFileMeta> dataFiles() {
            return dataFiles;
        }

        private Set<String> dataFileNames() {
            return dataFileNames;
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
            if (hasDeletionMetadata) {
                builder.withDataDeletionFiles(deletionFiles);
            }
            return builder.build();
        }
    }
}
