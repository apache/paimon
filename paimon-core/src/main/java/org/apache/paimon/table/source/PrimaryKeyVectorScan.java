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

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.TimeTravelUtil;
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

/** Plans complete bucket-local vector inputs from one captured primary-key table snapshot. */
public class PrimaryKeyVectorScan implements VectorScan {

    private final FileStoreTable table;
    private final int vectorFieldId;
    private final String indexType;
    @Nullable private final PartitionPredicate partitionFilter;

    public PrimaryKeyVectorScan(
            FileStoreTable table,
            int vectorFieldId,
            String indexType,
            @Nullable PartitionPredicate partitionFilter) {
        this.table = table;
        this.vectorFieldId = vectorFieldId;
        this.indexType = indexType;
        this.partitionFilter = partitionFilter;
    }

    @Override
    public Plan scan() {
        checkArgument(
                table.coreOptions().primaryKeyVectorIndexEnabled(),
                "Primary-key vector search requires a configured primary-key vector index.");
        @Nullable Snapshot snapshot = TimeTravelUtil.tryTravelOrLatest(table);
        if (snapshot == null) {
            return new Plan(0, Collections.emptyList());
        }

        SnapshotReader snapshotReader =
                table.newSnapshotReader().withSnapshot(snapshot).withMode(ScanMode.ALL).keepStats();
        if (table.coreOptions().bucket() == BucketMode.POSTPONE_BUCKET) {
            snapshotReader.onlyReadRealBuckets();
        }
        if (partitionFilter != null) {
            snapshotReader.withPartitionFilter(partitionFilter);
        }
        List<DataSplit> dataSplits = snapshotReader.read().dataSplits();

        IndexFileHandler indexFileHandler = table.store().newIndexFileHandler();
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
        return plan(snapshot.id(), dataSplits, vectorIndexEntries);
    }

    static Plan plan(
            long snapshotId,
            List<DataSplit> dataSplits,
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
        for (DataSplit split : dataSplits) {
            checkArgument(
                    split.snapshotId() == snapshotId,
                    "Data split snapshot %s does not match vector scan snapshot %s.",
                    split.snapshotId(),
                    snapshotId);
            checkArgument(
                    !split.isStreaming(), "Primary-key vector search requires a batch split.");
            Pair<BinaryRow, Integer> key = Pair.of(split.partition(), split.bucket());
            BucketAccumulator accumulator = buckets.get(key);
            if (accumulator == null) {
                accumulator = new BucketAccumulator(split);
                buckets.put(key, accumulator);
            }
            accumulator.add(split);
        }

        List<BucketVectorSearchSplit> result = new ArrayList<>(buckets.size());
        for (Map.Entry<Pair<BinaryRow, Integer>, BucketAccumulator> entry : buckets.entrySet()) {
            result.add(
                    new BucketVectorSearchSplit(
                            entry.getValue().build(),
                            payloads.getOrDefault(entry.getKey(), Collections.emptyList())));
        }
        return new Plan(snapshotId, result);
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
        private boolean hasDeletionFile;

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
                    "Deletion files must align with data files in a bucket split.");
            for (int i = 0; i < split.dataFiles().size(); i++) {
                DataFileMeta file = split.dataFiles().get(i);
                checkArgument(
                        dataFileNames.add(file.fileName()),
                        "Data file %s appears more than once in vector bucket planning.",
                        file.fileName());
                dataFiles.add(file);
                DeletionFile deletion = splitDeletions == null ? null : splitDeletions.get(i);
                deletionFiles.add(deletion);
                hasDeletionFile |= deletion != null;
            }
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
