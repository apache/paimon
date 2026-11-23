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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.Snapshot;
import org.apache.paimon.append.dataevolution.DataEvolutionDeletionVectorRewriteTask.IndexRewrite;
import org.apache.paimon.append.dataevolution.DataEvolutionDeletionVectorRewriteTask.Rewrite;
import org.apache.paimon.append.dataevolution.DataEvolutionDeletionVectorRewriteTask.Source;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RangeHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.table.BucketMode.UNAWARE_BUCKET;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.DataEvolutionUtils.checkContiguousRowRange;
import static org.apache.paimon.utils.DataEvolutionUtils.retrieveAnchorFile;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * Plans bitmap work without reading bitmap contents or changing data-compaction boundaries.
 *
 * <p>All compact messages are collected first, so one old index file has a single owner even when
 * its anchors are replaced by different data tasks.
 */
public class DataEvolutionDeletionVectorRewritePlanner {

    private static final int MAX_DELETE_ONLY_FILES_PER_TASK = 128;

    private final FileStoreTable table;
    private final Snapshot snapshot;

    public DataEvolutionDeletionVectorRewritePlanner(FileStoreTable table, Snapshot snapshot) {
        this.table = table;
        this.snapshot = snapshot;
    }

    public List<DataEvolutionDeletionVectorRewriteTask> plan(List<CommitMessage> messages) {
        if (!table.coreOptions().deletionVectorsEnabled() || messages.isEmpty()) {
            return Collections.emptyList();
        }
        Map<BinaryRow, PartitionPlan> partitions = new LinkedHashMap<>();
        RangeHelper<DataFileMeta> ranges = new RangeHelper<>(DataFileMeta::nonNullRowIdRange);
        for (CommitMessage message : messages) {
            CommitMessageImpl commit = (CommitMessageImpl) message;
            CompactIncrement increment = commit.compactIncrement();
            checkState(
                    commit.bucket() == UNAWARE_BUCKET && commit.totalBuckets() == null,
                    "DV rewrite requires unaware-bucket compact messages.");
            checkState(
                    increment.newIndexFiles().isEmpty() && increment.deletedIndexFiles().isEmpty(),
                    "DV rewrite input must not contain prepared index changes.");
            List<DataFileMeta> before = normalFiles(increment.compactBefore());
            List<DataFileMeta> after = normalFiles(increment.compactAfter());
            if (before.isEmpty() && after.isEmpty()) {
                continue;
            }
            PartitionPlan partition =
                    partitions.computeIfAbsent(commit.partition(), ignored -> new PartitionPlan());
            Map<String, Range> anchors = new LinkedHashMap<>();
            for (List<DataFileMeta> group : ranges.mergeOverlappingRanges(before)) {
                DataFileMeta anchor = retrieveAnchorFile(group, file -> file);
                checkState(
                        partition.removed.add(anchor.fileName()),
                        "Anchor %s belongs to multiple DV rewrite messages.",
                        anchor.fileName());
                anchors.put(anchor.fileName(), anchor.nonNullRowIdRange());
            }
            boolean materialized =
                    !before.isEmpty()
                            && increment.compactAfter().stream()
                                    .allMatch(file -> file.firstRowId() == null);
            if (!materialized) {
                checkState(
                        checkContiguousRowRange(before).equals(checkContiguousRowRange(after)),
                        "DV rewrite must preserve the input row-ID range.");
                Map<String, Range> outputs = new LinkedHashMap<>();
                after.forEach(file -> outputs.put(file.fileName(), file.nonNullRowIdRange()));
                partition.specifications.add(new Specification(anchors, outputs));
            }
        }
        if (partitions.isEmpty()) {
            return Collections.emptyList();
        }
        IndexFileHandler handler = table.store().newIndexFileHandler();
        Set<Pair<BinaryRow, Integer>> buckets =
                partitions.keySet().stream()
                        .map(partition -> Pair.of(partition, UNAWARE_BUCKET))
                        .collect(Collectors.toSet());
        for (IndexManifestEntry entry :
                handler.scanBucketEntries(snapshot, DELETION_VECTORS_INDEX, buckets)) {
            PartitionPlan partition = partitions.get(entry.partition());
            Set<String> removed = new LinkedHashSet<>();
            String path = null;
            for (DeletionVectorMeta dv : entry.indexFile().dvRanges().values()) {
                if (partition.removed.contains(dv.dataFileName())) {
                    if (path == null) {
                        path = handler.filePath(entry).toString();
                    }
                    checkState(
                            partition.deletions.put(
                                            dv.dataFileName(),
                                            new DeletionFile(
                                                    path,
                                                    dv.offset(),
                                                    dv.length(),
                                                    dv.cardinality()))
                                    == null,
                            "Duplicate DV for anchor %s.",
                            dv.dataFileName());
                    removed.add(dv.dataFileName());
                }
            }
            if (!removed.isEmpty()) {
                partition.indexes.add(new IndexRewrite(entry.indexFile(), removed));
            }
        }

        List<DataEvolutionDeletionVectorRewriteTask> result = new ArrayList<>();
        long target = table.coreOptions().dvIndexFileTargetSize().getBytes();
        partitions.forEach(
                (partition, plan) -> {
                    Batch batch = new Batch(partition, target, result);
                    for (Specification spec : plan.specifications) {
                        List<Source> sources = new ArrayList<>();
                        long weight = 0;
                        for (Map.Entry<String, Range> anchor : spec.anchors.entrySet()) {
                            DeletionFile deletion = plan.deletions.get(anchor.getKey());
                            if (deletion != null) {
                                sources.add(
                                        new Source(anchor.getKey(), anchor.getValue(), deletion));
                                weight += deletion.length();
                            }
                        }
                        if (!sources.isEmpty()) {
                            // Keep all contributors to an output anchor together, even across old
                            // indexes.
                            batch.ensureCapacity(weight);
                            batch.task.rewrites.add(new Rewrite(sources, spec.outputs));
                        }
                    }
                    for (IndexRewrite index : plan.indexes) {
                        long weight = 0;
                        for (DeletionVectorMeta dv : index.file.dvRanges().values()) {
                            if (!index.removed.contains(dv.dataFileName())) {
                                weight += dv.length();
                            }
                        }
                        batch.ensureCapacity(weight);
                        batch.task.indexes.add(index);
                    }
                    batch.finish();
                });
        return result;
    }

    private static List<DataFileMeta> normalFiles(List<DataFileMeta> files) {
        return files.stream()
                .filter(file -> !isBlobFile(file.fileName()) && !isVectorStoreFile(file.fileName()))
                .collect(Collectors.toList());
    }

    private static class PartitionPlan {
        private final Set<String> removed = new LinkedHashSet<>();
        private final Map<String, DeletionFile> deletions = new LinkedHashMap<>();
        private final List<Specification> specifications = new ArrayList<>();
        private final List<IndexRewrite> indexes = new ArrayList<>();
    }

    private static class Specification {
        private final Map<String, Range> anchors;
        private final Map<String, Range> outputs;

        private Specification(Map<String, Range> anchors, Map<String, Range> outputs) {
            this.anchors = anchors;
            this.outputs = outputs;
        }
    }

    private static class Batch {
        private final BinaryRow partition;
        private final long target;
        private final List<DataEvolutionDeletionVectorRewriteTask> result;
        private DataEvolutionDeletionVectorRewriteTask task;
        private long weight;

        private Batch(
                BinaryRow partition,
                long target,
                List<DataEvolutionDeletionVectorRewriteTask> result) {
            this.partition = partition;
            this.target = target;
            this.result = result;
            this.task = new DataEvolutionDeletionVectorRewriteTask(partition);
        }

        private void ensureCapacity(long nextWeight) {
            if (task.workCount() > 0
                    && (weight + nextWeight > target
                            || (nextWeight == 0
                                    && task.indexes.size() >= MAX_DELETE_ONLY_FILES_PER_TASK))) {
                finish();
            }
            weight += nextWeight;
        }

        private void finish() {
            if (task.workCount() > 0) {
                result.add(task);
                task = new DataEvolutionDeletionVectorRewriteTask(partition);
                weight = 0;
            }
        }
    }
}
