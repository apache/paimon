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

package org.apache.paimon.flink.dataevolution;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.Bitmap64DeletionVector;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.append.BaseAppendDeleteFileMaintainer;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.utils.BoundedOneInputOperator;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.SerializationUtils;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.table.BucketMode.UNAWARE_BUCKET;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * A bounded Flink operator which persists deletion vectors for a data-evolution table.
 *
 * <p>All updates of the same rewrite group must be shuffled to the same operator subtask before
 * this operator. A rewrite group contains all anchors backed by the same existing deletion-vector
 * index file. Anchors without an existing deletion vector are assigned to stable shards. This
 * ownership rule prevents two subtasks from concurrently replacing the same old index file while
 * still allowing an unaware-bucket partition to be processed in parallel.
 */
public class DataEvolutionDeleteOperator
        extends BoundedOneInputOperator<
                DataEvolutionDeleteOperator.DeletionVectorUpdate, Committable> {

    private static final long serialVersionUID = 1L;

    private final FileStoreTable table;
    private final long baseSnapshotId;

    private transient Map<String, RewriteGroupDeletionVectors> deletionVectorsByRewriteGroup;

    public DataEvolutionDeleteOperator(FileStoreTable table, long baseSnapshotId) {
        this.table = table;
        this.baseSnapshotId = baseSnapshotId;
    }

    @Override
    public void open() throws Exception {
        super.open();

        checkArgument(
                table.bucketMode() == BucketMode.BUCKET_UNAWARE,
                "Data-evolution delete only supports unaware bucket mode, but table bucket mode is %s.",
                table.bucketMode());
        checkArgument(
                table.coreOptions().dataEvolutionEnabled(),
                "Data-evolution delete requires data-evolution.enabled to be true.");
        checkArgument(
                table.coreOptions().deletionVectorsEnabled(),
                "Data-evolution delete requires deletion-vectors.enabled to be true.");

        // Fail early if the snapshot used to map row ids has already expired.
        table.snapshot(baseSnapshotId);

        deletionVectorsByRewriteGroup = new LinkedHashMap<>();
    }

    @Override
    public void processElement(StreamRecord<DeletionVectorUpdate> element) {
        DeletionVectorUpdate update =
                checkNotNull(element.getValue(), "Deletion-vector update is null.");
        String rewriteGroup = checkNotNull(update.getRewriteGroup(), "Rewrite group is null.");
        String bucketPath = checkNotNull(update.getBucketPath(), "Bucket path is null.");
        byte[] serializedPartition =
                checkNotNull(update.getSerializedPartition(), "Serialized partition is null.");
        String dataFilePath =
                checkNotNull(update.getDataFilePath(), "Anchor data file path is null.");
        byte[] serializedDeletionVector =
                checkNotNull(
                        update.getSerializedDeletionVector(),
                        "Serialized deletion vector is null.");

        RewriteGroupDeletionVectors rewriteGroupDeletionVectors =
                deletionVectorsByRewriteGroup.get(rewriteGroup);
        if (rewriteGroupDeletionVectors == null) {
            // BinaryRow created by deserializeBinaryRow points to the input byte array. Copy both
            // representations because Flink may reuse the input object after processElement
            // returns. Deserialization is needed only for the first update of a rewrite group.
            byte[] copiedPartition = Arrays.copyOf(serializedPartition, serializedPartition.length);
            BinaryRow partition = SerializationUtils.deserializeBinaryRow(copiedPartition).copy();
            rewriteGroupDeletionVectors =
                    new RewriteGroupDeletionVectors(
                            bucketPath, update.getOldIndexFileName(), partition, copiedPartition);
            deletionVectorsByRewriteGroup.put(rewriteGroup, rewriteGroupDeletionVectors);
        } else {
            checkArgument(
                    rewriteGroupDeletionVectors.bucketPath.equals(bucketPath),
                    "Rewrite group %s is associated with different bucket paths.",
                    rewriteGroup);
            checkArgument(
                    Objects.equals(
                            rewriteGroupDeletionVectors.oldIndexFileName,
                            update.getOldIndexFileName()),
                    "Rewrite group %s is associated with different old index files.",
                    rewriteGroup);
            checkArgument(
                    Arrays.equals(
                            rewriteGroupDeletionVectors.serializedPartition, serializedPartition),
                    "Rewrite group %s is associated with different partitions.",
                    rewriteGroup);
        }

        String dataFileName = new Path(dataFilePath).getName();
        DeletionVector deletionVector =
                DeletionVector.deserializeFromBytes(serializedDeletionVector);
        DeletionVector previous =
                rewriteGroupDeletionVectors.deletionVectorsByDataFile.putIfAbsent(
                        dataFileName, deletionVector);
        if (previous != null) {
            // This is not expected after anchor-level shuffling, but merging keeps the writer
            // correct if upstream parallelism or partitioning is changed in the future.
            previous.merge(deletionVector);
        }
    }

    @Override
    public void endInput() {
        if (deletionVectorsByRewriteGroup.isEmpty()) {
            return;
        }

        Snapshot baseSnapshot = table.snapshot(baseSnapshotId);
        IndexFileHandler indexFileHandler = table.store().newIndexFileHandler();
        Map<BinaryRow, Set<String>> ownedIndexFileNamesByPartition = new HashMap<>();
        for (RewriteGroupDeletionVectors rewriteGroupDeletionVectors :
                deletionVectorsByRewriteGroup.values()) {
            if (rewriteGroupDeletionVectors.oldIndexFileName != null) {
                ownedIndexFileNamesByPartition
                        .computeIfAbsent(
                                rewriteGroupDeletionVectors.partition.copy(),
                                ignored -> new HashSet<>())
                        .add(rewriteGroupDeletionVectors.oldIndexFileName);
            }
        }

        Map<BinaryRow, Map<String, IndexManifestEntry>> indexFilesByPartition = new HashMap<>();
        if (!ownedIndexFileNamesByPartition.isEmpty()) {
            for (IndexManifestEntry entry :
                    indexFileHandler.scan(
                            baseSnapshot,
                            candidate -> {
                                if (!candidate
                                        .indexFile()
                                        .indexType()
                                        .equals(DELETION_VECTORS_INDEX)) {
                                    return false;
                                }
                                Set<String> ownedIndexFileNames =
                                        ownedIndexFileNamesByPartition.get(candidate.partition());
                                return ownedIndexFileNames != null
                                        && ownedIndexFileNames.contains(
                                                candidate.indexFile().fileName());
                            })) {
                indexFilesByPartition
                        .computeIfAbsent(entry.partition().copy(), ignored -> new HashMap<>())
                        .put(entry.indexFile().fileName(), entry);
            }
        }

        for (RewriteGroupDeletionVectors rewriteGroupDeletionVectors :
                deletionVectorsByRewriteGroup.values()) {
            List<IndexManifestEntry> ownedIndexFiles;
            if (rewriteGroupDeletionVectors.oldIndexFileName == null) {
                ownedIndexFiles = Collections.emptyList();
            } else {
                Map<String, IndexManifestEntry> partitionIndexFiles =
                        indexFilesByPartition.get(rewriteGroupDeletionVectors.partition);
                IndexManifestEntry ownedIndexFile =
                        partitionIndexFiles == null
                                ? null
                                : partitionIndexFiles.get(
                                        rewriteGroupDeletionVectors.oldIndexFileName);
                ownedIndexFiles =
                        Collections.singletonList(
                                checkNotNull(
                                        ownedIndexFile,
                                        "Cannot find owned deletion-vector index file %s in the base snapshot.",
                                        rewriteGroupDeletionVectors.oldIndexFileName));
            }
            BaseAppendDeleteFileMaintainer maintainer =
                    BaseAppendDeleteFileMaintainer.forUnawareAppend(
                            indexFileHandler,
                            rewriteGroupDeletionVectors.partition,
                            ownedIndexFiles);

            for (Map.Entry<String, DeletionVector> entry :
                    rewriteGroupDeletionVectors.deletionVectorsByDataFile.entrySet()) {
                // The maintainer merges the new bitmap with an existing deletion vector, if any.
                maintainer.notifyNewDeletionVector(entry.getKey(), entry.getValue());
            }

            List<IndexFileMeta> addedIndexFiles = new ArrayList<>();
            List<IndexFileMeta> deletedIndexFiles = new ArrayList<>();
            for (IndexManifestEntry entry : maintainer.persist()) {
                if (entry.kind() == FileKind.ADD) {
                    addedIndexFiles.add(entry.indexFile());
                } else if (entry.kind() == FileKind.DELETE) {
                    deletedIndexFiles.add(entry.indexFile());
                } else {
                    throw new IllegalStateException(
                            "Unsupported index manifest entry kind: " + entry.kind());
                }
            }

            CommitMessage commitMessage =
                    new CommitMessageImpl(
                            maintainer.getPartition(),
                            UNAWARE_BUCKET,
                            null,
                            new DataIncrement(
                                    Collections.emptyList(),
                                    Collections.emptyList(),
                                    Collections.emptyList(),
                                    addedIndexFiles,
                                    deletedIndexFiles),
                            CompactIncrement.emptyIncrement());
            output.collect(
                    new StreamRecord<>(
                            new Committable(BatchWriteBuilder.COMMIT_IDENTIFIER, commitMessage)));
        }

        deletionVectorsByRewriteGroup.clear();
    }

    private static class RewriteGroupDeletionVectors {

        private final String bucketPath;
        @Nullable private final String oldIndexFileName;
        private final BinaryRow partition;
        private final byte[] serializedPartition;
        private final Map<String, DeletionVector> deletionVectorsByDataFile;

        private RewriteGroupDeletionVectors(
                String bucketPath,
                @Nullable String oldIndexFileName,
                BinaryRow partition,
                byte[] serializedPartition) {
            this.bucketPath = bucketPath;
            this.oldIndexFileName = oldIndexFileName;
            this.partition = partition;
            this.serializedPartition = serializedPartition;
            this.deletionVectorsByDataFile = new LinkedHashMap<>();
        }
    }

    /**
     * First-stage bounded operator which aggregates row positions into one compressed deletion
     * vector per anchor file.
     *
     * <p>Targets must be shuffled by anchor file before this operator. The resulting compressed
     * updates are shuffled again by rewrite group before they reach {@link
     * DataEvolutionDeleteOperator}. This keeps high-cardinality row-id traffic distributed even
     * when several anchors share one old deletion-vector index file.
     */
    public static class DeletionVectorAggregator
            extends BoundedOneInputOperator<DeletionTarget, DeletionVectorUpdate> {

        private static final long serialVersionUID = 1L;

        private final boolean useBitmap64;

        private transient Map<String, AggregatedDeletionVector> deletionVectorsByAnchor;

        public DeletionVectorAggregator(boolean useBitmap64) {
            this.useBitmap64 = useBitmap64;
        }

        @Override
        public void open() throws Exception {
            super.open();
            deletionVectorsByAnchor = new LinkedHashMap<>();
        }

        @Override
        public void processElement(StreamRecord<DeletionTarget> element) {
            DeletionTarget target = checkNotNull(element.getValue(), "Deletion target is null.");
            String dataFilePath =
                    checkNotNull(target.getDataFilePath(), "Anchor data file path is null.");
            checkNotNull(target.getRewriteGroup(), "Rewrite group is null.");
            checkNotNull(target.getBucketPath(), "Bucket path is null.");
            checkNotNull(target.getSerializedPartition(), "Serialized partition is null.");
            checkArgument(
                    target.getRowIndex() >= 0,
                    "Deletion-vector row index must be non-negative, but is %s.",
                    target.getRowIndex());

            String anchorKey = target.getBucketPath() + "\u0000" + dataFilePath;
            AggregatedDeletionVector aggregated = deletionVectorsByAnchor.get(anchorKey);
            if (aggregated == null) {
                aggregated = new AggregatedDeletionVector(target, newDeletionVector());
                deletionVectorsByAnchor.put(anchorKey, aggregated);
            } else {
                aggregated.validate(target);
            }
            aggregated.deletionVector.checkedDelete(target.getRowIndex());
        }

        @Override
        public void endInput() {
            for (AggregatedDeletionVector aggregated : deletionVectorsByAnchor.values()) {
                output.collect(
                        new StreamRecord<>(
                                new DeletionVectorUpdate(
                                        aggregated.rewriteGroup,
                                        aggregated.bucketPath,
                                        aggregated.oldIndexFileName,
                                        aggregated.serializedPartition,
                                        aggregated.dataFilePath,
                                        DeletionVector.serializeToBytes(
                                                aggregated.deletionVector))));
            }
            deletionVectorsByAnchor.clear();
        }

        private DeletionVector newDeletionVector() {
            return useBitmap64 ? new Bitmap64DeletionVector() : new BitmapDeletionVector();
        }

        private static class AggregatedDeletionVector {

            private final String rewriteGroup;
            private final String bucketPath;
            @Nullable private final String oldIndexFileName;
            private final byte[] serializedPartition;
            private final String dataFilePath;
            private final DeletionVector deletionVector;

            private AggregatedDeletionVector(DeletionTarget target, DeletionVector deletionVector) {
                this.rewriteGroup = target.getRewriteGroup();
                this.bucketPath = target.getBucketPath();
                this.oldIndexFileName = target.getOldIndexFileName();
                this.serializedPartition =
                        Arrays.copyOf(
                                target.getSerializedPartition(),
                                target.getSerializedPartition().length);
                this.dataFilePath = target.getDataFilePath();
                this.deletionVector = deletionVector;
            }

            private void validate(DeletionTarget target) {
                checkArgument(
                        rewriteGroup.equals(target.getRewriteGroup())
                                && bucketPath.equals(target.getBucketPath())
                                && Objects.equals(oldIndexFileName, target.getOldIndexFileName())
                                && Arrays.equals(
                                        serializedPartition, target.getSerializedPartition()),
                        "Anchor file %s is associated with inconsistent deletion metadata.",
                        dataFilePath);
            }
        }
    }

    /**
     * A network-serializable deletion target.
     *
     * <p>{@code dataFilePath} is the anchor data-file path and {@code rowIndex} is the row's local
     * position relative to the anchor file's row-id range. The partition is serialized because a
     * {@link BinaryRow} can point to reused memory.
     */
    public static class DeletionTarget implements Serializable {

        private static final long serialVersionUID = 1L;

        private String rewriteGroup;
        private String bucketPath;
        @Nullable private String oldIndexFileName;
        private byte[] serializedPartition;
        private String dataFilePath;
        private long rowIndex;

        /** Public no-argument constructor for Flink POJO serialization. */
        public DeletionTarget() {}

        public DeletionTarget(
                String rewriteGroup,
                String bucketPath,
                @Nullable String oldIndexFileName,
                byte[] serializedPartition,
                String dataFilePath,
                long rowIndex) {
            this.rewriteGroup = rewriteGroup;
            this.bucketPath = bucketPath;
            this.oldIndexFileName = oldIndexFileName;
            this.serializedPartition = serializedPartition;
            this.dataFilePath = dataFilePath;
            this.rowIndex = rowIndex;
        }

        public String getRewriteGroup() {
            return rewriteGroup;
        }

        public void setRewriteGroup(String rewriteGroup) {
            this.rewriteGroup = rewriteGroup;
        }

        public String getBucketPath() {
            return bucketPath;
        }

        public void setBucketPath(String bucketPath) {
            this.bucketPath = bucketPath;
        }

        public @Nullable String getOldIndexFileName() {
            return oldIndexFileName;
        }

        public void setOldIndexFileName(@Nullable String oldIndexFileName) {
            this.oldIndexFileName = oldIndexFileName;
        }

        public byte[] getSerializedPartition() {
            return serializedPartition;
        }

        public void setSerializedPartition(byte[] serializedPartition) {
            this.serializedPartition = serializedPartition;
        }

        public String getDataFilePath() {
            return dataFilePath;
        }

        public void setDataFilePath(String dataFilePath) {
            this.dataFilePath = dataFilePath;
        }

        public long getRowIndex() {
            return rowIndex;
        }

        public void setRowIndex(long rowIndex) {
            this.rowIndex = rowIndex;
        }
    }

    /** A compressed anchor deletion vector sent across the rewrite-group shuffle. */
    public static class DeletionVectorUpdate implements Serializable {

        private static final long serialVersionUID = 1L;

        private String rewriteGroup;
        private String bucketPath;
        @Nullable private String oldIndexFileName;
        private byte[] serializedPartition;
        private String dataFilePath;
        private byte[] serializedDeletionVector;

        /** Public no-argument constructor for Flink POJO serialization. */
        public DeletionVectorUpdate() {}

        public DeletionVectorUpdate(
                String rewriteGroup,
                String bucketPath,
                @Nullable String oldIndexFileName,
                byte[] serializedPartition,
                String dataFilePath,
                byte[] serializedDeletionVector) {
            this.rewriteGroup = rewriteGroup;
            this.bucketPath = bucketPath;
            this.oldIndexFileName = oldIndexFileName;
            this.serializedPartition = serializedPartition;
            this.dataFilePath = dataFilePath;
            this.serializedDeletionVector = serializedDeletionVector;
        }

        public String getRewriteGroup() {
            return rewriteGroup;
        }

        public void setRewriteGroup(String rewriteGroup) {
            this.rewriteGroup = rewriteGroup;
        }

        public String getBucketPath() {
            return bucketPath;
        }

        public void setBucketPath(String bucketPath) {
            this.bucketPath = bucketPath;
        }

        public @Nullable String getOldIndexFileName() {
            return oldIndexFileName;
        }

        public void setOldIndexFileName(@Nullable String oldIndexFileName) {
            this.oldIndexFileName = oldIndexFileName;
        }

        public byte[] getSerializedPartition() {
            return serializedPartition;
        }

        public void setSerializedPartition(byte[] serializedPartition) {
            this.serializedPartition = serializedPartition;
        }

        public String getDataFilePath() {
            return dataFilePath;
        }

        public void setDataFilePath(String dataFilePath) {
            this.dataFilePath = dataFilePath;
        }

        public byte[] getSerializedDeletionVector() {
            return serializedDeletionVector;
        }

        public void setSerializedDeletionVector(byte[] serializedDeletionVector) {
            this.serializedDeletionVector = serializedDeletionVector;
        }
    }
}
