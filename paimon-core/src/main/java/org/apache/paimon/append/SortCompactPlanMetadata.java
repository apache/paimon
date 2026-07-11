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

package org.apache.paimon.append;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexFileMetaSerializer;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestEntrySerializer;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.index.HashIndexFile.HASH_INDEX;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/**
 * Base-snapshot index metadata captured at sort compact planning time.
 *
 * <p>Flink carries this object in the job graph so commit recovery can still clean up deletion
 * vectors and hash indexes even if the base snapshot has expired before the committer runs again.
 * Index metadata is stored as Paimon byte arrays instead of non-{@link Serializable} POJOs.
 */
public final class SortCompactPlanMetadata implements Serializable {

    private static final long serialVersionUID = 1L;

    @Nullable private final byte[] serializedDeletionVectorEntries;
    @Nullable private final List<SerializedHashIndex> serializedHashIndexes;

    private SortCompactPlanMetadata(
            @Nullable byte[] serializedDeletionVectorEntries,
            @Nullable List<SerializedHashIndex> serializedHashIndexes) {
        this.serializedDeletionVectorEntries = serializedDeletionVectorEntries;
        this.serializedHashIndexes = serializedHashIndexes;
    }

    /** Capture index metadata from the base snapshot for the planned compact input. */
    public static SortCompactPlanMetadata capture(
            FileStoreTable table, long baseSnapshotId, List<DataSplit> compactInputSplits) {
        Set<BinaryRow> partitions = new HashSet<>();
        for (DataSplit split : compactInputSplits) {
            partitions.add(split.partition());
        }

        Map<BinaryRow, List<IndexManifestEntry>> baseDeletionVectorEntries = new HashMap<>();
        Map<BinaryRow, Map<Integer, IndexFileMeta>> baseHashIndexes = new HashMap<>();
        captureInto(
                table,
                baseSnapshotId,
                partitions,
                compactInputSplits,
                baseDeletionVectorEntries,
                baseHashIndexes);
        return fromCapturedMaps(baseDeletionVectorEntries, baseHashIndexes);
    }

    static void captureInto(
            FileStoreTable table,
            long baseSnapshotId,
            Set<BinaryRow> partitions,
            List<DataSplit> compactInputSplits,
            Map<BinaryRow, List<IndexManifestEntry>> baseDeletionVectorEntries,
            Map<BinaryRow, Map<Integer, IndexFileMeta>> baseHashIndexes) {
        Snapshot snapshot = resolveBaseSnapshot(table, baseSnapshotId);
        if (snapshot == null) {
            return;
        }

        IndexFileHandler indexFileHandler = table.store().newIndexFileHandler();
        if (table.coreOptions().deletionVectorsEnabled()
                && table.bucketMode() == BucketMode.BUCKET_UNAWARE) {
            for (IndexManifestEntry entry :
                    indexFileHandler.scan(snapshot, DELETION_VECTORS_INDEX)) {
                if (partitions.contains(entry.partition())) {
                    baseDeletionVectorEntries
                            .computeIfAbsent(entry.partition(), k -> new ArrayList<>())
                            .add(entry);
                }
            }
        }

        if (table.bucketMode() == BucketMode.HASH_DYNAMIC) {
            Set<Pair<BinaryRow, Integer>> buckets = new HashSet<>();
            for (DataSplit split : compactInputSplits) {
                buckets.add(Pair.of(split.partition(), split.bucket()));
            }
            Map<Pair<BinaryRow, Integer>, List<IndexFileMeta>> hashIndexes =
                    indexFileHandler.scanBuckets(snapshot, HASH_INDEX, buckets);
            for (Map.Entry<Pair<BinaryRow, Integer>, List<IndexFileMeta>> entry :
                    hashIndexes.entrySet()) {
                singleHashIndex(entry.getValue())
                        .ifPresent(
                                hashIndex ->
                                        baseHashIndexes
                                                .computeIfAbsent(
                                                        entry.getKey().getLeft(),
                                                        k -> new HashMap<>())
                                                .put(entry.getKey().getRight(), hashIndex));
            }
        }
    }

    private static Optional<IndexFileMeta> singleHashIndex(
            @Nullable List<IndexFileMeta> hashIndexes) {
        if (hashIndexes == null || hashIndexes.isEmpty()) {
            return Optional.empty();
        }
        if (hashIndexes.size() > 1) {
            throw new IllegalArgumentException(
                    "Find multiple hash index files for one bucket: " + hashIndexes);
        }
        return Optional.of(hashIndexes.get(0));
    }

    void copyInto(
            Map<BinaryRow, List<IndexManifestEntry>> baseDeletionVectorEntries,
            Map<BinaryRow, Map<Integer, IndexFileMeta>> baseHashIndexes) {
        if (serializedDeletionVectorEntries != null) {
            IndexManifestEntrySerializer entrySerializer = new IndexManifestEntrySerializer();
            try {
                for (IndexManifestEntry entry :
                        entrySerializer.deserializeList(serializedDeletionVectorEntries)) {
                    baseDeletionVectorEntries
                            .computeIfAbsent(entry.partition(), k -> new ArrayList<>())
                            .add(entry);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(
                        "Failed to deserialize captured deletion vector metadata.", e);
            }
        }

        if (serializedHashIndexes != null) {
            IndexFileMetaSerializer metaSerializer = new IndexFileMetaSerializer();
            for (SerializedHashIndex serializedHashIndex : serializedHashIndexes) {
                try {
                    BinaryRow partition = deserializeBinaryRow(serializedHashIndex.partition);
                    IndexFileMeta hashIndex =
                            metaSerializer.deserializeFromBytes(serializedHashIndex.indexFileMeta);
                    baseHashIndexes
                            .computeIfAbsent(partition, k -> new HashMap<>())
                            .put(serializedHashIndex.bucket, hashIndex);
                } catch (IOException e) {
                    throw new UncheckedIOException(
                            "Failed to deserialize captured hash index metadata.", e);
                }
            }
        }
    }

    private static SortCompactPlanMetadata fromCapturedMaps(
            Map<BinaryRow, List<IndexManifestEntry>> baseDeletionVectorEntries,
            Map<BinaryRow, Map<Integer, IndexFileMeta>> baseHashIndexes) {
        byte[] serializedDeletionVectorEntries = null;
        if (!baseDeletionVectorEntries.isEmpty()) {
            List<IndexManifestEntry> entries = new ArrayList<>();
            for (List<IndexManifestEntry> partitionEntries : baseDeletionVectorEntries.values()) {
                entries.addAll(partitionEntries);
            }
            IndexManifestEntrySerializer entrySerializer = new IndexManifestEntrySerializer();
            try {
                serializedDeletionVectorEntries = entrySerializer.serializeList(entries);
            } catch (IOException e) {
                throw new UncheckedIOException(
                        "Failed to serialize captured deletion vector metadata.", e);
            }
        }

        List<SerializedHashIndex> serializedHashIndexes = null;
        if (!baseHashIndexes.isEmpty()) {
            IndexFileMetaSerializer metaSerializer = new IndexFileMetaSerializer();
            serializedHashIndexes = new ArrayList<>();
            for (Map.Entry<BinaryRow, Map<Integer, IndexFileMeta>> partitionEntry :
                    baseHashIndexes.entrySet()) {
                byte[] partitionBytes = serializeBinaryRow(partitionEntry.getKey());
                for (Map.Entry<Integer, IndexFileMeta> bucketEntry :
                        partitionEntry.getValue().entrySet()) {
                    try {
                        serializedHashIndexes.add(
                                new SerializedHashIndex(
                                        partitionBytes,
                                        bucketEntry.getKey(),
                                        metaSerializer.serializeToBytes(bucketEntry.getValue())));
                    } catch (IOException e) {
                        throw new UncheckedIOException(
                                "Failed to serialize captured hash index metadata.", e);
                    }
                }
            }
        }

        return new SortCompactPlanMetadata(serializedDeletionVectorEntries, serializedHashIndexes);
    }

    @Nullable
    private static Snapshot resolveBaseSnapshot(FileStoreTable table, long snapshotId) {
        if (snapshotId <= 0) {
            return table.snapshotManager().latestSnapshot();
        }
        try {
            return table.snapshotManager().tryGetSnapshot(snapshotId);
        } catch (java.io.FileNotFoundException e) {
            return null;
        }
    }

    private static final class SerializedHashIndex implements Serializable {
        private static final long serialVersionUID = 1L;

        private final byte[] partition;
        private final int bucket;
        private final byte[] indexFileMeta;

        private SerializedHashIndex(byte[] partition, int bucket, byte[] indexFileMeta) {
            this.partition = partition;
            this.bucket = bucket;
            this.indexFileMeta = indexFileMeta;
        }
    }
}
