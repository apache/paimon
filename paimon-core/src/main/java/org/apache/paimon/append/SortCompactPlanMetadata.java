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
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestEntrySerializer;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;

/**
 * Base-snapshot index metadata captured at sort compact planning time.
 *
 * <p>Flink carries this object in the job graph so commit recovery can still clean up deletion
 * vectors even if the base snapshot has expired before the committer runs again. Index metadata is
 * stored as Paimon byte arrays instead of non-{@link Serializable} POJOs.
 */
public final class SortCompactPlanMetadata implements Serializable {

    private static final long serialVersionUID = 1L;

    @Nullable private final byte[] serializedDeletionVectorEntries;

    /**
     * Whether the base snapshot was readable when this metadata was captured. Distinguishes a
     * successful capture of an empty deletion-vector state from a failed capture.
     */
    private final boolean baseSnapshotCaptured;

    private SortCompactPlanMetadata(
            @Nullable byte[] serializedDeletionVectorEntries, boolean baseSnapshotCaptured) {
        this.serializedDeletionVectorEntries = serializedDeletionVectorEntries;
        this.baseSnapshotCaptured = baseSnapshotCaptured;
    }

    /** Capture index metadata from the base snapshot for the planned compact input. */
    public static SortCompactPlanMetadata capture(
            FileStoreTable table, long baseSnapshotId, List<DataSplit> compactInputSplits) {
        Set<BinaryRow> partitions = new HashSet<>();
        for (DataSplit split : compactInputSplits) {
            partitions.add(split.partition());
        }

        Map<BinaryRow, List<IndexManifestEntry>> baseDeletionVectorEntries = new HashMap<>();
        boolean captured =
                captureInto(table, baseSnapshotId, partitions, baseDeletionVectorEntries);
        return fromCapturedMap(baseDeletionVectorEntries, captured);
    }

    static boolean captureInto(
            FileStoreTable table,
            long baseSnapshotId,
            Set<BinaryRow> partitions,
            Map<BinaryRow, List<IndexManifestEntry>> baseDeletionVectorEntries) {
        Snapshot snapshot = resolveBaseSnapshot(table, baseSnapshotId);
        if (snapshot == null) {
            return false;
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
        return true;
    }

    void copyInto(Map<BinaryRow, List<IndexManifestEntry>> baseDeletionVectorEntries) {
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
    }

    /**
     * Whether the base snapshot was readable at capture time. An empty deletion-vector payload with
     * {@code true} means known-empty; with {@code false} means capture failed.
     */
    boolean baseSnapshotCaptured() {
        return baseSnapshotCaptured;
    }

    private static SortCompactPlanMetadata fromCapturedMap(
            Map<BinaryRow, List<IndexManifestEntry>> baseDeletionVectorEntries,
            boolean baseSnapshotCaptured) {
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

        return new SortCompactPlanMetadata(serializedDeletionVectorEntries, baseSnapshotCaptured);
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
}
