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

package org.apache.paimon.operation.commit;

import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.SpecialFields;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Utils for row tracking commit. */
public class RowTrackingCommitUtils {

    public static RowTrackingAssigned assignRowTracking(
            long newSnapshotId, long firstRowIdStart, List<ManifestEntry> deltaFiles) {
        return assignRowTracking(
                newSnapshotId, firstRowIdStart, deltaFiles, Collections.emptyMap());
    }

    public static RowTrackingAssigned assignRowTracking(
            long newSnapshotId,
            long firstRowIdStart,
            List<ManifestEntry> deltaFiles,
            Map<FileEntry.Identifier, Integer> fileGroups) {
        // assigned snapshot id to delta files
        List<ManifestEntry> snapshotAssigned = new ArrayList<>();
        assignSnapshotId(newSnapshotId, deltaFiles, snapshotAssigned);
        // assign row id for new files
        List<ManifestEntry> rowIdAssigned = new ArrayList<>();
        long nextRowIdStart =
                assignRowTrackingMeta(firstRowIdStart, snapshotAssigned, fileGroups, rowIdAssigned);
        return new RowTrackingAssigned(nextRowIdStart, rowIdAssigned);
    }

    private static void assignSnapshotId(
            long snapshotId, List<ManifestEntry> deltaFiles, List<ManifestEntry> snapshotAssigned) {
        for (ManifestEntry entry : deltaFiles) {
            long minSeqNumber = entry.file().minSequenceNumber();
            long maxSeqNumber = entry.file().maxSequenceNumber();
            if (minSeqNumber == 0L) {
                // Case 1: New file (e.g., from INSERT)
                // All records in this file get the current snapshot ID as sequence number
                snapshotAssigned.add(entry.assignSequenceNumber(snapshotId, snapshotId));
            } else if (maxSeqNumber == 0L) {
                // Case 2: File with some modified records
                // - min: Preserve original sequence number (from unmodified records)
                // - max: Assign current snapshot ID
                snapshotAssigned.add(entry.assignSequenceNumber(minSeqNumber, snapshotId));
            } else {
                // Case 3: Pure compact file (no modified records)
                // Preserve original min/max sequence numbers from source files
                snapshotAssigned.add(entry);
            }
        }
    }

    private static long assignRowTrackingMeta(
            long firstRowIdStart,
            List<ManifestEntry> deltaFiles,
            Map<FileEntry.Identifier, Integer> fileGroups,
            List<ManifestEntry> rowIdAssigned) {
        if (deltaFiles.isEmpty()) {
            return firstRowIdStart;
        }

        Object defaultGroup = new Object();
        Map<Object, List<ManifestEntry>> groupedFiles = new LinkedHashMap<>();
        for (ManifestEntry entry : deltaFiles) {
            Object group = fileGroups.isEmpty() ? defaultGroup : fileGroups.get(entry.identifier());
            // Entries not produced by an original CommitMessage do not share an implicit range.
            if (group == null) {
                group = entry;
            }
            groupedFiles.computeIfAbsent(group, ignored -> new ArrayList<>()).add(entry);
        }

        Map<ManifestEntry, ManifestEntry> assigned = new IdentityHashMap<>();
        long start = firstRowIdStart;
        for (List<ManifestEntry> group : groupedFiles.values()) {
            start = assignRowTrackingGroup(start, group, assigned);
        }

        for (ManifestEntry entry : deltaFiles) {
            rowIdAssigned.add(assigned.get(entry));
        }
        return start;
    }

    private static long assignRowTrackingGroup(
            long firstRowIdStart,
            List<ManifestEntry> deltaFiles,
            Map<ManifestEntry, ManifestEntry> assigned) {
        long start = firstRowIdStart;
        Map<ManifestEntry, Long> normalStarts = new IdentityHashMap<>();
        for (ManifestEntry entry : deltaFiles) {
            if (isUnassignedAppend(entry)
                    && !isBlobFile(entry.file().fileName())
                    && !isVectorStoreFile(entry.file().fileName())) {
                normalStarts.put(entry, start);
                start += entry.file().rowCount();
            }
        }

        Map<String, Long> blobStarts = new HashMap<>();
        long vectorStoreStart = firstRowIdStart;
        for (ManifestEntry entry : deltaFiles) {
            Optional<FileSource> fileSource = entry.file().fileSource();
            checkArgument(
                    fileSource.isPresent(),
                    "This is a bug, file source field for row-tracking table must present.");
            List<String> writeCols = entry.file().writeCols();
            boolean containsRowId =
                    writeCols != null && writeCols.contains(SpecialFields.ROW_ID.name());
            if (fileSource.get().equals(FileSource.APPEND)
                    && entry.file().firstRowId() == null
                    && !containsRowId) {
                long rowCount = entry.file().rowCount();
                if (isBlobFile(entry.file().fileName())) {
                    String blobFieldName = entry.file().writeCols().get(0);
                    long blobStart = blobStarts.getOrDefault(blobFieldName, firstRowIdStart);
                    if (blobStart >= start) {
                        throw new IllegalStateException(
                                String.format(
                                        "This is a bug, blobStart %d should be less than start %d when assigning a blob entry file.",
                                        blobStart, start));
                    }
                    assigned.put(entry, entry.assignFirstRowId(blobStart));
                    blobStarts.put(blobFieldName, blobStart + rowCount);
                } else if (isVectorStoreFile(entry.file().fileName())) {
                    if (vectorStoreStart >= start) {
                        throw new IllegalStateException(
                                String.format(
                                        "This is a bug, vectorStoreStart %d should be less than start %d when assigning a vector-store entry file.",
                                        vectorStoreStart, start));
                    }
                    assigned.put(entry, entry.assignFirstRowId(vectorStoreStart));
                    vectorStoreStart += rowCount;
                } else {
                    assigned.put(entry, entry.assignFirstRowId(normalStarts.get(entry)));
                }
            } else {
                // for compact file, do not assign first row id.
                assigned.put(entry, entry);
            }
        }
        return start;
    }

    private static boolean isUnassignedAppend(ManifestEntry entry) {
        List<String> writeCols = entry.file().writeCols();
        boolean containsRowId =
                writeCols != null && writeCols.contains(SpecialFields.ROW_ID.name());
        return entry.file().fileSource().orElse(null) == FileSource.APPEND
                && entry.file().firstRowId() == null
                && !containsRowId;
    }

    /** Assigned results. */
    public static class RowTrackingAssigned {
        public final long nextRowIdStart;
        public final List<ManifestEntry> assignedEntries;

        public RowTrackingAssigned(long nextRowIdStart, List<ManifestEntry> assignedEntries) {
            this.nextRowIdStart = nextRowIdStart;
            this.assignedEntries = assignedEntries;
        }
    }
}
