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

import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.SpecialFields;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Utils for row tracking commit. */
public class RowTrackingCommitUtils {

    public static RowTrackingAssigned assignRowTracking(
            long newSnapshotId, long firstRowIdStart, List<ManifestEntry> deltaFiles) {
        long start = Math.max(firstRowIdStart, rowIdEnd(deltaFiles));
        RowTrackingAssigner assigner = new RowTrackingAssigner(start);
        boolean replay = deltaFiles instanceof ReplayableList;
        List<ManifestEntry> assigned = replay ? null : new ArrayList<>(deltaFiles.size());
        // Validate and compute the final row-id frontier before publishing any manifests.
        for (ManifestEntry entry : deltaFiles) {
            ManifestEntry result = assigner.assign(assignSnapshotId(newSnapshotId, entry));
            if (!replay) {
                assigned.add(result);
            }
        }
        if (replay) {
            assigned =
                    new ReplayableList<>(
                            deltaFiles.size(),
                            () -> {
                                RowTrackingAssigner replayAssigner = new RowTrackingAssigner(start);
                                return Iterators.transform(
                                        deltaFiles.iterator(),
                                        entry ->
                                                replayAssigner.assign(
                                                        assignSnapshotId(newSnapshotId, entry)));
                            });
        }
        return new RowTrackingAssigned(assigner.start, assigned);
    }

    /** Preserves the existing partition grouping, without copying a single-partition replay. */
    public static List<ManifestEntry> groupByPartition(List<ManifestEntry> files) {
        if (files instanceof ReplayableList
                && files.stream().map(ManifestEntry::partition).distinct().limit(2).count() <= 1) {
            return files;
        }
        return files.stream()
                .collect(Collectors.groupingBy(ManifestEntry::partition))
                .values()
                .stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    /**
     * The end of the row ids that files added with a first row id they already have occupy. The
     * next row id of the table does not cover them when they come from another table, for example
     * files copied by {@code sys.copy}: new files must be assigned row ids after them, and the next
     * row id of the table must move past them, or later rows get the same row ids.
     */
    private static long rowIdEnd(List<ManifestEntry> deltaFiles) {
        long end = 0L;
        for (ManifestEntry entry : deltaFiles) {
            Long firstRowId = entry.file().firstRowId();
            if (entry.kind() == FileKind.ADD && firstRowId != null) {
                end = Math.max(end, firstRowId + entry.file().rowCount());
            }
        }
        return end;
    }

    private static ManifestEntry assignSnapshotId(long snapshotId, ManifestEntry entry) {
        long min = entry.file().minSequenceNumber();
        long max = entry.file().maxSequenceNumber();
        if (min == 0L) {
            return entry.assignSequenceNumber(snapshotId, snapshotId);
        } else if (max == 0L) {
            return entry.assignSequenceNumber(min, snapshotId);
        }
        return entry;
    }

    private static class RowTrackingAssigner {
        private long start;
        private long blobStartDefault;
        private final Map<String, Long> blobStarts = new HashMap<>();
        private long vectorStoreStart;

        private RowTrackingAssigner(long start) {
            this.start = start;
            this.blobStartDefault = start;
            this.vectorStoreStart = start;
        }

        private ManifestEntry assign(ManifestEntry entry) {
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
                    long blobStart = blobStarts.getOrDefault(blobFieldName, blobStartDefault);
                    if (blobStart >= start) {
                        throw new IllegalStateException(
                                String.format(
                                        "This is a bug, blobStart %d should be less than start %d when assigning a blob entry file.",
                                        blobStart, start));
                    }
                    blobStarts.put(blobFieldName, blobStart + rowCount);
                    return entry.assignFirstRowId(blobStart);
                } else if (isVectorStoreFile(entry.file().fileName())) {
                    if (vectorStoreStart >= start) {
                        throw new IllegalStateException(
                                String.format(
                                        "This is a bug, vectorStoreStart %d should be less than start %d when assigning a vector-store entry file.",
                                        vectorStoreStart, start));
                    }
                    long first = vectorStoreStart;
                    vectorStoreStart += rowCount;
                    return entry.assignFirstRowId(first);
                } else {
                    long first = start;
                    blobStartDefault = start;
                    blobStarts.clear();
                    // Skip the rows of normal files written without vector-store files.
                    vectorStoreStart = Math.max(vectorStoreStart, start);
                    start += rowCount;
                    return entry.assignFirstRowId(first);
                }
            }
            return entry;
        }
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
