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

import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.SpecialFields;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Utils for row tracking commit. */
public class RowTrackingCommitUtils {

    public static RowTrackingAssigned assignRowTracking(
            long newSnapshotId, long firstRowIdStart, List<ManifestEntry> deltaFiles) {
        // assigned snapshot id to delta files
        List<ManifestEntry> snapshotAssigned = new ArrayList<>();
        assignSnapshotId(newSnapshotId, deltaFiles, snapshotAssigned);
        // assign row id for new files
        List<ManifestEntry> rowIdAssigned = new ArrayList<>();
        long nextRowIdStart =
                assignRowTrackingMeta(firstRowIdStart, snapshotAssigned, rowIdAssigned);
        return new RowTrackingAssigned(nextRowIdStart, rowIdAssigned);
    }

    private static void assignSnapshotId(
            long snapshotId, List<ManifestEntry> deltaFiles, List<ManifestEntry> snapshotAssigned) {
        for (ManifestEntry entry : deltaFiles) {
            snapshotAssigned.add(entry.assignSequenceNumber(snapshotId, snapshotId));
        }
    }

    private static long assignRowTrackingMeta(
            long firstRowIdStart,
            List<ManifestEntry> deltaFiles,
            List<ManifestEntry> rowIdAssigned) {
        if (deltaFiles.isEmpty()) {
            return firstRowIdStart;
        }
        // assign row id for new files
        long start = firstRowIdStart;
        long blobStart = firstRowIdStart;
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
                    if (blobStart >= start) {
                        throw new IllegalStateException(
                                String.format(
                                        "This is a bug, blobStart %d should be less than start %d when assigning a blob entry file.",
                                        blobStart, start));
                    }
                    rowIdAssigned.add(entry.assignFirstRowId(blobStart));
                    blobStart += rowCount;
                } else {
                    rowIdAssigned.add(entry.assignFirstRowId(start));
                    blobStart = start;
                    start += rowCount;
                }
            } else {
                // for compact file, do not assign first row id.
                rowIdAssigned.add(entry);
            }
        }
        return start;
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
