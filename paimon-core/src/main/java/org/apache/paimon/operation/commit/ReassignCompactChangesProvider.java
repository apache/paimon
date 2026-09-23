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

import org.apache.paimon.Snapshot;
import org.apache.paimon.append.dataevolution.SerializationAssignment;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkState;

/** Reuses immutable compact outputs after verified, metadata-only row-id reassignments. */
public final class ReassignCompactChangesProvider implements CommitChangesProvider {

    private final FileIO fileIO;
    private final FileStorePathFactory paths;
    private final SnapshotManager snapshots;
    private final IndexManifestFile indexManifests;
    private final Set<Long> reassignments = new HashSet<>();
    private long checkedSnapshot;
    private CommitChanges changes;

    public ReassignCompactChangesProvider(
            FileIO fileIO,
            FileStorePathFactory paths,
            SnapshotManager snapshots,
            IndexManifestFile indexManifests,
            long scanSnapshotId,
            CommitChanges changes) {
        this.fileIO = fileIO;
        this.paths = paths;
        this.snapshots = snapshots;
        this.indexManifests = indexManifests;
        this.checkedSnapshot = scanSnapshotId;
        this.changes = changes;
    }

    public static boolean supports(CommitChanges changes) {
        if (!changes.changelogFiles.isEmpty()) {
            return false;
        }
        for (ManifestEntry entry : changes.tableFiles) {
            DataFileMeta file = entry.file();
            // Materialized deletions and physical _ROW_ID columns require a data rewrite.
            if (file.firstRowId() == null
                    || (file.writeCols() != null
                            && file.writeCols().contains(SpecialFields.ROW_ID.name()))) {
                return false;
            }
        }
        for (IndexManifestEntry entry : changes.indexFiles) {
            if (entry.indexFile().globalIndexMeta() == null
                    && entry.indexFile().dvRanges() == null) {
                return false;
            }
        }
        return !changes.tableFiles.isEmpty() || !changes.indexFiles.isEmpty();
    }

    @Override
    public CommitChanges provide(@Nullable Snapshot latest) {
        checkState(
                latest != null && latest.id() >= checkedSnapshot,
                "Compaction planning snapshot %s is ahead of the current table.",
                checkedSnapshot);
        // Like StrictModeChecker, only history after the boundary is needed. The boundary
        // snapshot itself may already have expired.
        for (long id = checkedSnapshot + 1; id <= latest.id(); id++) {
            checkState(
                    snapshots.snapshotExists(id),
                    "Cannot reuse compaction with missing snapshot %s.",
                    id);
            Snapshot snapshot = snapshots.snapshot(id);
            String plan =
                    snapshot.commitKind() == Snapshot.CommitKind.OVERWRITE
                            ? SerializationAssignment.planFile(snapshot)
                            : null;
            if (plan != null) {
                SerializationAssignment assignment;
                try {
                    assignment = SerializationAssignment.readPlan(fileIO, paths, plan);
                } catch (IOException e) {
                    throw new UncheckedIOException(
                            "Cannot read compaction reassignment plan " + plan, e);
                }
                checkState(
                        assignment.snapshotId() == id
                                && Objects.equals(snapshot.nextRowId(), assignment.nextRowId()),
                        "Reassignment plan does not match snapshot %s.",
                        id);
                changes = mapChanges(assignment);
                reassignments.add(id);
            }
            checkedSnapshot = id;
        }
        checkIndexInputs(latest);
        return changes;
    }

    @Override
    public Set<Long> rebasedReassignments() {
        return Collections.unmodifiableSet(reassignments);
    }

    private CommitChanges mapChanges(SerializationAssignment assignment) {
        List<ManifestEntry> files = new ArrayList<>(changes.tableFiles.size());
        for (ManifestEntry entry : changes.tableFiles) {
            Range range =
                    assignment.mapRowRange(entry.partition(), entry.file().nonNullRowIdRange());
            files.add(entry.assignFirstRowId(range.from));
        }
        List<IndexManifestEntry> indexes = new ArrayList<>(changes.indexFiles.size());
        for (IndexManifestEntry entry : changes.indexFiles) {
            IndexFileMeta file = entry.indexFile();
            GlobalIndexMeta meta = file.globalIndexMeta();
            if (meta == null) {
                // Deletion vectors use file-relative positions, not global row IDs.
                indexes.add(entry);
                continue;
            }
            Range range = assignment.mapRowRange(entry.partition(), meta.rowRange());
            GlobalIndexMeta mapped =
                    new GlobalIndexMeta(
                            range.from,
                            range.to,
                            meta.indexFieldId(),
                            meta.extraFieldIds(),
                            meta.indexMeta(),
                            meta.sourceMeta());
            indexes.add(
                    new IndexManifestEntry(
                            entry.kind(),
                            entry.partition(),
                            entry.bucket(),
                            new IndexFileMeta(
                                    file.indexType(),
                                    file.fileName(),
                                    file.fileSize(),
                                    file.rowCount(),
                                    file.dvRanges(),
                                    file.externalPath(),
                                    mapped)));
        }
        return new CommitChanges(files, changes.changelogFiles, indexes);
    }

    private void checkIndexInputs(Snapshot latest) {
        Map<String, IndexManifestEntry> deletes = new HashMap<>();
        for (IndexManifestEntry entry : changes.indexFiles) {
            if (entry.kind() == FileKind.DELETE) {
                deletes.put(entry.indexFile().fileName(), entry);
            }
        }
        if (deletes.isEmpty()) {
            return;
        }
        if (latest.indexManifest() != null) {
            for (IndexManifestEntry entry : indexManifests.read(latest.indexManifest())) {
                IndexManifestEntry expected = deletes.get(entry.indexFile().fileName());
                if (expected != null) {
                    checkState(
                            entry.kind() == FileKind.ADD
                                    && expected.partition().equals(entry.partition())
                                    && expected.bucket() == entry.bucket()
                                    && expected.indexFile().equals(entry.indexFile()),
                            "Compaction index input %s was modified.",
                            entry.indexFile().fileName());
                    deletes.remove(entry.indexFile().fileName());
                }
            }
        }
        checkState(deletes.isEmpty(), "Compaction index inputs were removed: %s", deletes.keySet());
    }
}
