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

package org.apache.paimon.table;

import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.FileDeletionBase;
import org.apache.paimon.operation.SnapshotDeletion;
import org.apache.paimon.operation.TagDeletion;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Predicate;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Helper class for {@link Table#rollbackTo} including utils to clean snapshots. */
public class RollbackHelper {

    private final SnapshotManager snapshotManager;
    private final TagManager tagManager;
    private final FileIO fileIO;
    private final SnapshotDeletion snapshotDeletion;
    private final TagDeletion tagDeletion;

    private final SortedMap<Snapshot, String> tags;
    private final Map<Snapshot, List<ManifestEntry>> tagManifestEntries;

    private Set<String> manifestsSkippingSet;

    public RollbackHelper(
            SnapshotManager snapshotManager,
            TagManager tagManager,
            FileIO fileIO,
            SnapshotDeletion snapshotDeletion,
            TagDeletion tagDeletion) {
        this.snapshotManager = snapshotManager;
        this.tagManager = tagManager;
        this.fileIO = fileIO;
        this.snapshotDeletion = snapshotDeletion;
        this.tagDeletion = tagDeletion;

        this.tags = tagManager.tags();
        this.tagManifestEntries = new HashMap<>();
    }

    /**
     * Clean snapshots and tags whose id is larger than given snapshot's in the reverse direction.
     */
    public void cleanLargerThan(Snapshot snapshot) {
        // snapshots are cleaned first because it will collect tag manifest entries
        cleanSnapshotsLargerThan(snapshot);
        cleanTagsLargerThan(snapshot);

        // modify the latest hint
        try {
            snapshotManager.commitLatestHint(snapshot.id());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void cleanSnapshotsLargerThan(Snapshot snapshot) {
        long snapshotId = snapshot.id();
        List<Snapshot> toBeCleaned = new ArrayList<>();

        long earliest =
                checkNotNull(
                        snapshotManager.earliestSnapshotId(), "Cannot find earliest snapshot.");
        long latest =
                checkNotNull(snapshotManager.latestSnapshotId(), "Cannot find latest snapshot.");

        // delete snapshot files first, cannot be read now
        // it is possible that some snapshots have been expired
        long to = Math.max(earliest, snapshotId + 1);
        for (long i = latest; i >= to; i--) {
            toBeCleaned.add(snapshotManager.snapshot(i));
            fileIO.deleteQuietly(snapshotManager.snapshotPath(i));
        }

        // delete data files
        for (Snapshot s : toBeCleaned) {
            cleanSnapshotDataFiles(s);
        }

        // delete directories
        snapshotDeletion.cleanDataDirectories();

        // delete manifest files
        cleanManifests(snapshot, toBeCleaned, snapshotDeletion);
    }

    // don't concern about tag data files because file deletion methods won't throw exception
    // when deleting non-existing data files
    private void cleanSnapshotDataFiles(Snapshot snapshot) {
        List<ManifestEntry> delta = new ArrayList<>();
        snapshotDeletion
                .tryReadManifestEntries(snapshot.deltaManifestList())
                .iterator()
                .forEachRemaining(delta::add);
        if (tags.containsKey(snapshot)) {
            // store tag data manifest entries to avoid redundant reading when cleaning tag
            List<ManifestEntry> base = new ArrayList<>();
            snapshotDeletion
                    .tryReadManifestEntries(snapshot.baseManifestList())
                    .iterator()
                    .forEachRemaining(base::add);

            List<ManifestEntry> data = new ArrayList<>(base);
            data.addAll(delta);
            tagManifestEntries.put(snapshot, data);
        }

        snapshotDeletion.deleteAddedDataFiles(delta);
        snapshotDeletion.deleteAddedDataFiles(snapshot.changelogManifestList());
    }

    private void cleanTagsLargerThan(Snapshot snapshot) {
        if (tags.isEmpty()) {
            return;
        }

        long snapshotId = snapshot.id();
        List<Snapshot> taggedSnapshots = new ArrayList<>(tags.keySet());
        List<Snapshot> toBeCleaned = new ArrayList<>();

        // delete tag files
        for (int i = taggedSnapshots.size() - 1; i >= 0; i--) {
            Snapshot tag = taggedSnapshots.get(i);
            if (tag.id() <= snapshotId) {
                break;
            }
            toBeCleaned.add(tag);
            fileIO.deleteQuietly(tagManager.tagPath(tags.get(tag)));
        }

        // delete data files
        Predicate<ManifestEntry> dataFileSkipper = tagDeletion.dataFileSkipper(snapshot);
        for (Snapshot s : toBeCleaned) {
            // try to find entries in cache first
            List<ManifestEntry> entries = tagManifestEntries.get(s);
            if (entries != null) {
                tagDeletion.cleanUnusedDataFiles(entries, dataFileSkipper);
            } else {
                tagDeletion.cleanUnusedDataFiles(s, dataFileSkipper);
            }
        }

        // delete directories
        tagDeletion.cleanDataDirectories();

        // delete manifest files
        cleanManifests(snapshot, toBeCleaned, tagDeletion);
    }

    private void cleanManifests(
            Snapshot snapshot, List<Snapshot> toBeCleaned, FileDeletionBase deletion) {
        if (manifestsSkippingSet == null) {
            manifestsSkippingSet = deletion.manifestSkippingSet(snapshot);
        }

        for (Snapshot s : toBeCleaned) {
            deletion.cleanUnusedManifests(s, manifestsSkippingSet);
        }
    }
}
