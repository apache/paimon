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

import org.apache.paimon.Changelog;
import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.ChangelogDeletion;
import org.apache.paimon.operation.SnapshotDeletion;
import org.apache.paimon.operation.TagDeletion;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Predicate;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Helper class for {@link Table#rollbackTo} including utils to clean snapshots. */
public class RollbackHelper {

    private static final Logger LOG = LoggerFactory.getLogger(RollbackHelper.class);

    private final SnapshotManager snapshotManager;
    private final TagManager tagManager;
    private final FileIO fileIO;
    private final SnapshotDeletion snapshotDeletion;
    private final ChangelogDeletion changelogDeletion;
    private final TagDeletion tagDeletion;

    public RollbackHelper(
            SnapshotManager snapshotManager,
            TagManager tagManager,
            FileIO fileIO,
            SnapshotDeletion snapshotDeletion,
            ChangelogDeletion changelogDeletion,
            TagDeletion tagDeletion) {
        this.snapshotManager = snapshotManager;
        this.tagManager = tagManager;
        this.fileIO = fileIO;
        this.snapshotDeletion = snapshotDeletion;
        this.changelogDeletion = changelogDeletion;
        this.tagDeletion = tagDeletion;
    }

    /** Clean snapshots and tags whose id is larger than given snapshot's. */
    public void cleanLargerThan(Snapshot retainedSnapshot) {
        // clean data files
        List<Snapshot> cleanedSnapshots = cleanSnapshotsDataFiles(retainedSnapshot);
        List<Changelog> cleanedChangelogs = cleanLongLivedChangelogDataFiles(retainedSnapshot);
        List<Snapshot> cleanedTags = cleanTagsDataFiles(retainedSnapshot);
        Set<Long> cleanedIds = new HashSet<>();

        // clean manifests
        // this can be used for snapshots and tags manifests cleaning both
        Set<String> manifestsSkippingSet = snapshotDeletion.manifestSkippingSet(retainedSnapshot);

        for (Snapshot snapshot : cleanedSnapshots) {
            snapshotDeletion.cleanUnusedManifests(snapshot, manifestsSkippingSet);
            cleanedIds.add(snapshot.id());
        }

        for (Changelog changelog : cleanedChangelogs) {
            changelogDeletion.cleanUnusedManifests(changelog, manifestsSkippingSet);
            cleanedIds.add(changelog.id());
        }

        for (Snapshot snapshot : cleanedTags) {
            if (cleanedIds.contains(snapshot.id())) {
                continue;
            }
            tagDeletion.cleanUnusedManifests(snapshot, manifestsSkippingSet);
        }
    }

    private List<Snapshot> cleanSnapshotsDataFiles(Snapshot retainedSnapshot) {
        long earliest =
                checkNotNull(
                        snapshotManager.earliestSnapshotId(), "Cannot find earliest snapshot.");
        long latest =
                checkNotNull(snapshotManager.latestSnapshotId(), "Cannot find latest snapshot.");

        // modify the latest hint
        try {
            snapshotManager.commitLatestHint(retainedSnapshot.id());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        // delete snapshot files first, cannot be read now
        // it is possible that some snapshots have been expired
        List<Snapshot> toBeCleaned = new ArrayList<>();
        long to = Math.max(earliest, retainedSnapshot.id() + 1);
        for (long i = latest; i >= to; i--) {
            // Ignore the non-existent snapshots
            if (snapshotManager.snapshotExists(i)) {
                toBeCleaned.add(snapshotManager.snapshot(i));
                fileIO.deleteQuietly(snapshotManager.snapshotPath(i));
            }
        }

        // delete data files of snapshots
        // don't worry about tag data files because file deletion methods won't throw exception
        // when deleting non-existing data files
        for (Snapshot snapshot : toBeCleaned) {
            snapshotDeletion.deleteAddedDataFiles(snapshot.deltaManifestList());
            if (snapshot.changelogManifestList() != null) {
                snapshotDeletion.deleteAddedDataFiles(snapshot.changelogManifestList());
            }
        }

        // delete directories
        snapshotDeletion.cleanEmptyDirectories();

        return toBeCleaned;
    }

    private List<Changelog> cleanLongLivedChangelogDataFiles(Snapshot retainedSnapshot) {
        Long earliest = snapshotManager.earliestLongLivedChangelogId();
        Long latest = snapshotManager.latestLongLivedChangelogId();
        if (earliest == null || latest == null) {
            return Collections.emptyList();
        }

        // it is possible that some snapshots have been expired
        List<Changelog> toBeCleaned = new ArrayList<>();
        long to = Math.max(earliest, retainedSnapshot.id() + 1);
        for (long i = latest; i >= to; i--) {
            toBeCleaned.add(snapshotManager.changelog(i));
        }

        // modify the latest hint
        try {
            if (toBeCleaned.size() > 0) {
                if (to == earliest) {
                    // all changelog has been cleaned, so we do not know the actual latest id
                    // set to -1
                    snapshotManager.commitLongLivedChangelogLatestHint(-1);
                } else {
                    snapshotManager.commitLongLivedChangelogLatestHint(to - 1);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        // delete data files of changelog
        for (Changelog changelog : toBeCleaned) {
            // delete changelog files first, cannot be read now
            fileIO.deleteQuietly(snapshotManager.longLivedChangelogPath(changelog.id()));
            // clean the deleted file
            changelogDeletion.cleanUnusedDataFiles(changelog, manifestEntry -> false);
        }

        // delete directories
        snapshotDeletion.cleanEmptyDirectories();

        return toBeCleaned;
    }

    private List<Snapshot> cleanTagsDataFiles(Snapshot retainedSnapshot) {
        SortedMap<Snapshot, List<String>> tags = tagManager.tags();
        if (tags.isEmpty()) {
            return Collections.emptyList();
        }

        List<Snapshot> taggedSnapshots = new ArrayList<>(tags.keySet());
        List<Snapshot> toBeCleaned = new ArrayList<>();

        // delete tag files
        for (int i = taggedSnapshots.size() - 1; i >= 0; i--) {
            Snapshot tag = taggedSnapshots.get(i);
            if (tag.id() <= retainedSnapshot.id()) {
                break;
            }
            toBeCleaned.add(tag);
            tags.get(tag).forEach(tagName -> fileIO.deleteQuietly(tagManager.tagPath(tagName)));
        }

        // delete data files
        Predicate<ManifestEntry> dataFileSkipper = null;
        boolean success = true;
        try {
            dataFileSkipper = tagDeletion.dataFileSkipper(retainedSnapshot);
        } catch (Exception e) {
            LOG.info(
                    "Skip cleaning data files for deleted tags due to failed to build skipping set.",
                    e);
            success = false;
        }

        if (success) {
            for (Snapshot s : toBeCleaned) {
                tagDeletion.cleanUnusedDataFiles(s, dataFileSkipper);
            }
            // delete directories
            tagDeletion.cleanEmptyDirectories();
        }

        return toBeCleaned;
    }
}
