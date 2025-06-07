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
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Helper class for {@link Table#rollbackTo} including utils to clean snapshots. */
public class RollbackHelper {

    private final SnapshotManager snapshotManager;
    private final ChangelogManager changelogManager;
    private final TagManager tagManager;
    private final FileIO fileIO;

    public RollbackHelper(
            SnapshotManager snapshotManager,
            ChangelogManager changelogManager,
            TagManager tagManager,
            FileIO fileIO) {
        this.snapshotManager = snapshotManager;
        this.changelogManager = changelogManager;
        this.tagManager = tagManager;
        this.fileIO = fileIO;
    }

    /** Clean snapshots and tags whose id is larger than given snapshot's and update latest hit. */
    public void cleanLargerThan(Snapshot retainedSnapshot) {
        try {
            cleanSnapshots(retainedSnapshot);
            cleanLongLivedChangelogs(retainedSnapshot);
            cleanTags(retainedSnapshot);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void createSnapshotFileIfNeeded(Snapshot taggedSnapshot) {
        // it is possible that the earliest snapshot is later than the rollback tag because of
        // snapshot expiration, in this case the `cleanLargerThan` method will delete all
        // snapshots, so we should write the tag file to snapshot directory and modify the
        // earliest hint
        if (!snapshotManager.snapshotExists(taggedSnapshot.id())) {
            try {
                fileIO.writeFile(
                        snapshotManager.snapshotPath(taggedSnapshot.id()),
                        taggedSnapshot.toJson(),
                        false);
                snapshotManager.commitEarliestHint(taggedSnapshot.id());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private void cleanSnapshots(Snapshot retainedSnapshot) throws IOException {
        long earliest =
                checkNotNull(
                        snapshotManager.earliestSnapshotId(), "Cannot find earliest snapshot.");
        long latest =
                checkNotNull(snapshotManager.latestSnapshotId(), "Cannot find latest snapshot.");

        // modify the latest hint
        snapshotManager.commitLatestHint(retainedSnapshot.id());

        // it is possible that some snapshots have been expired
        long to = Math.max(earliest, retainedSnapshot.id() + 1);
        for (long i = latest; i >= to; i--) {
            // Ignore the non-existent snapshots
            if (snapshotManager.snapshotExists(i)) {
                snapshotManager.deleteSnapshot(i);
            }
        }
    }

    private void cleanLongLivedChangelogs(Snapshot retainedSnapshot) throws IOException {
        Long earliest = changelogManager.earliestLongLivedChangelogId();
        Long latest = changelogManager.latestLongLivedChangelogId();
        if (earliest == null || latest == null) {
            return;
        }

        // it is possible that some snapshots have been expired
        List<Changelog> toBeCleaned = new ArrayList<>();
        long to = Math.max(earliest, retainedSnapshot.id() + 1);
        for (long i = latest; i >= to; i--) {
            toBeCleaned.add(changelogManager.changelog(i));
        }

        // modify the latest hint
        if (!toBeCleaned.isEmpty()) {
            if (to == earliest) {
                // all changelog has been cleaned, so we do not know the actual latest id
                // set to -1
                changelogManager.commitLongLivedChangelogLatestHint(-1);
            } else {
                changelogManager.commitLongLivedChangelogLatestHint(to - 1);
            }
        }

        // delete data files of changelog
        for (Changelog changelog : toBeCleaned) {
            fileIO.deleteQuietly(changelogManager.longLivedChangelogPath(changelog.id()));
        }
    }

    private void cleanTags(Snapshot retainedSnapshot) {
        SortedMap<Snapshot, List<String>> tags = tagManager.tags();
        if (tags.isEmpty()) {
            return;
        }

        List<Snapshot> taggedSnapshots = new ArrayList<>(tags.keySet());

        // delete tag files
        for (int i = taggedSnapshots.size() - 1; i >= 0; i--) {
            Snapshot tag = taggedSnapshots.get(i);
            if (tag.id() <= retainedSnapshot.id()) {
                break;
            }
            tags.get(tag).forEach(tagName -> fileIO.deleteQuietly(tagManager.tagPath(tagName)));
        }
    }
}
