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

package org.apache.paimon.flink.expire;

import org.apache.paimon.Changelog;
import org.apache.paimon.Snapshot;
import org.apache.paimon.manifest.ExpireFileEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.function.Predicate;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Abstract base class for snapshot expiration tasks. Each subclass represents a specific deletion
 * operation:
 *
 * <ul>
 *   <li>{@link DeleteDataFilesTask}: Delete data files
 *   <li>{@link DeleteChangelogFilesTask}: Delete changelog files
 *   <li>{@link DeleteManifestsTask}: Delete manifest files
 *   <li>{@link DeleteSnapshotTask}: Delete snapshot metadata file
 * </ul>
 */
public abstract class SnapshotExpireTask implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SnapshotExpireTask.class);

    protected final long snapshotId;

    protected SnapshotExpireTask(long snapshotId) {
        this.snapshotId = snapshotId;
    }

    public long snapshotId() {
        return snapshotId;
    }

    /** Execute this task using the given context. */
    public abstract DeletionReport execute(SnapshotExpireContext context);

    // ==================== Factory Methods ====================

    public static SnapshotExpireTask forDataFiles(long snapshotId) {
        return new DeleteDataFilesTask(snapshotId);
    }

    public static SnapshotExpireTask forChangelogFiles(long snapshotId) {
        return new DeleteChangelogFilesTask(snapshotId);
    }

    public static SnapshotExpireTask forManifests(long snapshotId) {
        return new DeleteManifestsTask(snapshotId);
    }

    public static SnapshotExpireTask forSnapshot(long snapshotId, boolean changelogDecoupled) {
        return new DeleteSnapshotTask(snapshotId, changelogDecoupled);
    }

    // ==================== Subclasses ====================

    /** Task that deletes data files for a snapshot. */
    public static class DeleteDataFilesTask extends SnapshotExpireTask {

        private static final long serialVersionUID = 1L;

        DeleteDataFilesTask(long snapshotId) {
            super(snapshotId);
        }

        @Override
        public DeletionReport execute(SnapshotExpireContext context) {
            Snapshot snapshot = context.loadSnapshot(snapshotId);
            if (snapshot == null) {
                return DeletionReport.skipped(snapshotId);
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Ready to delete merge tree files not used by snapshot #{}", snapshotId);
            }

            Predicate<ExpireFileEntry> skipper = null;
            try {
                skipper =
                        context.snapshotDeletion()
                                .createDataFileSkipperForTags(
                                        checkNotNull(context.taggedSnapshots()), snapshotId);
            } catch (Exception e) {
                LOG.warn(
                        String.format(
                                "Skip cleaning data files of snapshot '%s' due to failed to build skipping set.",
                                snapshotId),
                        e);
                return DeletionReport.skipped(snapshotId);
            }

            if (skipper != null) {
                context.snapshotDeletion().cleanUnusedDataFiles(snapshot, skipper);
            }
            return new DeletionReport(snapshotId);
        }

        @Override
        public String toString() {
            return "DeleteDataFiles{snapshotId=" + snapshotId + '}';
        }
    }

    /** Task that deletes changelog files for a snapshot. */
    public static class DeleteChangelogFilesTask extends SnapshotExpireTask {

        private static final long serialVersionUID = 1L;

        DeleteChangelogFilesTask(long snapshotId) {
            super(snapshotId);
        }

        @Override
        public DeletionReport execute(SnapshotExpireContext context) {
            Snapshot snapshot = context.loadSnapshot(snapshotId);
            if (snapshot == null) {
                return DeletionReport.skipped(snapshotId);
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Ready to delete changelog files from snapshot #{}", snapshotId);
            }

            if (snapshot.changelogManifestList() != null) {
                context.snapshotDeletion().deleteAddedDataFiles(snapshot.changelogManifestList());
            }
            return new DeletionReport(snapshotId);
        }

        @Override
        public String toString() {
            return "DeleteChangelogFiles{snapshotId=" + snapshotId + '}';
        }
    }

    /** Task that deletes manifest files for a snapshot. */
    public static class DeleteManifestsTask extends SnapshotExpireTask {

        private static final long serialVersionUID = 1L;

        DeleteManifestsTask(long snapshotId) {
            super(snapshotId);
        }

        @Override
        public DeletionReport execute(SnapshotExpireContext context) {
            Snapshot snapshot = context.loadSnapshot(snapshotId);
            if (snapshot == null) {
                return DeletionReport.skipped(snapshotId);
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Ready to delete manifests in snapshot #{}", snapshotId);
            }

            context.snapshotDeletion()
                    .cleanUnusedManifests(snapshot, checkNotNull(context.skippingSet()));
            return new DeletionReport(snapshotId);
        }

        @Override
        public String toString() {
            return "DeleteManifests{snapshotId=" + snapshotId + '}';
        }
    }

    /** Task that deletes snapshot metadata file. */
    public static class DeleteSnapshotTask extends SnapshotExpireTask {

        private static final long serialVersionUID = 1L;

        private final boolean changelogDecoupled;

        DeleteSnapshotTask(long snapshotId, boolean changelogDecoupled) {
            super(snapshotId);
            this.changelogDecoupled = changelogDecoupled;
        }

        @Override
        public DeletionReport execute(SnapshotExpireContext context) {
            Snapshot snapshot = context.loadSnapshot(snapshotId);
            if (snapshot == null) {
                return DeletionReport.skipped(snapshotId);
            }

            if (changelogDecoupled && context.changelogManager() != null) {
                try {
                    Changelog changelog = new Changelog(snapshot);
                    context.changelogManager().commitChangelog(changelog, changelog.id());
                    context.changelogManager().commitLongLivedChangelogLatestHint(changelog.id());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            context.snapshotManager().deleteSnapshot(snapshot.id());
            return new DeletionReport(snapshotId);
        }

        @Override
        public String toString() {
            return "DeleteSnapshot{snapshotId="
                    + snapshotId
                    + ", changelogDecoupled="
                    + changelogDecoupled
                    + '}';
        }
    }
}
