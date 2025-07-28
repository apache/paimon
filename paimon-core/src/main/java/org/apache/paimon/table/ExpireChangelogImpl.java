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
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.manifest.ExpireFileEntry;
import org.apache.paimon.operation.ChangelogDeletion;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static org.apache.paimon.table.ExpireSnapshotsImpl.findSkippingTags;

/** Cleanup the changelog in changelog directory. */
public class ExpireChangelogImpl implements ExpireSnapshots {

    public static final Logger LOG = LoggerFactory.getLogger(ExpireChangelogImpl.class);

    private final SnapshotManager snapshotManager;
    private final ChangelogManager changelogManager;
    private final ConsumerManager consumerManager;
    private final ChangelogDeletion changelogDeletion;
    private final TagManager tagManager;
    private final SchemaManager schemaManager;
    private final boolean detectExpirationSettingEnabled;

    private ExpireConfig expireConfig;
    private long latestSchemaId;

    public ExpireChangelogImpl(
            SnapshotManager snapshotManager,
            ChangelogManager changelogManager,
            TagManager tagManager,
            ChangelogDeletion changelogDeletion,
            SchemaManager schemaManager,
            boolean detectExpirationSettingEnabled) {
        this.snapshotManager = snapshotManager;
        this.changelogManager = changelogManager;
        this.tagManager = tagManager;
        this.consumerManager =
                new ConsumerManager(
                        snapshotManager.fileIO(),
                        snapshotManager.tablePath(),
                        snapshotManager.branch());
        this.changelogDeletion = changelogDeletion;
        this.expireConfig = ExpireConfig.builder().build();
        this.schemaManager = schemaManager;
        this.detectExpirationSettingEnabled = detectExpirationSettingEnabled;
        if (this.detectExpirationSettingEnabled) {
            this.latestSchemaId = this.schemaManager.latest().get().id();
        }
    }

    @Override
    public ExpireSnapshots config(ExpireConfig expireConfig) {
        this.expireConfig = expireConfig;
        return this;
    }

    @Override
    public int expire() {
        if (this.detectExpirationSettingEnabled) {
            TableSchema latestTableSchema = this.schemaManager.latest().get();
            if (this.latestSchemaId != latestTableSchema.id()) {
                this.expireConfig = CoreOptions.fromMap(latestTableSchema.options()).expireConfig();
                this.latestSchemaId = latestTableSchema.id();
            }
        }

        int retainMax = expireConfig.getChangelogRetainMax();
        int retainMin = expireConfig.getChangelogRetainMin();
        int maxDeletes = expireConfig.getChangelogMaxDeletes();
        long olderThanMills =
                System.currentTimeMillis() - expireConfig.getChangelogTimeRetain().toMillis();
        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        if (latestSnapshotId == null) {
            // no snapshot, nothing to expire
            return 0;
        }

        Long earliestSnapshotId = snapshotManager.earliestSnapshotId();
        if (earliestSnapshotId == null) {
            return 0;
        }

        Long latestChangelogId = changelogManager.latestLongLivedChangelogId();
        if (latestChangelogId == null) {
            return 0;
        }
        Long earliestChangelogId = changelogManager.earliestLongLivedChangelogId();
        if (earliestChangelogId == null) {
            return 0;
        }

        Preconditions.checkArgument(
                retainMax >= retainMin,
                String.format(
                        "retainMax (%s) must not be less than retainMin (%s).",
                        retainMax, retainMin));

        // the min snapshot to retain from 'changelog.num-retained.max'
        // (the maximum number of snapshots to retain)
        long min = Math.max(latestSnapshotId - retainMax + 1, earliestChangelogId);

        // the max exclusive snapshot to expire until
        // protected by 'changelog.num-retained.min'
        // (the minimum number of completed snapshots to retain)
        long maxExclusive = latestSnapshotId - retainMin + 1;

        // the snapshot being read by the consumer cannot be deleted
        maxExclusive =
                Math.min(maxExclusive, consumerManager.minNextSnapshot().orElse(Long.MAX_VALUE));

        // protected by 'snapshot.expire.limit'
        // (the maximum number of snapshots allowed to expire at a time)
        maxExclusive = Math.min(maxExclusive, earliestChangelogId + maxDeletes);

        // Only clean the snapshot in changelog dir
        maxExclusive = Math.min(maxExclusive, latestChangelogId);

        for (long id = min; id <= maxExclusive; id++) {
            if (changelogManager.longLivedChangelogExists(id)
                    && olderThanMills <= changelogManager.longLivedChangelog(id).timeMillis()) {
                return expireUntil(earliestChangelogId, id);
            }
        }
        return expireUntil(earliestChangelogId, maxExclusive);
    }

    public int expireUntil(long earliestId, long endExclusiveId) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Changelog expire range is [" + earliestId + ", " + endExclusiveId + ")");
        }

        List<Snapshot> taggedSnapshots = tagManager.taggedSnapshots();

        List<Snapshot> skippingSnapshots =
                findSkippingTags(taggedSnapshots, earliestId, endExclusiveId);
        skippingSnapshots.add(changelogManager.changelog(endExclusiveId));
        Set<String> manifestSkippSet = changelogDeletion.manifestSkippingSet(skippingSnapshots);
        for (long id = earliestId; id < endExclusiveId; id++) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ready to delete changelog files from changelog #" + id);
            }
            Changelog changelog = changelogManager.longLivedChangelog(id);
            Predicate<ExpireFileEntry> skipper;
            try {
                skipper = changelogDeletion.createDataFileSkipperForTags(taggedSnapshots, id);
            } catch (Exception e) {
                LOG.info(
                        String.format(
                                "Skip cleaning data files of changelog '%s' due to failed to build skipping set.",
                                id),
                        e);
                continue;
            }

            changelogDeletion.cleanUnusedDataFiles(changelog, skipper);
            changelogDeletion.cleanUnusedManifests(changelog, manifestSkippSet);
            changelogManager.fileIO().deleteQuietly(changelogManager.longLivedChangelogPath(id));
        }

        changelogDeletion.cleanEmptyDirectories();
        writeEarliestHintFile(endExclusiveId);
        return (int) (endExclusiveId - earliestId);
    }

    /** expire all separated changelogs, only used by ExpireChangelogsProcedure. */
    public void expireAll() {
        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        if (latestSnapshotId == null) {
            // no snapshot, nothing to expire
            return;
        }

        Long earliestSnapshotId = snapshotManager.earliestSnapshotId();
        if (earliestSnapshotId == null) {
            return;
        }

        Long latestChangelogId = changelogManager.latestLongLivedChangelogId();
        if (latestChangelogId == null) {
            return;
        }
        Long earliestChangelogId = changelogManager.earliestLongLivedChangelogId();
        if (earliestChangelogId == null) {
            return;
        }

        LOG.info(
                "Read earliest and latest changelog for expire all. earliestChangelogId is {}, latestChangelogId is {}",
                earliestChangelogId,
                latestChangelogId);

        List<Snapshot> taggedSnapshots = tagManager.taggedSnapshots();

        // files used by the earliest snapshot id should be skipped
        Preconditions.checkArgument(
                latestChangelogId < earliestSnapshotId,
                "latest changelog id should be less than earliest snapshot id."
                        + "please check your table!");
        List<Snapshot> skippingSnapshots =
                findSkippingTags(taggedSnapshots, earliestChangelogId, earliestSnapshotId);
        skippingSnapshots.add(snapshotManager.snapshot(earliestSnapshotId));

        Set<String> manifestSkippSet = changelogDeletion.manifestSkippingSet(skippingSnapshots);
        for (long id = earliestChangelogId; id <= latestChangelogId; id++) {

            LOG.info("Ready to delete changelog files from changelog #" + id);

            Changelog changelog;
            try {
                changelog = changelogManager.tryGetChangelog(id);
            } catch (FileNotFoundException e) {
                LOG.info("fail to get changelog #" + id);
                continue;
            }
            Predicate<ExpireFileEntry> skipper;
            try {
                skipper = changelogDeletion.createDataFileSkipperForTags(taggedSnapshots, id);
            } catch (Exception e) {
                LOG.info(
                        String.format(
                                "Skip cleaning data files of changelog '%s' due to failed to build skipping set.",
                                id),
                        e);
                continue;
            }

            changelogDeletion.cleanUnusedDataFiles(changelog, skipper);
            changelogDeletion.cleanUnusedManifests(changelog, manifestSkippSet);
            changelogManager.fileIO().deleteQuietly(changelogManager.longLivedChangelogPath(id));
        }

        // try delete changelog hint file
        try {
            changelogManager.deleteEarliestHint();
            changelogManager.deleteLatestHint();
        } catch (Exception e) {
            LOG.error("delete changelog hint file error.", e);
        }

        changelogDeletion.cleanEmptyDirectories();
    }

    private void writeEarliestHintFile(long earliest) {
        try {
            changelogManager.commitLongLivedChangelogEarliestHint(earliest);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
