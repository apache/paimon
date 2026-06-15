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
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ExpireFileEntry;
import org.apache.paimon.operation.SnapshotDeletion;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.SnapshotManager.findPreviousOrEqualSnapshot;
import static org.apache.paimon.utils.SnapshotManager.findPreviousSnapshot;

/** An implementation for {@link ExpireSnapshots}. */
public class ExpireSnapshotsImpl implements ExpireSnapshots {

    private static final Logger LOG = LoggerFactory.getLogger(ExpireSnapshotsImpl.class);

    private final SnapshotManager snapshotManager;
    private final ChangelogManager changelogManager;
    private final ConsumerManager consumerManager;
    private final SnapshotDeletion snapshotDeletion;
    private final Executor fileExecutor;
    private final TagManager tagManager;

    private ExpireConfig expireConfig;
    private Supplier<Long> currentTimeMillis = System::currentTimeMillis;

    public ExpireSnapshotsImpl(
            SnapshotManager snapshotManager,
            ChangelogManager changelogManager,
            SnapshotDeletion snapshotDeletion,
            TagManager tagManager) {
        this.snapshotManager = snapshotManager;
        this.changelogManager = changelogManager;
        this.consumerManager =
                new ConsumerManager(
                        snapshotManager.fileIO(),
                        snapshotManager.tablePath(),
                        snapshotManager.branch());
        this.snapshotDeletion = snapshotDeletion;
        this.tagManager = tagManager;
        this.expireConfig = ExpireConfig.builder().build();
        this.fileExecutor = snapshotDeletion.fileExecutor();
    }

    @VisibleForTesting
    public void setCurrentTimeMillis(Supplier<Long> currentTimeMillis) {
        this.currentTimeMillis = currentTimeMillis;
    }

    @Override
    public ExpireSnapshots config(ExpireConfig expireConfig) {
        this.expireConfig = expireConfig;
        return this;
    }

    @Override
    public int expire() {
        snapshotDeletion.setChangelogDecoupled(expireConfig.isChangelogDecoupled());
        int retainMax = expireConfig.getSnapshotRetainMax();
        int retainMin = expireConfig.getSnapshotRetainMin();
        int maxDeletes = expireConfig.getSnapshotMaxDeletes();
        long olderThanMills =
                currentTimeMillis.get() - expireConfig.getSnapshotTimeRetain().toMillis();

        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        if (latestSnapshotId == null) {
            // no snapshot, nothing to expire
            return 0;
        }

        Long earliest = snapshotManager.earliestSnapshotId();
        if (earliest == null) {
            return 0;
        }

        Preconditions.checkArgument(
                retainMax >= retainMin,
                String.format(
                        "retainMax (%s) must not be less than retainMin (%s).",
                        retainMax, retainMin));

        // the min snapshot to retain from 'snapshot.num-retained.max'
        // (the maximum number of snapshots to retain)
        long min = Math.max(latestSnapshotId - retainMax + 1, earliest);

        // the max exclusive snapshot to expire until
        // protected by 'snapshot.num-retained.min'
        // (the minimum number of completed snapshots to retain)
        long maxExclusive = latestSnapshotId - retainMin + 1;

        // the snapshot being read by the consumer cannot be deleted
        if (!expireConfig.isConsumerChangelogOnly()) {
            maxExclusive =
                    Math.min(
                            maxExclusive, consumerManager.minNextSnapshot().orElse(Long.MAX_VALUE));
        }

        // protected by 'snapshot.expire.limit'
        // (the maximum number of snapshots allowed to expire at a time)
        maxExclusive = Math.min(maxExclusive, earliest + maxDeletes);

        if (maxExclusive <= earliest) {
            return expireUntil(earliest, maxExclusive);
        }

        try {
            List<Snapshot> snapshots = collectSnapshots(earliest, maxExclusive);
            Map<Long, Snapshot> snapshotById = new HashMap<>();
            for (Snapshot snapshot : snapshots) {
                snapshotById.put(snapshot.id(), snapshot);
            }

            for (long id = min; id < maxExclusive; id++) {
                // Early exit the loop for 'snapshot.time-retained'
                // A snapshot can only be expired if its next snapshot has been alive
                // longer than snapshotTimeRetain, providing stronger protection
                Snapshot nextSnapshot = snapshotById.get(id + 1);
                if (nextSnapshot != null && olderThanMills <= nextSnapshot.timeMillis()) {
                    return innerExpireUntil(earliest, id, snapshots);
                }
            }

            return innerExpireUntil(earliest, maxExclusive, snapshots);
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    public int expireUntil(long earliestId, long endExclusiveId) {
        try {
            return innerExpireUntil(earliestId, endExclusiveId);
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private int innerExpireUntil(long earliestId, long endExclusiveId)
            throws ExecutionException, InterruptedException {
        if (endExclusiveId <= earliestId) {
            return innerExpireUntil(earliestId, endExclusiveId, new ArrayList<>());
        }
        return innerExpireUntil(
                earliestId, endExclusiveId, collectSnapshots(earliestId, endExclusiveId));
    }

    private int innerExpireUntil(
            long earliestId, long endExclusiveId, List<Snapshot> collectedSnapshots)
            throws ExecutionException, InterruptedException {
        long startTime = currentTimeMillis.get();

        if (endExclusiveId <= earliestId) {
            // No expire happens:
            // write the hint file in order to see the earliest snapshot directly next time
            // should avoid duplicate writes when the file exists
            if (snapshotManager.earliestFileNotExists()) {
                writeEarliestHint(earliestId);
            }

            // fast exit
            return 0;
        }

        // collect all snapshots
        List<Snapshot> snapshotsIncludingEnd =
                collectedSnapshots.stream()
                        .filter(s -> s.id() >= earliestId && s.id() <= endExclusiveId)
                        .collect(Collectors.toList());
        if (snapshotsIncludingEnd.isEmpty()) {
            return 0;
        }
        List<Snapshot> snapshotsExcludingEnd =
                snapshotsIncludingEnd.stream()
                        .filter(s -> s.id() != endExclusiveId)
                        .collect(Collectors.toList());
        long beginInclusiveId = snapshotsIncludingEnd.get(0).id();

        // tags to create data file skipper
        List<Snapshot> taggedSnapshots = tagManager.taggedSnapshots();

        // delete merge tree files
        // deleted merge tree files in a snapshot are not used by the next snapshot, so the range of
        // id should be (beginInclusiveId, endExclusiveId]
        snapshotDeletion.cleanDataFiles(
                collectDataFilesToDelete(snapshotsIncludingEnd, taggedSnapshots, beginInclusiveId));

        // delete changelog files
        if (!expireConfig.isChangelogDecoupled()) {
            snapshotDeletion.cleanDataFiles(collectChangelogFilesToDelete(snapshotsExcludingEnd));
        }

        // data files and changelog files in bucket directories has been deleted
        // then delete changed bucket directories if they are empty
        snapshotDeletion.cleanEmptyDirectories();

        // delete manifests and indexFiles
        List<Snapshot> skippingSnapshots =
                findSkippingTags(taggedSnapshots, beginInclusiveId, endExclusiveId);

        Snapshot lastSnapshot = snapshotsIncludingEnd.get(snapshotsIncludingEnd.size() - 1);
        if (lastSnapshot.id() != endExclusiveId) {
            // the end exclusive snapshot is gone
            // there is no need to proceed
            return 0;
        }
        skippingSnapshots.add(lastSnapshot);

        Set<String> skippingSet = null;
        try {
            skippingSet = ConcurrentHashMap.newKeySet();
            skippingSet.addAll(snapshotDeletion.manifestSkippingSet(skippingSnapshots));
        } catch (Exception e) {
            LOG.info("Skip cleaning manifest files due to failed to build skipping set.", e);
        }
        if (skippingSet != null) {
            snapshotDeletion.executeAll(
                    collectManifestDeletionTasks(snapshotsExcludingEnd, skippingSet));
        }

        // delete snapshot file finally
        deleteSnapshotFiles(snapshotsExcludingEnd);

        writeEarliestHint(endExclusiveId);
        long duration = currentTimeMillis.get() - startTime;
        LOG.info(
                "Finished expire snapshots, duration {} ms, range is [{}, {})",
                duration,
                beginInclusiveId,
                endExclusiveId);
        return snapshotsExcludingEnd.size();
    }

    private Collection<Path> collectDataFilesToDelete(
            List<Snapshot> snapshotsIncludingEnd,
            List<Snapshot> taggedSnapshots,
            long beginInclusiveId)
            throws ExecutionException, InterruptedException {
        Map<Long, Long> tagIdBySnapshotId = new HashMap<>();
        Map<Long, Snapshot> tags = new HashMap<>();
        int tagIndex = -1;
        for (Snapshot snapshot : snapshotsIncludingEnd) {
            long id = snapshot.id();
            if (id == beginInclusiveId) {
                continue;
            }

            tagIndex = advancePreviousSnapshot(taggedSnapshots, tagIndex, id);
            if (tagIndex >= 0) {
                Snapshot tag = taggedSnapshots.get(tagIndex);
                tagIdBySnapshotId.put(id, tag.id());
                tags.put(tag.id(), tag);
            }
        }

        Map<Long, Optional<Predicate<ExpireFileEntry>>> skippers =
                collectTagSkippers(tags.values());
        Predicate<ExpireFileEntry> deleteAll = entry -> false;
        List<CompletableFuture<List<Path>>> futures = new ArrayList<>();
        for (Snapshot snapshot : snapshotsIncludingEnd) {
            long id = snapshot.id();
            if (id == beginInclusiveId) {
                continue;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ready to delete merge tree files not used by snapshot #{}", id);
            }

            Long tagId = tagIdBySnapshotId.get(id);
            Optional<Predicate<ExpireFileEntry>> skipper =
                    tagId == null
                            ? Optional.of(deleteAll)
                            : skippers.getOrDefault(tagId, Optional.empty());
            if (!skipper.isPresent()) {
                LOG.info(
                        "Skip cleaning data files of snapshot '{}' due to failed to build skipping set.",
                        id);
                continue;
            }

            futures.add(
                    CompletableFuture.supplyAsync(
                            () ->
                                    snapshotDeletion.planDeletedInDeltaManifest(
                                            snapshot, skipper.get()),
                            fileExecutor));
        }
        return flatten(getAll(futures));
    }

    private Map<Long, Optional<Predicate<ExpireFileEntry>>> collectTagSkippers(
            Collection<Snapshot> tags) throws ExecutionException, InterruptedException {
        Map<Long, CompletableFuture<Optional<Predicate<ExpireFileEntry>>>> futures =
                new HashMap<>();
        for (Snapshot tag : tags) {
            futures.put(
                    tag.id(),
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return Optional.of(
                                            snapshotDeletion.createDataFileSkipperForTag(tag));
                                } catch (Exception e) {
                                    LOG.info(
                                            "Failed to build data file skipping set for tag snapshot '{}'.",
                                            tag.id(),
                                            e);
                                    return Optional.empty();
                                }
                            },
                            fileExecutor));
        }

        Map<Long, Optional<Predicate<ExpireFileEntry>>> skippers = new HashMap<>();
        for (Map.Entry<Long, CompletableFuture<Optional<Predicate<ExpireFileEntry>>>> entry :
                futures.entrySet()) {
            skippers.put(entry.getKey(), entry.getValue().get());
        }
        return skippers;
    }

    private Collection<Path> collectChangelogFilesToDelete(List<Snapshot> snapshots)
            throws ExecutionException, InterruptedException {
        List<CompletableFuture<List<Path>>> futures = new ArrayList<>();
        for (Snapshot snapshot : snapshots) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ready to delete changelog files from snapshot #{}", snapshot.id());
            }
            if (snapshot.changelogManifestList() != null) {
                futures.add(
                        CompletableFuture.supplyAsync(
                                () -> snapshotDeletion.planAddedInChangelogManifest(snapshot),
                                fileExecutor));
            }
        }
        return flatten(getAll(futures));
    }

    private Collection<Runnable> collectManifestDeletionTasks(
            List<Snapshot> snapshots, Set<String> skippingSet)
            throws ExecutionException, InterruptedException {
        List<CompletableFuture<List<Runnable>>> futures = new ArrayList<>();
        for (Snapshot snapshot : snapshots) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ready to delete manifests in snapshot #{}", snapshot.id());
            }
            futures.add(
                    CompletableFuture.supplyAsync(
                            () -> snapshotDeletion.planManifestsCleaner(snapshot, skippingSet),
                            fileExecutor));
        }
        return flatten(getAll(futures));
    }

    private <T> List<T> getAll(List<CompletableFuture<T>> futures)
            throws ExecutionException, InterruptedException {
        List<T> result = new ArrayList<>();
        for (CompletableFuture<T> future : futures) {
            result.add(future.get());
        }
        return result;
    }

    private <T> List<T> flatten(List<? extends Collection<T>> collections) {
        List<T> result = new ArrayList<>();
        for (Collection<T> collection : collections) {
            result.addAll(collection);
        }
        return result;
    }

    private void deleteSnapshotFiles(List<Snapshot> snapshots)
            throws ExecutionException, InterruptedException {
        if (expireConfig.isChangelogDecoupled()) {
            for (Snapshot snapshot : snapshots) {
                commitChangelog(new Changelog(snapshot));
                snapshotManager.deleteSnapshot(snapshot.id());
            }
            return;
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (Snapshot snapshot : snapshots) {
            futures.add(
                    CompletableFuture.runAsync(
                            () -> snapshotManager.deleteSnapshot(snapshot.id()), fileExecutor));
        }
        for (CompletableFuture<Void> future : futures) {
            future.get();
        }
    }

    private List<Snapshot> collectSnapshots(long earliestId, long endExclusiveId)
            throws InterruptedException, ExecutionException {
        List<CompletableFuture<Optional<Snapshot>>> futures = new ArrayList<>();
        for (long id = earliestId; id <= endExclusiveId; id++) {
            long snapshotId = id;
            CompletableFuture<Optional<Snapshot>> future =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return Optional.of(snapshotManager.tryGetSnapshot(snapshotId));
                                } catch (FileNotFoundException ignored) {
                                    return Optional.empty();
                                }
                            },
                            fileExecutor);
            futures.add(future);
        }
        List<Snapshot> snapshots = new ArrayList<>();
        for (CompletableFuture<Optional<Snapshot>> future : futures) {
            future.get().ifPresent(snapshots::add);
        }
        return snapshots;
    }

    private static int advancePreviousSnapshot(
            List<Snapshot> sortedSnapshots, int currentIndex, long targetSnapshotId) {
        while (currentIndex + 1 < sortedSnapshots.size()
                && sortedSnapshots.get(currentIndex + 1).id() < targetSnapshotId) {
            currentIndex++;
        }
        return currentIndex;
    }

    private void commitChangelog(Changelog changelog) {
        try {
            changelogManager.commitChangelog(changelog, changelog.id());
            changelogManager.commitLongLivedChangelogLatestHint(changelog.id());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeEarliestHint(long earliest) {
        try {
            snapshotManager.commitEarliestHint(earliest);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @VisibleForTesting
    public SnapshotDeletion snapshotDeletion() {
        return snapshotDeletion;
    }

    /** Find the skipping tags in sortedTags for range of [beginInclusive, endExclusive). */
    public static List<Snapshot> findSkippingTags(
            List<Snapshot> sortedTags, long beginInclusive, long endExclusive) {
        List<Snapshot> overlappedSnapshots = new ArrayList<>();
        int right = findPreviousSnapshot(sortedTags, endExclusive);
        if (right >= 0) {
            int left = Math.max(findPreviousOrEqualSnapshot(sortedTags, beginInclusive), 0);
            for (int i = left; i <= right; i++) {
                overlappedSnapshots.add(sortedTags.get(i));
            }
        }
        return overlappedSnapshots;
    }
}
