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

package org.apache.paimon.utils;

import org.apache.paimon.Changelog;
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.paimon.utils.BranchManager.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.utils.BranchManager.branchPath;
import static org.apache.paimon.utils.FileUtils.listVersionedFiles;
import static org.apache.paimon.utils.ThreadPoolUtils.createCachedThreadPool;
import static org.apache.paimon.utils.ThreadPoolUtils.randomlyOnlyExecute;

/** Manager for {@link Snapshot}, providing utility methods related to paths and snapshot hints. */
public class SnapshotManager implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotManager.class);

    private static final String SNAPSHOT_PREFIX = "snapshot-";
    private static final String CHANGELOG_PREFIX = "changelog-";
    public static final String EARLIEST = "EARLIEST";
    public static final String LATEST = "LATEST";
    private static final int READ_HINT_RETRY_NUM = 3;
    private static final int READ_HINT_RETRY_INTERVAL = 1;

    private final FileIO fileIO;
    private final Path tablePath;
    private final String branch;
    @Nullable private final Cache<Path, Snapshot> cache;

    public SnapshotManager(FileIO fileIO, Path tablePath) {
        this(fileIO, tablePath, DEFAULT_MAIN_BRANCH);
    }

    /** Specify the default branch for data writing. */
    public SnapshotManager(FileIO fileIO, Path tablePath, @Nullable String branchName) {
        this(fileIO, tablePath, branchName, null);
    }

    public SnapshotManager(
            FileIO fileIO,
            Path tablePath,
            @Nullable String branchName,
            @Nullable Cache<Path, Snapshot> cache) {
        this.fileIO = fileIO;
        this.tablePath = tablePath;
        this.branch = BranchManager.normalizeBranch(branchName);
        this.cache = cache;
    }

    public SnapshotManager copyWithBranch(String branchName) {
        return new SnapshotManager(fileIO, tablePath, branchName);
    }

    public FileIO fileIO() {
        return fileIO;
    }

    public Path tablePath() {
        return tablePath;
    }

    public String branch() {
        return branch;
    }

    public Path changelogDirectory() {
        return new Path(branchPath(tablePath, branch) + "/changelog");
    }

    public Path longLivedChangelogPath(long snapshotId) {
        return new Path(
                branchPath(tablePath, branch) + "/changelog/" + CHANGELOG_PREFIX + snapshotId);
    }

    public Path snapshotPath(long snapshotId) {
        return new Path(
                branchPath(tablePath, branch) + "/snapshot/" + SNAPSHOT_PREFIX + snapshotId);
    }

    public Path snapshotDirectory() {
        return new Path(branchPath(tablePath, branch) + "/snapshot");
    }

    public void invalidateCache() {
        if (cache != null) {
            cache.invalidateAll();
        }
    }

    public Snapshot snapshot(long snapshotId) {
        Path path = snapshotPath(snapshotId);
        Snapshot snapshot = cache == null ? null : cache.getIfPresent(path);
        if (snapshot == null) {
            snapshot = Snapshot.fromPath(fileIO, path);
            if (cache != null) {
                cache.put(path, snapshot);
            }
        }
        return snapshot;
    }

    public Snapshot tryGetSnapshot(long snapshotId) throws FileNotFoundException {
        Path path = snapshotPath(snapshotId);
        Snapshot snapshot = cache == null ? null : cache.getIfPresent(path);
        if (snapshot == null) {
            snapshot = Snapshot.tryFromPath(fileIO, path);
            if (cache != null) {
                cache.put(path, snapshot);
            }
        }
        return snapshot;
    }

    public Changelog changelog(long snapshotId) {
        Path changelogPath = longLivedChangelogPath(snapshotId);
        return Changelog.fromPath(fileIO, changelogPath);
    }

    public Changelog longLivedChangelog(long snapshotId) {
        return Changelog.fromPath(fileIO, longLivedChangelogPath(snapshotId));
    }

    public boolean snapshotExists(long snapshotId) {
        Path path = snapshotPath(snapshotId);
        try {
            return fileIO.exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to determine if snapshot #" + snapshotId + " exists in path " + path,
                    e);
        }
    }

    public boolean longLivedChangelogExists(long snapshotId) {
        Path path = longLivedChangelogPath(snapshotId);
        try {
            return fileIO.exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to determine if changelog #" + snapshotId + " exists in path " + path,
                    e);
        }
    }

    public @Nullable Snapshot latestSnapshot() {
        Long snapshotId = latestSnapshotId();
        return snapshotId == null ? null : snapshot(snapshotId);
    }

    public @Nullable Long latestSnapshotId() {
        try {
            return findLatest(snapshotDirectory(), SNAPSHOT_PREFIX, this::snapshotPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to find latest snapshot id", e);
        }
    }

    public @Nullable Snapshot earliestSnapshot() {
        Long snapshotId = earliestSnapshotId();
        return snapshotId == null ? null : snapshot(snapshotId);
    }

    public @Nullable Long earliestSnapshotId() {
        try {
            return findEarliest(snapshotDirectory(), SNAPSHOT_PREFIX, this::snapshotPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to find earliest snapshot id", e);
        }
    }

    public @Nullable Long earliestLongLivedChangelogId() {
        try {
            return findEarliest(
                    changelogDirectory(), CHANGELOG_PREFIX, this::longLivedChangelogPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to find earliest changelog id", e);
        }
    }

    public @Nullable Long latestLongLivedChangelogId() {
        try {
            return findLatest(changelogDirectory(), CHANGELOG_PREFIX, this::longLivedChangelogPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to find latest changelog id", e);
        }
    }

    public @Nullable Long latestChangelogId() {
        return latestSnapshotId();
    }

    public @Nullable Long pickOrLatest(Predicate<Snapshot> predicate) {
        Long latestId = latestSnapshotId();
        Long earliestId = earliestSnapshotId();
        if (latestId == null || earliestId == null) {
            return null;
        }

        for (long snapshotId = latestId; snapshotId >= earliestId; snapshotId--) {
            if (snapshotExists(snapshotId)) {
                Snapshot snapshot = snapshot(snapshotId);
                if (predicate.test(snapshot)) {
                    return snapshot.id();
                }
            }
        }

        return latestId;
    }

    private Snapshot changelogOrSnapshot(long snapshotId) {
        if (longLivedChangelogExists(snapshotId)) {
            return changelog(snapshotId);
        } else {
            return snapshot(snapshotId);
        }
    }

    /**
     * Returns the latest snapshot earlier than the timestamp mills. A non-existent snapshot may be
     * returned if all snapshots are equal to or later than the timestamp mills.
     */
    public @Nullable Long earlierThanTimeMills(long timestampMills, boolean startFromChangelog) {
        Long earliestSnapshot = earliestSnapshotId();
        Long earliest;
        if (startFromChangelog) {
            Long earliestChangelog = earliestLongLivedChangelogId();
            earliest = earliestChangelog == null ? earliestSnapshot : earliestChangelog;
        } else {
            earliest = earliestSnapshot;
        }
        Long latest = latestSnapshotId();
        if (earliest == null || latest == null) {
            return null;
        }

        if (changelogOrSnapshot(earliest).timeMillis() >= timestampMills) {
            return earliest - 1;
        }

        return binarySearch(
                earliest,
                latest,
                id -> changelogOrSnapshot(id).timeMillis() < timestampMills,
                false);
    }

    /**
     * Returns a {@link Snapshot} whoes commit time is earlier than or equal to given timestamp
     * mills. If there is no such a snapshot, returns null.
     */
    public @Nullable Snapshot earlierOrEqualTimeMills(long timestampMills) {
        Long earliest = earliestSnapshotId();
        Long latest = latestSnapshotId();
        if (earliest == null || latest == null) {
            return null;
        }

        Snapshot earliestSnapShot = snapshot(earliest);
        if (earliestSnapShot.timeMillis() > timestampMills) {
            return earliestSnapShot;
        }

        Long resultId =
                binarySearch(
                        earliest, latest, id -> snapshot(id).timeMillis() <= timestampMills, false);

        return resultId == null ? null : snapshot(resultId);
    }

    /**
     * Returns a {@link Snapshot} whoes commit time is later than or equal to given timestamp mills.
     * If there is no such a snapshot, returns null.
     */
    public @Nullable Snapshot laterOrEqualTimeMills(long timestampMills) {
        Long earliest = earliestSnapshotId();
        Long latest = latestSnapshotId();
        if (earliest == null || latest == null) {
            return null;
        }

        Snapshot latestSnapShot = snapshot(latest);
        if (latestSnapShot.timeMillis() < timestampMills) {
            return null;
        }

        Long resultId =
                binarySearch(
                        earliest, latest, id -> snapshot(id).timeMillis() >= timestampMills, true);

        return resultId == null ? null : snapshot(resultId);
    }

    public @Nullable Snapshot earlierOrEqualWatermark(long watermark) {
        Long earliest = earliestSnapshotId();
        Long latest = latestSnapshotId();
        // If latest == Long.MIN_VALUE don't need next binary search for watermark
        // which can reduce IO cost with snapshot
        if (earliest == null || latest == null || snapshot(latest).watermark() == Long.MIN_VALUE) {
            return null;
        }
        Long firstValidId = findFirstSnapshotWithWatermark(earliest, latest);
        if (firstValidId == null) {
            return null;
        }

        Long resultId =
                binarySearch(
                        firstValidId,
                        latest,
                        id ->
                                snapshot(id).watermark() != null
                                        && snapshot(id).watermark() <= watermark,
                        false);

        return resultId == null ? null : snapshot(resultId);
    }

    public @Nullable Snapshot laterOrEqualWatermark(long watermark) {
        Long earliest = earliestSnapshotId();
        Long latest = latestSnapshotId();
        // If latest == Long.MIN_VALUE don't need next binary search for watermark
        // which can reduce IO cost with snapshot
        if (earliest == null || latest == null || snapshot(latest).watermark() == Long.MIN_VALUE) {
            return null;
        }

        Long firstValidId = findFirstSnapshotWithWatermark(earliest, latest);
        if (firstValidId == null) {
            return null;
        }

        if (snapshot(firstValidId).watermark() >= watermark) {
            return snapshot(firstValidId);
        }

        Long resultId =
                binarySearch(
                        earliest,
                        latest,
                        id ->
                                snapshot(id).watermark() != null
                                        && snapshot(id).watermark() >= watermark,
                        true);

        return resultId == null ? null : snapshot(resultId);
    }

    private Long findFirstSnapshotWithWatermark(Long earliest, Long latest) {
        while (earliest <= latest) {
            Long watermark = snapshot(earliest).watermark();
            if (watermark != null) {
                return earliest;
            }
            earliest++;
        }
        return null;
    }

    @VisibleForTesting
    public @Nullable Long binarySearch(
            Long start,
            Long end,
            java.util.function.Predicate<Long> condition,
            boolean findEarliest) {
        Long result = null;
        while (start <= end) {
            long mid = start + (end - start) / 2;
            if (condition.test(mid)) {
                result = mid;
                if (findEarliest) {
                    end = mid - 1;
                } else {
                    start = mid + 1;
                }
            } else {
                if (findEarliest) {
                    start = mid + 1;
                } else {
                    end = mid - 1;
                }
            }
        }
        return result;
    }

    public long snapshotCount() throws IOException {
        return listVersionedFiles(fileIO, snapshotDirectory(), SNAPSHOT_PREFIX).count();
    }

    public Iterator<Snapshot> snapshots() throws IOException {
        return listVersionedFiles(fileIO, snapshotDirectory(), SNAPSHOT_PREFIX)
                .map(this::snapshot)
                .sorted(Comparator.comparingLong(Snapshot::id))
                .iterator();
    }

    public List<Path> snapshotPaths(Predicate<Long> predicate) throws IOException {
        return listVersionedFiles(fileIO, snapshotDirectory(), SNAPSHOT_PREFIX)
                .filter(predicate)
                .map(this::snapshotPath)
                .collect(Collectors.toList());
    }

    public Iterator<Snapshot> snapshotsWithId(List<Long> snapshotIds) {
        return snapshotIds.stream()
                .map(this::snapshot)
                .sorted(Comparator.comparingLong(Snapshot::id))
                .iterator();
    }

    public Iterator<Snapshot> snapshotsWithinRange(
            Optional<Long> optionalMaxSnapshotId, Optional<Long> optionalMinSnapshotId) {
        Long lowerBoundSnapshotId = earliestSnapshotId();
        Long upperBoundSnapshotId = latestSnapshotId();
        Long lowerId;
        Long upperId;

        // null check on lowerBoundSnapshotId & upperBoundSnapshotId
        if (lowerBoundSnapshotId == null || upperBoundSnapshotId == null) {
            return Collections.emptyIterator();
        }

        if (optionalMaxSnapshotId.isPresent()) {
            upperId = optionalMaxSnapshotId.get();
            if (upperId < lowerBoundSnapshotId) {
                throw new RuntimeException(
                        String.format(
                                "snapshot upper id:%s should not greater than earliestSnapshotId:%s",
                                upperId, lowerBoundSnapshotId));
            }
            upperBoundSnapshotId = upperId < upperBoundSnapshotId ? upperId : upperBoundSnapshotId;
        }

        if (optionalMinSnapshotId.isPresent()) {
            lowerId = optionalMinSnapshotId.get();
            if (lowerId > upperBoundSnapshotId) {
                throw new RuntimeException(
                        String.format(
                                "snapshot upper id:%s should not greater than latestSnapshotId:%s",
                                lowerId, upperBoundSnapshotId));
            }
            lowerBoundSnapshotId = lowerId > lowerBoundSnapshotId ? lowerId : lowerBoundSnapshotId;
        }

        // +1 here to include the upperBoundSnapshotId
        return LongStream.range(lowerBoundSnapshotId, upperBoundSnapshotId + 1)
                .mapToObj(this::snapshot)
                .sorted(Comparator.comparingLong(Snapshot::id))
                .iterator();
    }

    public Iterator<Changelog> changelogs() throws IOException {
        return listVersionedFiles(fileIO, changelogDirectory(), CHANGELOG_PREFIX)
                .map(this::changelog)
                .sorted(Comparator.comparingLong(Changelog::id))
                .iterator();
    }

    /**
     * If {@link FileNotFoundException} is thrown when reading the snapshot file, this snapshot may
     * be deleted by other processes, so just skip this snapshot.
     */
    public List<Snapshot> safelyGetAllSnapshots() throws IOException {
        List<Path> paths =
                listVersionedFiles(fileIO, snapshotDirectory(), SNAPSHOT_PREFIX)
                        .map(this::snapshotPath)
                        .collect(Collectors.toList());

        List<Snapshot> snapshots = Collections.synchronizedList(new ArrayList<>(paths.size()));
        collectSnapshots(
                path -> {
                    try {
                        // do not pollution cache
                        snapshots.add(Snapshot.tryFromPath(fileIO, path));
                    } catch (FileNotFoundException ignored) {
                    }
                },
                paths);

        return snapshots;
    }

    public List<Changelog> safelyGetAllChangelogs() throws IOException {
        List<Path> paths =
                listVersionedFiles(fileIO, changelogDirectory(), CHANGELOG_PREFIX)
                        .map(this::longLivedChangelogPath)
                        .collect(Collectors.toList());

        List<Changelog> changelogs = Collections.synchronizedList(new ArrayList<>(paths.size()));
        collectSnapshots(
                path -> {
                    try {
                        changelogs.add(Changelog.fromJson(fileIO.readFileUtf8(path)));
                    } catch (IOException e) {
                        if (!(e instanceof FileNotFoundException)) {
                            throw new RuntimeException(e);
                        }
                    }
                },
                paths);

        return changelogs;
    }

    private void collectSnapshots(Consumer<Path> pathConsumer, List<Path> paths)
            throws IOException {
        ExecutorService executor =
                createCachedThreadPool(
                        Runtime.getRuntime().availableProcessors(), "SNAPSHOT_COLLECTOR");

        try {
            randomlyOnlyExecute(executor, pathConsumer, paths);
        } catch (RuntimeException e) {
            throw new IOException(e);
        } finally {
            executor.shutdown();
        }
    }

    /**
     * Try to get non snapshot files. If any error occurred, just ignore it and return an empty
     * result.
     */
    public List<Pair<Path, Long>> tryGetNonSnapshotFiles(Predicate<FileStatus> fileStatusFilter) {
        return listPathWithFilter(snapshotDirectory(), fileStatusFilter, nonSnapshotFileFilter());
    }

    public List<Pair<Path, Long>> tryGetNonChangelogFiles(Predicate<FileStatus> fileStatusFilter) {
        return listPathWithFilter(changelogDirectory(), fileStatusFilter, nonChangelogFileFilter());
    }

    private List<Pair<Path, Long>> listPathWithFilter(
            Path directory, Predicate<FileStatus> fileStatusFilter, Predicate<Path> fileFilter) {
        try {
            FileStatus[] statuses = fileIO.listStatus(directory);
            if (statuses == null) {
                return Collections.emptyList();
            }

            return Arrays.stream(statuses)
                    .filter(fileStatusFilter)
                    .filter(status -> fileFilter.test(status.getPath()))
                    .map(status -> Pair.of(status.getPath(), status.getLen()))
                    .collect(Collectors.toList());
        } catch (IOException ignored) {
            return Collections.emptyList();
        }
    }

    private Predicate<Path> nonSnapshotFileFilter() {
        return path -> {
            String name = path.getName();
            return !name.startsWith(SNAPSHOT_PREFIX)
                    && !name.equals(EARLIEST)
                    && !name.equals(LATEST);
        };
    }

    private Predicate<Path> nonChangelogFileFilter() {
        return path -> {
            String name = path.getName();
            return !name.startsWith(CHANGELOG_PREFIX)
                    && !name.equals(EARLIEST)
                    && !name.equals(LATEST);
        };
    }

    public Optional<Snapshot> latestSnapshotOfUser(String user) {
        Long latestId = latestSnapshotId();
        if (latestId == null) {
            return Optional.empty();
        }

        long earliestId =
                Preconditions.checkNotNull(
                        earliestSnapshotId(),
                        "Latest snapshot id is not null, but earliest snapshot id is null. "
                                + "This is unexpected.");
        for (long id = latestId; id >= earliestId; id--) {
            Snapshot snapshot;
            try {
                snapshot = snapshot(id);
            } catch (Exception e) {
                long newEarliestId =
                        Preconditions.checkNotNull(
                                earliestSnapshotId(),
                                "Latest snapshot id is not null, but earliest snapshot id is null. "
                                        + "This is unexpected.");

                // this is a valid snapshot, should throw exception
                if (id >= newEarliestId) {
                    throw e;
                }

                // this is an expired snapshot
                LOG.warn(
                        "Snapshot #"
                                + id
                                + " is expired. The latest snapshot of current user("
                                + user
                                + ") is not found.");
                break;
            }

            if (user.equals(snapshot.commitUser())) {
                return Optional.of(snapshot);
            }
        }
        return Optional.empty();
    }

    /** Find the snapshot of the specified identifiers written by the specified user. */
    public List<Snapshot> findSnapshotsForIdentifiers(
            @Nonnull String user, List<Long> identifiers) {
        if (identifiers.isEmpty()) {
            return Collections.emptyList();
        }
        Long latestId = latestSnapshotId();
        if (latestId == null) {
            return Collections.emptyList();
        }
        long earliestId =
                Preconditions.checkNotNull(
                        earliestSnapshotId(),
                        "Latest snapshot id is not null, but earliest snapshot id is null. "
                                + "This is unexpected.");

        long minSearchedIdentifier = identifiers.stream().min(Long::compareTo).get();
        List<Snapshot> matchedSnapshots = new ArrayList<>();
        Set<Long> remainingIdentifiers = new HashSet<>(identifiers);
        for (long id = latestId; id >= earliestId && !remainingIdentifiers.isEmpty(); id--) {
            Snapshot snapshot = snapshot(id);
            if (user.equals(snapshot.commitUser())) {
                if (remainingIdentifiers.remove(snapshot.commitIdentifier())) {
                    matchedSnapshots.add(snapshot);
                }
                if (snapshot.commitIdentifier() <= minSearchedIdentifier) {
                    break;
                }
            }
        }
        return matchedSnapshots;
    }

    public void commitChangelog(Changelog changelog, long id) throws IOException {
        fileIO.writeFile(longLivedChangelogPath(id), changelog.toJson(), true);
    }

    /**
     * Traversal snapshots from latest to earliest safely, this is applied on the writer side
     * because the committer may delete obsolete snapshots, which may cause the writer to encounter
     * unreadable snapshots.
     */
    @Nullable
    public Snapshot traversalSnapshotsFromLatestSafely(Filter<Snapshot> checker) {
        Long latestId = latestSnapshotId();
        if (latestId == null) {
            return null;
        }
        Long earliestId = earliestSnapshotId();
        if (earliestId == null) {
            return null;
        }

        for (long id = latestId; id >= earliestId; id--) {
            Snapshot snapshot;
            try {
                snapshot = snapshot(id);
            } catch (Exception e) {
                Long newEarliestId = earliestSnapshotId();
                if (newEarliestId == null) {
                    return null;
                }

                // this is a valid snapshot, should throw exception
                if (id >= newEarliestId) {
                    throw e;
                }

                // ok, this is an expired snapshot
                return null;
            }

            if (checker.test(snapshot)) {
                return snapshot;
            }
        }
        return null;
    }

    private @Nullable Long findLatest(Path dir, String prefix, Function<Long, Path> file)
            throws IOException {
        Long snapshotId = readHint(LATEST, dir);
        if (snapshotId != null && snapshotId > 0) {
            long nextSnapshot = snapshotId + 1;
            // it is the latest only there is no next one
            if (!fileIO.exists(file.apply(nextSnapshot))) {
                return snapshotId;
            }
        }
        return findByListFiles(Math::max, dir, prefix);
    }

    private @Nullable Long findEarliest(Path dir, String prefix, Function<Long, Path> file)
            throws IOException {
        Long snapshotId = readHint(EARLIEST, dir);
        // null and it is the earliest only it exists
        if (snapshotId != null && fileIO.exists(file.apply(snapshotId))) {
            return snapshotId;
        }

        return findByListFiles(Math::min, dir, prefix);
    }

    public Long readHint(String fileName) {
        return readHint(fileName, snapshotDirectory());
    }

    public Long readHint(String fileName, Path dir) {
        Path path = new Path(dir, fileName);
        int retryNumber = 0;
        while (retryNumber++ < READ_HINT_RETRY_NUM) {
            try {
                return fileIO.readOverwrittenFileUtf8(path).map(Long::parseLong).orElse(null);
            } catch (Exception ignored) {
            }
            try {
                TimeUnit.MILLISECONDS.sleep(READ_HINT_RETRY_INTERVAL);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    private Long findByListFiles(BinaryOperator<Long> reducer, Path dir, String prefix)
            throws IOException {
        return listVersionedFiles(fileIO, dir, prefix).reduce(reducer).orElse(null);
    }

    public static int findPreviousSnapshot(List<Snapshot> sortedSnapshots, long targetSnapshotId) {
        for (int i = sortedSnapshots.size() - 1; i >= 0; i--) {
            if (sortedSnapshots.get(i).id() < targetSnapshotId) {
                return i;
            }
        }
        return -1;
    }

    public static int findPreviousOrEqualSnapshot(
            List<Snapshot> sortedSnapshots, long targetSnapshotId) {
        for (int i = sortedSnapshots.size() - 1; i >= 0; i--) {
            if (sortedSnapshots.get(i).id() <= targetSnapshotId) {
                return i;
            }
        }
        return -1;
    }

    public void deleteLatestHint() throws IOException {
        Path snapshotDir = snapshotDirectory();
        Path hintFile = new Path(snapshotDir, LATEST);
        fileIO.delete(hintFile, false);
    }

    public void commitLatestHint(long snapshotId) throws IOException {
        commitHint(snapshotId, LATEST, snapshotDirectory());
    }

    public void commitLongLivedChangelogLatestHint(long snapshotId) throws IOException {
        commitHint(snapshotId, LATEST, changelogDirectory());
    }

    public void commitLongLivedChangelogEarliestHint(long snapshotId) throws IOException {
        commitHint(snapshotId, EARLIEST, changelogDirectory());
    }

    public void commitEarliestHint(long snapshotId) throws IOException {
        commitHint(snapshotId, EARLIEST, snapshotDirectory());
    }

    private void commitHint(long snapshotId, String fileName, Path dir) throws IOException {
        Path hintFile = new Path(dir, fileName);
        int loopTime = 3;
        while (loopTime-- > 0) {
            try {
                fileIO.overwriteFileUtf8(hintFile, String.valueOf(snapshotId));
                return;
            } catch (IOException e) {
                try {
                    Thread.sleep(ThreadLocalRandom.current().nextInt(1000) + 500);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    // throw root cause
                    throw new RuntimeException(e);
                }
                if (loopTime == 0) {
                    throw e;
                }
            }
        }
    }
}
