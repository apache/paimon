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

import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.Instant;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.apache.paimon.utils.BranchManager.branchPath;
import static org.apache.paimon.utils.FileUtils.listVersionedFiles;
import static org.apache.paimon.utils.ThreadPoolUtils.createCachedThreadPool;
import static org.apache.paimon.utils.ThreadPoolUtils.randomlyOnlyExecute;

/** Manager for {@link Snapshot}, providing utility methods related to paths and snapshot hints. */
public class SnapshotManager implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotManager.class);

    public static final String SNAPSHOT_PREFIX = "snapshot-";

    public static final int EARLIEST_SNAPSHOT_DEFAULT_RETRY_NUM = 3;

    private final FileIO fileIO;
    private final Path tablePath;
    private final String branch;
    @Nullable private final SnapshotLoader snapshotLoader;
    @Nullable private final Cache<Path, Snapshot> cache;

    public SnapshotManager(
            FileIO fileIO,
            Path tablePath,
            @Nullable String branchName,
            @Nullable SnapshotLoader snapshotLoader,
            @Nullable Cache<Path, Snapshot> cache) {
        this.fileIO = fileIO;
        this.tablePath = tablePath;
        this.branch = BranchManager.normalizeBranch(branchName);
        this.snapshotLoader = snapshotLoader;
        this.cache = cache;
    }

    public SnapshotManager copyWithBranch(String branchName) {
        SnapshotLoader newSnapshotLoader = null;
        if (snapshotLoader != null) {
            newSnapshotLoader = snapshotLoader.copyWithBranch(branchName);
        }
        return new SnapshotManager(fileIO, tablePath, branchName, newSnapshotLoader, cache);
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
            snapshot = fromPath(fileIO, path);
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
            snapshot = tryFromPath(fileIO, path);
            if (cache != null) {
                cache.put(path, snapshot);
            }
        }
        return snapshot;
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

    public void deleteSnapshot(long snapshotId) {
        Path path = snapshotPath(snapshotId);
        if (cache != null) {
            cache.invalidate(path);
        }
        fileIO().deleteQuietly(path);
    }

    public @Nullable Snapshot latestSnapshot() {
        Snapshot snapshot;
        if (snapshotLoader != null) {
            try {
                snapshot = snapshotLoader.load().orElse(null);
            } catch (UnsupportedOperationException ignored) {
                snapshot = latestSnapshotFromFileSystem();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            snapshot = latestSnapshotFromFileSystem();
        }
        if (snapshot != null && cache != null) {
            cache.put(snapshotPath(snapshot.id()), snapshot);
        }
        return snapshot;
    }

    public @Nullable Snapshot latestSnapshotFromFileSystem() {
        Long snapshotId = latestSnapshotIdFromFileSystem();
        return snapshotId == null ? null : snapshot(snapshotId);
    }

    public @Nullable Long latestSnapshotId() {
        try {
            if (snapshotLoader != null) {
                try {
                    return snapshotLoader.load().map(Snapshot::id).orElse(null);
                } catch (UnsupportedOperationException ignored) {
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to find latest snapshot id", e);
        }
        return latestSnapshotIdFromFileSystem();
    }

    public @Nullable Long latestSnapshotIdFromFileSystem() {
        try {
            return findLatest(snapshotDirectory(), SNAPSHOT_PREFIX, this::snapshotPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to find latest snapshot id", e);
        }
    }

    public @Nullable Snapshot earliestSnapshot() {
        return earliestSnapshot(null);
    }

    public void rollback(Instant instant) {
        if (snapshotLoader != null) {
            try {
                snapshotLoader.rollback(instant);
                return;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        throw new UnsupportedOperationException("rollback is not supported");
    }

    private @Nullable Snapshot earliestSnapshot(@Nullable Long stopSnapshotId) {
        Long snapshotId = earliestSnapshotId();
        if (snapshotId == null) {
            return null;
        }

        if (stopSnapshotId == null) {
            stopSnapshotId = snapshotId + EARLIEST_SNAPSHOT_DEFAULT_RETRY_NUM;
        }

        do {
            try {
                return tryGetSnapshot(snapshotId);
            } catch (FileNotFoundException e) {
                snapshotId++;
                if (snapshotId > stopSnapshotId) {
                    return null;
                }
                LOG.warn(
                        "The earliest snapshot or changelog was once identified but disappeared. "
                                + "It might have been expired by other jobs operating on this table. "
                                + "Searching for the second earliest snapshot or changelog instead. ");
            }
        } while (true);
    }

    public boolean earliestFileNotExists() {
        return HintFileUtils.readHint(fileIO, HintFileUtils.EARLIEST, snapshotDirectory()) == null;
    }

    public @Nullable Long earliestSnapshotId() {
        try {
            return findEarliest(snapshotDirectory(), SNAPSHOT_PREFIX, this::snapshotPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to find earliest snapshot id", e);
        }
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

    /**
     * Returns a {@link Snapshot} whose commit time is earlier than or equal to given timestamp
     * mills. If there is no such a snapshot, returns null.
     */
    public @Nullable Snapshot earlierOrEqualTimeMills(long timestampMills) {
        Long latest = latestSnapshotId();
        if (latest == null) {
            return null;
        }

        Snapshot earliestSnapShot = earliestSnapshot(latest);
        if (earliestSnapShot == null || earliestSnapShot.timeMillis() > timestampMills) {
            return null;
        }
        long earliest = earliestSnapShot.id();

        Snapshot finalSnapshot = null;
        while (earliest <= latest) {
            long mid = earliest + (latest - earliest) / 2; // Avoid overflow
            Snapshot snapshot = snapshot(mid);
            long commitTime = snapshot.timeMillis();
            if (commitTime > timestampMills) {
                latest = mid - 1; // Search in the left half
            } else if (commitTime < timestampMills) {
                earliest = mid + 1; // Search in the right half
                finalSnapshot = snapshot;
            } else {
                finalSnapshot = snapshot; // Found the exact match
                break;
            }
        }
        return finalSnapshot;
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
        Snapshot finalSnapshot = null;
        while (earliest <= latest) {
            long mid = earliest + (latest - earliest) / 2; // Avoid overflow
            Snapshot snapshot = snapshot(mid);
            long commitTime = snapshot.timeMillis();
            if (commitTime > timestampMills) {
                latest = mid - 1; // Search in the left half
                finalSnapshot = snapshot;
            } else if (commitTime < timestampMills) {
                earliest = mid + 1; // Search in the right half
            } else {
                finalSnapshot = snapshot; // Found the exact match
                break;
            }
        }
        return finalSnapshot;
    }

    public @Nullable Snapshot earlierOrEqualWatermark(long watermark) {
        Long latest = latestSnapshotId();
        // If latest == Long.MIN_VALUE don't need next binary search for watermark
        // which can reduce IO cost with snapshot
        if (latest == null || snapshot(latest).watermark() == Long.MIN_VALUE) {
            return null;
        }

        Snapshot earliestSnapShot = earliestSnapshot(latest);
        if (earliestSnapShot == null) {
            return null;
        }
        long earliest = earliestSnapShot.id();

        Long earliestWatermark = null;
        // find the first snapshot with watermark
        if ((earliestWatermark = earliestSnapShot.watermark()) == null) {
            while (earliest < latest) {
                earliest++;
                earliestWatermark = snapshot(earliest).watermark();
                if (earliestWatermark != null) {
                    break;
                }
            }
        }
        if (earliestWatermark == null) {
            return null;
        }

        if (earliestWatermark >= watermark) {
            return snapshot(earliest);
        }
        Snapshot finalSnapshot = null;

        while (earliest <= latest) {
            long mid = earliest + (latest - earliest) / 2; // Avoid overflow
            Snapshot snapshot = snapshot(mid);
            Long commitWatermark = snapshot.watermark();
            if (commitWatermark == null) {
                // find the first snapshot with watermark
                while (mid >= earliest) {
                    mid--;
                    commitWatermark = snapshot(mid).watermark();
                    if (commitWatermark != null) {
                        break;
                    }
                }
            }
            if (commitWatermark == null) {
                earliest = mid + 1;
            } else {
                if (commitWatermark > watermark) {
                    latest = mid - 1; // Search in the left half
                } else if (commitWatermark < watermark) {
                    earliest = mid + 1; // Search in the right half
                    finalSnapshot = snapshot;
                } else {
                    finalSnapshot = snapshot; // Found the exact match
                    break;
                }
            }
        }
        return finalSnapshot;
    }

    public @Nullable Snapshot laterOrEqualWatermark(long watermark) {
        Long latest = latestSnapshotId();
        // If latest == Long.MIN_VALUE don't need next binary search for watermark
        // which can reduce IO cost with snapshot
        if (latest == null || snapshot(latest).watermark() == Long.MIN_VALUE) {
            return null;
        }

        Snapshot earliestSnapShot = earliestSnapshot(latest);
        if (earliestSnapShot == null) {
            return null;
        }
        long earliest = earliestSnapShot.id();

        Long earliestWatermark = null;
        // find the first snapshot with watermark
        if ((earliestWatermark = earliestSnapShot.watermark()) == null) {
            while (earliest < latest) {
                earliest++;
                earliestWatermark = snapshot(earliest).watermark();
                if (earliestWatermark != null) {
                    break;
                }
            }
        }
        if (earliestWatermark == null) {
            return null;
        }

        if (earliestWatermark >= watermark) {
            return snapshot(earliest);
        }
        Snapshot finalSnapshot = null;

        while (earliest <= latest) {
            long mid = earliest + (latest - earliest) / 2; // Avoid overflow
            Snapshot snapshot = snapshot(mid);
            Long commitWatermark = snapshot.watermark();
            if (commitWatermark == null) {
                // find the first snapshot with watermark
                while (mid >= earliest) {
                    mid--;
                    commitWatermark = snapshot(mid).watermark();
                    if (commitWatermark != null) {
                        break;
                    }
                }
            }
            if (commitWatermark == null) {
                earliest = mid + 1;
            } else {
                if (commitWatermark > watermark) {
                    latest = mid - 1; // Search in the left half
                    finalSnapshot = snapshot;
                } else if (commitWatermark < watermark) {
                    earliest = mid + 1; // Search in the right half
                } else {
                    finalSnapshot = snapshot; // Found the exact match
                    break;
                }
            }
        }
        return finalSnapshot;
    }

    public long snapshotCount() throws IOException {
        return snapshotIdStream().count();
    }

    public Iterator<Snapshot> snapshots() throws IOException {
        return snapshotIdStream()
                .map(this::snapshot)
                .sorted(Comparator.comparingLong(Snapshot::id))
                .iterator();
    }

    public List<Path> snapshotPaths(Predicate<Long> predicate) throws IOException {
        return snapshotIdStream()
                .filter(predicate)
                .map(this::snapshotPath)
                .collect(Collectors.toList());
    }

    public Stream<Long> snapshotIdStream() throws IOException {
        return listVersionedFiles(fileIO, snapshotDirectory(), SNAPSHOT_PREFIX);
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

    /**
     * If {@link FileNotFoundException} is thrown when reading the snapshot file, this snapshot may
     * be deleted by other processes, so just skip this snapshot.
     */
    public List<Snapshot> safelyGetAllSnapshots() throws IOException {
        List<Path> paths = snapshotIdStream().map(this::snapshotPath).collect(Collectors.toList());

        List<Snapshot> snapshots = Collections.synchronizedList(new ArrayList<>(paths.size()));
        collectSnapshots(
                path -> {
                    try {
                        // do not pollution cache
                        snapshots.add(tryFromPath(fileIO, path));
                    } catch (FileNotFoundException ignored) {
                    }
                },
                paths);

        return snapshots;
    }

    private static void collectSnapshots(Consumer<Path> pathConsumer, List<Path> paths)
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

    public Optional<Snapshot> latestSnapshotOfUser(String user) {
        return latestSnapshotOfUser(user, latestSnapshotId());
    }

    public Optional<Snapshot> latestSnapshotOfUserFromFilesystem(String user) {
        return latestSnapshotOfUser(user, latestSnapshotIdFromFileSystem());
    }

    private Optional<Snapshot> latestSnapshotOfUser(String user, Long latestId) {
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
        return HintFileUtils.findLatest(fileIO, dir, prefix, file);
    }

    private @Nullable Long findEarliest(Path dir, String prefix, Function<Long, Path> file)
            throws IOException {
        return HintFileUtils.findEarliest(fileIO, dir, prefix, file);
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
        HintFileUtils.deleteLatestHint(fileIO, snapshotDirectory());
    }

    public void commitLatestHint(long snapshotId) throws IOException {
        HintFileUtils.commitLatestHint(fileIO, snapshotId, snapshotDirectory());
    }

    public void commitEarliestHint(long snapshotId) throws IOException {
        HintFileUtils.commitEarliestHint(fileIO, snapshotId, snapshotDirectory());
    }

    public static Snapshot fromPath(FileIO fileIO, Path path) {
        try {
            return tryFromPath(fileIO, path);
        } catch (FileNotFoundException e) {
            String errorMessage =
                    String.format(
                            "Snapshot file %s does not exist. "
                                    + "It might have been expired by other jobs operating on this table. "
                                    + "In this case, you can avoid concurrent modification issues by configuring "
                                    + "write-only = true and use a dedicated compaction job, or configuring "
                                    + "different expiration thresholds for different jobs.",
                            path);
            throw new RuntimeException(errorMessage, e);
        }
    }

    public static Snapshot tryFromPath(FileIO fileIO, Path path) throws FileNotFoundException {
        int retryNumber = 0;
        Exception exception = null;
        while (retryNumber++ < 10) {
            String content;
            try {
                content = fileIO.readFileUtf8(path);
            } catch (FileNotFoundException e) {
                throw e;
            } catch (IOException e) {
                throw new RuntimeException("Fails to read snapshot from path " + path, e);
            }

            try {
                return Snapshot.fromJson(content);
            } catch (Exception e) {
                // retry
                exception = e;
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ie);
                }
            }
        }
        throw new RuntimeException("Retry fail after 10 times", exception);
    }
}
