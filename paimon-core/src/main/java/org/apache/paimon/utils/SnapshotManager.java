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
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;

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
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.BranchManager.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.utils.BranchManager.getBranchPath;
import static org.apache.paimon.utils.FileUtils.listVersionedFiles;

/** Manager for {@link Snapshot}, providing utility methods related to paths and snapshot hints. */
public class SnapshotManager implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String SNAPSHOT_PREFIX = "snapshot-";
    public static final String EARLIEST = "EARLIEST";
    public static final String LATEST = "LATEST";
    private static final int READ_HINT_RETRY_NUM = 3;
    private static final int READ_HINT_RETRY_INTERVAL = 1;

    private final FileIO fileIO;
    private final Path tablePath;

    public SnapshotManager(FileIO fileIO, Path tablePath) {
        this.fileIO = fileIO;
        this.tablePath = tablePath;
    }

    public FileIO fileIO() {
        return fileIO;
    }

    public Path tablePath() {
        return tablePath;
    }

    public Path snapshotDirectory() {
        return new Path(tablePath + "/snapshot");
    }

    public Path snapshotPath(long snapshotId) {
        return new Path(tablePath + "/snapshot/" + SNAPSHOT_PREFIX + snapshotId);
    }

    public Path branchSnapshotDirectory(String branchName) {
        return new Path(getBranchPath(tablePath, branchName) + "/snapshot");
    }

    public Path branchSnapshotPath(String branchName, long snapshotId) {
        return new Path(
                getBranchPath(tablePath, branchName) + "/snapshot/" + SNAPSHOT_PREFIX + snapshotId);
    }

    public Path snapshotPathByBranch(String branchName, long snapshotId) {
        return branchName.equals(DEFAULT_MAIN_BRANCH)
                ? snapshotPath(snapshotId)
                : branchSnapshotPath(branchName, snapshotId);
    }

    public Path snapshotDirByBranch(String branchName) {
        return branchName.equals(DEFAULT_MAIN_BRANCH)
                ? snapshotDirectory()
                : branchSnapshotDirectory(branchName);
    }

    public Snapshot snapshot(long snapshotId) {
        return snapshot(DEFAULT_MAIN_BRANCH, snapshotId);
    }

    public Snapshot snapshot(String branchName, long snapshotId) {
        Path snapshotPath = snapshotPathByBranch(branchName, snapshotId);
        return Snapshot.fromPath(fileIO, snapshotPath);
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

    public @Nullable Snapshot latestSnapshot() {
        return latestSnapshot(DEFAULT_MAIN_BRANCH);
    }

    public @Nullable Snapshot latestSnapshot(String branchName) {
        Long snapshotId = latestSnapshotId(branchName);
        return snapshotId == null ? null : snapshot(branchName, snapshotId);
    }

    public @Nullable Long latestSnapshotId() {
        return latestSnapshotId(DEFAULT_MAIN_BRANCH);
    }

    public @Nullable Long latestSnapshotId(String branchName) {
        try {
            return findLatest(branchName);
        } catch (IOException e) {
            throw new RuntimeException("Failed to find latest snapshot id", e);
        }
    }

    public @Nullable Snapshot earliestSnapshot() {
        Long snapshotId = earliestSnapshotId();
        return snapshotId == null ? null : snapshot(snapshotId);
    }

    public @Nullable Long earliestSnapshotId() {
        return earliestSnapshotId(DEFAULT_MAIN_BRANCH);
    }

    public @Nullable Long earliestSnapshotId(String branchName) {
        try {
            return findEarliest(branchName);
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
     * Returns a snapshot earlier than the timestamp mills. A non-existent snapshot may be returned
     * if all snapshots are later than the timestamp mills.
     */
    public @Nullable Long earlierThanTimeMills(long timestampMills) {
        Long earliest = earliestSnapshotId();
        Long latest = latestSnapshotId();
        if (earliest == null || latest == null) {
            return null;
        }

        for (long i = latest; i >= earliest; i--) {
            long commitTime = snapshot(i).timeMillis();
            if (commitTime < timestampMills) {
                return i;
            }
        }
        return earliest - 1;
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

        if (snapshot(earliest).timeMillis() > timestampMills) {
            return null;
        }
        Snapshot finnalSnapshot = null;
        while (earliest <= latest) {
            long mid = earliest + (latest - earliest) / 2; // Avoid overflow
            Snapshot snapshot = snapshot(mid);
            long commitTime = snapshot.timeMillis();
            if (commitTime > timestampMills) {
                latest = mid - 1; // Search in the left half
            } else if (commitTime < timestampMills) {
                earliest = mid + 1; // Search in the right half
                finnalSnapshot = snapshot;
            } else {
                finnalSnapshot = snapshot; // Found the exact match
                break;
            }
        }
        return finnalSnapshot;
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

    /**
     * If {@link FileNotFoundException} is thrown when reading the snapshot file, this snapshot may
     * be deleted by other processes, so just skip this snapshot.
     */
    public List<Snapshot> safelyGetAllSnapshots() throws IOException {
        List<Path> paths =
                listVersionedFiles(fileIO, snapshotDirectory(), SNAPSHOT_PREFIX)
                        .map(this::snapshotPath)
                        .collect(Collectors.toList());

        List<Snapshot> snapshots = new ArrayList<>();
        for (Path path : paths) {
            Snapshot.safelyFromPath(fileIO, path).ifPresent(snapshots::add);
        }

        return snapshots;
    }

    /**
     * Try to get non snapshot files. If any error occurred, just ignore it and return an empty
     * result.
     */
    public List<Path> tryGetNonSnapshotFiles(Predicate<FileStatus> fileStatusFilter) {
        try {
            FileStatus[] statuses = fileIO.listStatus(snapshotDirectory());
            if (statuses == null) {
                return Collections.emptyList();
            }

            return Arrays.stream(statuses)
                    .filter(fileStatusFilter)
                    .map(FileStatus::getPath)
                    .filter(nonSnapshotFileFilter())
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
            Snapshot snapshot = snapshot(id);
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

                // this is a valid snapshot, should not throw exception
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

    private @Nullable Long findLatest(String branchName) throws IOException {
        Path snapshotDir = snapshotDirByBranch(branchName);
        if (!fileIO.exists(snapshotDir)) {
            return null;
        }

        Long snapshotId = readHint(LATEST, branchName);
        if (snapshotId != null) {
            long nextSnapshot = snapshotId + 1;
            // it is the latest only there is no next one
            if (!snapshotExists(nextSnapshot)) {
                return snapshotId;
            }
        }

        return findByListFiles(Math::max, branchName);
    }

    private @Nullable Long findEarliest(String branchName) throws IOException {
        Path snapshotDir = snapshotDirByBranch(branchName);
        if (!fileIO.exists(snapshotDir)) {
            return null;
        }

        Long snapshotId = readHint(EARLIEST, branchName);
        // null and it is the earliest only it exists
        if (snapshotId != null && snapshotExists(snapshotId)) {
            return snapshotId;
        }

        return findByListFiles(Math::min, branchName);
    }

    public Long readHint(String fileName) {
        return readHint(fileName, DEFAULT_MAIN_BRANCH);
    }

    public Long readHint(String fileName, String branchName) {
        Path snapshotDir = snapshotDirByBranch(branchName);
        Path path = new Path(snapshotDir, fileName);
        int retryNumber = 0;
        while (retryNumber++ < READ_HINT_RETRY_NUM) {
            try {
                return Long.parseLong(fileIO.readFileUtf8(path));
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

    private Long findByListFiles(BinaryOperator<Long> reducer, String branchName)
            throws IOException {
        Path snapshotDir = snapshotDirByBranch(branchName);
        return listVersionedFiles(fileIO, snapshotDir, SNAPSHOT_PREFIX)
                .reduce(reducer)
                .orElse(null);
    }

    public void commitLatestHint(long snapshotId) throws IOException {
        commitLatestHint(snapshotId, DEFAULT_MAIN_BRANCH);
    }

    public void commitLatestHint(long snapshotId, String branchName) throws IOException {
        commitHint(snapshotId, LATEST, branchName);
    }

    public void commitEarliestHint(long snapshotId) throws IOException {
        commitEarliestHint(snapshotId, DEFAULT_MAIN_BRANCH);
    }

    public void commitEarliestHint(long snapshotId, String branchName) throws IOException {
        commitHint(snapshotId, EARLIEST, branchName);
    }

    private void commitHint(long snapshotId, String fileName, String branchName)
            throws IOException {
        Path snapshotDir = snapshotDirByBranch(branchName);
        Path hintFile = new Path(snapshotDir, fileName);
        fileIO.delete(hintFile, false);
        fileIO.writeFileUtf8(hintFile, String.valueOf(snapshotId));
    }
}
