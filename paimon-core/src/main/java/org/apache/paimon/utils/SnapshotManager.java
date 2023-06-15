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
import org.apache.paimon.Snapshot.CommitKind;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.SnapshotDeletion;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.shade.guava30.com.google.common.collect.Iterables;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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

    public Snapshot snapshot(long snapshotId) {
        return Snapshot.fromPath(fileIO, snapshotPath(snapshotId));
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
        Long snapshotId = latestSnapshotId();
        return snapshotId == null ? null : snapshot(snapshotId);
    }

    public @Nullable Long latestSnapshotId() {
        try {
            return findLatest();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find latest snapshot id", e);
        }
    }

    public @Nullable Long earliestSnapshotId() {
        try {
            return findEarliest();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find earliest snapshot id", e);
        }
    }

    public @Nullable Long latestCompactedSnapshotId() {
        return pickFromLatest(s -> s.commitKind() == CommitKind.COMPACT);
    }

    public @Nullable Long pickFromLatest(Predicate<Snapshot> predicate) {
        Long latestId = latestSnapshotId();
        Long earliestId = earliestSnapshotId();
        if (latestId == null || earliestId == null) {
            return null;
        }

        if (snapshot(latestId).parentId() != null) {
            Snapshot snapshot = Iterables.getFirst(select(currentAncestors(), predicate), null);
            if (snapshot != null) {
                return snapshot.id();
            }

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

        return null;
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

        if (snapshot(latest).parentId() != null) {
            for (Snapshot snapshot : currentAncestors()) {
                if (snapshot.timeMillis() < timestampMills) {
                    return snapshot.id();
                }
            }

            return earliest - 1;
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
        Long latestId = latestSnapshotId();
        if (latestId != null && snapshot(latestId).parentId() != null) {
            return Iterables.size(currentAncestors());
        }

        return listVersionedFiles(fileIO, snapshotDirectory(), SNAPSHOT_PREFIX).count();
    }

    public Iterator<Snapshot> snapshots() throws IOException {
        Long latestId = latestSnapshotId();
        Stream<Snapshot> stream;
        if (latestId != null && snapshot(latestId).parentId() != null) {
            stream = Arrays.stream(Iterables.toArray(currentAncestors(), Snapshot.class));
        } else {
            stream =
                    listVersionedFiles(fileIO, snapshotDirectory(), SNAPSHOT_PREFIX)
                            .map(this::snapshot);
        }

        return stream.sorted(Comparator.comparingLong(Snapshot::id)).iterator();
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

        if (snapshot(latestId).parentId() != null) {
            Snapshot snapshot =
                    Iterables.getFirst(
                            select(currentAncestors(), s -> s.commitUser().equals(user)), null);
            if (snapshot != null) {
                return Optional.of(snapshot);
            }

            return Optional.empty();
        }

        for (long id = latestId; id >= earliestId; id--) {
            Snapshot snapshot = snapshot(id);
            if (user.equals(snapshot.commitUser())) {
                return Optional.of(snapshot);
            }
        }
        return Optional.empty();
    }

    /**
     * Traversal snapshots from latest to earliest safely, this is applied on the writer side
     * because the committer may delete obsolete snapshots, which may cause the writer to encounter
     * unreadable snapshots.
     */
    public void traversalSnapshotsFromLatestSafely(Function<Snapshot, Boolean> consumer) {
        Long latestId = latestSnapshotId();
        if (latestId == null) {
            return;
        }
        Long earliestId = earliestSnapshotId();
        if (earliestId == null) {
            return;
        }

        for (long id = latestId; id >= earliestId; id--) {
            Snapshot snapshot;
            try {
                snapshot = snapshot(id);
            } catch (Exception e) {
                Long newEarliestId = earliestSnapshotId();
                if (newEarliestId == null) {
                    return;
                }

                // this is a valid snapshot, should not throw exception
                if (id >= newEarliestId) {
                    throw e;
                }

                // ok, this is an expired snapshot
                return;
            }

            if (consumer.apply(snapshot)) {
                return;
            }
        }
    }

    private @Nullable Long findLatest() throws IOException {
        Path snapshotDir = snapshotDirectory();
        if (!fileIO.exists(snapshotDir)) {
            return null;
        }

        Long snapshotId = readHint(LATEST);
        if (snapshotId != null) {
            long nextSnapshot = snapshotId + 1;
            // it is the latest only there is no next one
            if (!snapshotExists(nextSnapshot)) {
                return snapshotId;
            }
        }

        return findByListFiles(Math::max);
    }

    private @Nullable Long findEarliest() throws IOException {
        Path snapshotDir = snapshotDirectory();
        if (!fileIO.exists(snapshotDir)) {
            return null;
        }

        Long snapshotId = readHint(EARLIEST);
        // null and it is the earliest only it exists
        if (snapshotId != null && snapshotExists(snapshotId)) {
            return snapshotId;
        }

        return findByListFiles(Math::min);
    }

    public Long readHint(String fileName) {
        Path snapshotDir = snapshotDirectory();
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

    private Long findByListFiles(BinaryOperator<Long> reducer) throws IOException {
        Path snapshotDir = snapshotDirectory();
        return listVersionedFiles(fileIO, snapshotDir, SNAPSHOT_PREFIX)
                .reduce(reducer)
                .orElse(null);
    }

    public void commitLatestHint(long snapshotId) throws IOException {
        commitHint(snapshotId, LATEST);
    }

    public void commitEarliestHint(long snapshotId) throws IOException {
        commitHint(snapshotId, EARLIEST);
    }

    private void commitHint(long snapshotId, String fileName) throws IOException {
        Path snapshotDir = snapshotDirectory();
        Path hintFile = new Path(snapshotDir, fileName);
        fileIO.delete(hintFile, false);
        fileIO.writeFileUtf8(hintFile, String.valueOf(snapshotId));
    }

    public void rollbackTo(SnapshotDeletion deletion, long snapshotId) throws IOException {
        if (!snapshotExists(snapshotId)) {
            throw new IllegalArgumentException("Rollback snapshot not exist: " + snapshotId);
        }

        Long latest = findLatest();
        if (latest == null) {
            return;
        }

        // first modify hint
        commitLatestHint(snapshotId);

        // delete snapshots first, cannot be read now.
        List<Snapshot> snapshots = new ArrayList<>();
        for (long i = latest; i > snapshotId; i--) {
            snapshots.add(snapshot(i));
            fileIO().deleteQuietly(snapshotPath(i));
        }

        // delete data files
        Map<BinaryRow, Set<Integer>> deletionBuckets = new HashMap<>();
        for (Snapshot snapshot : snapshots) {
            // delete data files
            deletion.deleteAddedDataFiles(snapshot.deltaManifestList(), deletionBuckets);
            deletion.deleteAddedDataFiles(snapshot.changelogManifestList(), deletionBuckets);
        }

        // delete directories
        deletion.tryDeleteDirectories(deletionBuckets);

        // delete manifest files.
        Set<String> manifestSkipped = deletion.collectManifestSkippingSet(snapshot(snapshotId));
        for (Snapshot snapshot : snapshots) {
            deletion.deleteManifestFiles(manifestSkipped, snapshot);
        }
    }

    private static <T> Iterable<T> select(Iterable<T> it, Predicate<T> predicate) {
        return () -> StreamSupport.stream(it.spliterator(), false).filter(predicate).iterator();
    }

    private Iterable<Snapshot> currentAncestors() {
        return ancestorsOf(
                latestSnapshot(),
                (id) -> {
                    if (snapshotExists(id)) {
                        return snapshot(id);
                    }

                    return null;
                });
    }

    private Iterable<Snapshot> ancestorsOf(Snapshot snapshot, Function<Long, Snapshot> lookup) {
        if (snapshot != null) {
            return () ->
                    new Iterator<Snapshot>() {
                        private Snapshot next = snapshot;
                        private boolean consumed = false; // include the snapshot in its history

                        @Override
                        public boolean hasNext() {
                            if (!consumed) {
                                return true;
                            }

                            if (next == null) {
                                return false;
                            }

                            Long parentId = next.parentId();
                            if (parentId != null && parentId == -1) {
                                return false;
                            }

                            if (parentId != null) {
                                this.next = lookup.apply(parentId);
                                if (next != null) {
                                    this.consumed = false;
                                    return true;
                                }
                            } else {
                                Long earliestId = earliestSnapshotId();
                                if (earliestId == null) {
                                    return false;
                                }

                                for (long snapshotId = next.id() - 1;
                                        snapshotId >= earliestId;
                                        snapshotId--) {
                                    if (snapshotExists(snapshotId)) {
                                        this.next = snapshot(snapshotId);
                                        this.consumed = false;
                                        return true;
                                    }
                                }
                            }

                            return false;
                        }

                        @Override
                        public Snapshot next() {
                            if (hasNext()) {
                                this.consumed = true;
                                return next;
                            }

                            throw new NoSuchElementException();
                        }
                    };
        } else {
            return ImmutableList.of();
        }
    }
}
