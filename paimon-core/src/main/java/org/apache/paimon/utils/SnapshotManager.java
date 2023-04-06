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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.function.Predicate;

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
        return pickSnapshot(s -> s.commitKind() == CommitKind.COMPACT);
    }

    public @Nullable Long pickSnapshot(Predicate<Snapshot> predicate) {
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

        for (long i = latest; i >= earliest; i--) {
            Snapshot snapshot = snapshot(i);
            long commitTime = snapshot.timeMillis();
            if (commitTime <= timestampMills) {
                return snapshot;
            }
        }
        return null;
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
            } catch (InterruptedException ignored) {
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
}
