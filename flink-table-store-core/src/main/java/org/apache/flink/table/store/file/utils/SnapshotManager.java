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

package org.apache.flink.table.store.file.utils;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BinaryOperator;

import static org.apache.flink.table.store.file.utils.FileUtils.listVersionedFiles;

/** Manager for {@link Snapshot}, providing utility methods related to paths and snapshot hints. */
public class SnapshotManager {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotManager.class);

    private static final String SNAPSHOT_PREFIX = "snapshot-";
    public static final String EARLIEST = "EARLIEST";
    public static final String LATEST = "LATEST";

    private final Path tablePath;

    public SnapshotManager(Path tablePath) {
        this.tablePath = tablePath;
    }

    public Path snapshotDirectory() {
        return new Path(tablePath + "/snapshot");
    }

    public Path snapshotPath(long snapshotId) {
        return new Path(tablePath + "/snapshot/" + SNAPSHOT_PREFIX + snapshotId);
    }

    public Snapshot snapshot(long snapshotId) {
        return Snapshot.fromPath(snapshotPath(snapshotId));
    }

    public boolean snapshotExists(long snapshotId) {
        Path path = snapshotPath(snapshotId);
        try {
            return path.getFileSystem().exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to determine if snapshot #" + snapshotId + " exists in path " + path,
                    e);
        }
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

    public long snapshotCount() throws IOException {
        return listVersionedFiles(snapshotDirectory(), SNAPSHOT_PREFIX).count();
    }

    public Iterator<Snapshot> snapshots() throws IOException {
        return listVersionedFiles(snapshotDirectory(), SNAPSHOT_PREFIX)
                .map(this::snapshot)
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
        FileSystem fs = snapshotDir.getFileSystem();
        if (!fs.exists(snapshotDir)) {
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
        FileSystem fs = snapshotDir.getFileSystem();
        if (!fs.exists(snapshotDir)) {
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
        try {
            if (path.getFileSystem().exists(path)) {
                return Long.parseLong(FileUtils.readFileUtf8(path));
            }
        } catch (Exception e) {
            LOG.info(
                    "Failed to read hint file " + fileName + ". Falling back to listing files.", e);
        }
        return null;
    }

    private Long findByListFiles(BinaryOperator<Long> reducer) throws IOException {
        Path snapshotDir = snapshotDirectory();
        return listVersionedFiles(snapshotDir, SNAPSHOT_PREFIX).reduce(reducer).orElse(null);
    }

    public void commitLatestHint(long snapshotId) throws IOException {
        commitHint(snapshotId, LATEST);
    }

    public void commitEarliestHint(long snapshotId) throws IOException {
        commitHint(snapshotId, EARLIEST);
    }

    private void commitHint(long snapshotId, String fileName) throws IOException {
        Path snapshotDir = snapshotDirectory();
        FileSystem fs = snapshotDir.getFileSystem();
        Path hintFile = new Path(snapshotDir, fileName);
        Path tempFile = new Path(snapshotDir, UUID.randomUUID() + "-" + fileName + ".temp");
        FileUtils.writeFileUtf8(tempFile, String.valueOf(snapshotId));
        fs.delete(hintFile, false);
        boolean success = fs.rename(tempFile, hintFile);
        if (!success) {
            fs.delete(tempFile, false);
        }
    }
}
