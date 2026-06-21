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
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link HintFileUtils}. */
public class HintFileUtilsTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testObjectStoreMissingLatestHintReturnsNullWithoutList() {
        NoListObjectStoreFileIO fileIO = new NoListObjectStoreFileIO();
        SnapshotManager snapshotManager = snapshotManager(fileIO);

        assertThat(snapshotManager.latestSnapshotId()).isNull();
        assertThat(fileIO.listStatusCalls).isZero();
        assertThat(fileIO.existsCalls).isZero();
    }

    @Test
    public void testObjectStoreMissingLatestHintRecoveryUsesList() throws IOException {
        ListingObjectStoreFileIO fileIO = new ListingObjectStoreFileIO();
        SnapshotManager snapshotManager = snapshotManager(fileIO);
        writeSnapshot(snapshotManager, 1);

        assertThat(
                        snapshotManager.latestSnapshotIdFromFileSystem(
                                HintFileUtils.LatestLookupMode.RECOVERY_REQUIRING_LIST))
                .isEqualTo(1);
        assertThat(fileIO.listStatusCalls).isGreaterThan(0);
    }

    @Test
    public void testObjectStoreMissingLatestHintRecoveryDeniedFailsWithListRequiredMessage() {
        NoListObjectStoreFileIO fileIO = new NoListObjectStoreFileIO();
        SnapshotManager snapshotManager = snapshotManager(fileIO);

        assertThatThrownBy(
                        () ->
                                snapshotManager.latestSnapshotIdFromFileSystem(
                                        HintFileUtils.LatestLookupMode.RECOVERY_REQUIRING_LIST))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to find latest snapshot id")
                .hasStackTraceContaining("requires ListBucket");
    }

    @Test
    public void testObjectStoreMalformedLatestHintFailsClosedWithoutList() throws IOException {
        NoListObjectStoreFileIO fileIO = new NoListObjectStoreFileIO();
        SnapshotManager snapshotManager = snapshotManager(fileIO);
        writeFile(
                new Path(snapshotManager.snapshotDirectory(), HintFileUtils.LATEST),
                "not-a-number");

        assertThatThrownBy(snapshotManager::latestSnapshotId)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Cannot safely use LATEST hint because it is malformed.");
        assertThat(fileIO.listStatusCalls).isZero();
        assertThat(fileIO.existsCalls).isZero();
    }

    @Test
    public void testObjectStoreAccessDeniedLatestHintReturnsNullWithoutList() {
        NoListObjectStoreFileIO fileIO = new NoListObjectStoreFileIO();
        fileIO.denyLatestHintRead = true;
        SnapshotManager snapshotManager = snapshotManager(fileIO);

        assertThat(snapshotManager.latestSnapshotId()).isNull();
        assertThat(fileIO.listStatusCalls).isZero();
        assertThat(fileIO.existsCalls).isZero();
    }

    @Test
    public void testObjectStoreAccessDeniedLatestHintFailsClosedWhenEarliestHintExists()
            throws IOException {
        NoListObjectStoreFileIO fileIO = new NoListObjectStoreFileIO();
        SnapshotManager snapshotManager = snapshotManager(fileIO);
        writeFile(new Path(snapshotManager.snapshotDirectory(), HintFileUtils.EARLIEST), "2");
        fileIO.denyLatestHintRead = true;

        assertThatThrownBy(snapshotManager::latestSnapshotId)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Cannot safely treat missing LATEST hint as a new table")
                .hasStackTraceContaining("Recovery requires ListBucket");
        assertThat(fileIO.listStatusCalls).isZero();
        assertThat(fileIO.existsCalls).isZero();
    }

    @Test
    public void testObjectStoreAccessDeniedLatestHintDoesNotUseDefaultOverwrittenReadExistsProbe() {
        DefaultOverwrittenReadNoListObjectStoreFileIO fileIO =
                new DefaultOverwrittenReadNoListObjectStoreFileIO();
        SnapshotManager snapshotManager = snapshotManager(fileIO);

        assertThat(snapshotManager.latestSnapshotId()).isNull();
        assertThat(fileIO.listStatusCalls).isZero();
        assertThat(fileIO.existsCalls).isZero();
    }

    @Test
    public void testObjectStoreLatestHintMissingSnapshotFailsClosedWithoutList()
            throws IOException {
        NoListObjectStoreFileIO fileIO = new NoListObjectStoreFileIO();
        SnapshotManager snapshotManager = snapshotManager(fileIO);
        writeFile(new Path(snapshotManager.snapshotDirectory(), HintFileUtils.LATEST), "2");

        assertThatThrownBy(snapshotManager::latestSnapshotId)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining(
                        "Cannot safely use LATEST hint 2 because "
                                + snapshotManager.snapshotPath(2)
                                + " is missing.");
        assertThat(fileIO.listStatusCalls).isZero();
        assertThat(fileIO.existsCalls).isZero();
    }

    @Test
    public void testObjectStoreMalformedLatestSnapshotFailsClosedWithoutList() throws IOException {
        NoListObjectStoreFileIO fileIO = new NoListObjectStoreFileIO();
        SnapshotManager snapshotManager = snapshotManager(fileIO);
        writeFile(new Path(snapshotManager.snapshotDirectory(), HintFileUtils.LATEST), "2");
        writeFile(snapshotManager.snapshotPath(2), "not-json");

        assertThatThrownBy(snapshotManager::latestSnapshotId)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("snapshot-2 is malformed.");
        assertThat(fileIO.listStatusCalls).isZero();
        assertThat(fileIO.existsCalls).isZero();
    }

    @Test
    public void testObjectStoreLatestSnapshotIdMismatchFailsClosedWithoutList() throws IOException {
        NoListObjectStoreFileIO fileIO = new NoListObjectStoreFileIO();
        SnapshotManager snapshotManager = snapshotManager(fileIO);
        writeFile(new Path(snapshotManager.snapshotDirectory(), HintFileUtils.LATEST), "2");
        writeFile(snapshotManager.snapshotPath(2), createSnapshot(3).toJson());

        assertThatThrownBy(snapshotManager::latestSnapshotId)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("contains snapshot 3.");
        assertThat(fileIO.listStatusCalls).isZero();
        assertThat(fileIO.existsCalls).isZero();
    }

    @Test
    public void testObjectStoreStaleLatestHintWithNextSnapshotFailsClosedWithoutList()
            throws IOException {
        NoListObjectStoreFileIO fileIO = new NoListObjectStoreFileIO();
        SnapshotManager snapshotManager = snapshotManager(fileIO);
        writeFile(new Path(snapshotManager.snapshotDirectory(), HintFileUtils.LATEST), "1");
        writeSnapshot(snapshotManager, 1);
        writeSnapshot(snapshotManager, 2);

        assertThatThrownBy(snapshotManager::latestSnapshotId)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Cannot safely use LATEST hint 1")
                .hasMessageContaining("snapshot-2 already exists")
                .hasStackTraceContaining("Recovery requires ListBucket");
        assertThat(fileIO.listStatusCalls).isZero();
        assertThat(fileIO.existsCalls).isZero();
    }

    @Test
    public void testObjectStoreUsesLatestHintWithoutProbingNextSnapshot() throws IOException {
        NoListObjectStoreFileIO fileIO = new NoListObjectStoreFileIO();
        SnapshotManager snapshotManager = snapshotManager(fileIO);
        writeFile(new Path(snapshotManager.snapshotDirectory(), HintFileUtils.LATEST), "1");
        writeSnapshot(snapshotManager, 1);

        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(1);
        assertThat(fileIO.listStatusCalls).isZero();
        assertThat(fileIO.existsCalls).isZero();
    }

    @Test
    public void testObjectStoreNonSnapshotPrefixUsesListFallback() throws IOException {
        ListingObjectStoreFileIO fileIO = new ListingObjectStoreFileIO();
        Path dir = new Path(tempDir.toString());
        writeFile(new Path(dir, "changelog-1"), "not-a-snapshot-json");

        assertThat(
                        HintFileUtils.findLatest(
                                fileIO, dir, "changelog-", id -> new Path(dir, "changelog-" + id)))
                .isEqualTo(1);
        assertThat(fileIO.listStatusCalls).isGreaterThan(0);
    }

    private SnapshotManager snapshotManager(LocalFileIO fileIO) {
        return new SnapshotManager(fileIO, new Path(tempDir.toString()), null, null, null);
    }

    private static void writeSnapshot(SnapshotManager snapshotManager, long id) throws IOException {
        writeFile(snapshotManager.snapshotPath(id), createSnapshot(id).toJson());
    }

    private static void writeFile(Path path, String content) throws IOException {
        LocalFileIO.create().overwriteFileUtf8(path, content);
    }

    private static Snapshot createSnapshot(long id) {
        return new Snapshot(
                id,
                1L,
                "manifest-list-base",
                10L,
                "manifest-list-delta",
                20L,
                null,
                null,
                null,
                "user",
                id,
                Snapshot.CommitKind.APPEND,
                System.currentTimeMillis(),
                100L,
                100L,
                null,
                null,
                null,
                null,
                null);
    }

    private static class NoListObjectStoreFileIO extends LocalFileIO {

        private int listStatusCalls;
        private int existsCalls;
        private boolean denyLatestHintRead;

        @Override
        public boolean isObjectStore() {
            return true;
        }

        @Override
        public boolean supportsAtomicCreateWithoutOverwrite(Path path) {
            return true;
        }

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            listStatusCalls++;
            throw new AccessDeniedException(path.toString());
        }

        @Override
        public boolean exists(Path path) throws IOException {
            existsCalls++;
            throw new AccessDeniedException(path.toString());
        }

        @Override
        public Optional<String> readOverwrittenFileUtf8(Path path) throws IOException {
            if (denyLatestHintRead && HintFileUtils.LATEST.equals(path.getName())) {
                throw new AccessDeniedException(path.toString());
            }
            try {
                return Optional.of(readFileUtf8(path));
            } catch (java.io.FileNotFoundException e) {
                return Optional.empty();
            }
        }

        @Override
        public String readFileUtf8(Path path) throws IOException {
            if (denyLatestHintRead && HintFileUtils.LATEST.equals(path.getName())) {
                throw new AccessDeniedException(path.toString());
            }
            return super.readFileUtf8(path);
        }
    }

    private static class DefaultOverwrittenReadNoListObjectStoreFileIO extends LocalFileIO {

        private int listStatusCalls;
        private int existsCalls;

        @Override
        public boolean isObjectStore() {
            return true;
        }

        @Override
        public boolean supportsAtomicCreateWithoutOverwrite(Path path) {
            return true;
        }

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            listStatusCalls++;
            throw new AccessDeniedException(path.toString());
        }

        @Override
        public boolean exists(Path path) throws IOException {
            existsCalls++;
            throw new AccessDeniedException(path.toString());
        }

        @Override
        public String readFileUtf8(Path path) throws IOException {
            if (HintFileUtils.LATEST.equals(path.getName())) {
                throw new AccessDeniedException(path.toString());
            }
            return super.readFileUtf8(path);
        }
    }

    private static class ListingObjectStoreFileIO extends LocalFileIO {

        private int listStatusCalls;

        @Override
        public boolean isObjectStore() {
            return true;
        }

        @Override
        public boolean supportsAtomicCreateWithoutOverwrite(Path path) {
            return true;
        }

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            listStatusCalls++;
            return super.listStatus(path);
        }
    }
}
