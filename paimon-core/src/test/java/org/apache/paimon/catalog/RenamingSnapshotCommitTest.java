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

package org.apache.paimon.catalog;

import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.utils.HintFileUtils;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link RenamingSnapshotCommit} to verify robustness when object storage rename returns
 * false while the target file is actually created, and to verify failure when target is missing.
 */
public class RenamingSnapshotCommitTest {

    /**
     * Simulate the object storage scenario where rename actually succeeds but returns false, which
     * causes {@link FileIO#tryToWriteAtomic(Path, String)} to return false while the target file
     * exists with correct content.
     */
    @Test
    public void testCommitSucceedsAndRenameReturnsFalse(@TempDir java.nio.file.Path tmp)
            throws Exception {
        FileIO fileIO = new AlwaysFalseRenameLocalFileIO(true);
        Path tablePath = new Path(tmp.toUri());
        SnapshotManager snapshotManager = new SnapshotManager(fileIO, tablePath, null, null, null);

        RenamingSnapshotCommit commit = new RenamingSnapshotCommit(snapshotManager, Lock.empty());

        Snapshot snapshot = createSnapshot(1L);

        boolean committed = commit.commit(snapshot, "main", Collections.emptyList());

        assertThat(committed).isTrue();

        // snapshot file should exist and match content
        Path snapshotPath = snapshotManager.snapshotPath(snapshot.id());
        assertThat(fileIO.exists(snapshotPath)).isTrue();
        Snapshot loaded = Snapshot.fromJson(fileIO.readFileUtf8(snapshotPath));
        assertThat(loaded).isEqualTo(snapshot);

        // LATEST hint should be committed
        Path latestHint = new Path(snapshotManager.snapshotDirectory(), HintFileUtils.LATEST);
        assertThat(fileIO.readOverwrittenFileUtf8(latestHint).orElse(null))
                .isEqualTo(String.valueOf(snapshot.id()));
    }

    /**
     * Simulate the scenario where rename fails and the target snapshot file is not created; the
     * commit should throw IOException according to the new check.
     */
    @Test
    public void testCommitTargetSnapshotMissing(@TempDir java.nio.file.Path tmp) throws Exception {
        FileIO fileIO = new AlwaysFalseRenameLocalFileIO(false);
        Path tablePath = new Path(tmp.toUri());
        SnapshotManager snapshotManager = new SnapshotManager(fileIO, tablePath, null, null, null);

        RenamingSnapshotCommit commit = new RenamingSnapshotCommit(snapshotManager, Lock.empty());

        Snapshot snapshot = createSnapshot(2L);

        IOException ex =
                assertThrows(
                        IOException.class,
                        () -> commit.commit(snapshot, "main", Collections.emptyList()));

        assertThat(ex)
                .hasMessageContaining("Commit snapshot " + snapshot.id() + " failed")
                .hasMessageContaining("not found");

        // ensure snapshot file is not accidentally created
        assertThat(fileIO.exists(snapshotManager.snapshotPath(snapshot.id()))).isFalse();
    }

    private static Snapshot createSnapshot(long id) throws IOException {
        long schemaId = 1L;
        String baseManifestList = "manifest-list-base";
        String deltaManifestList = "manifest-list-delta";
        String changelogManifestList = null;
        Long baseManifestListSize = 10L;
        Long deltaManifestListSize = 20L;
        Long changelogManifestListSize = null;
        String indexManifest = null;
        String commitUser = "user";
        long commitIdentifier = id;
        Snapshot.CommitKind commitKind = Snapshot.CommitKind.APPEND;
        long timeMillis = System.currentTimeMillis();
        Map<Integer, Long> logOffsets = new HashMap<>();
        Long totalRecordCount = 100L;
        Long deltaRecordCount = 100L;

        return new Snapshot(
                id,
                schemaId,
                baseManifestList,
                baseManifestListSize,
                deltaManifestList,
                deltaManifestListSize,
                changelogManifestList,
                changelogManifestListSize,
                indexManifest,
                commitUser,
                commitIdentifier,
                commitKind,
                timeMillis,
                logOffsets,
                totalRecordCount,
                deltaRecordCount,
                null,
                null,
                null,
                null,
                null);
    }

    /**
     * A FileIO based on LocalFileIO that always returns false from rename().
     *
     * <p>When {@code actuallyMove} is true, it performs the move then returns false; otherwise it
     * returns false without moving, simulating a failed rename.
     */
    private static class AlwaysFalseRenameLocalFileIO extends LocalFileIO {

        private final boolean actuallyMove;

        AlwaysFalseRenameLocalFileIO(boolean actuallyMove) {
            this.actuallyMove = actuallyMove;
        }

        @Override
        public boolean rename(Path src, Path dst) throws IOException {
            if (actuallyMove) {
                // perform actual move to create target, but report failure
                boolean ignored = super.rename(src, dst);
                return false;
            } else {
                // do not move, report failure
                // ensure parent directory exists to avoid side effects in tests
                java.nio.file.Path parent = java.nio.file.Paths.get(dst.toUri());
                Files.createDirectories(parent.getParent());
                return false;
            }
        }
    }
}
