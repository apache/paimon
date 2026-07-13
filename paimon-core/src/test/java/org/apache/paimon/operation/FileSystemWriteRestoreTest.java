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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link FileSystemWriteRestore}. */
class FileSystemWriteRestoreTest {

    @Test
    void testRestoreFromPinnedSnapshot() {
        Snapshot pinned = mock(Snapshot.class);
        Snapshot latest = mock(Snapshot.class);
        SnapshotManager snapshotManager = mock(SnapshotManager.class);
        when(snapshotManager.snapshot(5L)).thenReturn(pinned);
        when(snapshotManager.latestSnapshotFromFileSystem()).thenReturn(latest);

        FileStoreScan scan = mock(FileStoreScan.class);
        FileStoreScan.Plan plan = mock(FileStoreScan.Plan.class);
        when(scan.withSnapshot(pinned)).thenReturn(scan);
        when(scan.withPartitionBucket(EMPTY_ROW, 0)).thenReturn(scan);
        when(scan.plan()).thenReturn(plan);
        when(plan.files()).thenReturn(Collections.emptyList());

        IndexFileMeta ann = new IndexFileMeta("test-vector-ann", "ann", 1, 1, null, null, null);
        IndexFileHandler indexFileHandler = mock(IndexFileHandler.class);
        when(indexFileHandler.scanSourceIndexes(pinned, EMPTY_ROW, 0))
                .thenReturn(Collections.singletonList(ann));

        FileSystemWriteRestore restore =
                new FileSystemWriteRestore(
                        new CoreOptions(new HashMap<>()),
                        snapshotManager,
                        scan,
                        indexFileHandler,
                        5L);

        RestoreFiles restored = restore.restoreFiles(EMPTY_ROW, 0, false, false, true);

        assertThat(restored.snapshot()).isSameAs(pinned);
        assertThat(restored.vectorIndexPayloads()).containsExactly(ann);
        verify(scan).withSnapshot(pinned);
        verify(indexFileHandler).scanSourceIndexes(pinned, EMPTY_ROW, 0);
        verify(snapshotManager, never()).latestSnapshotFromFileSystem();
    }

    @Test
    void testRestoreVectorIndexPayloadsWithoutDirectory() {
        Snapshot snapshot = mock(Snapshot.class);
        SnapshotManager snapshotManager = mock(SnapshotManager.class);
        when(snapshotManager.latestSnapshotFromFileSystem()).thenReturn(snapshot);

        FileStoreScan scan = mock(FileStoreScan.class);
        FileStoreScan.Plan plan = mock(FileStoreScan.Plan.class);
        when(scan.withSnapshot(snapshot)).thenReturn(scan);
        when(scan.withPartitionBucket(EMPTY_ROW, 0)).thenReturn(scan);
        when(scan.plan()).thenReturn(plan);
        when(plan.files()).thenReturn(Collections.emptyList());

        IndexFileMeta ann = new IndexFileMeta("test-vector-ann", "ann", 1, 1, null, null, null);
        IndexFileHandler indexFileHandler = mock(IndexFileHandler.class);
        when(indexFileHandler.scanSourceIndexes(snapshot, EMPTY_ROW, 0))
                .thenReturn(Collections.singletonList(ann));

        FileSystemWriteRestore restore =
                new FileSystemWriteRestore(
                        new CoreOptions(new HashMap<>()), snapshotManager, scan, indexFileHandler);

        RestoreFiles restored = restore.restoreFiles(EMPTY_ROW, 0, false, false, true);

        assertThat(restored.vectorIndexPayloads()).containsExactly(ann);
    }
}
