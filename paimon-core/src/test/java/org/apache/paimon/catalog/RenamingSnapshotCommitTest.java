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
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.utils.SnapshotManagerTest.createSnapshotWithMillis;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RenamingSnapshotCommit}. */
public class RenamingSnapshotCommitTest {

    @TempDir java.nio.file.Path tempDir;

    private Path tablePath;

    @BeforeEach
    void setUp() throws IOException {
        tablePath = new Path(tempDir.toString());
        LocalFileIO.create().mkdirs(new Path(tablePath, "snapshot"));
    }

    @Test
    public void testConditionalWritePathSkipsLock() throws Exception {
        ConditionalWriteFileIO fileIO = new ConditionalWriteFileIO();
        RenamingSnapshotCommit commit = newCommit(fileIO, Lock.empty());

        assertThat(commit.commit(newSnapshot(1), "main", Collections.emptyList())).isTrue();
        assertThat(fileIO.conditionalWriteCalls.get()).isEqualTo(1);
    }

    @Test
    public void testConditionalWriteFailsOnConflict() throws Exception {
        ConditionalWriteFileIO fileIO = new ConditionalWriteFileIO();
        RenamingSnapshotCommit commit = newCommit(fileIO, Lock.empty());

        assertThat(commit.commit(newSnapshot(1), "main", Collections.emptyList())).isTrue();
        assertThat(commit.commit(newSnapshot(1), "main", Collections.emptyList())).isFalse();
    }

    @Test
    public void testFallbackPathUsesLock() throws Exception {
        AtomicInteger lockCalls = new AtomicInteger();
        RenamingSnapshotCommit commit = newCommit(LocalFileIO.create(), trackingLock(lockCalls));

        assertThat(commit.commit(newSnapshot(1), "main", Collections.emptyList())).isTrue();
        assertThat(lockCalls.get()).isEqualTo(1);
    }

    @Test
    public void testFallbackPathFailsOnConflict() throws Exception {
        AtomicInteger lockCalls = new AtomicInteger();
        RenamingSnapshotCommit commit = newCommit(LocalFileIO.create(), trackingLock(lockCalls));

        assertThat(commit.commit(newSnapshot(1), "main", Collections.emptyList())).isTrue();
        assertThat(commit.commit(newSnapshot(1), "main", Collections.emptyList())).isFalse();
        assertThat(lockCalls.get()).isEqualTo(2);
    }

    private RenamingSnapshotCommit newCommit(LocalFileIO fileIO, Lock lock) {
        return new RenamingSnapshotCommit(
                new SnapshotManager(fileIO, tablePath, "main", null, null), lock);
    }

    private static Snapshot newSnapshot(long id) {
        return createSnapshotWithMillis(id, System.currentTimeMillis());
    }

    private static Lock trackingLock(AtomicInteger counter) {
        return new Lock() {
            @Override
            public <T> T runWithLock(java.util.concurrent.Callable<T> callable) throws Exception {
                counter.incrementAndGet();
                return callable.call();
            }

            @Override
            public void close() {}
        };
    }

    private static class ConditionalWriteFileIO extends LocalFileIO {
        final AtomicInteger conditionalWriteCalls = new AtomicInteger();

        @Override
        public boolean supportsConditionalWrite() {
            return true;
        }

        @Override
        public boolean tryToWriteAtomicIfAbsent(Path path, String content) throws IOException {
            conditionalWriteCalls.incrementAndGet();
            if (exists(path)) {
                return false;
            }
            writeFile(path, content, false);
            return true;
        }
    }
}
