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

package org.apache.paimon.flink.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GlobalFullCompactionSinkWrite}. */
public class GlobalFullCompactionSinkWriteTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testStopsCheckingSnapshotsAfterFullCompactionIsObserved() throws Exception {
        SnapshotReadCountingFileIO fileIO = new SnapshotReadCountingFileIO();
        FileStoreTable table = createTable(fileIO);
        String commitUser = UUID.randomUUID().toString();
        int deltaCommits = 4;
        CoreOptions options = table.coreOptions();
        IOManager ioManager = new IOManagerAsync();
        GlobalFullCompactionSinkWrite write =
                new GlobalFullCompactionSinkWrite(
                        table,
                        commitUser,
                        new NoopStoreSinkWriteState(0),
                        ioManager,
                        false,
                        false,
                        deltaCommits,
                        true,
                        new MemoryPoolFactory(
                                new HeapMemorySegmentPool(
                                        options.writeBufferSize(), options.pageSize())),
                        null);
        try (TableCommitImpl commit = table.newCommit(commitUser)) {
            for (long checkpoint = 1; checkpoint <= deltaCommits; checkpoint++) {
                write.write(GenericRow.of((int) checkpoint, checkpoint * 10));
                commit.commit(checkpoint, commitMessages(write.prepareCommit(false, checkpoint)));
            }
            // checkpoint 4 triggered a full compaction and its COMPACT snapshot is the latest one
            Snapshot latest = table.snapshotManager().latestSnapshot();
            assertThat(latest.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);
            assertThat(latest.commitIdentifier()).isEqualTo(4L);
            assertThat(latest.commitUser()).isEqualTo(commitUser);

            // checkpoint 5 finds that snapshot and learns that the compaction succeeded
            write.write(GenericRow.of(5, 50L));
            commit.commit(5L, commitMessages(write.prepareCommit(false, 5L)));

            // later checkpoints have nothing left to check. The table writer may still look at
            // the latest snapshot to find its last committed identifier, but nothing may walk
            // back through older snapshots anymore.
            for (long checkpoint = 6; checkpoint <= 7; checkpoint++) {
                write.write(GenericRow.of((int) checkpoint, checkpoint * 10));
                Long latestSnapshotId = table.snapshotManager().latestSnapshotId();
                fileIO.snapshotIdsRead.clear();
                List<Committable> committables = write.prepareCommit(false, checkpoint);
                assertThat(fileIO.snapshotIdsRead)
                        .as("snapshot files read by prepareCommit for checkpoint %s", checkpoint)
                        .isSubsetOf(latestSnapshotId);
                commit.commit(checkpoint, commitMessages(committables));
            }
        } finally {
            write.close();
            ioManager.close();
        }
    }

    private static List<CommitMessage> commitMessages(List<Committable> committables) {
        return committables.stream().map(Committable::commitMessage).collect(Collectors.toList());
    }

    private FileStoreTable createTable(LocalFileIO fileIO) throws Exception {
        Path tablePath = new Path(tempDir.toString());
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT().notNull())
                        .column("b", DataTypes.BIGINT())
                        .primaryKey("a")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(
                                CoreOptions.CHANGELOG_PRODUCER.key(),
                                CoreOptions.ChangelogProducer.FULL_COMPACTION.toString())
                        .build();
        new FileSystemSchemaManager(fileIO, tablePath).createTable(schema);
        return FileStoreTableFactory.create(fileIO, tablePath);
    }

    /** Local file system that records which snapshot files are opened for reading. */
    private static class SnapshotReadCountingFileIO extends LocalFileIO {

        private static final long serialVersionUID = 1L;

        private final List<Long> snapshotIdsRead = new CopyOnWriteArrayList<>();

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            String name = path.getName();
            if (name.startsWith("snapshot-")) {
                snapshotIdsRead.add(Long.parseLong(name.substring("snapshot-".length())));
            }
            return super.newInputStream(path);
        }
    }
}
