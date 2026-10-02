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
import org.apache.paimon.data.BinaryRow;
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
import org.apache.paimon.table.sink.PartitionBucketMapping;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

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

    @Test
    public void testFullCompactionWithPartitionBucketCounts() throws Exception {
        FileStoreTable table =
                createPartitionedTable(
                        new LocalFileIO(), CoreOptions.ChangelogProducer.FULL_COMPACTION);
        String commitUser = UUID.randomUUID().toString();
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
                        1,
                        true,
                        new MemoryPoolFactory(
                                new HeapMemorySegmentPool(
                                        options.writeBufferSize(), options.pageSize())),
                        null,
                        PartitionBucketMapping.loadFromTable(table),
                        FileStoreTable::newWrite);
        try {
            write.write(GenericRow.of(1, 1, 10L));
            assertThatCode(() -> write.prepareCommit(false, 1)).doesNotThrowAnyException();
        } finally {
            write.close();
            ioManager.close();
        }
    }

    @Test
    public void testLookupRestoresActiveBucketWithPartitionBucketCounts() throws Exception {
        FileStoreTable table =
                createPartitionedTable(new LocalFileIO(), CoreOptions.ChangelogProducer.LOOKUP);
        String initialUser = UUID.randomUUID().toString();
        CommitMessage activeBucket;
        IOManager ioManager = new IOManagerAsync();
        try {
            try (TableWriteImpl<?> initialWrite =
                            table.newWrite(initialUser).withIOManager(ioManager);
                    TableCommitImpl commit = table.newCommit(initialUser)) {
                initialWrite.writeAndReturn(
                        GenericRow.of(1, 1, 10L), PartitionBucketMapping.loadFromTable(table));
                List<CommitMessage> messages = initialWrite.prepareCommit(false, 0);
                activeBucket = messages.get(0);
                commit.commit(0, messages);
            }

            LookupSinkWrite write =
                    new LookupSinkWrite(
                            table,
                            UUID.randomUUID().toString(),
                            new ActiveBucketState(activeBucket.partition(), activeBucket.bucket()),
                            ioManager,
                            false,
                            false,
                            true,
                            new MemoryPoolFactory(
                                    new HeapMemorySegmentPool(
                                            table.coreOptions().writeBufferSize(),
                                            table.coreOptions().pageSize())),
                            null,
                            PartitionBucketMapping.loadFromTable(table),
                            FileStoreTable::newWrite);
            write.close();
        } finally {
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

    private FileStoreTable createPartitionedTable(
            LocalFileIO fileIO, CoreOptions.ChangelogProducer changelogProducer) throws Exception {
        Path tablePath = new Path(tempDir.toString());
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT().notNull())
                        .column("a", DataTypes.INT().notNull())
                        .column("b", DataTypes.BIGINT())
                        .primaryKey("pt", "a")
                        .partitionKeys("pt")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.BUCKET_PER_PARTITION_COUNT_ENABLED.key(), "true")
                        .option(CoreOptions.CHANGELOG_PRODUCER.key(), changelogProducer.toString())
                        .build();
        new FileSystemSchemaManager(fileIO, tablePath).createTable(schema);
        return FileStoreTableFactory.create(fileIO, tablePath);
    }

    private static class ActiveBucketState implements StoreSinkWriteState {

        private final List<StateValue> activeBuckets;

        private ActiveBucketState(BinaryRow partition, int bucket) {
            this.activeBuckets =
                    Collections.singletonList(new StateValue(partition, bucket, new byte[0]));
        }

        @Override
        public List<StateValue> get(String tableName, String key) {
            return activeBuckets;
        }

        @Override
        public void put(String tableName, String key, List<StateValue> stateValues) {}

        @Override
        public void snapshotState() {}

        @Override
        public int getSubtaskId() {
            return 0;
        }
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
