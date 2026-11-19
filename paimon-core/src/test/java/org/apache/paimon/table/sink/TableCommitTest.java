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

package org.apache.paimon.table.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.SeekableInputStreamWrapper;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.FailingFileIO;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.paimon.CoreOptions.COMMIT_LAST_SAFE_SNAPSHOT;
import static org.apache.paimon.table.sink.BatchWriteBuilder.COMMIT_IDENTIFIER;
import static org.apache.paimon.utils.FileStorePathFactoryTest.createNonPartFactory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TableCommit}. */
public class TableCommitTest {

    @TempDir java.nio.file.Path tempDir;

    private static final Map<String, Set<Long>> commitCallbackResult = new ConcurrentHashMap<>();

    @Test
    public void testCommitCallbackWithFailureFixedBucket() throws Exception {
        innerTestCommitCallbackWithFailure(1);
    }

    @Test
    public void testCommitCallbackWithFailureDynamicBucket() throws Exception {
        innerTestCommitCallbackWithFailure(-1);
    }

    private void innerTestCommitCallbackWithFailure(int bucket) throws Exception {
        int numIdentifiers = 30;
        String testId = UUID.randomUUID().toString();
        commitCallbackResult.put(testId, new HashSet<>());

        try {
            testCommitCallbackWithFailureImpl(bucket, numIdentifiers, testId);
        } finally {
            commitCallbackResult.remove(testId);
        }
    }

    private void testCommitCallbackWithFailureImpl(int bucket, int numIdentifiers, String testId)
            throws Exception {
        String failingName = UUID.randomUUID().toString();
        // no failure when creating table and writing data
        FailingFileIO.reset(failingName, 0, 1);

        String path = FailingFileIO.getFailingPath(failingName, tempDir.toString());

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"k", "v"});

        Options conf = new Options();
        conf.set(CoreOptions.PATH, path);
        conf.set(CoreOptions.BUCKET, bucket);
        conf.set(CoreOptions.COMMIT_CALLBACKS, TestCommitCallback.class.getName());
        conf.set(
                CoreOptions.COMMIT_CALLBACK_PARAM
                        .key()
                        .replace("#", TestCommitCallback.class.getName()),
                testId);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), new Path(path)),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.singletonList("k"),
                                conf.toMap(),
                                ""));

        FileStoreTable table =
                FileStoreTableFactory.create(
                        new FailingFileIO(),
                        new Path(path),
                        tableSchema,
                        CatalogEnvironment.empty());

        String commitUser = UUID.randomUUID().toString();
        StreamTableWrite write = table.newWrite(commitUser);
        Map<Long, List<CommitMessage>> commitMessages = new HashMap<>();
        for (int i = 0; i < numIdentifiers; i++) {
            if (bucket == -1) {
                write.write(GenericRow.of(i, i * 1000L), 0);
            } else {
                write.write(GenericRow.of(i, i * 1000L));
            }
            commitMessages.put((long) i, write.prepareCommit(true, i));
        }
        write.close();

        StreamTableCommit commit = table.newCommit(commitUser);
        // enable failure when committing
        FailingFileIO.reset(failingName, 3, 1000);
        while (true) {
            try {
                commit.filterAndCommit(commitMessages);
                break;
            } catch (Throwable t) {
                // artificial exception is intended
                Optional<FailingFileIO.ArtificialException> artificialException =
                        ExceptionUtils.findThrowable(t, FailingFileIO.ArtificialException.class);
                // this test emulates an extremely slow commit procedure,
                // so conflicts may occur due to back pressuring
                Optional<Throwable> conflictException =
                        ExceptionUtils.findThrowableWithMessage(
                                t, "Conflicts during commits are normal");
                if (artificialException.isPresent() || conflictException.isPresent()) {
                    continue;
                }
                throw t;
            }
        }
        commit.close();

        assertThat(commitCallbackResult.get(testId))
                .isEqualTo(LongStream.range(0, numIdentifiers).boxed().collect(Collectors.toSet()));
    }

    /** {@link CommitCallback} for test. */
    public static class TestCommitCallback implements CommitCallback {

        private final String testId;

        public TestCommitCallback(String testId) {
            this.testId = testId;
        }

        @Override
        public void call(Context context) {
            commitCallbackResult.get(testId).add(context.snapshot.commitIdentifier());
        }

        @Override
        public void retry(ManifestCommittable committable) {
            commitCallbackResult.get(testId).add(committable.identifier());
        }

        @Override
        public void close() throws Exception {}
    }

    @Test
    public void testRecoverDeletedFiles() throws Exception {
        String path = tempDir.toString();
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"k", "v"});

        Options options = new Options();
        options.set(CoreOptions.PATH, path);
        options.set(CoreOptions.BUCKET, 1);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), new Path(path)),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.singletonList("k"),
                                options.toMap(),
                                ""));

        FileStoreTable table =
                FileStoreTableFactory.create(
                        LocalFileIO.create(),
                        new Path(path),
                        tableSchema,
                        CatalogEnvironment.empty());

        String commitUser = UUID.randomUUID().toString();
        StreamTableWrite write = table.newWrite(commitUser);
        write.write(GenericRow.of(0, 0L));
        List<CommitMessage> messages0 = write.prepareCommit(true, 0);

        write.write(GenericRow.of(1, 1L));
        List<CommitMessage> messages1 = write.prepareCommit(true, 1);
        write.close();

        StreamTableCommit commit = table.newCommit(commitUser);
        commit.commit(0, messages0);

        // delete files for commit0 and commit1
        for (CommitMessageImpl message :
                Arrays.asList(
                        (CommitMessageImpl) messages0.get(0),
                        (CommitMessageImpl) messages1.get(0))) {
            DataFilePathFactory pathFactory =
                    createNonPartFactory(new Path(path))
                            .createDataFilePathFactory(message.partition(), message.bucket());
            Path file =
                    message.newFilesIncrement().newFiles().get(0).collectFiles(pathFactory).get(0);
            LocalFileIO.create().delete(file, true);
        }

        // commit 0, fine, it will be filtered
        commit.filterAndCommit(singletonMap(0L, messages0));

        // commit 1, exception now.
        assertThatThrownBy(() -> commit.filterAndCommit(singletonMap(1L, messages1)))
                .hasMessageContaining(
                        "Cannot recover from this checkpoint because some files in the"
                                + " snapshot that need to be resubmitted have been deleted");
    }

    @Test
    public void testGiveUpCommitWhenTotalBucketsChanged() throws Exception {
        String path = tempDir.toString();
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"k", "v"});

        Options options = new Options();
        options.set(CoreOptions.PATH, path);
        options.set(CoreOptions.BUCKET, 1);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), new Path(path)),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.singletonList("k"),
                                options.toMap(),
                                ""));
        FileStoreTable table =
                FileStoreTableFactory.create(
                        LocalFileIO.create(),
                        new Path(path),
                        tableSchema,
                        CatalogEnvironment.empty());

        String commitUser = UUID.randomUUID().toString();
        try (TableWriteImpl<?> write = table.newWrite(commitUser);
                TableCommitImpl commit = table.newCommit(commitUser)) {
            write.write(GenericRow.of(0, 0L));
            commit.commit(1, write.prepareCommit(false, 1));
        }

        options = new Options(table.options());
        options.set(CoreOptions.BUCKET, 2);
        table = table.copy(tableSchema.copy(options.toMap()));

        commitUser = UUID.randomUUID().toString();
        try (TableWriteImpl<?> write = table.newWrite(commitUser);
                TableCommitImpl commit = table.newCommit(commitUser)) {
            write.getWrite().withIgnoreNumBucketCheck(true);
            for (int i = 1; i < 10; i++) {
                write.write(GenericRow.of(i, (long) i));
            }
            for (int i = 0; i < 2; i++) {
                write.compact(BinaryRow.EMPTY_ROW, i, true);
            }
            assertThatThrownBy(() -> commit.commit(1, write.prepareCommit(true, 1)))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("changed from 1 to 2 without overwrite");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testGiveUpCommitWhenAppendFoundTotalBucketsChanged(boolean checkAppend)
            throws Exception {
        String path = tempDir.toString();
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"k", "v"});

        Options options = new Options();
        options.set(CoreOptions.PATH, path);
        options.set(CoreOptions.BUCKET, 1);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), new Path(path)),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.singletonList("k"),
                                options.toMap(),
                                ""));
        FileStoreTable table =
                FileStoreTableFactory.create(
                        LocalFileIO.create(),
                        new Path(path),
                        tableSchema,
                        CatalogEnvironment.empty());

        String commitUser1 = UUID.randomUUID().toString();
        TableWriteImpl<?> write1 = table.newWrite(commitUser1);
        TableCommitImpl commit1 = table.newCommit(commitUser1);
        for (int i = 1; i < 10; i++) {
            write1.write(GenericRow.of(i, (long) i));
        }

        // mock rescale
        String commitUser2 = UUID.randomUUID().toString();
        options = new Options(table.options());
        options.set(CoreOptions.BUCKET, 2);
        FileStoreTable rescaleTable = table.copy(tableSchema.copy(options.toMap()));
        try (TableWriteImpl<?> write = rescaleTable.newWrite(commitUser2);
                TableCommitImpl commit =
                        rescaleTable.newCommit(commitUser2).withOverwrite(Collections.emptyMap())) {
            for (int i = 1; i < 10; i++) {
                write.write(GenericRow.of(i, (long) i));
            }
            commit.commit(1, write.prepareCommit(false, 1));
        }

        if (checkAppend) {
            commit1.appendCommitCheckConflict(true);
            assertThatThrownBy(() -> commit1.commit(1, write1.prepareCommit(false, 1)))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("changed from 2 to 1 without overwrite");
        } else {
            // the commit result is error, but here verify that no check if
            // appendCommitCheckConflict was not set
            assertThatCode(() -> commit1.commit(1, write1.prepareCommit(false, 1)))
                    .doesNotThrowAnyException();
        }
        write1.close();
        commit1.close();
    }

    @ParameterizedTest
    @CsvSource({
        "commit.last-safe-snapshot, true",
        "commit.last-safe-snapshot, false",
        "commit.strict-mode.last-safe-snapshot, true",
        "commit.strict-mode.last-safe-snapshot, false"
    })
    public void testStrictModeForCompact(String lastSafeKey, boolean strictEnabled)
            throws Exception {
        String path = tempDir.toString();
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"pt", "k", "v"});

        Options options = new Options();
        options.set(CoreOptions.PATH, path);
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER, 10);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), new Path(path)),
                        new Schema(
                                rowType.getFields(),
                                Collections.singletonList("pt"),
                                Arrays.asList("pt", "k"),
                                options.toMap(),
                                ""));
        FileStoreTable table =
                FileStoreTableFactory.create(
                        LocalFileIO.create(),
                        new Path(path),
                        tableSchema,
                        CatalogEnvironment.empty());
        BinaryRow pt1 = partitionRow(1);
        BinaryRow pt2 = partitionRow(2);

        String user1 = UUID.randomUUID().toString();
        TableWriteImpl<?> write1 = table.newWrite(user1);
        TableCommitImpl commit1 = table.newCommit(user1);

        write1.write(GenericRow.of(1, 0, 0L));
        write1.compact(pt1, 0, true);
        commit1.commit(1, write1.prepareCommit(true, 1));

        // test skip this commit check
        String user2 = UUID.randomUUID().toString();
        Map<String, String> commitOptions = new HashMap<>();
        commitOptions.put(lastSafeKey, "2");
        commitOptions.put(
                CoreOptions.COMMIT_STRICT_MODE_ENABLED.key(), String.valueOf(strictEnabled));
        table = table.copy(commitOptions);
        TableWriteImpl<?> write2 = table.newWrite(user2);
        TableCommitImpl commit2 = table.newCommit(user2);

        write2.write(GenericRow.of(2, 1, 1L));
        commit2.commit(1, write2.prepareCommit(false, 1));

        // COMPACT on a different partition should be ignored
        write1.write(GenericRow.of(1, 4, 4L));
        write1.compact(pt1, 0, true);
        commit1.commit(2, write1.prepareCommit(true, 2));

        write2.write(GenericRow.of(2, 5, 5L));
        assertThatCode(() -> commit2.commit(2, write2.prepareCommit(false, 2)))
                .doesNotThrowAnyException();

        // COMPACT on the same partition is rejected only when strict mode is enabled.
        write1.write(GenericRow.of(2, 6, 6L));
        write1.compact(pt2, 0, true);
        commit1.commit(3, write1.prepareCommit(true, 3));

        write2.write(GenericRow.of(2, 7, 7L));
        if (strictEnabled) {
            assertThatThrownBy(() -> commit2.commit(3, write2.prepareCommit(false, 3)))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining(
                            "Giving up committing as commit.strict-mode.enabled is true.");
        } else {
            commit2.commit(3, write2.prepareCommit(false, 3));
            assertThat(table.snapshotManager().latestSnapshot().commitUser()).isEqualTo(user2);
        }

        write1.close();
        commit1.close();
        write2.close();
        commit2.close();
    }

    @Test
    public void testStrictModeForOverwrite() throws Exception {
        String path = tempDir.toString();
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"pt", "k", "v"});

        Options options = new Options();
        options.set(CoreOptions.PATH, path);
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER, 10);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), new Path(path)),
                        new Schema(
                                rowType.getFields(),
                                Collections.singletonList("pt"),
                                Arrays.asList("pt", "k"),
                                options.toMap(),
                                ""));
        FileStoreTable table =
                FileStoreTableFactory.create(
                        LocalFileIO.create(),
                        new Path(path),
                        tableSchema,
                        CatalogEnvironment.empty());
        BinaryRow pt1 = partitionRow(1);
        BinaryRow pt2 = partitionRow(2);

        // user1 writes pt=1 and pt=2
        String user1 = UUID.randomUUID().toString();
        TableWriteImpl<?> write1 = table.newWrite(user1);
        TableCommitImpl commit1 = table.newCommit(user1);
        write1.write(GenericRow.of(1, 0, 0L));
        write1.write(GenericRow.of(2, 0, 0L));
        commit1.commit(1, write1.prepareCommit(false, 1));

        // test skip this commit check
        String user2 = UUID.randomUUID().toString();
        FileStoreTable tableWithStrict =
                table.copy(singletonMap(COMMIT_LAST_SAFE_SNAPSHOT.key(), "2"));
        TableWriteImpl<?> write2 = tableWithStrict.newWrite(user2);
        TableCommitImpl commit2 = tableWithStrict.newCommit(user2);

        write2.write(GenericRow.of(1, 1, 1L));
        write2.compact(pt1, 0, true);
        commit2.commit(1, write2.prepareCommit(true, 1));

        // user1 OVERWRITE on pt=1, snapshot 3
        TableCommitImpl commit1Ow1 = table.newCommit(user1).withOverwrite(singletonMap("pt", "1"));
        write1.write(GenericRow.of(1, 4, 4L));
        commit1Ow1.commit(2, write1.prepareCommit(false, 2));

        // user2 COMPACT on pt=2: OVERWRITE pt=1 does not overlap so no error
        write2.write(GenericRow.of(2, 5, 5L));
        write2.compact(pt2, 0, true);
        assertThatCode(() -> commit2.commit(2, write2.prepareCommit(true, 2)))
                .doesNotThrowAnyException();

        // user1 OVERWRITE on pt=1 again, snapshot 5
        TableCommitImpl commit1Ow2 = table.newCommit(user1).withOverwrite(singletonMap("pt", "1"));
        write1.write(GenericRow.of(1, 6, 6L));
        commit1Ow2.commit(3, write1.prepareCommit(false, 3));

        // user2 COMPACT on pt=1: OVERWRITE pt=1 overlaps, should throw
        write2.write(GenericRow.of(1, 7, 7L));
        write2.compact(pt1, 0, true);
        assertThatThrownBy(() -> commit2.commit(3, write2.prepareCommit(true, 3)))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining(
                        "Giving up committing as commit.strict-mode.enabled is true.");

        write1.close();
        commit1.close();
        commit1Ow1.close();
        commit1Ow2.close();
        write2.close();
        commit2.close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testStrictModeForIndexOnlyCompact(boolean dataEvolutionEnabled) throws Exception {
        String path = tempDir.toString();
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"pt", "k", "v"});

        Options options = new Options();
        options.set(CoreOptions.PATH, path);
        options.set(CoreOptions.BUCKET, dataEvolutionEnabled ? -1 : 1);
        if (!dataEvolutionEnabled) {
            options.set(CoreOptions.BUCKET_KEY, "k");
        }
        options.set(CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER, 10);
        options.set(CoreOptions.ROW_TRACKING_ENABLED, dataEvolutionEnabled);
        options.set(CoreOptions.DATA_EVOLUTION_ENABLED, dataEvolutionEnabled);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), new Path(path)),
                        new Schema(
                                rowType.getFields(),
                                Collections.singletonList("pt"),
                                Collections.emptyList(),
                                options.toMap(),
                                ""));
        FileStoreTable table =
                FileStoreTableFactory.create(
                        LocalFileIO.create(),
                        new Path(path),
                        tableSchema,
                        CatalogEnvironment.empty());
        BinaryRow pt1 = partitionRow(1);

        String user1 = UUID.randomUUID().toString();
        TableWriteImpl<?> write1 = table.newWrite(user1);
        TableCommitImpl commit1 = table.newCommit(user1);
        write1.write(GenericRow.of(1, 0, 0L));
        commit1.commit(1, write1.prepareCommit(false, 1));

        String user2 = UUID.randomUUID().toString();
        FileStoreTable tableWithStrict =
                table.copy(singletonMap(COMMIT_LAST_SAFE_SNAPSHOT.key(), "1"));
        TableWriteImpl<?> write2 = tableWithStrict.newWrite(user2);
        TableCommitImpl commit2 = tableWithStrict.newCommit(user2);
        write2.write(GenericRow.of(1, 1, 1L));

        IndexFileMeta btreeIndex =
                new IndexFileMeta(
                        "btree",
                        "index-only-compact",
                        1,
                        1,
                        new GlobalIndexMeta(0, 0, 1, null, null),
                        null);
        commit1.commit(
                2,
                Collections.singletonList(
                        new CommitMessageImpl(
                                pt1,
                                0,
                                1,
                                DataIncrement.emptyIncrement(),
                                new CompactIncrement(
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Collections.singletonList(btreeIndex),
                                        Collections.emptyList()))));

        if (dataEvolutionEnabled) {
            assertThatCode(() -> commit2.commit(1, write2.prepareCommit(true, 1)))
                    .doesNotThrowAnyException();
        } else {
            assertThatThrownBy(() -> commit2.commit(1, write2.prepareCommit(true, 1)))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining(
                            "Giving up committing as commit.strict-mode.enabled is true.");
        }

        write1.close();
        commit1.close();
        write2.close();
        commit2.close();
    }

    @Test
    public void testStrictModeForDvOnlyOverwrite() throws Exception {
        // Regression test for the partition-overlap check on DV-only OVERWRITE
        // snapshots: such snapshots have no data-file delta, so the check must
        // also look at index-manifest delta.
        String path = tempDir.toString();
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"pt", "k", "v"});

        Options options = new Options();
        options.set(CoreOptions.PATH, path);
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.BUCKET_KEY, "k");
        options.set(CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER, 10);
        options.set(CoreOptions.DELETION_VECTORS_ENABLED, true);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), new Path(path)),
                        new Schema(
                                rowType.getFields(),
                                Collections.singletonList("pt"),
                                Collections.emptyList(),
                                options.toMap(),
                                ""));
        FileStoreTable table =
                FileStoreTableFactory.create(
                        LocalFileIO.create(),
                        new Path(path),
                        tableSchema,
                        CatalogEnvironment.empty());
        BinaryRow pt1 = partitionRow(1);
        BinaryRow pt2 = partitionRow(2);

        // user1 writes pt=1 and pt=2 -> snapshot 1 (APPEND)
        String user1 = UUID.randomUUID().toString();
        TableWriteImpl<?> write1 = table.newWrite(user1);
        TableCommitImpl commit1 = table.newCommit(user1);
        write1.write(GenericRow.of(1, 0, 0L));
        write1.write(GenericRow.of(2, 0, 0L));
        commit1.commit(1, write1.prepareCommit(false, 1));

        // user2 with strict mode, last-safe-snapshot=2 (skip its own snapshot 2)
        String user2 = UUID.randomUUID().toString();
        FileStoreTable tableWithStrict =
                table.copy(singletonMap(COMMIT_LAST_SAFE_SNAPSHOT.key(), "2"));
        TableWriteImpl<?> write2 = tableWithStrict.newWrite(user2);
        TableCommitImpl commit2 = tableWithStrict.newCommit(user2);
        write2.write(GenericRow.of(1, 1, 1L));
        commit2.commit(1, write2.prepareCommit(false, 1));

        // user1 DV-only OVERWRITE on pt=2 -> snapshot 3 (OVERWRITE, no data-file delta)
        commitDvOnly(table, user1, pt2, 2);

        // user2 COMPACT on pt=1: DV-only on pt=2 does not overlap -> no error
        write2.write(GenericRow.of(1, 5, 5L));
        write2.compact(pt1, 0, true);
        assertThatCode(() -> commit2.commit(2, write2.prepareCommit(true, 2)))
                .doesNotThrowAnyException();

        // user1 DV-only OVERWRITE on pt=1 -> snapshot 5 (OVERWRITE, no data-file delta)
        commitDvOnly(table, user1, pt1, 3);

        // user2 COMPACT on pt=1: DV-only on pt=1 overlaps -> must throw
        write2.write(GenericRow.of(1, 7, 7L));
        write2.compact(pt1, 0, true);
        assertThatThrownBy(() -> commit2.commit(3, write2.prepareCommit(true, 3)))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining(
                        "Giving up committing as commit.strict-mode.enabled is true.");

        write1.close();
        commit1.close();
        write2.close();
        commit2.close();
    }

    @Test
    public void testStrictModeIgnoresDeletionVectorsInheritedByCompaction() throws Exception {
        // A streaming upsert job on a primary-key table with deletion vectors keeps compacting
        // pt=2 while a job pinned to the snapshot it started from (e.g. rescale) writes pt=1.
        FileStoreTable table = createPkDvTable();
        String streamingUser = UUID.randomUUID().toString();
        try (TableWriteImpl<?> write =
                        table.newWrite(streamingUser)
                                .withIOManager(new IOManagerImpl(tempDir.toString()));
                TableCommitImpl commit = table.newCommit(streamingUser)) {
            for (int k = 0; k < 3; k++) {
                write.write(GenericRow.of(1, k, 0L));
                write.write(GenericRow.of(2, k, 0L));
            }
            commit.commit(0, write.prepareCommit(true, 0));
            // updating a key of pt=1 gives pt=1 a deletion vector before the pinned job starts
            write.write(GenericRow.of(1, 0, 1L));
            commit.commit(1, write.prepareCommit(true, 1));
            long jobStart = table.snapshotManager().latestSnapshotId();

            // the compaction of this pt=2 update changes only pt=2's deletion vectors, while
            // its index manifest still lists pt=1's
            write.write(GenericRow.of(2, 0, 2L));
            commit.commit(2, write.prepareCommit(true, 2));
            assertThat(table.snapshotManager().latestSnapshot().commitKind())
                    .isEqualTo(Snapshot.CommitKind.COMPACT);

            FileStoreTable pinned =
                    table.copy(
                            singletonMap(
                                    COMMIT_LAST_SAFE_SNAPSHOT.key(), String.valueOf(jobStart)));
            String pinnedUser = UUID.randomUUID().toString();
            try (TableWriteImpl<?> pinnedWrite =
                            pinned.newWrite(pinnedUser)
                                    .withIOManager(new IOManagerImpl(tempDir.toString()));
                    TableCommitImpl pinnedCommit = pinned.newCommit(pinnedUser)) {
                pinnedWrite.write(GenericRow.of(1, 9, 9L));
                assertThatCode(() -> pinnedCommit.commit(0, pinnedWrite.prepareCommit(false, 0)))
                        .doesNotThrowAnyException();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testStrictModeDetectsIndexRemovalWithoutDataDelta(boolean targetHasIndexManifest)
            throws Exception {
        // Rolling back a DV-only commit is an OVERWRITE that drops the DV entry without any
        // data-file delta, so the index check must also flag partitions whose entries the
        // checked snapshot removed, including when its index manifest becomes null.
        FileStoreTable table = createDvAppendTable();
        String user1 = UUID.randomUUID().toString();
        commitRow(table, user1, GenericRow.of(1, 0, 0L), GenericRow.of(2, 0, 0L));
        long commitId = 2;
        if (targetHasIndexManifest) {
            commitDvOnly(table, user1, partitionRow(2), commitId++);
        }
        SnapshotManager snapshotManager = table.snapshotManager();
        Snapshot target = snapshotManager.latestSnapshot();
        commitDvOnly(table, user1, partitionRow(1), commitId);
        long lastSafe = snapshotManager.latestSnapshotId();

        try (FileStoreCommit commit = table.store().newCommit(user1, table)) {
            assertThat(commit.rollbackToAsLatest(target)).isTrue();
        }
        Snapshot rollback = snapshotManager.latestSnapshot();
        assertThat(rollback.indexManifest() != null).isEqualTo(targetHasIndexManifest);
        assertThat(
                        table.store()
                                .newScan()
                                .withSnapshot(rollback)
                                .withKind(ScanMode.DELTA)
                                .plan()
                                .files())
                .isEmpty();

        FileStoreTable tableWithStrict =
                table.copy(singletonMap(COMMIT_LAST_SAFE_SNAPSHOT.key(), String.valueOf(lastSafe)));
        // pt=2's index entries are untouched by the rollback -> no error
        assertThatCode(
                        () ->
                                commitRow(
                                        tableWithStrict,
                                        UUID.randomUUID().toString(),
                                        GenericRow.of(2, 1, 1L)))
                .doesNotThrowAnyException();
        // the rollback removed pt=1's DV entry -> must throw
        assertThatThrownBy(
                        () ->
                                commitRow(
                                        tableWithStrict,
                                        UUID.randomUUID().toString(),
                                        GenericRow.of(1, 1, 1L)))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("found a OVERWRITE snapshot (id: " + rollback.id() + ")")
                .hasMessageContaining(
                        "Giving up committing as commit.strict-mode.enabled is true.");
    }

    private FileStoreTable createPkDvTable() throws Exception {
        String path = tempDir.toString();
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"pt", "k", "v"});

        Options options = new Options();
        options.set(CoreOptions.PATH, path);
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.DELETION_VECTORS_ENABLED, true);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), new Path(path)),
                        new Schema(
                                rowType.getFields(),
                                Collections.singletonList("pt"),
                                Arrays.asList("pt", "k"),
                                options.toMap(),
                                ""));
        return FileStoreTableFactory.create(
                LocalFileIO.create(), new Path(path), tableSchema, CatalogEnvironment.empty());
    }

    private FileStoreTable createDvAppendTable() throws Exception {
        String path = tempDir.toString();
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"pt", "k", "v"});

        Options options = new Options();
        options.set(CoreOptions.PATH, path);
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.BUCKET_KEY, "k");
        options.set(CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER, 10);
        options.set(CoreOptions.DELETION_VECTORS_ENABLED, true);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), new Path(path)),
                        new Schema(
                                rowType.getFields(),
                                Collections.singletonList("pt"),
                                Collections.emptyList(),
                                options.toMap(),
                                ""));
        return FileStoreTableFactory.create(
                LocalFileIO.create(), new Path(path), tableSchema, CatalogEnvironment.empty());
    }

    private static void commitRow(FileStoreTable table, String user, GenericRow... rows)
            throws Exception {
        try (TableWriteImpl<?> write = table.newWrite(user);
                TableCommitImpl commit = table.newCommit(user)) {
            for (GenericRow row : rows) {
                write.write(row);
            }
            commit.commit(1, write.prepareCommit(false, 1));
        }
    }

    @Test
    public void testStrictModeShortCircuitsWhenIndexManifestUnchanged() throws Exception {
        String path = tempDir.toString();
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"pt", "k", "v"});

        Options options = new Options();
        options.set(CoreOptions.PATH, path);
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.BUCKET_KEY, "k");
        options.set(CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER, 10);
        options.set(CoreOptions.DELETION_VECTORS_ENABLED, true);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), new Path(path)),
                        new Schema(
                                rowType.getFields(),
                                Collections.singletonList("pt"),
                                Collections.emptyList(),
                                options.toMap(),
                                ""));
        FileStoreTable table =
                FileStoreTableFactory.create(
                        LocalFileIO.create(),
                        new Path(path),
                        tableSchema,
                        CatalogEnvironment.empty());
        BinaryRow pt3 = partitionRow(3);

        // user1 writes pt=1 and pt=3 -> snapshot 1 (APPEND)
        String user1 = UUID.randomUUID().toString();
        TableWriteImpl<?> write1 = table.newWrite(user1);
        TableCommitImpl commit1 = table.newCommit(user1);
        write1.write(GenericRow.of(1, 0, 0L));
        write1.write(GenericRow.of(3, 0, 0L));
        commit1.commit(1, write1.prepareCommit(false, 1));

        // user1 commits a DV on pt=3 -> snapshot 2 (OVERWRITE, dv-only).
        // indexManifest now records pt=3.
        commitDvOnly(table, user1, pt3, 2);

        // user1 OVERWRITE on pt=1 -> snapshot 3 (OVERWRITE).
        // pt=1 has no DV, so writeIndexFiles produces no new index file and
        // snapshot 3 reuses snapshot 2's indexManifest file name.
        TableCommitImpl commit1Ow = table.newCommit(user1).withOverwrite(singletonMap("pt", "1"));
        write1.write(GenericRow.of(1, 9, 9L));
        commit1Ow.commit(3, write1.prepareCommit(false, 3));

        // user2 with last-safe=2 writes pt=3 -> APPEND.
        // Snapshot 3 is OVERWRITE on pt=1, its data delta does not touch pt=3,
        // and although its indexManifest still contains pt=3 (inherited from
        // snapshot 2), the short-circuit must skip the full-set intersection
        // and avoid throwing.
        String user2 = UUID.randomUUID().toString();
        FileStoreTable tableWithStrict =
                table.copy(singletonMap(COMMIT_LAST_SAFE_SNAPSHOT.key(), "2"));
        TableWriteImpl<?> write2 = tableWithStrict.newWrite(user2);
        TableCommitImpl commit2 = tableWithStrict.newCommit(user2);
        write2.write(GenericRow.of(3, 1, 1L));
        assertThatCode(() -> commit2.commit(1, write2.prepareCommit(false, 1)))
                .doesNotThrowAnyException();

        write1.close();
        commit1.close();
        commit1Ow.close();
        write2.close();
        commit2.close();
    }

    private void commitDvOnly(FileStoreTable table, String user, BinaryRow partition, long commitId)
            throws Exception {
        // Pick a real data file in the partition; ConflictDetection requires the
        // DV to reference an existing data file name.
        String dataFileName =
                table.store().newScan().withPartitionFilter(Collections.singletonList(partition))
                        .plan().files().stream()
                        .findFirst()
                        .map(e -> e.file().fileName())
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "No data file in partition for DV commit"));
        IndexFileHandler indexFileHandler = table.store().newIndexFileHandler();
        BucketedDvMaintainer dvMaintainer =
                BucketedDvMaintainer.factory(indexFileHandler)
                        .create(partition, 0, Collections.emptyList());
        dvMaintainer.notifyNewDeletion(dataFileName, 0);
        IndexFileMeta dvIndex = dvMaintainer.writeDeletionVectorsIndex().get();
        try (TableCommitImpl commit = table.newCommit(user)) {
            commit.commit(
                    commitId,
                    Collections.singletonList(
                            new CommitMessageImpl(
                                    partition,
                                    0,
                                    1,
                                    DataIncrement.indexIncrement(
                                            Collections.singletonList(dvIndex)),
                                    CompactIncrement.emptyIncrement())));
        }
    }

    @Test
    public void testStrictModeForOverwriteCheckAppend() throws Exception {
        String path = tempDir.toString();
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"pt", "k", "v"});

        Options options = new Options();
        options.set(CoreOptions.PATH, path);
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER, 10);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), new Path(path)),
                        new Schema(
                                rowType.getFields(),
                                Collections.singletonList("pt"),
                                Arrays.asList("pt", "k"),
                                options.toMap(),
                                ""));
        FileStoreTable table =
                FileStoreTableFactory.create(
                        LocalFileIO.create(),
                        new Path(path),
                        tableSchema,
                        CatalogEnvironment.empty());
        BinaryRow pt1 = partitionRow(1);

        String user1 = UUID.randomUUID().toString();
        FileStoreTable fixedBucketWriteTable = table;
        TableWriteImpl<?> write1 = fixedBucketWriteTable.newWrite(user1);
        TableCommitImpl commit1 = fixedBucketWriteTable.newCommit(user1);

        write1.write(GenericRow.of(1, 0, 0L));
        write1.compact(pt1, 0, true);
        commit1.commit(1, write1.prepareCommit(true, 1));

        // test skip this commit check

        String user2 = UUID.randomUUID().toString();
        table = table.copy(singletonMap(COMMIT_LAST_SAFE_SNAPSHOT.key(), "2"));
        TableWriteImpl<?> write2 = table.newWrite(user2);
        TableCommitImpl commit2 = table.newCommit(user2).withOverwrite(singletonMap("pt", "1"));

        write2.write(GenericRow.of(1, 1, 1L));
        commit2.commit(1, write2.prepareCommit(false, 1));

        // APPEND with postpone bucket files should be ignored
        write1.close();
        commit1.close();
        Map<String, String> postponeWriteOptions = new HashMap<>();
        postponeWriteOptions.put(CoreOptions.BUCKET.key(), "-2");
        postponeWriteOptions.put(CoreOptions.POSTPONE_BATCH_WRITE_FIXED_BUCKET.key(), "false");
        FileStoreTable postponeWriteTable = fixedBucketWriteTable.copy(postponeWriteOptions);
        write1 = postponeWriteTable.newWrite(user1);
        commit1 = postponeWriteTable.newCommit(user1);
        write1.write(GenericRow.of(1, 2, 2L));
        commit1.commit(2, write1.prepareCommit(false, 2));

        write2.write(GenericRow.of(1, 3, 3L));
        commit2.commit(2, write2.prepareCommit(false, 2));

        // APPEND with fixed bucket files on a different partition should be ignored
        write1.close();
        commit1.close();
        write1 = fixedBucketWriteTable.newWrite(user1);
        commit1 = fixedBucketWriteTable.newCommit(user1);
        write1.write(GenericRow.of(2, 4, 4L));
        commit1.commit(3, write1.prepareCommit(false, 3));

        write2.write(GenericRow.of(1, 5, 5L));
        assertThatCode(() -> commit2.commit(3, write2.prepareCommit(false, 3)))
                .doesNotThrowAnyException();

        // APPEND with fixed bucket files on the overwritten partition should be checked
        write1.close();
        commit1.close();
        write1 = fixedBucketWriteTable.newWrite(user1);
        commit1 = fixedBucketWriteTable.newCommit(user1);
        write1.write(GenericRow.of(1, 6, 6L));
        commit1.commit(4, write1.prepareCommit(false, 4));

        write2.write(GenericRow.of(1, 7, 7L));
        assertThatThrownBy(() -> commit2.commit(4, write2.prepareCommit(false, 4)))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining(
                        "Giving up committing as commit.strict-mode.enabled is true.");

        write1.close();
        commit1.close();
        write2.close();
        commit2.close();
    }

    private static BinaryRow partitionRow(int pt) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, pt);
        writer.complete();
        return row;
    }

    @Test
    public void testStrictModeScanStateNotLeakedAcrossSnapshots() throws Exception {
        // Regression test: the FileStoreScan used by snapshot conflict checks is mutable.
        // When the APPEND branch (for an earlier APPEND snapshot) calls
        // onlyReadRealBuckets(), the flag is latched on the shared scan instance and
        // must not leak into the later COMPACT/OVERWRITE branch, otherwise entries
        // whose bucket < 0 (e.g. postpone bucket) are silently filtered out and an
        // overlapping OVERWRITE snapshot can be missed.
        String path = tempDir.toString();
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"pt", "k", "v"});

        Options options = new Options();
        options.set(CoreOptions.PATH, path);
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER, 10);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), new Path(path)),
                        new Schema(
                                rowType.getFields(),
                                Collections.singletonList("pt"),
                                Arrays.asList("pt", "k"),
                                options.toMap(),
                                ""));
        FileStoreTable fixedTable =
                FileStoreTableFactory.create(
                        LocalFileIO.create(),
                        new Path(path),
                        tableSchema,
                        CatalogEnvironment.empty());

        // Switch to postpone bucket so that APPEND/OVERWRITE files land in bucket=-2.
        Map<String, String> postponeOptions = new HashMap<>();
        postponeOptions.put(CoreOptions.BUCKET.key(), "-2");
        postponeOptions.put(CoreOptions.POSTPONE_BATCH_WRITE_FIXED_BUCKET.key(), "false");
        FileStoreTable postponeTable = fixedTable.copy(postponeOptions);

        String user1 = UUID.randomUUID().toString();

        // snapshot 1: APPEND on pt=1 with postpone bucket (bucket=-2).
        // When later checked in the APPEND branch, onlyReadRealBuckets() will filter
        // these entries out, so this snapshot alone does not throw; but it DOES
        // latch onlyReadRealBuckets=true onto the shared scan instance.
        TableWriteImpl<?> write1 = postponeTable.newWrite(user1);
        TableCommitImpl commit1 = postponeTable.newCommit(user1);
        write1.write(GenericRow.of(1, 0, 0L));
        commit1.commit(1, write1.prepareCommit(false, 1));
        write1.close();
        commit1.close();

        // snapshot 2: OVERWRITE on pt=1, also writing postpone bucket files.
        // The OVERWRITE branch should detect the partition overlap against a later
        // user's OVERWRITE on pt=1.
        write1 = postponeTable.newWrite(user1);
        commit1 = postponeTable.newCommit(user1).withOverwrite(singletonMap("pt", "1"));
        write1.write(GenericRow.of(1, 1, 1L));
        commit1.commit(2, write1.prepareCommit(false, 2));
        write1.close();
        commit1.close();

        // user2 commits OVERWRITE on pt=1 with strict mode enabled.
        // Expected: throw because snapshot 2 (another user's OVERWRITE) touches pt=1.
        // With the leaking onlyReadRealBuckets flag, snapshot 2's bucket=-2 entries
        // get filtered out and the conflict is silently missed.
        String user2 = UUID.randomUUID().toString();
        FileStoreTable tableWithStrict =
                fixedTable.copy(singletonMap(COMMIT_LAST_SAFE_SNAPSHOT.key(), "0"));
        TableWriteImpl<?> write2 = tableWithStrict.newWrite(user2);
        TableCommitImpl commit2 =
                tableWithStrict.newCommit(user2).withOverwrite(singletonMap("pt", "1"));
        write2.write(GenericRow.of(1, 2, 2L));
        assertThatThrownBy(() -> commit2.commit(3, write2.prepareCommit(false, 3)))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining(
                        "Giving up committing as commit.strict-mode.enabled is true.");

        write2.close();
        commit2.close();
    }

    @Test
    public void testStrictModeForNonOverwriteNoCheckAppend() throws Exception {
        String path = tempDir.toString();
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"k", "v"});

        Options options = new Options();
        options.set(CoreOptions.PATH, path);
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER, 10);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), new Path(path)),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.singletonList("k"),
                                options.toMap(),
                                ""));
        FileStoreTable table =
                FileStoreTableFactory.create(
                        LocalFileIO.create(),
                        new Path(path),
                        tableSchema,
                        CatalogEnvironment.empty());
        String user1 = UUID.randomUUID().toString();
        FileStoreTable fixedBucketWriteTable = table;
        TableWriteImpl<?> write1 = fixedBucketWriteTable.newWrite(user1);
        TableCommitImpl commit1 = fixedBucketWriteTable.newCommit(user1);

        write1.write(GenericRow.of(0, 0L));
        write1.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit1.commit(1, write1.prepareCommit(true, 1));

        // test skip this commit check

        String user2 = UUID.randomUUID().toString();
        table = table.copy(singletonMap(COMMIT_LAST_SAFE_SNAPSHOT.key(), "2"));
        TableWriteImpl<?> write2 = table.newWrite(user2);
        TableCommitImpl commit2 = table.newCommit(user2);

        write2.write(GenericRow.of(1, 1L));
        commit2.commit(1, write2.prepareCommit(false, 1));

        // Non overwrite doesn't check APPEND with fixed bucket files
        write1 = fixedBucketWriteTable.newWrite(user1);
        commit1 = fixedBucketWriteTable.newCommit(user1);
        write1.write(GenericRow.of(2, 2L));
        commit1.commit(2, write1.prepareCommit(false, 2));

        write2.write(GenericRow.of(3, 3L));
        commit2.commit(2, write2.prepareCommit(false, 3));
    }

    @Test
    public void testStrictModeForNonPartitionedTable() throws Exception {
        String path = tempDir.toString();
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"k", "v"});

        Options options = new Options();
        options.set(CoreOptions.PATH, path);
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER, 10);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), new Path(path)),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.singletonList("k"),
                                options.toMap(),
                                ""));
        FileStoreTable table =
                FileStoreTableFactory.create(
                        LocalFileIO.create(),
                        new Path(path),
                        tableSchema,
                        CatalogEnvironment.empty());

        String user1 = UUID.randomUUID().toString();
        TableWriteImpl<?> write1 = table.newWrite(user1);
        TableCommitImpl commit1 = table.newCommit(user1);
        write1.write(GenericRow.of(0, 0L));
        write1.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit1.commit(1, write1.prepareCommit(true, 1));

        // test skip this commit check
        String user2 = UUID.randomUUID().toString();
        FileStoreTable tableWithStrict =
                table.copy(singletonMap(COMMIT_LAST_SAFE_SNAPSHOT.key(), "2"));
        TableWriteImpl<?> write2 = tableWithStrict.newWrite(user2);
        TableCommitImpl commit2 = tableWithStrict.newCommit(user2);

        write2.write(GenericRow.of(1, 1L));
        commit2.commit(1, write2.prepareCommit(false, 1));

        // For a non-partitioned table, any COMPACT snapshot from another user must be
        // detected as a conflict since all entries share BinaryRow.EMPTY_ROW.
        write1.write(GenericRow.of(2, 2L));
        write1.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit1.commit(2, write1.prepareCommit(true, 2));

        write2.write(GenericRow.of(3, 3L));
        assertThatThrownBy(() -> commit2.commit(2, write2.prepareCommit(false, 2)))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining(
                        "Giving up committing as commit.strict-mode.enabled is true.");

        // APPEND with fixed bucket files should be caught when user2 commits OVERWRITE,
        // since non-partitioned table entries share BinaryRow.EMPTY_ROW so they always
        // overlap with user2's OVERWRITE target.
        TableWriteImpl<?> write1Append = table.newWrite(user1);
        TableCommitImpl commit1Append = table.newCommit(user1);
        write1Append.write(GenericRow.of(4, 4L));
        commit1Append.commit(3, write1Append.prepareCommit(false, 3));

        FileStoreTable tableWithStrict2 =
                table.copy(singletonMap(COMMIT_LAST_SAFE_SNAPSHOT.key(), "3"));
        TableWriteImpl<?> write2Ow = tableWithStrict2.newWrite(user2);
        TableCommitImpl commit2Ow =
                tableWithStrict2.newCommit(user2).withOverwrite(Collections.emptyMap());
        write2Ow.write(GenericRow.of(5, 5L));
        assertThatThrownBy(() -> commit2Ow.commit(4, write2Ow.prepareCommit(false, 4)))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining(
                        "Giving up committing as commit.strict-mode.enabled is true.");

        write1Append.close();
        commit1Append.close();
        write2Ow.close();
        commit2Ow.close();

        write1.close();
        commit1.close();
        write2.close();
        commit2.close();
    }

    @Test
    public void testExpireForEmptyCommit() throws Exception {
        String path = tempDir.toString();
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"k", "v"});

        Options options = new Options();
        options.set(CoreOptions.PATH, path);
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX, 2);
        options.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN, 2);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), new Path(path)),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.singletonList("k"),
                                options.toMap(),
                                ""));
        FileStoreTable table =
                FileStoreTableFactory.create(
                        LocalFileIO.create(),
                        new Path(path),
                        tableSchema,
                        CatalogEnvironment.empty());
        SnapshotManager snapshotManager = table.snapshotManager();
        String user1 = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(user1);
        TableCommitImpl commit = table.copy(singletonMap("write-only", "true")).newCommit(user1);

        for (int i = 0; i < 5; i++) {
            write.write(GenericRow.of(i, (long) i));
            commit.commit(i, write.prepareCommit(true, i));
        }
        assertThat(snapshotManager.earliestSnapshotId()).isEqualTo(1);
        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(6);

        // expire for empty commit: false
        commit = table.newCommit(user1).ignoreEmptyCommit(true).expireForEmptyCommit(false);
        commit.commit(7, write.prepareCommit(true, 7));
        assertThat(snapshotManager.earliestSnapshotId()).isEqualTo(1);
        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(6);

        // expire for empty commit: default true
        commit = table.newCommit(user1).ignoreEmptyCommit(true);
        commit.commit(7, write.prepareCommit(true, 7));
        assertThat(snapshotManager.earliestSnapshotId()).isEqualTo(5);
        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(6);
    }

    @Test
    public void testRecoverCompactedChangelogFiles() throws Exception {
        String path = tempDir.toString();
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"k", "v"});

        Options options = new Options();
        options.set(CoreOptions.PATH, path);
        options.set(CoreOptions.BUCKET, 3);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), new Path(path)),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.singletonList("k"),
                                options.toMap(),
                                ""));

        FileStoreTable table =
                FileStoreTableFactory.create(
                        LocalFileIO.create(),
                        new Path(path),
                        tableSchema,
                        CatalogEnvironment.empty());

        // Create fake compacted changelog files that should resolve to real files
        String realChangelogFile =
                "compacted-changelog-8e049c65-5ce4-4ce7-b1b0-78ce694ab351$0-39253.cc-parquet";
        String fakeChangelogFile1 =
                "compacted-changelog-8e049c65-5ce4-4ce7-b1b0-78ce694ab351$0-39253-39253-35699.cc-parquet";
        String fakeChangelogFile2 =
                "compacted-changelog-8e049c65-5ce4-4ce7-b1b0-78ce694ab351$0-39253-74952-37725.cc-parquet";

        // Create directory structure
        Path bucket0Dir = new Path(path, "bucket-0");
        Path bucket1Dir = new Path(path, "bucket-1");
        Path bucket2Dir = new Path(path, "bucket-2");
        LocalFileIO.create().mkdirs(bucket0Dir);
        LocalFileIO.create().mkdirs(bucket1Dir);
        LocalFileIO.create().mkdirs(bucket2Dir);

        // Create the real compacted changelog file
        Path realFilePath = new Path(bucket0Dir, realChangelogFile);
        LocalFileIO.create().newOutputStream(realFilePath, false).close();

        DataFileMeta realFileMeta =
                DataFileMeta.forAppend(
                        realChangelogFile,
                        3000L,
                        300L,
                        SimpleStats.EMPTY_STATS,
                        0L,
                        0L,
                        1L,
                        Collections.emptyList(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);

        // Create fake DataFileMeta for compacted changelog files
        DataFileMeta fakeFileMeta1 =
                DataFileMeta.forAppend(
                        fakeChangelogFile1,
                        1000L,
                        100L,
                        SimpleStats.EMPTY_STATS,
                        0L,
                        0L,
                        1L,
                        Collections.emptyList(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);

        DataFileMeta fakeFileMeta2 =
                DataFileMeta.forAppend(
                        fakeChangelogFile2,
                        2000L,
                        200L,
                        SimpleStats.EMPTY_STATS,
                        0L,
                        0L,
                        1L,
                        Collections.emptyList(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);

        // Create commit message with fake compacted changelog files
        BinaryRow partition = BinaryRow.EMPTY_ROW;
        CommitMessageImpl commitMessage0 =
                new CommitMessageImpl(
                        partition,
                        0,
                        3,
                        new DataIncrement(
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.singletonList(realFileMeta)),
                        new CompactIncrement(
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.emptyList()));
        CommitMessageImpl commitMessage1 =
                new CommitMessageImpl(
                        partition,
                        1,
                        3,
                        new DataIncrement(
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.singletonList(fakeFileMeta1)),
                        new CompactIncrement(
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.emptyList()));
        CommitMessageImpl commitMessage2 =
                new CommitMessageImpl(
                        partition,
                        2,
                        3,
                        new DataIncrement(
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.singletonList(fakeFileMeta2)),
                        new CompactIncrement(
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.emptyList()));

        ManifestCommittable committable = new ManifestCommittable(1L);
        committable.addFileCommittable(commitMessage0);
        committable.addFileCommittable(commitMessage1);
        committable.addFileCommittable(commitMessage2);

        String commitUser = UUID.randomUUID().toString();
        try (TableCommitImpl commit = table.newCommit(commitUser)) {
            // This should succeed because fake files resolve to the existing real file
            commit.filterAndCommitMultiple(Collections.singletonList(committable), false);
        }

        // Now delete the real file and test that the check fails
        LocalFileIO.create().delete(realFilePath, false);

        // Create a new committable with a larger identifier to simulate recovery from checkpoint
        // This identifier must be larger than the previously committed identifier (1L)
        ManifestCommittable newCommittable = new ManifestCommittable(2L);
        newCommittable.addFileCommittable(commitMessage0);
        newCommittable.addFileCommittable(commitMessage1);
        newCommittable.addFileCommittable(commitMessage2);

        try (TableCommitImpl commit = table.newCommit(commitUser)) {
            assertThatThrownBy(
                            () ->
                                    commit.filterAndCommitMultiple(
                                            Collections.singletonList(newCommittable), false))
                    .hasMessageContaining(
                            "Cannot recover from this checkpoint because some files in the"
                                    + " snapshot that need to be resubmitted have been deleted");
        }
    }

    @Test
    public void testCompactConflictWithUpgrade() throws Exception {
        String path = tempDir.toString();
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});

        Options options = new Options();
        options.set(CoreOptions.PATH, path);
        options.set(CoreOptions.BUCKET, 1);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), new Path(path)),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.singletonList("k"),
                                options.toMap(),
                                ""));
        FileStoreTable table =
                FileStoreTableFactory.create(
                        LocalFileIO.create(),
                        new Path(path),
                        tableSchema,
                        CatalogEnvironment.empty());
        String user1 = UUID.randomUUID().toString();
        FileStoreTable overwriteUpgrade = table.copy(singletonMap("write-only", "true"));
        TableWriteImpl<?> write1 = overwriteUpgrade.newWrite(user1);
        TableCommitImpl commit1 =
                overwriteUpgrade.newCommit(user1).withOverwrite(Collections.emptyMap());

        String user2 = UUID.randomUUID().toString();
        FileStoreTable compactTable =
                table.copy(singletonMap("compaction.force-up-level-0", "true"));
        TableWriteImpl<?> write2 = compactTable.newWrite(user2);
        TableCommitImpl commit2 = compactTable.newCommit(user2);

        write1.write(GenericRow.of(1, 1));
        write1.write(GenericRow.of(3, 3));

        write2.write(GenericRow.of(2, 2));
        write2.write(GenericRow.of(4, 4));

        commit1.commit(1, write1.prepareCommit(false, 1));
        assertThatThrownBy(() -> commit2.commit(1, write2.prepareCommit(true, 1)))
                .hasMessageContaining("LSM conflicts detected! Give up committing.");

        write1.close();
        commit1.close();
        write2.close();
        commit2.close();
    }

    @Test
    @Timeout(120)
    public void testEndInputMaintenanceKeepsDataFilesOfRetainedTag() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.BUCKET, 1);
        // so the held manifest read does not block the other maintenance pass on a single-CPU JVM
        options.set(CoreOptions.SCAN_MANIFEST_PARALLELISM, 2);
        // keep expired snapshots as changelogs, so changelog expiration deletes their data files
        options.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN, 1);
        options.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX, 1);
        options.set(CoreOptions.CHANGELOG_NUM_RETAINED_MAX, 100);
        HoldingFileIO fileIO = new HoldingFileIO();
        FileStoreTable table = createTable(fileIO, Collections.singletonList("k"), options);
        String user = UUID.randomUUID().toString();
        // write without compaction, which would conflict with the overwrite
        FileStoreTable writeOnlyTable = table.copy(singletonMap("write-only", "true"));

        // Snapshot 2 overwrites the only data file of tag-1, so only tag-1 keeps that file.
        // Snapshots 1 to 5 become changelogs 1 to 5.
        try (TableWriteImpl<?> write = writeOnlyTable.newWrite(user);
                TableCommitImpl commit = table.newCommit(user)) {
            for (int i = 1; i <= 6; i++) {
                write.write(GenericRow.of(i, (long) i));
                commit.withOverwrite(i == 2 ? Collections.emptyMap() : null);
                commit.commit(i, write.prepareCommit(false, i));
                if (i == 1 || i == 3) {
                    table.createTag("tag-" + i, i);
                }
            }
        }

        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(
                CoreOptions.SNAPSHOT_EXPIRE_EXECUTION_MODE.key(),
                CoreOptions.ExpireExecutionMode.ASYNC.toString());
        dynamicOptions.put(CoreOptions.CHANGELOG_NUM_RETAINED_MAX.key(), "2");
        FileStoreTable asyncTable = table.copy(dynamicOptions);
        ChangelogManager changelogManager = asyncTable.changelogManager();
        Set<String> changelog2DeltaManifests =
                asyncTable.store().manifestListFactory().create()
                        .read(changelogManager.changelog(2).deltaManifestList()).stream()
                        .map(ManifestFileMeta::fileName)
                        .collect(Collectors.toSet());

        ExecutorService endInputExecutor = Executors.newSingleThreadExecutor();
        TableCommitImpl commit = asyncTable.newCommit(user);
        // Hold the checkpoint maintenance after it read the deleted files of changelog 2, before
        // it filters them with the files of tag-1. Changelog 1 is deleted just before that.
        HoldingFileIO.Hold checkpointHold =
                fileIO.holdAfterReading(
                        file ->
                                changelog2DeltaManifests.contains(file.getName())
                                        && !changelogManager.longLivedChangelogExists(1));
        try (TableWriteImpl<?> write = writeOnlyTable.newWrite(user)) {
            write.write(GenericRow.of(7, 7L));
            commit.commit(7, write.prepareCommit(false, 7));
            assertThat(checkpointHold.awaitHeld(Duration.ofMinutes(1))).isTrue();

            write.write(GenericRow.of(8, 8L));
            List<CommitMessage> messages = write.prepareCommit(true, COMMIT_IDENTIFIER);
            Future<?> endInput =
                    endInputExecutor.submit(
                            () ->
                                    commit.filterAndCommit(
                                            singletonMap(COMMIT_IDENTIFIER, messages)));
            try {
                endInput.get(5, TimeUnit.SECONDS);
            } catch (TimeoutException ignored) {
                // the end-input maintenance waits for the held maintenance
            }
            checkpointHold.release();
            endInput.get();
        } finally {
            checkpointHold.release();
            commit.close();
            endInputExecutor.shutdownNow();
        }

        // tag-1 is still retained, so it reads the only row of snapshot 1, key 1, from its file
        FileStoreTable tag1 = table.copy(singletonMap(CoreOptions.SCAN_TAG_NAME.key(), "tag-1"));
        List<Integer> keys = new ArrayList<>();
        ReadBuilder readBuilder = tag1.newReadBuilder();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(row -> keys.add(row.getInt(0)));
        }
        assertThat(keys).containsExactly(1);
    }

    @Test
    @Timeout(120)
    public void testEndInputMaintenanceCreatesEveryAutomaticTag() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.BUCKET, -1);
        options.set(
                CoreOptions.SNAPSHOT_EXPIRE_EXECUTION_MODE, CoreOptions.ExpireExecutionMode.ASYNC);
        options.set(CoreOptions.TAG_AUTOMATIC_CREATION, CoreOptions.TagCreationMode.WATERMARK);
        options.set(CoreOptions.TAG_CREATION_PERIOD, CoreOptions.TagCreationPeriod.HOURLY);
        options.set(CoreOptions.SINK_WATERMARK_TIME_ZONE, "UTC");
        HoldingFileIO fileIO = new HoldingFileIO();
        FileStoreTable table = createTable(fileIO, Collections.emptyList(), options);
        SnapshotManager snapshotManager = table.snapshotManager();
        String user = UUID.randomUUID().toString();
        // Snapshot i has a watermark at 10:30 + i hours (UTC). The first snapshot creates a tag
        // at once, and each later watermark passes the end of the next hourly tag. So maintenance
        // passes that run one after another create tags for snapshots 1, 2 and 3.
        long watermark = Instant.parse("2024-01-01T10:30:00Z").toEpochMilli();
        long hour = Duration.ofHours(1).toMillis();

        ExecutorService endInputExecutor = Executors.newSingleThreadExecutor();
        Thread endInputThread = endInputExecutor.submit(Thread::currentThread).get();
        TableCommitImpl commit = table.newCommit(user);
        Thread maintainThread = commit.getMaintainExecutor().submit(Thread::currentThread).get();
        // Hold the maintenance of checkpoint 1 after it read snapshot 1, before it creates a tag.
        HoldingFileIO.Hold checkpointHold =
                fileIO.holdAfterReading(
                        file ->
                                file.getName().equals("snapshot-1")
                                        && Thread.currentThread() == maintainThread);
        // Hold a final maintenance running on the end-input thread after it read snapshot 2,
        // before it creates a tag. The condition on snapshot 3 skips the reads of the commit.
        HoldingFileIO.Hold endInputHold =
                fileIO.holdAfterReading(
                        file ->
                                file.getName().equals("snapshot-2")
                                        && Thread.currentThread() == endInputThread
                                        && snapshotManager.snapshotExists(3));
        try (TableWriteImpl<?> write = table.newWrite(user)) {
            for (int i = 1; i <= 2; i++) {
                write.write(GenericRow.of(i, (long) i));
                commit.commit(committable(i, watermark + i * hour, write.prepareCommit(false, i)));
            }
            assertThat(checkpointHold.awaitHeld(Duration.ofMinutes(1))).isTrue();

            write.write(GenericRow.of(3, 3L));
            ManifestCommittable last =
                    committable(
                            COMMIT_IDENTIFIER,
                            watermark + 3 * hour,
                            write.prepareCommit(true, COMMIT_IDENTIFIER));
            Future<?> endInput =
                    endInputExecutor.submit(
                            () -> commit.filterAndCommitMultiple(singletonList(last)));
            // the end-input maintenance never gets held when it waits for the held maintenance
            endInputHold.awaitHeld(Duration.ofSeconds(5));
            checkpointHold.release();
            // wait for the maintenance queued before
            commit.getMaintainExecutor().submit(() -> {}).get();
            endInputHold.release();
            endInput.get();
        } finally {
            checkpointHold.release();
            endInputHold.release();
            commit.close();
            endInputExecutor.shutdownNow();
        }

        assertThat(table.tagManager().tags().keySet())
                .extracting(Snapshot::id)
                .containsExactly(1L, 2L, 3L);
    }

    @Test
    @Timeout(120)
    public void testRetryEndInputFinishesMaintenanceAfterSnapshotPublished() throws Exception {
        FailingTagFileIO fileIO = new FailingTagFileIO(1);
        FileStoreTable table = createTable(fileIO, Collections.emptyList(), watermarkTagOptions());
        String user = UUID.randomUUID().toString();
        ManifestCommittable last = endInputCommittable(table, user);

        // The first snapshot of a watermark tag period creates its tag at once, so the end-input
        // maintenance writes the tag of snapshot 1. That write fails once, after snapshot 1 is
        // published.
        assertThatThrownBy(() -> filterAndCommit(table, user, last))
                .hasStackTraceContaining(FailingTagFileIO.MESSAGE);
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(1L);
        assertThat(table.tagManager().tags()).isEmpty();

        // CommitterOperator reopens the committer with the same user and retries the exact
        // terminal committable. The data is already committed, so the retry commits nothing, but
        // it finishes the maintenance and creates the tag the failed attempt missed.
        assertThat(filterAndCommit(table, user, last)).isEqualTo(0);
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(1L);
        assertThat(table.tagManager().tags().keySet()).extracting(Snapshot::id).containsExactly(1L);
        assertThat(fileIO.tagWrites.get()).isEqualTo(2);
    }

    @Test
    @Timeout(120)
    public void testRetryEndInputFailsWhileMaintenanceIsStillMissing() throws Exception {
        FailingTagFileIO fileIO = new FailingTagFileIO(2);
        FileStoreTable table = createTable(fileIO, Collections.emptyList(), watermarkTagOptions());
        String user = UUID.randomUUID().toString();
        ManifestCommittable last = endInputCommittable(table, user);

        assertThatThrownBy(() -> filterAndCommit(table, user, last))
                .hasStackTraceContaining(FailingTagFileIO.MESSAGE);

        // The tag write fails again on the retry. The retry must fail too, and not report success
        // while the tag of snapshot 1 is missing, since the committer closes right after it.
        assertThatThrownBy(() -> filterAndCommit(table, user, last))
                .hasStackTraceContaining(FailingTagFileIO.MESSAGE);
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(1L);
        assertThat(table.tagManager().tags()).isEmpty();
        // one failed tag write per attempt, as configured by FailingTagFileIO(2)
        assertThat(fileIO.tagWrites.get()).isEqualTo(2);
    }

    @Test
    @Timeout(120)
    public void testRetryOfCommittedCheckpointDoesNotRunMaintenance() throws Exception {
        FailingTagFileIO fileIO = new FailingTagFileIO(1);
        FileStoreTable table = createTable(fileIO, Collections.emptyList(), watermarkTagOptions());
        String user = UUID.randomUUID().toString();
        long watermark = Instant.parse("2024-01-01T10:30:00Z").toEpochMilli();

        ManifestCommittable checkpoint;
        try (TableWriteImpl<?> write = table.newWrite(user)) {
            write.write(GenericRow.of(1, 1L));
            checkpoint = committable(1, watermark, write.prepareCommit(false, 1));
        }
        // The snapshot is published and its maintenance fails to write the tag. Maintenance of a
        // checkpoint commit runs on the maintain executor and its failure does not fail the commit.
        try (TableCommitImpl commit = table.newCommit(user)) {
            commit.commit(checkpoint);
            commit.getMaintainExecutor().submit(() -> {}).get();
        }
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(1L);
        assertThat(table.tagManager().tags()).isEmpty();
        assertThat(fileIO.tagWrites.get()).isEqualTo(1);

        // The retry of a checkpoint that is already committed is not the end-input commit, so it
        // must not run maintenance. With maintenance it would write the tag, which no longer
        // fails, and the second write below would show up.
        assertThat(filterAndCommit(table, user, checkpoint)).isEqualTo(0);
        assertThat(table.tagManager().tags()).isEmpty();
        assertThat(fileIO.tagWrites.get()).isEqualTo(1);
    }

    private static Options watermarkTagOptions() {
        Options options = new Options();
        options.set(CoreOptions.BUCKET, -1);
        options.set(
                CoreOptions.SNAPSHOT_EXPIRE_EXECUTION_MODE, CoreOptions.ExpireExecutionMode.ASYNC);
        options.set(CoreOptions.TAG_AUTOMATIC_CREATION, CoreOptions.TagCreationMode.WATERMARK);
        options.set(CoreOptions.TAG_CREATION_PERIOD, CoreOptions.TagCreationPeriod.HOURLY);
        options.set(CoreOptions.SINK_WATERMARK_TIME_ZONE, "UTC");
        return options;
    }

    private static ManifestCommittable endInputCommittable(FileStoreTable table, String user)
            throws Exception {
        try (TableWriteImpl<?> write = table.newWrite(user)) {
            write.write(GenericRow.of(1, 1L));
            return committable(
                    COMMIT_IDENTIFIER,
                    Instant.parse("2024-01-01T10:30:00Z").toEpochMilli(),
                    write.prepareCommit(true, COMMIT_IDENTIFIER));
        }
    }

    /** Opens a committer like a restarted CommitterOperator, retries the committable, closes it. */
    private static int filterAndCommit(
            FileStoreTable table, String user, ManifestCommittable committable) throws Exception {
        try (TableCommitImpl commit = table.newCommit(user)) {
            return commit.filterAndCommitMultiple(singletonList(committable));
        }
    }

    private static ManifestCommittable committable(
            long identifier, long watermark, List<CommitMessage> messages) {
        ManifestCommittable committable = new ManifestCommittable(identifier, watermark);
        messages.forEach(committable::addFileCommittable);
        return committable;
    }

    private FileStoreTable createTable(
            LocalFileIO fileIO, List<String> primaryKeys, Options options) throws Exception {
        Path path = new Path(tempDir.toString());
        options.set(CoreOptions.PATH, path.toString());
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"k", "v"});
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(fileIO, path),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                primaryKeys,
                                options.toMap(),
                                ""));
        return FileStoreTableFactory.create(fileIO, path, tableSchema, CatalogEnvironment.empty());
    }

    /** A {@link LocalFileIO} that fails the first writes of tag files and counts all of them. */
    private static class FailingTagFileIO extends LocalFileIO {

        private static final String MESSAGE = "Injected tag write failure";

        private final int failures;
        private final AtomicInteger tagWrites = new AtomicInteger();

        private FailingTagFileIO(int failures) {
            this.failures = failures;
        }

        @Override
        public void overwriteFileUtf8(Path path, String content) throws IOException {
            if (path.getParent().getName().equals("tag")
                    && tagWrites.incrementAndGet() <= failures) {
                throw new IOException(MESSAGE);
            }
            super.overwriteFileUtf8(path, content);
        }
    }

    /**
     * A {@link LocalFileIO} that holds the thread closing a matching file after reading it, until
     * the test releases it. Each hold applies once.
     */
    private static class HoldingFileIO extends LocalFileIO {

        private final List<Hold> holds = new CopyOnWriteArrayList<>();

        private Hold holdAfterReading(Predicate<Path> condition) {
            Hold hold = new Hold(condition);
            holds.add(hold);
            return hold;
        }

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            return new SeekableInputStreamWrapper(super.newInputStream(path)) {
                @Override
                public void close() throws IOException {
                    super.close();
                    for (Hold hold : holds) {
                        hold.holdIfMatches(path);
                    }
                }
            };
        }

        private static class Hold {

            private final Predicate<Path> condition;
            private final AtomicBoolean matched = new AtomicBoolean(false);
            private final CountDownLatch held = new CountDownLatch(1);
            private final CountDownLatch released = new CountDownLatch(1);

            private Hold(Predicate<Path> condition) {
                this.condition = condition;
            }

            private void holdIfMatches(Path path) throws IOException {
                if (released.getCount() > 0
                        && condition.test(path)
                        && matched.compareAndSet(false, true)) {
                    held.countDown();
                    try {
                        released.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new InterruptedIOException();
                    }
                }
            }

            private boolean awaitHeld(Duration timeout) throws InterruptedException {
                return held.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
            }

            private void release() {
                released.countDown();
            }
        }
    }
}
