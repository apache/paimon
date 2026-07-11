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

package org.apache.paimon.spark.procedure;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.TestAppendFileStore;
import org.apache.paimon.TestKeyValueGenerator;
import org.apache.paimon.append.SortCompactCommitMessageRewriter;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.Snapshot.CommitKind.COMPACT;
import static org.apache.paimon.io.DataFileTestUtils.newFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link SortCompactSparkCommit}. */
public class SortCompactSparkCommitTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testPostCommitFailureDoesNotAbortWrittenMessages() throws Exception {
        FileStoreTable table = createAppendTable(Collections.emptyMap());
        DataFileMeta old = newFile("data-0.orc", 0, 0, 100, 100);
        DataFileMeta sorted = newFile("sorted-0.orc", 0, 0, 100, 100);
        DataSplit split =
                DataSplit.builder()
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Collections.singletonList(old))
                        .build();
        CommitMessageImpl written =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        table.coreOptions().bucket(),
                        new DataIncrement(
                                Collections.singletonList(sorted),
                                Collections.emptyList(),
                                Collections.emptyList()),
                        CompactIncrement.emptyIncrement());
        AtomicBoolean tableCommitted = new AtomicBoolean(false);
        AtomicBoolean aborted = new AtomicBoolean(false);
        SortCompactCommitMessageRewriter abortTrackingRewriter =
                new SortCompactCommitMessageRewriter(table, 0L, Collections.singletonList(split)) {
                    @Override
                    public void abortWrittenMessages(List<CommitMessage> writtenMessages) {
                        aborted.set(true);
                        super.abortWrittenMessages(writtenMessages);
                    }
                };

        assertThatThrownBy(
                        () ->
                                SortCompactSparkCommit.commit(
                                        abortTrackingRewriter,
                                        compactMessages -> tableCommitted.set(true),
                                        compactMessages -> {
                                            throw new RuntimeException("post-commit failed");
                                        },
                                        Collections.singletonList(written)))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("post-commit failed");

        assertThat(tableCommitted).isTrue();
        assertThat(aborted).isFalse();
    }

    @Test
    public void testTableCommitFailureAbortsWrittenMessages() throws Exception {
        FileStoreTable table = createAppendTable(Collections.emptyMap());
        DataFileMeta old = newFile("data-0.orc", 0, 0, 100, 100);
        DataFileMeta sorted = newFile("sorted-0.orc", 0, 0, 100, 100);
        DataSplit split =
                DataSplit.builder()
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Collections.singletonList(old))
                        .build();
        CommitMessageImpl written =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        table.coreOptions().bucket(),
                        new DataIncrement(
                                Collections.singletonList(sorted),
                                Collections.emptyList(),
                                Collections.emptyList()),
                        CompactIncrement.emptyIncrement());
        AtomicInteger abortCount = new AtomicInteger(0);
        SortCompactCommitMessageRewriter rewriter =
                new SortCompactCommitMessageRewriter(table, 0L, Collections.singletonList(split)) {
                    @Override
                    public void abortWrittenMessages(List<CommitMessage> writtenMessages) {
                        abortCount.incrementAndGet();
                    }
                };

        assertThatThrownBy(
                        () ->
                                SortCompactSparkCommit.commit(
                                        rewriter,
                                        compactMessages -> {
                                            throw new RuntimeException("table commit failed");
                                        },
                                        compactMessages -> {},
                                        Collections.singletonList(written)))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("table commit failed");

        assertThat(abortCount).hasValue(1);
    }

    @Test
    public void testTableCommitFailureAfterSnapshotDoesNotAbortWrittenMessages() throws Exception {
        TestAppendFileStore store = createAppendStore(Collections.emptyMap());
        FileStoreTable table =
                FileStoreTableFactory.create(
                        store.fileIO(), store.options().path(), store.schema());
        store.commit(store.writeDataFiles(BinaryRow.EMPTY_ROW, 0, Collections.singletonList("data-0.orc")));

        long baseSnapshotId = table.snapshotManager().latestSnapshotId();
        DataFileMeta old = newFile("data-0.orc", 0, 0, 100, 100);
        CommitMessageImpl written =
                store.writeDataFiles(BinaryRow.EMPTY_ROW, 0, Collections.singletonList("sorted-0.orc"));
        DataSplit split =
                DataSplit.builder()
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Collections.singletonList(old))
                        .build();

        AtomicBoolean aborted = new AtomicBoolean(false);
        SortCompactCommitMessageRewriter rewriter =
                new SortCompactCommitMessageRewriter(table, baseSnapshotId, Collections.singletonList(split)) {
                    @Override
                    public void abortWrittenMessages(List<CommitMessage> writtenMessages) {
                        aborted.set(true);
                        super.abortWrittenMessages(writtenMessages);
                    }
                };

        assertThatThrownBy(
                        () ->
                                SortCompactSparkCommit.commit(
                                        rewriter,
                                        compactMessages -> {
                                            try (BatchTableCommit commit =
                                                    table.newBatchWriteBuilder().newCommit()) {
                                                commit.commit(compactMessages);
                                            }
                                            throw new RuntimeException("failed after snapshot");
                                        },
                                        compactMessages -> {},
                                        Collections.singletonList(written)))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("failed after snapshot");

        assertThat(aborted).isFalse();
        assertThat(table.snapshotManager().latestSnapshot().commitKind()).isEqualTo(COMPACT);
    }

    private FileStoreTable createAppendTable(Map<String, String> dynamicOptions) throws Exception {
        TestAppendFileStore store = createAppendStore(dynamicOptions);
        return FileStoreTableFactory.create(
                store.fileIO(), store.options().path(), store.schema());
    }

    private TestAppendFileStore createAppendStore(Map<String, String> dynamicOptions)
            throws Exception {
        String root = TraceableFileIO.SCHEME + "://" + tempDir.toString();
        Path path = new Path(tempDir.toUri());
        FileIO fileIO = FileIOFinder.find(new Path(root));
        SchemaManager schemaManage = new SchemaManager(new LocalFileIO(), path);

        Map<String, String> options = new HashMap<>(dynamicOptions);
        options.put(CoreOptions.PATH.key(), root);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        schemaManage,
                        new Schema(
                                TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFields(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                options,
                                null));
        return new TestAppendFileStore(
                fileIO,
                schemaManage,
                new CoreOptions(options),
                tableSchema,
                RowType.of(),
                RowType.of(),
                TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                (new Path(root)).getName());
    }
}
