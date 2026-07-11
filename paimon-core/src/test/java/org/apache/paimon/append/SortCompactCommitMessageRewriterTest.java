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

package org.apache.paimon.append;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.TestAppendFileStore;
import org.apache.paimon.TestKeyValueGenerator;
import org.apache.paimon.catalog.RenamingSnapshotCommit;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.io.DataFileTestUtils.newFile;
import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link SortCompactCommitMessageRewriter}. */
public class SortCompactCommitMessageRewriterTest {

    @TempDir java.nio.file.Path tempDir;

    private static DataFileMeta asCompactAfter(DataFileMeta file) {
        return file.assignFileSource(FileSource.COMPACT);
    }

    @Test
    public void testRewriteToCompactMessages() throws Exception {
        FileStoreTable table = createAppendTable(Collections.emptyMap());

        DataFileMeta old0 = newFile("data-0.orc", 0, 0, 100, 100);
        DataFileMeta old1 = newFile("data-1.orc", 0, 101, 200, 200);
        DataFileMeta sorted = newFile("sorted-0.orc", 0, 0, 200, 200);

        DataSplit split =
                DataSplit.builder()
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Arrays.asList(old0, old1))
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

        List<CommitMessage> result =
                new SortCompactCommitMessageRewriter(table, 0L, Collections.singletonList(split))
                        .rewrite(Collections.singletonList(written));

        assertThat(result).hasSize(1);
        CommitMessageImpl compact = (CommitMessageImpl) result.get(0);
        assertThat(compact.newFilesIncrement().isEmpty()).isTrue();
        CompactIncrement ci = compact.compactIncrement();
        assertThat(ci.compactBefore()).containsExactly(old0, old1);
        assertThat(ci.compactAfter()).containsExactly(asCompactAfter(sorted));
        assertThat(ci.changelogFiles()).isEmpty();
    }

    @Test
    public void testDetectBatchCompactCommitSucceeded() throws Exception {
        TestAppendFileStore store = createAppendStore(tempDir, Collections.emptyMap());
        FileStoreTable table =
                FileStoreTableFactory.create(
                        store.fileIO(), store.options().path(), store.schema());
        store.commit(
                store.writeDataFiles(
                        BinaryRow.EMPTY_ROW, 0, Collections.singletonList("data-0.orc")));

        long baseSnapshotId = table.snapshotManager().latestSnapshotId();
        DataFileMeta old = newFile("data-0.orc", 0, 0, 100, 100);
        CommitMessageImpl written =
                store.writeDataFiles(
                        BinaryRow.EMPTY_ROW, 0, Collections.singletonList("sorted-0.orc"));
        DataSplit split =
                DataSplit.builder()
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Collections.singletonList(old))
                        .build();

        SortCompactCommitMessageRewriter rewriter =
                new SortCompactCommitMessageRewriter(
                        table, baseSnapshotId, Collections.singletonList(split));
        long snapshotIdBeforeCommit = rewriter.latestSnapshotIdOrZero();
        List<CommitMessage> compactMessages = rewriter.rewrite(Collections.singletonList(written));
        assertThat(rewriter.isBatchCompactCommitSucceeded(snapshotIdBeforeCommit, compactMessages))
                .isFalse();

        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(compactMessages);
        }

        assertThat(rewriter.isBatchCompactCommitSucceeded(snapshotIdBeforeCommit, compactMessages))
                .isTrue();
        assertThat(table.snapshotManager().latestSnapshot().commitKind())
                .isEqualTo(Snapshot.CommitKind.COMPACT);
        assertThat(table.snapshotManager().latestSnapshot().commitIdentifier())
                .isEqualTo(BatchWriteBuilder.COMMIT_IDENTIFIER);
    }

    @Test
    public void testDetectBatchCompactCommitSucceededWhenNewerAppendExists() throws Exception {
        TestAppendFileStore store = createAppendStore(tempDir, Collections.emptyMap());
        FileStoreTable table =
                FileStoreTableFactory.create(
                        store.fileIO(), store.options().path(), store.schema());
        store.commit(
                store.writeDataFiles(
                        BinaryRow.EMPTY_ROW, 0, Collections.singletonList("data-0.orc")));

        long baseSnapshotId = table.snapshotManager().latestSnapshotId();
        DataFileMeta old = newFile("data-0.orc", 0, 0, 100, 100);
        CommitMessageImpl written =
                store.writeDataFiles(
                        BinaryRow.EMPTY_ROW, 0, Collections.singletonList("sorted-0.orc"));
        DataSplit split =
                DataSplit.builder()
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Collections.singletonList(old))
                        .build();

        SortCompactCommitMessageRewriter rewriter =
                new SortCompactCommitMessageRewriter(
                        table, baseSnapshotId, Collections.singletonList(split));
        long snapshotIdBeforeCommit = rewriter.latestSnapshotIdOrZero();

        List<CommitMessage> compactMessages = rewriter.rewrite(Collections.singletonList(written));
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(compactMessages);
        }
        store.commit(
                store.writeDataFiles(
                        BinaryRow.EMPTY_ROW, 0, Collections.singletonList("concurrent.orc")));

        assertThat(table.snapshotManager().latestSnapshot().commitKind())
                .isEqualTo(Snapshot.CommitKind.APPEND);
        assertThat(rewriter.isBatchCompactCommitSucceeded(snapshotIdBeforeCommit, compactMessages))
                .isTrue();
    }

    @Test
    public void testDetectBatchCompactCommitSucceededAcrossExpiredSnapshotGap() throws Exception {
        TestAppendFileStore store = createAppendStore(tempDir, Collections.emptyMap());
        FileStoreTable table =
                FileStoreTableFactory.create(
                        store.fileIO(), store.options().path(), store.schema());
        store.commit(
                store.writeDataFiles(
                        BinaryRow.EMPTY_ROW, 0, Collections.singletonList("data-0.orc")));

        long baseSnapshotId = table.snapshotManager().latestSnapshotId();
        DataFileMeta old = newFile("data-0.orc", 0, 0, 100, 100);
        CommitMessageImpl written =
                store.writeDataFiles(
                        BinaryRow.EMPTY_ROW, 0, Collections.singletonList("sorted-0.orc"));
        DataSplit split =
                DataSplit.builder()
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Collections.singletonList(old))
                        .build();

        SortCompactCommitMessageRewriter rewriter =
                new SortCompactCommitMessageRewriter(
                        table, baseSnapshotId, Collections.singletonList(split));
        long snapshotIdBeforeCommit = rewriter.latestSnapshotIdOrZero();
        List<CommitMessage> compactMessages = rewriter.rewrite(Collections.singletonList(written));

        store.commit(
                store.writeDataFiles(
                        BinaryRow.EMPTY_ROW, 0, Collections.singletonList("filler.orc")));
        long fillerSnapshotId = table.snapshotManager().latestSnapshotId();

        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(compactMessages);
        }
        table.snapshotManager().deleteSnapshot(fillerSnapshotId);

        assertThat(rewriter.isBatchCompactCommitSucceeded(snapshotIdBeforeCommit, compactMessages))
                .isTrue();
    }

    @Test
    public void testDetectBatchCompactCommitSucceededWhenCompactSnapshotExpired() throws Exception {
        TestAppendFileStore store = createAppendStore(tempDir, Collections.emptyMap());
        FileStoreTable table =
                FileStoreTableFactory.create(
                        store.fileIO(), store.options().path(), store.schema());
        store.commit(
                store.writeDataFiles(
                        BinaryRow.EMPTY_ROW, 0, Collections.singletonList("data-0.orc")));

        long baseSnapshotId = table.snapshotManager().latestSnapshotId();
        DataFileMeta old = newFile("data-0.orc", 0, 0, 100, 100);
        CommitMessageImpl written =
                store.writeDataFiles(
                        BinaryRow.EMPTY_ROW, 0, Collections.singletonList("sorted-0.orc"));
        DataSplit split =
                DataSplit.builder()
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Collections.singletonList(old))
                        .build();

        SortCompactCommitMessageRewriter rewriter =
                new SortCompactCommitMessageRewriter(
                        table, baseSnapshotId, Collections.singletonList(split));
        long snapshotIdBeforeCommit = rewriter.latestSnapshotIdOrZero();
        List<CommitMessage> compactMessages = rewriter.rewrite(Collections.singletonList(written));

        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(compactMessages);
        }
        long compactSnapshotId = table.snapshotManager().latestSnapshotId();

        store.commit(
                store.writeDataFiles(
                        BinaryRow.EMPTY_ROW, 0, Collections.singletonList("concurrent.orc")));
        table.snapshotManager().deleteSnapshot(compactSnapshotId);

        assertThat(table.snapshotManager().latestSnapshot().commitKind())
                .isEqualTo(Snapshot.CommitKind.APPEND);
        assertThat(rewriter.isBatchCompactCommitSucceeded(snapshotIdBeforeCommit, compactMessages))
                .isTrue();
    }

    @Test
    public void testDetectBatchCompactCommitSucceededIgnoresUnrelatedCompact() throws Exception {
        TestAppendFileStore store = createAppendStore(tempDir, Collections.emptyMap());
        FileStoreTable table =
                FileStoreTableFactory.create(
                        store.fileIO(), store.options().path(), store.schema());
        store.commit(
                store.writeDataFiles(
                        BinaryRow.EMPTY_ROW, 0, Arrays.asList("data-0.orc", "data-1.orc")));

        long baseSnapshotId = table.snapshotManager().latestSnapshotId();
        DataFileMeta old = newFile("data-0.orc", 0, 0, 100, 100);
        CommitMessageImpl written =
                store.writeDataFiles(
                        BinaryRow.EMPTY_ROW, 0, Collections.singletonList("sorted-0.orc"));
        DataSplit split =
                DataSplit.builder()
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Collections.singletonList(old))
                        .build();

        SortCompactCommitMessageRewriter rewriter =
                new SortCompactCommitMessageRewriter(
                        table, baseSnapshotId, Collections.singletonList(split));
        long snapshotIdBeforeCommit = rewriter.latestSnapshotIdOrZero();
        List<CommitMessage> compactMessages = rewriter.rewrite(Collections.singletonList(written));

        DataFileMeta otherOld = newFile("data-1.orc", 0, 101, 200, 200);
        CommitMessageImpl otherWritten =
                store.writeDataFiles(
                        BinaryRow.EMPTY_ROW, 0, Collections.singletonList("sorted-1.orc"));
        DataSplit otherSplit =
                DataSplit.builder()
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Collections.singletonList(otherOld))
                        .build();
        List<CommitMessage> otherCompactMessages =
                new SortCompactCommitMessageRewriter(
                                table, baseSnapshotId, Collections.singletonList(otherSplit))
                        .rewrite(Collections.singletonList(otherWritten));
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(otherCompactMessages);
        }

        assertThat(rewriter.isBatchCompactCommitSucceeded(snapshotIdBeforeCommit, compactMessages))
                .isFalse();
    }

    @Test
    public void testRewriteMultipleBuckets() throws Exception {
        FileStoreTable table = createAppendTable(Collections.emptyMap());

        DataFileMeta oldBucket0 = newFile("data-0.orc", 0, 0, 100, 100);
        DataFileMeta oldBucket1 = newFile("data-1.orc", 0, 0, 100, 100);
        DataFileMeta sortedBucket0 = newFile("sorted-0.orc", 0, 0, 100, 100);
        DataFileMeta sortedBucket1 = newFile("sorted-1.orc", 0, 0, 100, 100);

        DataSplit split0 =
                DataSplit.builder()
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Collections.singletonList(oldBucket0))
                        .build();
        DataSplit split1 =
                DataSplit.builder()
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(1)
                        .withBucketPath("bucket-1")
                        .withDataFiles(Collections.singletonList(oldBucket1))
                        .build();

        CommitMessageImpl written0 =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        table.coreOptions().bucket(),
                        new DataIncrement(
                                Collections.singletonList(sortedBucket0),
                                Collections.emptyList(),
                                Collections.emptyList()),
                        CompactIncrement.emptyIncrement());
        CommitMessageImpl written1 =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        1,
                        table.coreOptions().bucket(),
                        new DataIncrement(
                                Collections.singletonList(sortedBucket1),
                                Collections.emptyList(),
                                Collections.emptyList()),
                        CompactIncrement.emptyIncrement());

        List<CommitMessage> result =
                new SortCompactCommitMessageRewriter(table, 0L, Arrays.asList(split0, split1))
                        .rewrite(Arrays.asList(written0, written1));

        assertThat(result).hasSize(2);
        for (CommitMessage message : result) {
            CommitMessageImpl compact = (CommitMessageImpl) message;
            assertThat(compact.newFilesIncrement().isEmpty()).isTrue();
            assertThat(compact.compactIncrement().compactBefore()).hasSize(1);
            assertThat(compact.compactIncrement().compactAfter()).hasSize(1);
        }
        CommitMessageImpl compact0 =
                (CommitMessageImpl) result.stream().filter(m -> m.bucket() == 0).findFirst().get();
        assertThat(compact0.compactIncrement().compactBefore()).containsExactly(oldBucket0);
        assertThat(compact0.compactIncrement().compactAfter())
                .containsExactly(asCompactAfter(sortedBucket0));
        CommitMessageImpl compact1 =
                (CommitMessageImpl) result.stream().filter(m -> m.bucket() == 1).findFirst().get();
        assertThat(compact1.compactIncrement().compactBefore()).containsExactly(oldBucket1);
        assertThat(compact1.compactIncrement().compactAfter())
                .containsExactly(asCompactAfter(sortedBucket1));
    }

    @Test
    public void testRewriteKeepsPlannedDeletesIndependentFromOutputBuckets() throws Exception {
        FileStoreTable table = createAppendTable(Collections.emptyMap());

        DataFileMeta oldBucket0 = newFile("data-0.orc", 0, 0, 100, 100);
        DataFileMeta sortedBucket1 = newFile("sorted-1.orc", 0, 0, 100, 100);

        DataSplit split =
                DataSplit.builder()
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withTotalBuckets(2)
                        .withDataFiles(Collections.singletonList(oldBucket0))
                        .build();

        CommitMessageImpl written =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        1,
                        2,
                        new DataIncrement(
                                Collections.singletonList(sortedBucket1),
                                Collections.emptyList(),
                                Collections.emptyList()),
                        CompactIncrement.emptyIncrement());

        List<CommitMessage> result =
                new SortCompactCommitMessageRewriter(table, 0L, Collections.singletonList(split))
                        .rewrite(Collections.singletonList(written));

        assertThat(result).hasSize(2);
        CommitMessageImpl compactDelete =
                (CommitMessageImpl) result.stream().filter(m -> m.bucket() == 0).findFirst().get();
        assertThat(compactDelete.totalBuckets()).isEqualTo(2);
        assertThat(compactDelete.compactIncrement().compactBefore()).containsExactly(oldBucket0);
        assertThat(compactDelete.compactIncrement().compactAfter()).isEmpty();

        CommitMessageImpl compactAdd =
                (CommitMessageImpl) result.stream().filter(m -> m.bucket() == 1).findFirst().get();
        assertThat(compactAdd.totalBuckets()).isEqualTo(2);
        assertThat(compactAdd.compactIncrement().compactBefore()).isEmpty();
        assertThat(compactAdd.compactIncrement().compactAfter())
                .containsExactly(asCompactAfter(sortedBucket1));
    }

    @Test
    public void testRewriteWithDeletionVectors() throws Exception {
        TestAppendFileStore store =
                createAppendStore(
                        tempDir,
                        Collections.singletonMap(
                                CoreOptions.DELETION_VECTORS_ENABLED.key(), "true"));

        // write deletion vectors for two old files
        Map<String, List<Integer>> dvs = new HashMap<>();
        dvs.put("data-0.orc", Arrays.asList(1, 3, 5));
        dvs.put("data-1.orc", Arrays.asList(2, 4, 6));
        CommitMessageImpl dvMessage = store.writeDVIndexFiles(BinaryRow.EMPTY_ROW, 0, dvs);
        store.commit(dvMessage);

        FileStoreTable table =
                FileStoreTableFactory.create(
                        store.fileIO(), store.options().path(), store.schema());
        long baseSnapshotId = table.snapshotManager().latestSnapshotId();

        DataFileMeta old0 = newFile("data-0.orc", 0, 0, 100, 100);
        DataFileMeta old1 = newFile("data-1.orc", 0, 101, 200, 200);
        DataFileMeta sorted = newFile("sorted-0.orc", 0, 0, 200, 200);

        DataSplit split =
                DataSplit.builder()
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Arrays.asList(old0, old1))
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

        List<CommitMessage> result =
                new SortCompactCommitMessageRewriter(
                                table, baseSnapshotId, Collections.singletonList(split))
                        .rewrite(Collections.singletonList(written));

        CommitMessageImpl compact = (CommitMessageImpl) result.get(0);
        // all old files are removed, so their DV index entries must be cleaned up
        assertThat(compact.compactIncrement().deletedIndexFiles()).isNotEmpty();
        assertThat(compact.compactIncrement().newIndexFiles()).isEmpty();
        assertThat(compact.compactIncrement().compactBefore()).containsExactly(old0, old1);
        assertThat(compact.compactIncrement().compactAfter())
                .containsExactly(asCompactAfter(sorted));
        assertThat(compact.newFilesIncrement().isEmpty()).isTrue();
    }

    @Test
    public void testRewriteInputOnlyGroupWithDeletionVectors() throws Exception {
        TestAppendFileStore store =
                createAppendStore(
                        tempDir,
                        Collections.singletonMap(
                                CoreOptions.DELETION_VECTORS_ENABLED.key(), "true"));

        Map<String, List<Integer>> dvs = new HashMap<>();
        dvs.put("data-0.orc", Arrays.asList(1, 3, 5));
        CommitMessageImpl dvMessage = store.writeDVIndexFiles(BinaryRow.EMPTY_ROW, 0, dvs);
        store.commit(dvMessage);

        FileStoreTable table =
                FileStoreTableFactory.create(
                        store.fileIO(), store.options().path(), store.schema());
        long baseSnapshotId = table.snapshotManager().latestSnapshotId();

        DataFileMeta old = newFile("data-0.orc", 0, 0, 100, 100);
        DataSplit split =
                DataSplit.builder()
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Collections.singletonList(old))
                        .build();

        List<CommitMessage> result =
                new SortCompactCommitMessageRewriter(
                                table, baseSnapshotId, Collections.singletonList(split))
                        .rewrite(Collections.emptyList());

        assertThat(result).hasSize(1);
        CommitMessageImpl compact = (CommitMessageImpl) result.get(0);
        assertThat(compact.newFilesIncrement().isEmpty()).isTrue();
        assertThat(compact.compactIncrement().compactBefore()).containsExactly(old);
        assertThat(compact.compactIncrement().compactAfter()).isEmpty();
        assertThat(compact.compactIncrement().deletedIndexFiles()).isNotEmpty();
        assertThat(compact.compactIncrement().newIndexFiles()).isEmpty();
    }

    @Test
    public void testRewritePartialMessagesMustBeMerged() throws Exception {
        FileStoreTable table = createAppendTable(Collections.emptyMap());

        DataFileMeta oldBucket0 = newFile("data-0.orc", 0, 0, 100, 100);
        DataFileMeta oldBucket1 = newFile("data-1.orc", 0, 0, 100, 100);
        DataFileMeta sortedBucket0 = newFile("sorted-0.orc", 0, 0, 100, 100);
        DataFileMeta sortedBucket1 = newFile("sorted-1.orc", 0, 0, 100, 100);

        DataSplit split0 =
                DataSplit.builder()
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Collections.singletonList(oldBucket0))
                        .build();
        DataSplit split1 =
                DataSplit.builder()
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(1)
                        .withBucketPath("bucket-1")
                        .withDataFiles(Collections.singletonList(oldBucket1))
                        .build();

        CommitMessageImpl writtenBucket0 =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        table.coreOptions().bucket(),
                        new DataIncrement(
                                Collections.singletonList(sortedBucket0),
                                Collections.emptyList(),
                                Collections.emptyList()),
                        CompactIncrement.emptyIncrement());
        CommitMessageImpl writtenBucket1 =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        1,
                        table.coreOptions().bucket(),
                        new DataIncrement(
                                Collections.singletonList(sortedBucket1),
                                Collections.emptyList(),
                                Collections.emptyList()),
                        CompactIncrement.emptyIncrement());

        SortCompactCommitMessageRewriter rewriter =
                new SortCompactCommitMessageRewriter(table, 0L, Arrays.asList(split0, split1));

        // Rewriting partial outputs separately would duplicate compactBefore for every commit.
        List<CommitMessage> partial0 = rewriter.rewrite(Collections.singletonList(writtenBucket0));
        List<CommitMessage> partial1 = rewriter.rewrite(Collections.singletonList(writtenBucket1));
        assertThat(partial0).hasSize(2);
        assertThat(partial1).hasSize(2);

        List<CommitMessage> merged =
                rewriter.rewrite(Arrays.asList(writtenBucket0, writtenBucket1));
        assertThat(merged).hasSize(2);
        for (CommitMessage message : merged) {
            CommitMessageImpl compact = (CommitMessageImpl) message;
            assertThat(compact.newFilesIncrement().isEmpty()).isTrue();
            assertThat(compact.compactIncrement().compactBefore()).hasSize(1);
            assertThat(compact.compactIncrement().compactAfter()).hasSize(1);
        }
    }

    @Test
    public void testRewriteSnapshotOrderingKeepsWriterSequenceRange() throws Exception {
        FileStoreTable table = createPrimaryKeyTable(Collections.emptyMap());

        DataFileMeta oldBucket0 = newFile(1, 1);
        DataFileMeta oldBucket1 = newFile(10, 20);
        DataFileMeta sorted =
                DataFileMeta.create(
                        "sorted-0.orc",
                        100,
                        2,
                        DataFileMeta.EMPTY_MIN_KEY,
                        DataFileMeta.EMPTY_MAX_KEY,
                        EMPTY_STATS,
                        EMPTY_STATS,
                        1,
                        20,
                        0,
                        0,
                        Collections.emptyList(),
                        0L,
                        null,
                        FileSource.APPEND,
                        null,
                        null,
                        null,
                        null);

        DataSplit split0 =
                DataSplit.builder()
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Collections.singletonList(oldBucket0))
                        .build();
        DataSplit split1 =
                DataSplit.builder()
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(1)
                        .withBucketPath("bucket-1")
                        .withDataFiles(Collections.singletonList(oldBucket1))
                        .build();

        CommitMessageImpl written =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        1,
                        table.coreOptions().bucket(),
                        new DataIncrement(
                                Collections.singletonList(sorted),
                                Collections.emptyList(),
                                Collections.emptyList()),
                        CompactIncrement.emptyIncrement());

        List<CommitMessage> result =
                new SortCompactCommitMessageRewriter(table, 0L, Arrays.asList(split0, split1))
                        .rewrite(Collections.singletonList(written));

        CommitMessageImpl compact =
                (CommitMessageImpl) result.stream().filter(m -> m.bucket() == 1).findFirst().get();
        DataFileMeta compactAfter = compact.compactIncrement().compactAfter().get(0);
        assertThat(compactAfter.fileSource()).contains(FileSource.COMPACT);
        assertThat(compactAfter.minSequenceNumber()).isEqualTo(1);
        assertThat(compactAfter.maxSequenceNumber()).isEqualTo(20);
    }

    @Test
    public void testRewriteUsesCapturedBaseSnapshotMetadata() throws Exception {
        TestAppendFileStore store =
                createAppendStore(
                        tempDir,
                        Collections.singletonMap(
                                CoreOptions.DELETION_VECTORS_ENABLED.key(), "true"));

        Map<String, List<Integer>> dvs = new HashMap<>();
        dvs.put("data-0.orc", Arrays.asList(1, 3, 5));
        CommitMessageImpl dvMessage = store.writeDVIndexFiles(BinaryRow.EMPTY_ROW, 0, dvs);
        store.commit(dvMessage);

        FileStoreTable table =
                FileStoreTableFactory.create(
                        store.fileIO(), store.options().path(), store.schema());
        long baseSnapshotId = table.snapshotManager().latestSnapshotId();

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

        SortCompactCommitMessageRewriter rewriter =
                new SortCompactCommitMessageRewriter(
                        table, baseSnapshotId, Collections.singletonList(split));
        table.snapshotManager().deleteSnapshot(baseSnapshotId);

        List<CommitMessage> result = rewriter.rewrite(Collections.singletonList(written));

        CommitMessageImpl compact = (CommitMessageImpl) result.get(0);
        assertThat(compact.compactIncrement().deletedIndexFiles()).isNotEmpty();
        assertThat(compact.compactIncrement().compactBefore()).containsExactly(old);
        assertThat(compact.compactIncrement().compactAfter())
                .containsExactly(asCompactAfter(sorted));
    }

    @Test
    public void testPlanMetadataRoundTripSerialization() throws Exception {
        TestAppendFileStore store =
                createAppendStore(
                        tempDir,
                        Collections.singletonMap(
                                CoreOptions.DELETION_VECTORS_ENABLED.key(), "true"));

        Map<String, List<Integer>> dvs = new HashMap<>();
        dvs.put("data-0.orc", Arrays.asList(1, 3, 5));
        CommitMessageImpl dvMessage = store.writeDVIndexFiles(BinaryRow.EMPTY_ROW, 0, dvs);
        store.commit(dvMessage);

        FileStoreTable table =
                FileStoreTableFactory.create(
                        store.fileIO(), store.options().path(), store.schema());
        long baseSnapshotId = table.snapshotManager().latestSnapshotId();
        DataSplit split =
                DataSplit.builder()
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(
                                Collections.singletonList(newFile("data-0.orc", 0, 0, 100, 100)))
                        .build();

        SortCompactPlanMetadata captured =
                SortCompactPlanMetadata.capture(
                        table, baseSnapshotId, Collections.singletonList(split));
        SortCompactPlanMetadata restored;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(captured);
            try (ObjectInputStream ois =
                    new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
                restored = (SortCompactPlanMetadata) ois.readObject();
            }
        }

        Map<BinaryRow, List<IndexManifestEntry>> dvEntries = new HashMap<>();
        Map<BinaryRow, Map<Integer, IndexFileMeta>> hashIndexes = new HashMap<>();
        captured.copyInto(dvEntries, hashIndexes);
        Map<BinaryRow, List<IndexManifestEntry>> restoredDvEntries = new HashMap<>();
        Map<BinaryRow, Map<Integer, IndexFileMeta>> restoredHashIndexes = new HashMap<>();
        restored.copyInto(restoredDvEntries, restoredHashIndexes);

        assertThat(restoredDvEntries).isEqualTo(dvEntries);
        assertThat(restoredHashIndexes).isEqualTo(hashIndexes);
    }

    @Test
    public void testRewriteWithoutCapturedMetadataSkipsDvCleanupWhenBaseSnapshotExpired()
            throws Exception {
        TestAppendFileStore store =
                createAppendStore(
                        tempDir,
                        Collections.singletonMap(
                                CoreOptions.DELETION_VECTORS_ENABLED.key(), "true"));

        Map<String, List<Integer>> dvs = new HashMap<>();
        dvs.put("data-0.orc", Arrays.asList(1, 3, 5));
        CommitMessageImpl dvMessage = store.writeDVIndexFiles(BinaryRow.EMPTY_ROW, 0, dvs);
        store.commit(dvMessage);

        FileStoreTable table =
                FileStoreTableFactory.create(
                        store.fileIO(), store.options().path(), store.schema());
        long baseSnapshotId = table.snapshotManager().latestSnapshotId();

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

        SortCompactPlanMetadata planMetadata =
                SortCompactPlanMetadata.capture(
                        table, baseSnapshotId, Collections.singletonList(split));
        table.snapshotManager().deleteSnapshot(baseSnapshotId);

        List<CommitMessage> withoutCapturedMetadata =
                new SortCompactCommitMessageRewriter(
                                table, baseSnapshotId, Collections.singletonList(split))
                        .rewrite(Collections.singletonList(written));
        assertThat(
                        ((CommitMessageImpl) withoutCapturedMetadata.get(0))
                                .compactIncrement()
                                .deletedIndexFiles())
                .isEmpty();

        List<CommitMessage> withCapturedMetadata =
                new SortCompactCommitMessageRewriter(
                                table,
                                baseSnapshotId,
                                Collections.singletonList(split),
                                planMetadata)
                        .rewrite(Collections.singletonList(written));
        assertThat(
                        ((CommitMessageImpl) withCapturedMetadata.get(0))
                                .compactIncrement()
                                .deletedIndexFiles())
                .isNotEmpty();
    }

    @Test
    public void testRewriteRejectsInlineCompactionOutput() throws Exception {
        FileStoreTable table = createAppendTable(Collections.emptyMap());

        DataFileMeta old = newFile("data-0.orc", 0, 0, 100, 100);
        DataFileMeta l0File = newFile("l0-0.orc", 0, 0, 100, 100);
        DataFileMeta compactedFile = newFile("compacted-0.orc", 0, 0, 100, 100);

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
                                Collections.singletonList(l0File),
                                Collections.emptyList(),
                                Collections.emptyList()),
                        new CompactIncrement(
                                Collections.singletonList(old),
                                Collections.singletonList(compactedFile),
                                Collections.emptyList()));

        SortCompactCommitMessageRewriter rewriter =
                new SortCompactCommitMessageRewriter(table, 0L, Collections.singletonList(split));

        assertThatThrownBy(() -> rewriter.rewrite(Collections.singletonList(written)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("inline compaction changes");
    }

    @Test
    public void testRewriteCleansHashIndexForDynamicBucket() throws Exception {
        FileStoreTable table = createPrimaryKeyTable(Collections.emptyMap());
        IndexFileHandler indexFileHandler = table.store().newIndexFileHandler();
        BinaryRow partition = BinaryRow.EMPTY_ROW;

        IndexFileMeta hashIndex =
                indexFileHandler.hashIndex(partition, 0).write(new int[] {1, 2, 5});
        bootstrapHashIndexSnapshot(table, partition, 0, hashIndex);

        long baseSnapshotId = table.snapshotManager().latestSnapshotId();
        IndexFileMeta baseHashIndex =
                indexFileHandler
                        .scanHashIndex(table.snapshotManager().latestSnapshot(), partition, 0)
                        .orElseThrow(() -> new IllegalStateException("Hash index should exist."));

        DataFileMeta old = newFile("data-0.orc", 0, 0, 100, 100);
        DataFileMeta sorted = newFile("sorted-0.orc", 0, 0, 100, 100);
        DataSplit split =
                DataSplit.builder()
                        .withPartition(partition)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Collections.singletonList(old))
                        .build();

        CommitMessageImpl written =
                new CommitMessageImpl(
                        partition,
                        0,
                        -1,
                        new DataIncrement(
                                Collections.singletonList(sorted),
                                Collections.emptyList(),
                                Collections.emptyList()),
                        CompactIncrement.emptyIncrement());

        List<CommitMessage> result =
                new SortCompactCommitMessageRewriter(
                                table, baseSnapshotId, Collections.singletonList(split))
                        .rewrite(Collections.singletonList(written));

        CommitMessageImpl compact = (CommitMessageImpl) result.get(0);
        assertThat(compact.compactIncrement().deletedIndexFiles()).contains(baseHashIndex);
    }

    private void bootstrapHashIndexSnapshot(
            FileStoreTable table, BinaryRow partition, int bucket, IndexFileMeta hashIndex)
            throws Exception {
        IndexManifestFile indexManifestFile = table.store().indexManifestFileFactory().create();
        String indexManifest =
                indexManifestFile.writeWithoutRolling(
                        Collections.singletonList(
                                new IndexManifestEntry(
                                        FileKind.ADD, partition, bucket, hashIndex)));
        Snapshot snapshot =
                new Snapshot(
                        Snapshot.FIRST_SNAPSHOT_ID,
                        table.schema().id(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        indexManifest,
                        "test-user",
                        0L,
                        Snapshot.CommitKind.APPEND,
                        System.currentTimeMillis(),
                        0L,
                        0L,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);
        new RenamingSnapshotCommit(table.snapshotManager(), Lock.empty())
                .commit(snapshot, table.snapshotManager().branch(), Collections.emptyList());
    }

    private FileStoreTable createPrimaryKeyTable(Map<String, String> dynamicOptions)
            throws Exception {
        String root = TraceableFileIO.SCHEME + "://" + tempDir.toString();
        Path path = new Path(tempDir.toUri());
        FileIO fileIO = FileIOFinder.find(new Path(root));
        SchemaManager schemaManage = new SchemaManager(new LocalFileIO(), path);

        Map<String, String> options = new HashMap<>(dynamicOptions);
        options.put(CoreOptions.PATH.key(), root);
        options.put(CoreOptions.BUCKET.key(), "-1");
        options.put(CoreOptions.WRITE_ONLY.key(), "true");
        options.put(CoreOptions.SEQUENCE_SNAPSHOT_ORDERING.key(), "true");
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        schemaManage,
                        new Schema(
                                Arrays.asList(
                                        new org.apache.paimon.types.DataField(
                                                0, "k", org.apache.paimon.types.DataTypes.INT()),
                                        new org.apache.paimon.types.DataField(
                                                1, "v", org.apache.paimon.types.DataTypes.INT())),
                                Collections.emptyList(),
                                Collections.singletonList("k"),
                                options,
                                null));
        return FileStoreTableFactory.create(fileIO, new CoreOptions(options).path(), tableSchema);
    }

    private FileStoreTable createAppendTable(Map<String, String> dynamicOptions) throws Exception {
        TestAppendFileStore store = createAppendStore(tempDir, dynamicOptions);
        return FileStoreTableFactory.create(store.fileIO(), store.options().path(), store.schema());
    }

    private TestAppendFileStore createAppendStore(
            java.nio.file.Path tempDir, Map<String, String> dynamicOptions) throws Exception {
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
