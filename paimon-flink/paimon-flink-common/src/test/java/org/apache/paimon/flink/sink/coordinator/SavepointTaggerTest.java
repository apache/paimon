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

package org.apache.paimon.flink.sink.coordinator;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.sink.CommitterTestBase;
import org.apache.paimon.flink.sink.SavepointTagUtils;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link SavepointTagger}. */
public class SavepointTaggerTest extends CommitterTestBase {

    private String commitUser;

    @BeforeEach
    public void before() {
        super.before();
        commitUser = UUID.randomUUID().toString();
    }

    @Test
    public void testTagUpToCreatesTagForCommittedSavepoint() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        SavepointTagger tagger = createTagger(table);

        tagger.add(1L);
        commitSnapshot(table, 1L);
        commitSnapshot(table, 2L);

        tagger.tagUpTo(2L);

        assertThat(table.tagManager().tagExists(savepointTag(1L))).isTrue();
        assertThat(table.tagManager().tagCount()).isEqualTo(1);
    }

    @Test
    public void testTagUpToBoundaryIsInclusive() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        SavepointTagger tagger = createTagger(table);

        tagger.add(2L);
        commitSnapshot(table, 2L);

        // tagUpTo uses headSet(checkpointId, true), so a pending id equal to checkpointId is
        // tagged.
        tagger.tagUpTo(2L);

        assertThat(table.tagManager().tagExists(savepointTag(2L))).isTrue();
    }

    @Test
    public void testTagUpToLeavesLaterPendingUntagged() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        SavepointTagger tagger = createTagger(table);

        tagger.add(5L);
        commitSnapshot(table, 5L);

        // 5 is above the checkpoint watermark, so it stays pending and no tag is created.
        tagger.tagUpTo(4L);

        assertThat(table.tagManager().tagCount()).isEqualTo(0);
    }

    @Test
    public void testTagUpToWithNoPendingCreatesNothing() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        SavepointTagger tagger = createTagger(table);

        commitSnapshot(table, 1L);

        tagger.tagUpTo(1L);

        assertThat(table.tagManager().tagCount()).isEqualTo(0);
    }

    @Test
    public void testTagUpToIsIdempotent() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        SavepointTagger tagger = createTagger(table);

        tagger.add(1L);
        commitSnapshot(table, 1L);
        commitSnapshot(table, 2L);

        // A later checkpoint may re-tag an already-tagged snapshot; the second call is a no-op.
        tagger.tagUpTo(2L);
        tagger.tagUpTo(2L);

        assertThat(table.tagManager().tagCount()).isEqualTo(1);
    }

    @Test
    public void testDropAbortedRemovesCreatedTag() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        SavepointTagger tagger = createTagger(table);

        tagger.add(1L);
        commitSnapshot(table, 1L);
        commitSnapshot(table, 2L);
        tagger.tagUpTo(2L);
        assertThat(table.tagManager().tagCount()).isEqualTo(1);

        // A cumulative commit may have tagged an aborted savepoint; dropping it removes the tag.
        tagger.dropAborted(1L);

        assertThat(table.tagManager().tagCount()).isEqualTo(0);
    }

    @Test
    public void testDropAbortedWithoutTagIsNoop() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        SavepointTagger tagger = createTagger(table);

        commitSnapshot(table, 1L);

        tagger.dropAborted(1L);

        assertThat(table.tagManager().tagCount()).isEqualTo(0);
    }

    @Test
    public void testDropAbortedRemovesPendingBeforeTagging() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        SavepointTagger tagger = createTagger(table);

        tagger.add(1L);
        // Aborted before any tagging round, so the pending intent must not survive to tagUpTo.
        tagger.dropAborted(1L);
        commitSnapshot(table, 1L);
        commitSnapshot(table, 2L);

        tagger.tagUpTo(2L);

        assertThat(table.tagManager().tagCount()).isEqualTo(0);
    }

    @Test
    public void testDropAbortedKeepsTagFromDifferentCommitUser() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        SavepointTagger tagger = createTagger(table);

        createSavepointTag(table, UUID.randomUUID().toString(), 1L, 1L);

        tagger.dropAborted(1L);

        assertThat(table.tagManager().tagExists(savepointTag(1L))).isTrue();
    }

    @Test
    public void testDropAbortedKeepsTagForDifferentCommitIdentifier() throws Exception {
        FileStoreTable table = createUnawareBucketTable();
        SavepointTagger tagger = createTagger(table);

        createSavepointTag(table, commitUser, 1L, 2L);

        tagger.dropAborted(1L);

        assertThat(table.tagManager().tagExists(savepointTag(1L))).isTrue();
    }

    private SavepointTagger createTagger(FileStoreTable table) {
        return new SavepointTagger(
                table.snapshotManager(),
                table.tagManager(),
                table.store().newTagDeletion(),
                table.store().createTagCallbacks(table),
                table.coreOptions().tagDefaultTimeRetained(),
                commitUser);
    }

    private void commitSnapshot(FileStoreTable table, long commitIdentifier) throws Exception {
        commitSnapshot(table, commitUser, commitIdentifier);
    }

    private void createSavepointTag(
            FileStoreTable table,
            String commitUser,
            long commitIdentifier,
            long snapshotCommitIdentifier)
            throws Exception {
        commitSnapshot(table, commitUser, snapshotCommitIdentifier);
        table.tagManager()
                .createTag(
                        table.snapshotManager().latestSnapshot(),
                        savepointTag(commitIdentifier),
                        table.coreOptions().tagDefaultTimeRetained(),
                        table.store().createTagCallbacks(table),
                        false);
    }

    private void commitSnapshot(FileStoreTable table, String commitUser, long commitIdentifier)
            throws Exception {
        try (StreamTableWrite write =
                        table.newStreamWriteBuilder().withCommitUser(commitUser).newWrite();
                StreamTableCommit commit =
                        table.newStreamWriteBuilder().withCommitUser(commitUser).newCommit()) {
            write.write(GenericRow.of((int) commitIdentifier, commitIdentifier));
            List<CommitMessage> messages = write.prepareCommit(false, commitIdentifier);
            commit.commit(commitIdentifier, messages);
        }
    }

    private FileStoreTable createUnawareBucketTable() throws Exception {
        return createFileStoreTable(
                options -> {
                    options.set(CoreOptions.BUCKET, -1);
                    options.remove("bucket-key");
                });
    }

    private static String savepointTag(long checkpointId) {
        return SavepointTagUtils.tagNameOf(checkpointId);
    }
}
