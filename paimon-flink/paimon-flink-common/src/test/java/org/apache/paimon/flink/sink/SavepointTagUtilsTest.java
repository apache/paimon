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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.tag.Tag;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

/** Tests for {@link SavepointTagUtils}. */
public class SavepointTagUtilsTest extends CommitterTestBase {

    @Test
    public void testIsSavepointTagFor() throws Exception {
        FileStoreTable table = createFileStoreTable();
        Tag tag = createSavepointTag(table, "user", 1L, 1L);

        assertThat(SavepointTagUtils.isSavepointTagFor(tag, "user", 1L)).isTrue();
        assertThat(SavepointTagUtils.isSavepointTagFor(tag, "other-user", 1L)).isFalse();
        assertThat(SavepointTagUtils.isSavepointTagFor(tag, "user", 2L)).isFalse();
    }

    @Test
    public void testIsSavepointTagForWithNullCommitUser() throws Exception {
        FileStoreTable table = createFileStoreTable();
        Tag tag = createSavepointTag(table, "user", 1L, 1L);

        assertThatNullPointerException()
                .isThrownBy(() -> SavepointTagUtils.isSavepointTagFor(tag, null, 1L));
    }

    @Test
    public void testDeleteTagIfMatches() throws Exception {
        FileStoreTable table = createFileStoreTable();
        createSavepointTag(table, "user", 1L, 1L);

        deleteTagIfMatches(table, "user", 1L);

        assertThat(table.tagManager().tagExists(SavepointTagUtils.tagNameOf(1L))).isFalse();
    }

    @Test
    public void testDeleteTagIfMatchesKeepsTagFromDifferentCommitUser() throws Exception {
        FileStoreTable table = createFileStoreTable();
        createSavepointTag(table, "user", 1L, 1L);

        deleteTagIfMatches(table, "other-user", 1L);

        assertThat(table.tagManager().tagExists(SavepointTagUtils.tagNameOf(1L))).isTrue();
    }

    @Test
    public void testDeleteTagIfMatchesKeepsTagForDifferentCommitIdentifier() throws Exception {
        FileStoreTable table = createFileStoreTable();
        createSavepointTag(table, "user", 1L, 2L);

        deleteTagIfMatches(table, "user", 1L);

        assertThat(table.tagManager().tagExists(SavepointTagUtils.tagNameOf(1L))).isTrue();
    }

    @Test
    public void testDeleteTagIfMatchesWhenTagDoesNotExist() throws Exception {
        FileStoreTable table = createFileStoreTable();

        deleteTagIfMatches(table, "user", 1L);

        assertThat(table.tagManager().tagCount()).isEqualTo(0);
    }

    private Tag createSavepointTag(
            FileStoreTable table,
            String commitUser,
            long savepointIdentifier,
            long snapshotCommitIdentifier)
            throws Exception {
        try (StreamTableWrite write =
                        table.newStreamWriteBuilder().withCommitUser(commitUser).newWrite();
                StreamTableCommit commit =
                        table.newStreamWriteBuilder().withCommitUser(commitUser).newCommit()) {
            write.write(GenericRow.of(1, 10L));
            List<CommitMessage> messages = write.prepareCommit(false, snapshotCommitIdentifier);
            commit.commit(snapshotCommitIdentifier, messages);
        }

        String tagName = SavepointTagUtils.tagNameOf(savepointIdentifier);
        table.tagManager()
                .createTag(
                        table.snapshotManager().latestSnapshot(),
                        tagName,
                        table.coreOptions().tagDefaultTimeRetained(),
                        table.store().createTagCallbacks(table),
                        false);
        return table.tagManager().get(tagName).get();
    }

    private void deleteTagIfMatches(
            FileStoreTable table, String commitUser, long commitIdentifier) {
        SavepointTagUtils.deleteTagIfMatches(
                table.tagManager(),
                commitUser,
                commitIdentifier,
                table.store().newTagDeletion(),
                table.snapshotManager(),
                table.store().createTagCallbacks(table));
    }
}
