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

package org.apache.paimon.index;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.PrimaryKeyTableTestBase;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.RowKind;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test of {@code dynamic-bucket.reuse-deleted-slots} across the writer and the assigner: the
 * persisted hash index must hold the live keys only, so a restarted assigner sees the freed slots
 * and a key never ends up in two buckets.
 */
public class DynamicBucketReuseDeletedSlotsTest extends PrimaryKeyTableTestBase {

    private IndexFileHandler fileHandler;
    private StreamWriteBuilder writeBuilder;
    private StreamTableWrite write;
    private StreamTableCommit commit;

    @BeforeEach
    public void beforeEach() {
        fileHandler = table.store().newIndexFileHandler();
        writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();
    }

    @AfterEach
    public void afterEach() throws Exception {
        write.close();
        commit.close();
    }

    @Override
    protected Options tableOptions() {
        Options options = new Options();
        options.set(CoreOptions.BUCKET, -1);
        options.set(CoreOptions.DYNAMIC_BUCKET_REUSE_DELETED_SLOTS, true);
        options.set(CoreOptions.DYNAMIC_BUCKET_TARGET_ROW_NUM, 2L);
        return options;
    }

    private HashBucketAssigner newAssigner() {
        return new HashBucketAssigner(
                table.snapshotManager(), commitUser, fileHandler, 1, 1, 0, 2, -1);
    }

    private int[] hashesOf(List<CommitMessage> messages) throws IOException {
        CommitMessageImpl message = (CommitMessageImpl) messages.get(0);
        IndexFileMeta file = message.newFilesIncrement().newIndexFiles().get(0);
        return fileHandler.hashIndex(message.partition(), message.bucket()).readList(file).stream()
                .mapToInt(Integer::intValue)
                .toArray();
    }

    @Test
    public void testDeleteRemovesHashFromPersistedIndex() throws Exception {
        write.write(GenericRow.of(1, 1, 1), 0);
        write.write(GenericRow.of(1, 2, 2), 0);
        commit.commit(0, write.prepareCommit(true, 0));

        // a delete takes its hash out of the persisted index
        write.write(GenericRow.ofKind(RowKind.DELETE, 1, 1, 1), 0);
        List<CommitMessage> messages = write.prepareCommit(true, 1);
        assertThat(hashesOf(messages)).hasSize(1);
        commit.commit(1, messages);

        // -U is the first half of an update and does not remove the key
        write.write(GenericRow.ofKind(RowKind.UPDATE_BEFORE, 1, 2, 2), 0);
        assertThat(write.prepareCommit(true, 2))
                .allSatisfy(
                        m ->
                                assertThat(((CommitMessageImpl) m).newFilesIncrement().newIndexFiles())
                                        .isEmpty());
    }

    @Test
    public void testRestartedAssignerSeesTheFreedSlot() throws Exception {
        // bucket 0 holds two keys, which is the target, so the third key opens bucket 1
        HashBucketAssigner assigner = newAssigner();
        int hash1 = key(1);
        int hash2 = key(2);
        int hash3 = key(3);
        assertThat(assigner.assign(row(1), hash1)).isEqualTo(0);
        assertThat(assigner.assign(row(1), hash2)).isEqualTo(0);
        assertThat(assigner.assign(row(1), hash3)).isEqualTo(1);
        write.write(GenericRow.of(1, 1, 1), 0);
        write.write(GenericRow.of(1, 2, 2), 0);
        write.write(GenericRow.of(1, 3, 3), 1);
        commit.commit(0, write.prepareCommit(true, 0));

        // key 1 is deleted: the assigner frees the slot and the writer drops the hash
        assigner.delete(row(1), hash1);
        write.write(GenericRow.ofKind(RowKind.DELETE, 1, 1, 1), 0);
        commit.commit(1, write.prepareCommit(true, 1));

        // after a restart the counts are rebuilt from the persisted index: bucket 0 has one key
        HashBucketAssigner restarted = newAssigner();
        int hash4 = key(4);
        assertThat(restarted.assign(row(1), hash4)).isEqualTo(0);
        // key 3 stays in bucket 1, and no key is in two buckets
        assertThat(restarted.assign(row(1), hash3)).isEqualTo(1);
        assertThat(restarted.assign(row(1), hash2)).isEqualTo(0);
    }

    /** The hash the assigner and the writer use for the primary key {@code (pk, pt)}. */
    private int key(int pk) {
        return new RowPartitionKeyExtractor(table.schema())
                .trimmedPrimaryKey(GenericRow.of(1, pk, pk))
                .hashCode();
    }
}
