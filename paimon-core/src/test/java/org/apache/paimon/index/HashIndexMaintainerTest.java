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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link HashIndexMaintainer}. */
public class HashIndexMaintainerTest extends PrimaryKeyTableTestBase {

    private IndexFileHandler fileHandler;
    private StreamWriteBuilder writeBuilder;
    private StreamTableWrite write;
    private StreamTableCommit commit;

    @BeforeEach
    public void beforeEach() throws Exception {
        fileHandler = table.store().newIndexFileHandler();
        writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();
    }

    @Override
    protected Options tableOptions() {
        Options options = new Options();
        options.set(CoreOptions.BUCKET, -1);
        return options;
    }

    private Pair<InternalRow, Integer> createRow(int partition, int bucket, int key, int value) {
        return Pair.of(GenericRow.of(partition, key, value), bucket);
    }

    private Map<BinaryRow, Map<Integer, int[]>> readIndex(List<CommitMessage> messages) {
        Map<BinaryRow, Map<Integer, int[]>> index = new HashMap<>();
        for (CommitMessage commitMessage : messages) {
            CommitMessageImpl message = (CommitMessageImpl) commitMessage;
            List<IndexFileMeta> files = message.indexIncrement().newIndexFiles();
            if (files.isEmpty()) {
                continue;
            }
            int[] ints =
                    fileHandler.readHashIndexList(files.get(0)).stream()
                            .mapToInt(Integer::intValue)
                            .toArray();
            index.computeIfAbsent(message.partition(), k -> new HashMap<>())
                    .put(message.bucket(), ints);
        }
        return index;
    }

    @Test
    public void testAssignBucket() throws Exception {
        assertThatThrownBy(() -> write.write(GenericRow.of(1, 1, 1)))
                .hasMessageContaining(
                        "Can't extract bucket from row in dynamic bucket mode, "
                                + "you should use 'TableWrite.write(InternalRow row, int bucket)' method.");

        // commit two partitions
        write(write, createRow(1, 1, 1, 1));
        write(write, createRow(2, 2, 2, 2));
        List<CommitMessage> commitMessages = write.prepareCommit(true, 0);
        Map<BinaryRow, Map<Integer, int[]>> index = readIndex(commitMessages);
        assertThat(index).containsOnlyKeys(row(1), row(2));
        assertThat(index.get(row(1))).containsOnlyKeys(1);
        assertThat(index.get(row(1)).get(1)).containsExactlyInAnyOrder(1465514398);
        assertThat(index.get(row(2)).get(2)).containsExactlyInAnyOrder(1340390384);
        commit.commit(0, commitMessages);

        // only one partition
        write(write, createRow(1, 1, 2, 2));
        commitMessages = write.prepareCommit(true, 1);
        index = readIndex(commitMessages);
        assertThat(index).containsOnlyKeys(row(1));
        assertThat(index.get(row(1))).containsOnlyKeys(1);
        assertThat(index.get(row(1)).get(1)).containsExactlyInAnyOrder(1340390384, 1465514398);
        commit.commit(1, commitMessages);

        // restore
        write = writeBuilder.newWrite();
        write(write, createRow(1, 1, 3, 3));
        commitMessages = write.prepareCommit(true, 2);
        index = readIndex(commitMessages);
        assertThat(index).containsOnlyKeys(row(1));
        assertThat(index.get(row(1))).containsOnlyKeys(1);
        assertThat(index.get(row(1)).get(1))
                .containsExactlyInAnyOrder(-771300025, 1340390384, 1465514398);

        write.close();
        commit.close();
    }

    @Test
    public void testNotCreateNewFile() throws Exception {
        // commit two partitions
        write(write, createRow(1, 1, 1, 1));
        write(write, createRow(2, 2, 2, 2));
        commit.commit(0, write.prepareCommit(true, 0));

        // same record
        write(write, createRow(1, 1, 1, 1));
        List<CommitMessage> commitMessages = write.prepareCommit(true, 1);
        assertThat(readIndex(commitMessages)).isEmpty();

        write.close();
        commit.close();
    }

    private void write(StreamTableWrite write, Pair<InternalRow, Integer> rowWithBucket)
            throws Exception {
        write.write(rowWithBucket.getKey(), rowWithBucket.getValue());
    }
}
