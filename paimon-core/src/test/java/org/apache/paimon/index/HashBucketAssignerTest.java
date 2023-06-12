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

import org.apache.paimon.catalog.PrimaryKeyTableTestBase;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.IndexIncrement;
import org.apache.paimon.io.NewFilesIncrement;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableCommit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link HashBucketAssigner}. */
public class HashBucketAssignerTest extends PrimaryKeyTableTestBase {

    private IndexFileHandler fileHandler;
    private StreamTableCommit commit;

    @BeforeEach
    public void beforeEach() throws Exception {
        fileHandler = table.store().newIndexFileHandler();
        commit = table.newStreamWriteBuilder().withCommitUser(commitUser).newCommit();
    }

    private HashBucketAssigner createAssigner(int numAssigners, int assignId) {
        return new HashBucketAssigner(
                table.snapshotManager(), commitUser, fileHandler, numAssigners, assignId, 5);
    }

    @Test
    public void testAssign() {
        HashBucketAssigner assigner = createAssigner(2, 0);

        // assign
        assertThat(assigner.assign(row(1), 0)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 2)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 4)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 6)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 8)).isEqualTo(0);

        // full
        assertThat(assigner.assign(row(1), 10)).isEqualTo(2);

        // another partition
        assertThat(assigner.assign(row(2), 12)).isEqualTo(0);

        // read assigned
        assertThat(assigner.assign(row(1), 6)).isEqualTo(0);

        // not mine
        assertThatThrownBy(() -> assigner.assign(row(1), 1))
                .hasMessageContaining("This is a bug, record assign id");
    }

    private CommitMessage createCommitMessage(BinaryRow partition, int bucket, IndexFileMeta file) {
        return new CommitMessageImpl(
                partition,
                bucket,
                new NewFilesIncrement(Collections.emptyList(), Collections.emptyList()),
                new CompactIncrement(
                        Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
                new IndexIncrement(Collections.singletonList(file)));
    }

    @Test
    public void testAssignRestore() {
        IndexFileMeta bucket0 = fileHandler.writeHashIndex(new int[] {0, 2});
        IndexFileMeta bucket1 = fileHandler.writeHashIndex(new int[] {3, 5});
        commit.commit(
                0,
                Arrays.asList(
                        createCommitMessage(row(1), 0, bucket0),
                        createCommitMessage(row(1), 1, bucket1)));

        HashBucketAssigner assigner0 = createAssigner(3, 0);
        HashBucketAssigner assigner2 = createAssigner(3, 2);

        // read assigned
        assertThat(assigner0.assign(row(1), 0)).isEqualTo(0);
        assertThat(assigner2.assign(row(1), 2)).isEqualTo(0);
        assertThat(assigner0.assign(row(1), 3)).isEqualTo(1);
        assertThat(assigner2.assign(row(1), 5)).isEqualTo(1);

        // new assign
        assertThat(assigner0.assign(row(1), 6)).isEqualTo(0);
        assertThat(assigner0.assign(row(1), 9)).isEqualTo(0);
        assertThat(assigner0.assign(row(1), 12)).isEqualTo(0);
        assertThat(assigner0.assign(row(1), 15)).isEqualTo(3);
    }

    @Test
    public void testIndexEliminate() {
        HashBucketAssigner assigner = createAssigner(1, 0);

        // checkpoint 0
        assertThat(assigner.assign(row(1), 0)).isEqualTo(0);
        assertThat(assigner.assign(row(2), 0)).isEqualTo(0);
        assigner.prepareCommit(0);
        commit.commit(
                0,
                Arrays.asList(
                        createCommitMessage(row(1), 0, fileHandler.writeHashIndex(new int[] {0})),
                        createCommitMessage(row(2), 0, fileHandler.writeHashIndex(new int[] {0}))));
        assertThat(assigner.currentPartitions()).containsExactlyInAnyOrder(row(1), row(2));

        // checkpoint 1, but no commit
        assertThat(assigner.assign(row(1), 1)).isEqualTo(0);
        assigner.prepareCommit(1);
        assertThat(assigner.currentPartitions()).containsExactlyInAnyOrder(row(1));

        // checkpoint 2
        assigner.prepareCommit(2);
        assertThat(assigner.currentPartitions()).containsExactlyInAnyOrder(row(1));

        // checkpoint 3 and commit checkpoint 1
        commit.commit(
                1,
                Collections.singletonList(
                        createCommitMessage(row(1), 0, fileHandler.writeHashIndex(new int[] {1}))));
        assigner.prepareCommit(3);
        assertThat(assigner.currentPartitions()).isEmpty();
    }

    @Test
    public void testFileSize() {
        List<Integer> ints = new ArrayList<>();
        Random rnd = new Random();
        for (int i = 0; i < 10_000_000; i++) {
            ints.add(rnd.nextInt());
        }

        IndexFileMeta file =
                fileHandler.writeHashIndex(ints.stream().mapToInt(Integer::intValue).toArray());
        System.out.println(file.fileName());
        System.out.println(file.fileSize());
    }
}
