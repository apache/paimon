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
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.IndexIncrement;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableCommit;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;

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

    @AfterEach
    public void afterEach() throws Exception {
        commit.close();
    }

    private HashBucketAssigner createAssigner(int numChannels, int numAssigners, int assignId) {
        return new HashBucketAssigner(
                table.snapshotManager(),
                commitUser,
                fileHandler,
                numChannels,
                numAssigners,
                assignId,
                5,
                -1);
    }

    private HashBucketAssigner createAssigner(
            int numChannels, int numAssigners, int assignId, int maxBucketsNum) {
        return new HashBucketAssigner(
                table.snapshotManager(),
                commitUser,
                fileHandler,
                numChannels,
                numAssigners,
                assignId,
                5,
                maxBucketsNum);
    }

    @Test
    public void testAssign() {
        HashBucketAssigner assigner = createAssigner(2, 2, 0);

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

    @Test
    public void testAssignWithUpperBound() {
        HashBucketAssigner assigner = createAssigner(2, 2, 0, 2);

        // assign
        assertThat(assigner.assign(row(1), 0)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 2)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 4)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 6)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 8)).isEqualTo(0);

        // full
        assertThat(assigner.assign(row(1), 10)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 12)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 14)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 16)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 18)).isEqualTo(0);

        // another partition
        assertThat(assigner.assign(row(2), 12)).isEqualTo(0);

        // read assigned
        assertThat(assigner.assign(row(1), 6)).isEqualTo(0);

        // not mine
        assertThatThrownBy(() -> assigner.assign(row(1), 1))
                .hasMessageContaining("This is a bug, record assign id");

        // exceed buckets upper bound
        // partition 1
        int hash = 18;
        for (int i = 0; i < 200; i++) {
            int bucket = assigner.assign(row(1), hash += 2);
            Assertions.assertThat(bucket).isIn(0, 2);
        }
        // partition 2
        hash = 12;
        for (int i = 0; i < 200; i++) {
            int bucket = assigner.assign(row(2), hash += 2);
            Assertions.assertThat(bucket).isIn(0, 2);
        }
    }

    @Test
    public void testAssignWithUpperBoundMultiAssigners() {
        HashBucketAssigner assigner0 = createAssigner(2, 2, 0, 3);
        HashBucketAssigner assigner1 = createAssigner(2, 2, 1, 3);

        // assigner0: assign
        assertThat(assigner0.assign(row(1), 0)).isEqualTo(0);
        assertThat(assigner0.assign(row(1), 2)).isEqualTo(0);
        assertThat(assigner0.assign(row(1), 4)).isEqualTo(0);
        assertThat(assigner0.assign(row(1), 6)).isEqualTo(0);
        assertThat(assigner0.assign(row(1), 8)).isEqualTo(0);

        // assigner0: full
        assertThat(assigner0.assign(row(1), 10)).isEqualTo(2);
        assertThat(assigner0.assign(row(1), 12)).isEqualTo(2);
        assertThat(assigner0.assign(row(1), 14)).isEqualTo(2);
        assertThat(assigner0.assign(row(1), 16)).isEqualTo(2);
        assertThat(assigner0.assign(row(1), 18)).isEqualTo(2);

        // assigner0: exceed buckets upper bound
        int hash = 18;
        for (int i = 0; i < 200; i++) {
            int bucket = assigner0.assign(row(2), hash += 2);
            Assertions.assertThat(bucket).isIn(0, 2);
        }

        // assigner1: assign
        assertThat(assigner1.assign(row(1), 1)).isEqualTo(1);
        assertThat(assigner1.assign(row(1), 3)).isEqualTo(1);
        assertThat(assigner1.assign(row(1), 5)).isEqualTo(1);
        assertThat(assigner1.assign(row(1), 7)).isEqualTo(1);
        assertThat(assigner1.assign(row(1), 9)).isEqualTo(1);

        // assigner1: exceed buckets upper bound
        hash = 9;
        for (int i = 0; i < 200; i++) {
            int bucket = assigner1.assign(row(2), hash += 2);
            Assertions.assertThat(bucket).isIn(1);
        }
    }

    @ParameterizedTest(name = "maxBuckets: {0}")
    @ValueSource(ints = {-1, 1, 2})
    public void testPartitionCopy(int maxBucketsNum) {
        HashBucketAssigner assigner = createAssigner(1, 1, 0, maxBucketsNum);

        BinaryRow partition = row(1);
        assertThat(assigner.assign(partition, 0)).isEqualTo(0);
        assertThat(assigner.assign(partition, 1)).isEqualTo(0);

        partition.setInt(0, 2);
        assertThat(assigner.assign(partition, 5)).isEqualTo(0);
        assertThat(assigner.assign(partition, 6)).isEqualTo(0);

        assertThat(assigner.currentPartitions()).contains(row(1));
        assertThat(assigner.currentPartitions()).contains(row(2));
    }

    private CommitMessage createCommitMessage(BinaryRow partition, int bucket, IndexFileMeta file) {
        return new CommitMessageImpl(
                partition,
                bucket,
                new DataIncrement(
                        Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
                new CompactIncrement(
                        Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
                new IndexIncrement(Collections.singletonList(file)));
    }

    @Test
    public void testAssignRestore() {
        IndexFileMeta bucket0 = fileHandler.writeHashIndex(new int[] {2, 5});
        IndexFileMeta bucket2 = fileHandler.writeHashIndex(new int[] {4, 7});
        commit.commit(
                0,
                Arrays.asList(
                        createCommitMessage(row(1), 0, bucket0),
                        createCommitMessage(row(1), 2, bucket2)));

        HashBucketAssigner assigner0 = createAssigner(3, 3, 0);
        HashBucketAssigner assigner2 = createAssigner(3, 3, 2);

        // read assigned
        assertThat(assigner0.assign(row(1), 2)).isEqualTo(0);
        assertThat(assigner2.assign(row(1), 4)).isEqualTo(2);
        assertThat(assigner0.assign(row(1), 5)).isEqualTo(0);
        assertThat(assigner2.assign(row(1), 7)).isEqualTo(2);

        // new assign
        assertThat(assigner0.assign(row(1), 8)).isEqualTo(0);
        assertThat(assigner0.assign(row(1), 11)).isEqualTo(0);
        assertThat(assigner0.assign(row(1), 14)).isEqualTo(0);
        assertThat(assigner0.assign(row(1), 17)).isEqualTo(3);
    }

    @Test
    public void testAssignRestoreWithUpperBound() {
        IndexFileMeta bucket0 = fileHandler.writeHashIndex(new int[] {2, 5});
        IndexFileMeta bucket2 = fileHandler.writeHashIndex(new int[] {4, 7});
        commit.commit(
                0,
                Arrays.asList(
                        createCommitMessage(row(1), 0, bucket0),
                        createCommitMessage(row(1), 2, bucket2)));

        HashBucketAssigner assigner0 = createAssigner(3, 3, 0, 1);
        HashBucketAssigner assigner2 = createAssigner(3, 3, 2, 1);

        // read assigned
        assertThat(assigner0.assign(row(1), 2)).isEqualTo(0);
        assertThat(assigner2.assign(row(1), 4)).isEqualTo(2);
        assertThat(assigner0.assign(row(1), 5)).isEqualTo(0);
        assertThat(assigner2.assign(row(1), 7)).isEqualTo(2);

        // new assign
        assertThat(assigner0.assign(row(1), 8)).isEqualTo(0);
        assertThat(assigner0.assign(row(1), 11)).isEqualTo(0);
        assertThat(assigner0.assign(row(1), 14)).isEqualTo(0);
        assertThat(assigner2.assign(row(1), 16)).isEqualTo(2);
        // exceed buckets upper bound
        assertThat(assigner0.assign(row(1), 17)).isEqualTo(0);
    }

    @Test
    public void testAssignDecoupled() {
        HashBucketAssigner assigner1 = createAssigner(3, 2, 1);
        assertThat(assigner1.assign(row(1), 0)).isEqualTo(1);
        assertThat(assigner1.assign(row(1), 2)).isEqualTo(1);
        assertThat(assigner1.assign(row(1), 4)).isEqualTo(1);
        assertThat(assigner1.assign(row(1), 6)).isEqualTo(1);
        assertThat(assigner1.assign(row(1), 8)).isEqualTo(1);
        assertThat(assigner1.assign(row(1), 10)).isEqualTo(3);

        HashBucketAssigner assigner2 = createAssigner(3, 2, 2);
        assertThat(assigner2.assign(row(1), 1)).isEqualTo(0);
        assertThat(assigner2.assign(row(1), 3)).isEqualTo(0);
        assertThat(assigner2.assign(row(1), 5)).isEqualTo(0);
        assertThat(assigner2.assign(row(1), 7)).isEqualTo(0);
        assertThat(assigner2.assign(row(1), 9)).isEqualTo(0);
        assertThat(assigner2.assign(row(1), 11)).isEqualTo(2);

        HashBucketAssigner assigner0 = createAssigner(3, 2, 0);
        assertThat(assigner0.assign(row(2), 1)).isEqualTo(0);
        assertThat(assigner0.assign(row(2), 3)).isEqualTo(0);
        assertThat(assigner0.assign(row(2), 5)).isEqualTo(0);
        assertThat(assigner0.assign(row(2), 7)).isEqualTo(0);
        assertThat(assigner0.assign(row(2), 9)).isEqualTo(0);
        assertThat(assigner0.assign(row(2), 11)).isEqualTo(2);
    }

    @Test
    public void testIndexEliminate() {
        HashBucketAssigner assigner = createAssigner(1, 1, 0);

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
}
