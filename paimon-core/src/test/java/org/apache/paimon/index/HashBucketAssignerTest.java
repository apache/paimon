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
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableCommit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.time.Duration;
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
                -1,
                -1,
                null);
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
                maxBucketsNum,
                -1,
                null);
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
            assertThat(bucket).isIn(0, 2);
        }
        // partition 2
        hash = 12;
        for (int i = 0; i < 200; i++) {
            int bucket = assigner.assign(row(2), hash += 2);
            assertThat(bucket).isIn(0, 2);
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
            assertThat(bucket).isIn(0, 2);
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
            assertThat(bucket).isIn(1);
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

    private CommitMessage createCommitMessage(
            BinaryRow partition, int bucket, int totalBuckets, IndexFileMeta file) {
        return new CommitMessageImpl(
                partition,
                bucket,
                totalBuckets,
                new DataIncrement(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonList(file),
                        Collections.emptyList()),
                new CompactIncrement(
                        Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));
    }

    @Test
    public void testAssignRestore() throws IOException {
        commit.commit(
                0,
                Arrays.asList(
                        createCommitMessage(
                                row(1),
                                0,
                                3,
                                fileHandler.hashIndex(row(1), 0).write(new int[] {2, 5})),
                        createCommitMessage(
                                row(1),
                                2,
                                3,
                                fileHandler.hashIndex(row(1), 2).write(new int[] {4, 7}))));

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
    public void testAssignRestoreWithUpperBound() throws IOException {
        commit.commit(
                0,
                Arrays.asList(
                        createCommitMessage(
                                row(1),
                                0,
                                3,
                                fileHandler.hashIndex(row(1), 0).write(new int[] {2, 5})),
                        createCommitMessage(
                                row(1),
                                2,
                                3,
                                fileHandler.hashIndex(row(1), 2).write(new int[] {4, 7}))));

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
    public void testIndexEliminate() throws IOException {
        HashBucketAssigner assigner = createAssigner(1, 1, 0);

        // checkpoint 0
        assertThat(assigner.assign(row(1), 0)).isEqualTo(0);
        assertThat(assigner.assign(row(2), 0)).isEqualTo(0);
        assigner.prepareCommit(0);
        commit.commit(
                0,
                Arrays.asList(
                        createCommitMessage(
                                row(1),
                                0,
                                1,
                                fileHandler.hashIndex(row(1), 0).write(new int[] {0})),
                        createCommitMessage(
                                row(2),
                                0,
                                1,
                                fileHandler.hashIndex(row(2), 0).write(new int[] {0}))));

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
                        createCommitMessage(
                                row(1),
                                0,
                                1,
                                fileHandler.hashIndex(row(1), 0).write(new int[] {1}))));

        assigner.prepareCommit(3);
        assertThat(assigner.currentPartitions()).isEmpty();
    }

    /**
     * Test that bucket refresh is triggered when a bucket approaches target capacity, that
     * buckets newly added on disk become discoverable by subsequent assignments, AND that the
     * refresh respects assigner ownership (only buckets owned by this assigner are surfaced).
     *
     * <p>Setup uses two assigners: assigner 0 owns even buckets (0, 2, ...), assigner 1 owns
     * odd buckets (1, 3, ...). Without the fix:
     *
     * <ul>
     *   <li>The async refresh would never run, so bucket 2 (added after load) would stay
     *       invisible.
     *   <li>Even if the refresh ran, it would surface bucket 1 (owned by assigner 1) into
     *       assigner 0's in-memory map, breaking the ownership invariant.
     * </ul>
     */
    @Test
    public void testRefreshTriggeredWhenBucketNearFull() throws IOException {
        // Initial on-disk state seen by assigner 0 at load time: only bucket 0 (its own) with
        // 2 rows. Bucket 1 also exists on disk but is owned by assigner 1.
        commit.commit(
                0,
                Arrays.asList(
                        createCommitMessage(
                                row(1),
                                0,
                                2,
                                fileHandler.hashIndex(row(1), 0).write(new int[] {0, 2})),
                        createCommitMessage(
                                row(1),
                                1,
                                2,
                                fileHandler.hashIndex(row(1), 1).write(new int[] {1, 3}))));

        // numChannels=2, numAssigners=2, assignId=0 -> assigner 0 owns even buckets only.
        // targetBucketRowNumber=5, threshold=2 -> refresh triggers when a bucket reaches 3 rows.
        HashBucketAssigner assigner =
                new HashBucketAssigner(
                        table.snapshotManager(),
                        commitUser,
                        fileHandler,
                        2,
                        2,
                        0,
                        5,
                        -1,
                        2,
                        Duration.ofMillis(1));

        // After the assigner has loaded, simulate another writer (or compaction) committing a
        // new even bucket (2) that this assigner does not yet know about. We also commit
        // additional rows to bucket 1 (owned by assigner 1) to make sure the refresh has
        // something to filter out.
        commit.commit(
                1,
                Arrays.asList(
                        createCommitMessage(
                                row(1),
                                2,
                                2,
                                fileHandler.hashIndex(row(1), 2).write(new int[] {4, 6})),
                        createCommitMessage(
                                row(1),
                                1,
                                2,
                                fileHandler.hashIndex(row(1), 1).write(new int[] {5}))));

        // Push bucket 0 to its near-full threshold (3 rows). This must schedule the async
        // refresh before returning; otherwise bucket 2 stays invisible.
        assertThat(assigner.assign(row(1), 0)).isEqualTo(0); // 3 rows

        // Poll until the async refresh has surfaced bucket 2. Keep assigning even hashes
        // (which belong to assigner 0) and expect at least one of them to land on bucket 2
        // within the timeout. The same loop also asserts ownership: assigner 0 must NEVER
        // return bucket 1, even though bucket 1 has space on disk.
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        int hash = 100;
        boolean bucket2Seen = false;
        while (System.nanoTime() < deadline) {
            int assigned = assigner.assign(row(1), hash);
            assertThat(assigned)
                    .as("assigner 0 must only return buckets it owns (even ones)")
                    .isEven();
            if (assigned == 2) {
                bucket2Seen = true;
                break;
            }
            hash += 2;
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        assertThat(bucket2Seen)
                .as("bucket 2 should become assignable after the async refresh runs")
                .isTrue();
    }

    /**
     * Test that refresh is NOT triggered when threshold is disabled (-1). This ensures backward
     * compatibility with existing behavior.
     */
    @Test
    public void testRefreshDisabledWhenThresholdIsNegative() {
        // Create assigner with threshold=-1 (disabled)
        HashBucketAssigner assigner =
                new HashBucketAssigner(
                        table.snapshotManager(),
                        commitUser,
                        fileHandler,
                        1,
                        1,
                        0,
                        5, // targetBucketRowNumber
                        -1, // maxBucketsNum
                        -1, // minEmptyBucketsBeforeAsyncCheck (DISABLED)
                        Duration.ofHours(1));

        // Assign rows - even when bucket is near full, no refresh should trigger
        assertThat(assigner.assign(row(1), 0)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 1)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 2)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 3)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 4)).isEqualTo(0);

        // Bucket is full, should create new bucket
        assertThat(assigner.assign(row(1), 5)).isEqualTo(1);
    }

    /**
     * Test that refresh respects the minimum refresh interval. Multiple assignments within the
     * interval should not trigger multiple refreshes.
     */
    @Test
    public void testRefreshRespectsMinimumInterval() throws IOException {
        // Create assigner with 10-second minimum refresh interval
        HashBucketAssigner assigner =
                new HashBucketAssigner(
                        table.snapshotManager(),
                        commitUser,
                        fileHandler,
                        1,
                        1,
                        0,
                        5, // targetBucketRowNumber
                        -1,
                        2, // threshold
                        Duration.ofSeconds(10)); // Long interval

        // Setup initial state with bucket on disk
        commit.commit(
                0,
                Collections.singletonList(
                        createCommitMessage(
                                row(1),
                                0,
                                1,
                                fileHandler.hashIndex(row(1), 0).write(new int[] {0, 1}))));

        // Recreate assigner to load from disk
        assigner =
                new HashBucketAssigner(
                        table.snapshotManager(),
                        commitUser,
                        fileHandler,
                        1,
                        1,
                        0,
                        5,
                        -1,
                        2,
                        Duration.ofSeconds(10));

        // First assignment at threshold should trigger refresh
        assertThat(assigner.assign(row(1), 0)).isEqualTo(0); // 3 rows, triggers refresh

        // Immediate subsequent assignments should NOT trigger refresh (within interval)
        assertThat(assigner.assign(row(1), 10)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 11)).isEqualTo(0);

        // All assignments should succeed without issues
        assertThat(assigner.assign(row(1), 12)).isEqualTo(0);
    }

    /**
     * Test refresh with multiple partitions to ensure each partition is handled independently. Data
     * skew scenario where different partitions have different bucket counts.
     */
    @Test
    public void testRefreshWithMultiplePartitionsDataSkew() throws IOException {
        HashBucketAssigner assigner =
                new HashBucketAssigner(
                        table.snapshotManager(),
                        commitUser,
                        fileHandler,
                        1,
                        1,
                        0,
                        5, // targetBucketRowNumber
                        -1,
                        2, // threshold
                        Duration.ofMillis(100));

        // Setup: partition 1 has 1 bucket, partition 2 has 3 buckets (data skew)
        commit.commit(
                0,
                Arrays.asList(
                        createCommitMessage(
                                row(1),
                                0,
                                1,
                                fileHandler.hashIndex(row(1), 0).write(new int[] {0, 1})),
                        createCommitMessage(
                                row(2),
                                0,
                                3,
                                fileHandler.hashIndex(row(2), 0).write(new int[] {10, 11})),
                        createCommitMessage(
                                row(2),
                                1,
                                3,
                                fileHandler.hashIndex(row(2), 1).write(new int[] {12})),
                        createCommitMessage(
                                row(2),
                                2,
                                3,
                                fileHandler.hashIndex(row(2), 2).write(new int[] {13}))));

        // Recreate assigner
        assigner =
                new HashBucketAssigner(
                        table.snapshotManager(),
                        commitUser,
                        fileHandler,
                        1,
                        1,
                        0,
                        5,
                        -1,
                        2,
                        Duration.ofMillis(100));

        // Partition 1: should trigger refresh when reaching threshold
        assertThat(assigner.assign(row(1), 0)).isEqualTo(0); // 3 rows, triggers refresh

        // Partition 2: independently should also trigger refresh for its buckets
        assertThat(assigner.assign(row(2), 10)).isEqualTo(0); // 3 rows, triggers refresh

        // Wait for async refresh
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Both partitions should work correctly
        assertThat(assigner.assign(row(1), 20)).isIn(0, 1);
        assertThat(assigner.assign(row(2), 30)).isIn(0, 1, 2);
    }

    /**
     * Test that refresh correctly discovers buckets freed by compaction. Simulates a scenario where
     * compaction removes data from a full bucket, making it available again.
     */
    @Test
    public void testRefreshDiscoversFreedBucketsAfterCompaction() throws IOException {
        // Initial state: bucket 0 is full (5 rows), bucket 1 has space.
        commit.commit(
                0,
                Arrays.asList(
                        createCommitMessage(
                                row(1),
                                0,
                                2,
                                fileHandler
                                        .hashIndex(row(1), 0)
                                        .write(new int[] {0, 1, 2, 3, 4})),
                        createCommitMessage(
                                row(1),
                                1,
                                2,
                                fileHandler.hashIndex(row(1), 1).write(new int[] {5, 6}))));

        HashBucketAssigner assigner =
                new HashBucketAssigner(
                        table.snapshotManager(),
                        commitUser,
                        fileHandler,
                        1,
                        1,
                        0,
                        5,
                        -1,
                        2,
                        Duration.ofMillis(1));

        // Simulate compaction: bucket 0 now has only 2 rows (freed by compaction). The assigner
        // does not learn about this until it refreshes from disk.
        commit.commit(
                1,
                Collections.singletonList(
                        createCommitMessage(
                                row(1),
                                0,
                                2,
                                fileHandler.hashIndex(row(1), 0).write(new int[] {0, 1}))));

        // Push bucket 1 to its near-full threshold (3 rows >= 5-2). This must schedule the
        // async refresh before returning; otherwise the freed bucket 0 stays invisible.
        assertThat(assigner.assign(row(1), 5)).isEqualTo(1);

        // Poll until bucket 0 (freed by compaction) becomes assignable again. Without the fix
        // the refresh never runs and the loop exhausts the timeout.
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        int hash = 100;
        boolean freedBucketRediscovered = false;
        while (System.nanoTime() < deadline) {
            int assigned = assigner.assign(row(1), hash++);
            if (assigned == 0) {
                freedBucketRediscovered = true;
                break;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        assertThat(freedBucketRediscovered)
                .as("bucket 0 freed by compaction should become assignable after refresh")
                .isTrue();
    }
}
