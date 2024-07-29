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

package org.apache.paimon.flink;

import org.apache.paimon.Snapshot;
import org.apache.paimon.branch.TableBranch;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.TagDeletion;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for table with branches using SQL. */
public class BranchSqlITCase extends CatalogITCaseBase {

    protected final FileIO fileIO = new LocalFileIO();

    @Test
    public void testAlterBranchTable() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " pt INT"
                        + ", k INT"
                        + ", v STRING"
                        + ", PRIMARY KEY (pt, k) NOT ENFORCED"
                        + " ) PARTITIONED BY (pt) WITH ("
                        + " 'bucket' = '2'"
                        + " )");

        sql(
                "INSERT INTO T VALUES"
                        + " (1, 10, 'apple'),"
                        + " (1, 20, 'banana'),"
                        + " (2, 10, 'cat'),"
                        + " (2, 20, 'dog')");

        sql("CALL sys.create_branch('default.T', 'test', 1)");

        FileStoreTable branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.schema().fields().size()).isEqualTo(3);

        sql(
                "INSERT INTO T VALUES"
                        + " (1, 10, 'APPLE'),"
                        + " (2, 20, 'DOG'),"
                        + " (2, 30, 'horse')");

        // Add v2 column for branch table.
        sql("ALTER TABLE `T$branch_test` ADD (v2 INT)");

        branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.schema().fields().size()).isEqualTo(4);

        sql(
                "INSERT INTO `T$branch_test` VALUES "
                        + "(1, 10, 'cherry', 100)"
                        + ", (2, 20, 'bird', 200)"
                        + ", (2, 40, 'wolf', 400)");

        assertThat(collectResult("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        "+I[1, 10, APPLE]",
                        "+I[1, 20, banana]",
                        "+I[2, 30, horse]",
                        "+I[2, 10, cat]",
                        "+I[2, 20, DOG]");

        assertThat(collectResult("SELECT * FROM T$branch_test"))
                .containsExactlyInAnyOrder(
                        "+I[1, 10, cherry, 100]",
                        "+I[1, 20, banana, null]",
                        "+I[2, 10, cat, null]",
                        "+I[2, 20, bird, 200]",
                        "+I[2, 40, wolf, 400]");
    }

    @Test
    public void testCreateBranchFromTag() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " pt INT"
                        + ", k INT"
                        + ", v STRING"
                        + ", PRIMARY KEY (pt, k) NOT ENFORCED"
                        + " ) PARTITIONED BY (pt) WITH ("
                        + " 'bucket' = '2'"
                        + " )");

        // snapshot 1.
        sql("INSERT INTO T VALUES" + " (1, 10, 'apple')," + " (1, 20, 'banana')");
        // snapshot 2.
        sql("INSERT INTO T VALUES" + " (2, 10, 'cat')," + " (2, 20, 'dog')");

        sql("CALL sys.create_tag('default.T', 'tag1', 1)");
        sql("CALL sys.create_tag('default.T', 'tag2', 2)");

        sql("CALL sys.create_branch('default.T', 'test', 'tag1')");
        sql("CALL sys.create_branch('default.T', 'test2', 'tag2')");

        FileStoreTable branchTable = paimonTable("T$branch_test");
        assertThat(branchTable.tagManager().tagExists("tag1")).isEqualTo(true);

        assertThat(collectResult("SELECT * FROM T$branch_test"))
                .containsExactlyInAnyOrder("+I[1, 10, apple]", "+I[1, 20, banana]");

        FileStoreTable branchTable2 = paimonTable("T$branch_test2");
        assertThat(branchTable2.tagManager().tagExists("tag2")).isEqualTo(true);

        assertThat(collectResult("SELECT * FROM T$branch_test2"))
                .containsExactlyInAnyOrder(
                        "+I[1, 10, apple]",
                        "+I[1, 20, banana]",
                        "+I[2, 10, cat]",
                        "+I[2, 20, dog]");
    }

    @Test
    public void testCreateBranchFromSnapshot() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " pt INT"
                        + ", k INT"
                        + ", v STRING"
                        + ", PRIMARY KEY (pt, k) NOT ENFORCED"
                        + " ) PARTITIONED BY (pt) WITH ("
                        + " 'bucket' = '2'"
                        + " )");

        // snapshot 1.
        sql("INSERT INTO T VALUES(1, 10, 'apple')");

        // snapshot 2.
        sql("INSERT INTO T VALUES(1, 20, 'dog')");

        sql("CALL sys.create_branch('default.T', 'test', 1)");
        sql("CALL sys.create_branch('default.T', 'test2', 2)");

        assertThat(collectResult("SELECT created_from_snapshot FROM `T$branches`"))
                .containsExactlyInAnyOrder("+I[1]", "+I[2]");

        assertThat(paimonTable("T$branch_test").snapshotManager().snapshotExists(1))
                .isEqualTo(true);

        assertThat(paimonTable("T$branch_test2").snapshotManager().snapshotExists(2))
                .isEqualTo(true);
    }

    @Test
    public void testCreateEmptyBranch() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " pt INT"
                        + ", k INT"
                        + ", v STRING"
                        + ", PRIMARY KEY (pt, k) NOT ENFORCED"
                        + " ) PARTITIONED BY (pt) WITH ("
                        + " 'bucket' = '2'"
                        + " )");

        // snapshot 1.
        sql("INSERT INTO T VALUES(1, 10, 'apple')");

        // snapshot 2.
        sql("INSERT INTO T VALUES(1, 20, 'dog')");

        assertThat(collectResult("SELECT * FROM T"))
                .containsExactlyInAnyOrder("+I[1, 10, apple]", "+I[1, 20, dog]");

        // create en empty branch.
        sql("CALL sys.create_branch('default.T', 'empty_branch')");

        sql("INSERT INTO `T$branch_empty_branch` VALUES (3, 30, 'banana')");

        assertThat(collectResult("SELECT * FROM T$branch_empty_branch"))
                .containsExactlyInAnyOrder("+I[3, 30, banana]");
    }

    @Test
    public void testDeleteBranchTable() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " pt INT"
                        + ", k INT"
                        + ", v STRING"
                        + ", PRIMARY KEY (pt, k) NOT ENFORCED"
                        + " ) PARTITIONED BY (pt) WITH ("
                        + " 'bucket' = '2'"
                        + " )");

        // snapshot 1.
        sql("INSERT INTO T VALUES(1, 10, 'apple')");

        // snapshot 2.
        sql("INSERT INTO T VALUES(1, 20, 'dog')");

        sql("CALL sys.create_branch('default.T', 'test', 1)");
        sql("CALL sys.create_branch('default.T', 'test2', 2)");

        assertThat(collectResult("SELECT branch_name, created_from_snapshot FROM `T$branches`"))
                .containsExactlyInAnyOrder("+I[test, 1]", "+I[test2, 2]");

        sql("CALL sys.delete_branch('default.T', 'test')");

        assertThat(collectResult("SELECT branch_name, created_from_snapshot FROM `T$branches`"))
                .containsExactlyInAnyOrder("+I[test2, 2]");
    }

    @Test
    public void testBranchManagerGetBranchSnapshotsList() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " pt INT"
                        + ", k INT"
                        + ", v STRING"
                        + ", PRIMARY KEY (pt, k) NOT ENFORCED"
                        + " ) PARTITIONED BY (pt) WITH ("
                        + " 'bucket' = '2'"
                        + " )");

        sql("INSERT INTO T VALUES (1, 10, 'hxh')");
        sql("INSERT INTO T VALUES (1, 20, 'hxh')");
        sql("INSERT INTO T VALUES (1, 30, 'hxh')");

        FileStoreTable table = paimonTable("T");
        checkSnapshots(table.snapshotManager(), 1, 3);

        sql("CALL sys.create_branch('default.T', 'test1', 1)");
        sql("CALL sys.create_branch('default.T', 'test2', 2)");
        sql("CALL sys.create_branch('default.T', 'test3', 3)");

        assertThat(collectResult("SELECT created_from_snapshot FROM `T$branches`"))
                .containsExactlyInAnyOrder("+I[1]", "+I[2]", "+I[3]");
    }

    @Test
    public void testBranchFastForward() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " pt INT"
                        + ", k INT"
                        + ", v STRING"
                        + ", PRIMARY KEY (pt, k) NOT ENFORCED"
                        + " ) PARTITIONED BY (pt) WITH ("
                        + " 'bucket' = '2'"
                        + " )");

        FileStoreTable table = paimonTable("T");
        SnapshotManager snapshotManager = table.snapshotManager();

        sql("INSERT INTO T VALUES (1, 10, 'hunter')");
        sql("INSERT INTO T VALUES (1, 20, 'hunter')");
        sql("INSERT INTO T VALUES (1, 30, 'hunter')");

        checkSnapshots(snapshotManager, 1, 3);

        assertThat(collectResult("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        "+I[1, 10, hunter]", "+I[1, 20, hunter]", "+I[1, 30, hunter]");

        sql("CALL sys.create_branch('default.T', 'test', 1)");

        sql("INSERT INTO `T$branch_test` VALUES (2, 10, 'hunterX')");

        checkSnapshots(paimonTable("T$branch_test").snapshotManager(), 1, 2);

        // query branch data.
        assertThat(collectResult("SELECT * FROM T$branch_test"))
                .containsExactlyInAnyOrder("+I[1, 10, hunter]", "+I[2, 10, hunterX]");

        sql("CALL sys.fast_forward('default.T', 'test')");

        // Branch `test` replaces the main branch.
        assertThat(collectResult("SELECT * FROM T"))
                .containsExactlyInAnyOrder("+I[1, 10, hunter]", "+I[2, 10, hunterX]");

        checkSnapshots(snapshotManager, 1, 2);
    }

    /** Expiring Snapshots should skip those referenced by branches. */
    @Test
    public void testSnapshotExpireSkipTheReferencedByBranches() throws Exception {

        sql(
                "CREATE TABLE T ("
                        + " pt INT"
                        + ", k INT"
                        + ", v STRING"
                        + ", PRIMARY KEY (pt, k) NOT ENFORCED"
                        + " ) PARTITIONED BY (pt) WITH ("
                        + " 'bucket' = '2'"
                        + " )");

        FileStoreTable table = paimonTable("T");
        SnapshotManager snapshotManager = table.snapshotManager();
        BranchManager branchManager = table.branchManager();

        sql("INSERT INTO T VALUES (1, 10, 'hunter')");
        sql("INSERT INTO T VALUES (1, 20, 'hunter')");
        sql("INSERT INTO T VALUES (1, 30, 'hunter')");
        sql("INSERT INTO T VALUES (1, 40, 'hunter')");
        sql("INSERT INTO T VALUES (1, 50, 'hunter')");

        checkSnapshots(snapshotManager, 1, 5);

        // create 2 branch from the snapshot-1.
        sql("CALL sys.create_branch('default.T', 'test', 1)");
        sql("CALL sys.create_branch('default.T', 'test_1', 1)");

        // create tag from snapshot 2.
        sql("CALL sys.create_tag('default.T', 'tag', 2)");
        // create branch2 from tag.
        sql("CALL sys.create_branch('default.T', 'test2', 'tag')");

        sql("CALL sys.create_branch('default.T', 'test3', 3)");

        // We have created 4 branches using 3 snapshots.
        assertThat(branchManager.branchesCreateSnapshots().keySet().stream().map(Snapshot::id))
                .containsExactlyInAnyOrder(1L, 2L, 3L);

        // Only retain snapshot 5,6 and snapshot 1-4 will expire. So all referenced snapshots will
        // expire.
        sql(
                "INSERT INTO T /*+ OPTIONS("
                        + "'snapshot.num-retained.min'= '2',"
                        + "'snapshot.num-retained.max'= '2' ) */"
                        + " VALUES (1, 60, 'hunter')");

        checkSnapshots(snapshotManager, 5, 6);

        branchManager = table.branchManager();
        // Snapshot 1-4 has expired, but still be referenced by branches;
        assertThat(branchManager.branchesCreateSnapshots().keySet().stream().map(Snapshot::id))
                .containsExactlyInAnyOrder(1L, 2L, 3L);

        // The data of snapshot 1-3 is still can be read by branches.
        assertThat(collectResult("SELECT * FROM T$branch_test"))
                .containsExactlyInAnyOrder("+I[1, 10, hunter]");

        assertThat(collectResult("SELECT * FROM T$branch_test2"))
                .containsExactlyInAnyOrder("+I[1, 10, hunter]", "+I[1, 20, hunter]");

        assertThat(collectResult("SELECT * FROM T$branch_test3"))
                .containsExactlyInAnyOrder(
                        "+I[1, 10, hunter]", "+I[1, 20, hunter]", "+I[1, 30, hunter]");
    }

    /**
     * Let's say snapshot has expired, but the manifest is still referenced by some branches, so
     * deleting the tag should skip those snapshots.
     */
    @Test
    public void testDeleteTagsSkipTheReferencedByBranches() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " pt INT"
                        + ", k INT"
                        + ", v STRING"
                        + ", PRIMARY KEY (pt, k) NOT ENFORCED"
                        + " ) PARTITIONED BY (pt) WITH ("
                        + " 'bucket' = '2'"
                        + " )");

        FileStoreTable table = paimonTable("T");
        SnapshotManager snapshotManager = table.snapshotManager();
        BranchManager branchManager = table.branchManager();

        sql("INSERT INTO T VALUES (1, 10, 'hunter')");
        sql("INSERT INTO T VALUES (2, 20, 'hunter2')");

        checkSnapshots(snapshotManager, 1, 2);

        // create branch from the snapshot-1.
        sql("CALL sys.create_branch('default.T', 'test', 1)");

        // create tag from snapshot-1.
        sql("CALL sys.create_tag('default.T', 'tag', 1)");

        // Step1: Expire snapshot, only retain snapshot 2,3 and snapshot-1 will expire.
        sql(
                "INSERT INTO T /*+ OPTIONS("
                        + "'snapshot.num-retained.min'= '2',"
                        + "'snapshot.num-retained.max'= '2' ) */"
                        + " VALUES (3, 30, 'hunter3')");

        checkSnapshots(snapshotManager, 2, 3);
        assertThat(branchManager.branchesCreateSnapshots().size()).isEqualTo(1);

        Snapshot referencedSnapshot =
                branchManager.branchesCreateSnapshots().keySet().iterator().next();
        assertThat(referencedSnapshot.id()).isEqualTo(1L);

        // query branches data.
        assertThat(collectResult("SELECT * FROM T$branch_test"))
                .containsExactlyInAnyOrder("+I[1, 10, hunter]");

        // Step2: Delete tag.
        sql("CALL sys.delete_tag('default.T', 'tag')");

        // Step3: Branches data still can be read.
        assertThat(collectResult("SELECT * FROM T$branch_test"))
                .containsExactlyInAnyOrder("+I[1, 10, hunter]");
    }

    @Test
    public void testDeleteBranchSkipTheReferencedByTags() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " pt INT"
                        + ", k INT"
                        + ", v STRING"
                        + ", PRIMARY KEY (pt, k) NOT ENFORCED"
                        + " ) PARTITIONED BY (pt) WITH ("
                        + " 'bucket' = '2'"
                        + " )");

        FileStoreTable table = paimonTable("T");
        SnapshotManager snapshotManager = table.snapshotManager();

        sql("INSERT INTO T VALUES (1, 10, 'hunter')");
        sql("INSERT INTO T VALUES (2, 20, 'hunter2')");

        checkSnapshots(snapshotManager, 1, 2);

        // Create branch from the snapshot 1.
        sql("CALL sys.create_branch('default.T', 'test', 1)");

        sql("CALL sys.create_tag('default.T', 'tag', 1)");

        // Only retain snapshot 2,3 and snapshot 1 will expire.
        sql(
                "INSERT INTO T /*+ OPTIONS("
                        + "'snapshot.num-retained.min'= '2',"
                        + "'snapshot.num-retained.max'= '2' ) */"
                        + " VALUES (3, 30, 'hunter3')");

        // Delete branch.
        sql("CALL sys.delete_branch('default.T', 'test')");

        // The tag still can be read.
        assertThat(collectResult("SELECT * FROM T /*+ OPTIONS('scan.tag-name'='tag') */"))
                .containsExactlyInAnyOrder("+I[1, 10, hunter]");
    }

    @Test
    public void testDeleteBranchSkipTheReferencedByBranches() throws Exception {

        sql(
                "CREATE TABLE T ("
                        + " pt INT"
                        + ", k INT"
                        + ", v STRING"
                        + ", PRIMARY KEY (pt, k) NOT ENFORCED"
                        + " ) PARTITIONED BY (pt) WITH ("
                        + " 'bucket' = '2'"
                        + " )");

        FileStoreTable table = paimonTable("T");
        SnapshotManager snapshotManager = table.snapshotManager();

        sql("INSERT INTO T VALUES (1, 10, 'hunter')");
        sql("INSERT INTO T VALUES (2, 20, 'hunter2')");

        checkSnapshots(snapshotManager, 1, 2);

        // Create branch from the snapshot 1.
        sql("CALL sys.create_branch('default.T', 'test', 1)");

        sql("CALL sys.create_branch('default.T', 'test2', 1)");

        // Only retain snapshot 2,3 and snapshot 1 will expire.
        sql(
                "INSERT INTO T /*+ OPTIONS("
                        + "'snapshot.num-retained.min'= '2',"
                        + "'snapshot.num-retained.max'= '2' ) */"
                        + " VALUES (3, 30, 'hunter3')");

        // Delete branch test.
        sql("CALL sys.delete_branch('default.T', 'test')");

        // branch test2 still can be read.
        assertThat(collectResult("SELECT * FROM T$branch_test2"))
                .containsExactlyInAnyOrder("+I[1, 10, hunter]");
    }

    @Test
    public void testDeleteBranchTriggerCleanSnapshots() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " pt INT"
                        + ", k INT"
                        + ", v STRING"
                        + ", PRIMARY KEY (pt, k) NOT ENFORCED"
                        + " ) PARTITIONED BY (pt) WITH ("
                        + " 'bucket' = '2'"
                        + " )");

        FileStoreTable table = paimonTable("T");
        SnapshotManager snapshotManager = table.snapshotManager();

        sql("INSERT INTO T VALUES (1, 10, 'hunter')");
        sql("INSERT INTO T VALUES (2, 20, 'hunter2')");

        checkSnapshots(snapshotManager, 1, 2);

        // this branch will be ignored.
        sql("CALL sys.create_branch('default.T', 'empty')");

        // Case1 : Deleting a branch does not affect unexpired snapshots.
        sql("CALL sys.create_branch('default.T', 'test', 1)");

        sql("CALL sys.delete_branch('default.T', 'test')");

        assertThat(collectResult("SELECT * FROM T /*+ OPTIONS('scan.snapshot-id'='1') */"))
                .containsExactlyInAnyOrder("+I[1, 10, hunter]");

        // Case2 : Deleting a branch does not affect snapshots that are referenced by branches or
        // tags.
        sql("CALL sys.create_branch('default.T', 'test', 1)");

        // Create branch and tag and reference snapshot 1.
        sql("CALL sys.create_branch('default.T', 'test2', 1)");
        sql("CALL sys.create_tag('default.T', 'tag', 1)");

        // Only retain 2,3 snapshot, snapshot-1 will be expired.
        sql(
                "INSERT INTO T /*+ OPTIONS("
                        + "'snapshot.num-retained.min'= '2',"
                        + "'snapshot.num-retained.max'= '2' ) */"
                        + " VALUES (3, 30, 'hunter3')");

        sql("CALL sys.delete_branch('default.T', 'test')");

        // The branch test2 still can be read.
        assertThat(collectResult("SELECT * FROM T$branch_test2"))
                .containsExactlyInAnyOrder("+I[1, 10, hunter]");
        sql("CALL sys.delete_branch('default.T', 'test2')");

        // The tag still can be read.
        assertThat(collectResult("SELECT * FROM T /*+ OPTIONS('scan.tag-name'='tag') */"))
                .containsExactlyInAnyOrder("+I[1, 10, hunter]");
        sql("CALL sys.delete_tag('default.T', 'tag')");

        // Case 3: Deleting a branch will also clean up unreferenced and expired snapshots.
        checkSnapshots(snapshotManager, 2, 3);
        sql("CALL sys.create_branch('default.T', 'test', 2)");

        // Only retain snapshot 3,4, snapshot 2 will expire.
        sql(
                "INSERT INTO T /*+ OPTIONS("
                        + "'snapshot.num-retained.min'= '2',"
                        + "'snapshot.num-retained.max'= '2' ) */"
                        + " VALUES (4, 40, 'hunter4')");
        checkSnapshots(snapshotManager, 3, 4);

        BranchManager branchManager = table.branchManager();
        assertThat(branchManager.branches().keySet().stream().map(TableBranch::getBranchName))
                .containsExactlyInAnyOrder("test", "empty");

        Snapshot snapshotsToClean =
                branchManager.branchesCreateSnapshots().keySet().iterator().next();
        assertThat(snapshotsToClean.id()).isEqualTo(2L);

        // Query branches data.
        assertThat(collectResult("SELECT * FROM T$branch_test"))
                .containsExactlyInAnyOrder("+I[1, 10, hunter]", "+I[2, 20, hunter2]");

        // Verify that the manifest files exists.
        assertThat(manifestFileExist("T", snapshotsToClean.baseManifestList())).isTrue();
        assertThat(manifestFileExist("T", snapshotsToClean.deltaManifestList())).isTrue();

        // Delete branch.
        sql("CALL sys.delete_branch('default.T', 'test')");

        // Verify that the manifest file has been deleted.
        assertThat(manifestFileExist("T", snapshotsToClean.baseManifestList())).isFalse();
        assertThat(manifestFileExist("T", snapshotsToClean.deltaManifestList())).isFalse();

        assertThat(branchManager.branchesCreateSnapshots().isEmpty()).isTrue();
    }

    @Test
    public void testDeleteBranchCleanUnusedDataFiles() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " pt INT"
                        + ", k INT"
                        + ", v STRING"
                        + ", PRIMARY KEY (pt, k) NOT ENFORCED"
                        + " ) PARTITIONED BY (pt) WITH ("
                        + " 'bucket' = '1'"
                        + " )");

        FileStoreTable table = paimonTable("T");
        SnapshotManager snapshotManager = table.snapshotManager();
        TagManager tagManager = table.tagManager();
        TagDeletion tagDeletion = table.store().newTagDeletion();

        sql("INSERT INTO T VALUES (1, 10, 'hunter')");
        sql("INSERT INTO T VALUES (2, 20, 'hunter2')");
        sql("INSERT INTO T VALUES (3, 30, 'hunter3')");
        sql("INSERT INTO T VALUES (4, 40, 'hunter4')");

        checkSnapshots(snapshotManager, 1, 4);

        // The following three snapshots will expire.
        sql("CALL sys.create_branch('default.T', 'test', 1)");
        sql("CALL sys.create_branch('default.T', 'test2', 2)");
        sql("CALL sys.create_tag('default.T', 'tag3', 3)");

        // Only retain snapshot-6 and snapshot 1-5 will expire.
        // Here we need to update all the partition data, the snapshot-6 is a compaction snapshot.
        // Snapshot 6 records which data files should be deleted.
        sql(
                "INSERT INTO T /*+ OPTIONS("
                        + "'full-compaction.delta-commits'='1',"
                        + "'snapshot.num-retained.min'= '1',"
                        + "'snapshot.num-retained.max'= '1' ) */"
                        + " VALUES (1, 10, 'hunter')"
                        + ",(2, 20, 'hunter2')"
                        + ",(3, 30, 'hunter3')"
                        + ",(4, 40, 'hunter4')");

        checkSnapshots(snapshotManager, 6, 6);

        // delete branch should skip the nearest left neighbor snapshot and the nearest right
        // neighbor [branch_test, tag3].
        sql("CALL sys.delete_branch('default.T', 'test2')");

        // The tag still can be read.
        assertThat(collectResult("SELECT * FROM T /*+ OPTIONS('scan.tag-name'='tag3') */"))
                .containsExactlyInAnyOrder(
                        "+I[1, 10, hunter]", "+I[2, 20, hunter2]", "+I[3, 30, hunter3]");

        // The branch-test still can be read.
        assertThat(collectResult("SELECT * FROM T$branch_test"))
                .containsExactlyInAnyOrder("+I[1, 10, hunter]");

        sql("CALL sys.delete_branch('default.T', 'test')");

        // The tag still can be read.
        assertThat(collectResult("SELECT * FROM T /*+ OPTIONS('scan.tag-name'='tag3') */"))
                .containsExactlyInAnyOrder(
                        "+I[1, 10, hunter]", "+I[2, 20, hunter2]", "+I[3, 30, hunter3]");

        Snapshot taggedSnapshot = tagManager.taggedSnapshot("tag3");
        Collection<ManifestEntry> manifestEntries =
                tagDeletion.getDataFilesFromSnapshot(taggedSnapshot);

        assertThat(manifestEntries).allMatch(entry -> dataFileExist("T", entry));

        sql("CALL sys.delete_tag('default.T', 'tag3')");

        // All unused datafiles should be deleted.
        assertThat(manifestEntries).allMatch(entry -> !dataFileExist("T", entry));

        assertThat(collectResult("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        "+I[1, 10, hunter]",
                        "+I[2, 20, hunter2]",
                        "+I[3, 30, hunter3]",
                        "+I[4, 40, hunter4]");
    }

    @Test
    public void testFallbackBranchBatchRead() throws Exception {
        sql(
                "CREATE TABLE t ( pt INT NOT NULL, k INT NOT NULL, v STRING ) PARTITIONED BY (pt) WITH ( 'bucket' = '-1' )");
        sql("INSERT INTO t VALUES (1, 10, 'apple'), (1, 20, 'banana')");

        sql("CALL sys.create_branch('default.t', 'pk')");
        sql("ALTER TABLE `t$branch_pk` SET ( 'primary-key' = 'pt, k', 'bucket' = '2' )");
        sql("ALTER TABLE t SET ( 'scan.fallback-branch' = 'pk' )");

        sql("INSERT INTO `t$branch_pk` VALUES (1, 20, 'cat'), (1, 30, 'dog')");
        assertThat(collectResult("SELECT v, k FROM t"))
                .containsExactlyInAnyOrder("+I[apple, 10]", "+I[banana, 20]");
        assertThat(collectResult("SELECT v, k FROM `t$branch_pk`"))
                .containsExactlyInAnyOrder("+I[cat, 20]", "+I[dog, 30]");

        sql("INSERT INTO `t$branch_pk` VALUES (2, 10, 'tiger'), (2, 20, 'wolf')");
        assertThat(collectResult("SELECT v, k FROM t"))
                .containsExactlyInAnyOrder(
                        "+I[apple, 10]", "+I[banana, 20]", "+I[tiger, 10]", "+I[wolf, 20]");
        assertThat(collectResult("SELECT v, k FROM `t$branch_pk`"))
                .containsExactlyInAnyOrder(
                        "+I[cat, 20]", "+I[dog, 30]", "+I[tiger, 10]", "+I[wolf, 20]");
        assertThat(collectResult("SELECT v, k FROM t WHERE pt = 1"))
                .containsExactlyInAnyOrder("+I[apple, 10]", "+I[banana, 20]");
        assertThat(collectResult("SELECT v, k FROM `t$branch_pk` WHERE pt = 1"))
                .containsExactlyInAnyOrder("+I[cat, 20]", "+I[dog, 30]");
        assertThat(collectResult("SELECT v, k FROM t WHERE pt = 2"))
                .containsExactlyInAnyOrder("+I[tiger, 10]", "+I[wolf, 20]");
        assertThat(collectResult("SELECT v, k FROM `t$branch_pk` WHERE pt = 2"))
                .containsExactlyInAnyOrder("+I[tiger, 10]", "+I[wolf, 20]");

        sql("INSERT INTO `t$branch_pk` VALUES (2, 10, 'lion')");
        assertThat(collectResult("SELECT v, k FROM t"))
                .containsExactlyInAnyOrder(
                        "+I[apple, 10]", "+I[banana, 20]", "+I[lion, 10]", "+I[wolf, 20]");
        assertThat(collectResult("SELECT v, k FROM `t$branch_pk`"))
                .containsExactlyInAnyOrder(
                        "+I[cat, 20]", "+I[dog, 30]", "+I[lion, 10]", "+I[wolf, 20]");

        sql("INSERT OVERWRITE t PARTITION (pt = 1) VALUES (10, 'pear'), (20, 'mango')");
        assertThat(collectResult("SELECT v, k FROM t"))
                .containsExactlyInAnyOrder(
                        "+I[pear, 10]", "+I[mango, 20]", "+I[lion, 10]", "+I[wolf, 20]");
        assertThat(collectResult("SELECT v, k FROM `t$branch_pk`"))
                .containsExactlyInAnyOrder(
                        "+I[cat, 20]", "+I[dog, 30]", "+I[lion, 10]", "+I[wolf, 20]");

        sql("ALTER TABLE t RESET ( 'scan.fallback-branch' )");
        assertThat(collectResult("SELECT v, k FROM t"))
                .containsExactlyInAnyOrder("+I[pear, 10]", "+I[mango, 20]");
        assertThat(collectResult("SELECT v, k FROM `t$branch_pk`"))
                .containsExactlyInAnyOrder(
                        "+I[cat, 20]", "+I[dog, 30]", "+I[lion, 10]", "+I[wolf, 20]");
    }

    @Test
    public void testDifferentRowTypes() {
        sql(
                "CREATE TABLE t ( pt INT NOT NULL, k INT NOT NULL, v STRING ) PARTITIONED BY (pt) WITH ( 'bucket' = '-1' )");
        sql("CALL sys.create_branch('default.t', 'pk')");
        sql("ALTER TABLE `t$branch_pk` SET ( 'primary-key' = 'pt, k', 'bucket' = '2' )");
        sql("ALTER TABLE `t$branch_pk` ADD (v2 INT)");
        sql("ALTER TABLE t SET ( 'scan.fallback-branch' = 'pk' )");

        assertThatThrownBy(() -> sql("INSERT INTO t VALUES (1, 10, 'apple')"))
                .hasMessageContaining("Branch main and pk does not have the same row type");
    }

    private List<String> collectResult(String sql) throws Exception {
        List<String> result = new ArrayList<>();
        try (CloseableIterator<Row> it = tEnv.executeSql(sql).collect()) {
            while (it.hasNext()) {
                result.add(it.next().toString());
            }
        }
        return result;
    }

    private void checkSnapshots(SnapshotManager sm, int earliest, int latest) throws IOException {
        assertThat(sm.snapshotCount()).isEqualTo(latest - earliest + 1);
        assertThat(sm.earliestSnapshotId()).isEqualTo(earliest);
        assertThat(sm.latestSnapshotId()).isEqualTo(latest);
    }

    private boolean manifestFileExist(String tableName, String manifest) throws IOException {
        return fileIO.exists(new Path(getTableDirectory(tableName) + "/manifest/" + manifest));
    }

    private boolean dataFileExist(String tableName, ManifestEntry entry) {
        if (entry.kind() == FileKind.ADD) {
            try {
                String partition = String.valueOf(entry.partition().getInt(0));
                return fileIO.exists(
                        new Path(
                                String.format(
                                        getTableDirectory(tableName) + "/pt=%s/bucket-%s/%s",
                                        partition,
                                        entry.bucket(),
                                        entry.file().fileName())));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return true;
    }
}
