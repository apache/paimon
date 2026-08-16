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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.flink.action.ActionBase;
import org.apache.paimon.flink.action.ActionFactory;
import org.apache.paimon.flink.action.ExpireChangelogsAction;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.BlockingIterator;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** IT Case for {@link ExpireChangelogsProcedure}. */
public class ExpireChangelogsProcedureITCase extends CatalogITCaseBase {

    @Test
    public void testExpireChangelogsProcedure() throws Exception {
        sql(
                "CREATE TABLE word_count ( word STRING PRIMARY KEY NOT ENFORCED, cnt INT)"
                        + " WITH ( 'num-sorted-run.compaction-trigger' = '9999', 'changelog-producer' = 'input', "
                        + "'snapshot.num-retained.min' = '4', 'snapshot.num-retained.max' = '4', "
                        + "'changelog.num-retained.min' = '10', 'changelog.num-retained.max' = '10' )");
        FileStoreTable table = paimonTable("word_count");
        SnapshotManager snapshotManager = table.snapshotManager();
        ChangelogManager changelogManager = table.changelogManager();

        // ------------------------------------------------------------------------
        //  Basic Function Tests
        // ------------------------------------------------------------------------

        // initially prepare 10 snapshots
        for (int i = 0; i < 10; ++i) {
            sql("INSERT INTO word_count VALUES ('" + i + "', " + i + ")");
        }
        // expected snapshots (7, 8, 9, 10)
        checkSnapshots(snapshotManager, 7, 10);
        // expected changelogs (1, 2, 3, 4, 5, 6)
        checkChangelogs(changelogManager, 1, 6);

        // retain_max => 8, expected changelogs (3, 4, 5, 6)
        sql("CALL sys.expire_changelogs(`table` => 'default.word_count', retain_max => 8)");
        checkChangelogs(changelogManager, 3, 6);

        // older_than => timestamp of snapshot 10, max_deletes => 1, expected snapshots (3, 4, 5, 6)
        Timestamp ts7 = new Timestamp(snapshotManager.latestSnapshot().timeMillis());
        sql(
                "CALL sys.expire_changelogs(`table` => 'default.word_count', older_than => '"
                        + ts7
                        + "', max_deletes => 1)");
        checkChangelogs(changelogManager, 4, 6);

        // older_than => timestamp of snapshot 7, retain_min => 6, expected snapshots (5, 6)
        sql(
                "CALL sys.expire_changelogs(`table` => 'default.word_count', older_than => '"
                        + ts7
                        + "', retain_min => 6)");
        checkChangelogs(changelogManager, 5, 6);

        // older_than => timestamp of snapshot 7, expected snapshots (6)
        sql(
                "CALL sys.expire_changelogs(`table`  => 'default.word_count', older_than => '"
                        + ts7
                        + "')");
        checkChangelogs(changelogManager, 6, 6);

        // prepare 2 more snapshots
        for (int i = 10; i < 12; ++i) {
            sql("INSERT INTO word_count VALUES ('" + i + "', " + i + ")");
        }
        // expected snapshots (9, 10, 11, 12)
        checkSnapshots(snapshotManager, 9, 12);
        // expected changelogs (6, 7, 8)
        checkChangelogs(changelogManager, 6, 8);

        // retain_max => 4, same as snapshot.num-retained.max
        // expected changelogs (8), retain 1 latest changelog at least
        sql("CALL sys.expire_changelogs(`table` => 'default.word_count', retain_max => 4)");
        checkChangelogs(changelogManager, 8, 8);
        checkBatchRead(12);

        // ------------------------------------------------------------------------
        //  Expire All Changelogs Tests
        // ------------------------------------------------------------------------

        // delete_all => true, delete all separated changelogs
        sql("CALL sys.expire_changelogs(`table` => 'default.word_count', delete_all => true)");
        // expected all changelogs and hints deleted
        checkAllDeleted(changelogManager);
        checkStreamRead(9, 4);
        checkBatchRead(12);

        // prepare 8 more snapshots
        for (int i = 12; i < 15; ++i) {
            sql("INSERT INTO word_count VALUES ('" + i + "', " + i + ")");
        }
        // expected snapshots [12, 15]
        checkSnapshots(snapshotManager, 12, 15);
        // changelog EARLIEST hint files have not been created
        assertThat(
                        changelogManager
                                .fileIO()
                                .exists(
                                        new Path(
                                                changelogManager.changelogDirectory(), "EARLIEST")))
                .isFalse();
        // expected changelogs [9, 11]
        checkChangelogs(changelogManager, 9, 11);

        // test can expire changelogs even if EARLIEST hint not exists
        sql("CALL sys.expire_changelogs(`table` => 'default.word_count', delete_all => true)");
        checkAllDeleted(changelogManager);

        // prepare 10 more snapshots
        for (int i = 15; i < 25; ++i) {
            sql("INSERT INTO word_count VALUES ('" + i + "', " + i + ")");
        }
        checkSnapshots(snapshotManager, 22, 25);
        checkChangelogs(changelogManager, 16, 21);

        // make changelog and snapshot lifecycle not decoupled
        sql("ALTER TABLE word_count SET ( 'changelog.num-retained.min' = '4')");
        sql("ALTER TABLE word_count SET ( 'changelog.num-retained.max' = '4')");
        // prepare 5 more snapshots
        for (int i = 25; i < 30; ++i) {
            sql("INSERT INTO word_count VALUES ('" + i + "', " + i + ")");
        }
        checkSnapshots(snapshotManager, 27, 30);
        // expected: the previous separated changelogs will not be expired in committer
        checkChangelogs(changelogManager, 16, 21);
        checkBatchRead(30);

        // make changelog and snapshot lifecycle decoupled again
        sql("ALTER TABLE word_count SET ( 'changelog.num-retained.max' = '50')");
        sql("ALTER TABLE word_count SET ( 'changelog.num-retained.min' = '50')");
        // prepare 5 more snapshots
        for (int i = 30; i < 35; ++i) {
            sql("INSERT INTO word_count VALUES ('" + i + "', " + i + ")");
        }
        checkSnapshots(snapshotManager, 32, 35);
        // actual changelogs: [16, 21] + [27, 31]
        checkChangelogs(changelogManager, 16, 31);
        assertThat(changelogManager.safelyGetAllChangelogs().size()).isEqualTo(11);
        // make changelog and snapshot lifecycle not decoupled again
        sql("ALTER TABLE word_count SET ( 'changelog.num-retained.min' = '4')");
        sql("ALTER TABLE word_count SET ( 'changelog.num-retained.max' = '4')");
        // prepare 5 more snapshots
        for (int i = 35; i < 40; ++i) {
            sql("INSERT INTO word_count VALUES ('" + i + "', " + i + ")");
        }
        // expected snapshot: [37, 38, 39, 40]
        // expected changelogs: [16, 17, 18, 19, 20, 21, 27, 28, 29, 30, 31]
        checkSnapshots(snapshotManager, 37, 40);
        checkChangelogs(changelogManager, 16, 31);

        sql("CALL sys.expire_changelogs(`table` => 'default.word_count', delete_all => true)");
        checkAllDeleted(changelogManager);
        checkStreamRead(37, 4);
        checkBatchRead(40);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testExpireChangelogsAction(boolean forceStartFlinkJob) throws Exception {
        sql(
                "CREATE TABLE word_count ( word STRING PRIMARY KEY NOT ENFORCED, cnt INT)"
                        + " WITH ( 'num-sorted-run.compaction-trigger' = '9999', 'changelog-producer' = 'input', "
                        + "'snapshot.num-retained.min' = '4', 'snapshot.num-retained.max' = '4', "
                        + "'changelog.num-retained.min' = '10', 'changelog.num-retained.max' = '10' )");
        FileStoreTable table = paimonTable("word_count");
        StreamExecutionEnvironment env =
                streamExecutionEnvironmentBuilder().streamingMode().build();
        SnapshotManager snapshotManager = table.snapshotManager();
        ChangelogManager changelogManager = table.changelogManager();

        // initially prepare 10 snapshots
        for (int i = 0; i < 10; ++i) {
            sql("INSERT INTO word_count VALUES ('" + i + "', " + i + ")");
        }
        // expected snapshots (7, 8, 9, 10)
        checkSnapshots(snapshotManager, 7, 10);
        // expected changelogs (1, 2, 3, 4, 5, 6)
        checkChangelogs(changelogManager, 1, 6);

        Timestamp ts5 = new Timestamp(changelogManager.changelog(5).timeMillis());

        createAction(
                        ExpireChangelogsAction.class,
                        "expire_changelogs",
                        "--warehouse",
                        path,
                        "--database",
                        "default",
                        "--table",
                        "word_count",
                        "--retain_max",
                        "8",
                        "--retain_min",
                        "4",
                        "--older_than",
                        ts5.toString(),
                        "--max_deletes",
                        "3",
                        "--force_start_flink_job",
                        Boolean.toString(forceStartFlinkJob))
                .withStreamExecutionEnvironment(env)
                .run();
        checkChangelogs(changelogManager, 4, 6);

        // expire all
        createAction(
                        ExpireChangelogsAction.class,
                        "expire_changelogs",
                        "--warehouse",
                        path,
                        "--database",
                        "default",
                        "--table",
                        "word_count",
                        "--delete_all",
                        "true",
                        "--force_start_flink_job",
                        Boolean.toString(forceStartFlinkJob))
                .withStreamExecutionEnvironment(env)
                .run();
        checkAllDeleted(changelogManager);
    }

    private void checkSnapshots(SnapshotManager sm, int earliest, int latest) throws IOException {
        assertThat(sm.snapshotCount()).isEqualTo(latest - earliest + 1);
        assertThat(sm.earliestSnapshotId()).isEqualTo(earliest);
        assertThat(sm.latestSnapshotId()).isEqualTo(latest);
    }

    private void checkChangelogs(ChangelogManager cm, int earliest, int latest) {
        assertThat(cm.earliestLongLivedChangelogId()).isEqualTo(earliest);
        assertThat(cm.latestLongLivedChangelogId()).isEqualTo(latest);
    }

    private void checkAllDeleted(ChangelogManager cm) throws IOException {
        assertThat(cm.fileIO().listStatus(cm.changelogDirectory()).length).isEqualTo(0);
    }

    private void checkBatchRead(int snapshotId) {
        List<Row> rows =
                sql(
                        String.format(
                                "select * from word_count /*+ OPTIONS('scan.snapshot-id' = '%s') */",
                                snapshotId));
        assertThat(rows.size()).isEqualTo(snapshotId);
    }

    private void checkStreamRead(int snapshotId, int expect) throws Exception {
        BlockingIterator<Row, Row> iter =
                streamSqlBlockIter(
                        String.format(
                                "select * from word_count /*+ OPTIONS('scan.snapshot-id' = '%s') */",
                                snapshotId));
        List<Row> rows = iter.collect(expect, 60, TimeUnit.SECONDS);
        List<Row> expectedRows = new ArrayList<>();
        for (int i = snapshotId - 1; i < snapshotId - 1 + expect; i++) {
            expectedRows.add(Row.of(String.valueOf(i), i));
        }
        assertThat(rows).hasSameElementsAs(expectedRows);
    }

    private <T extends ActionBase> T createAction(Class<T> clazz, String... args) {
        return ActionFactory.createAction(args)
                .filter(clazz::isInstance)
                .map(clazz::cast)
                .orElseThrow(() -> new RuntimeException("Failed to create action"));
    }
}
