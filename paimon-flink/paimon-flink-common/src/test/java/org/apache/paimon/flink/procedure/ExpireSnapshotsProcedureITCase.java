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
import org.apache.paimon.flink.action.ExpireSnapshotsAction;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Timestamp;

import static org.assertj.core.api.Assertions.assertThat;

/** IT Case for {@link ExpireSnapshotsProcedure}. */
public class ExpireSnapshotsProcedureITCase extends CatalogITCaseBase {

    @Test
    public void testExpireSnapshotsProcedure() throws Exception {
        sql(
                "CREATE TABLE word_count ( word STRING PRIMARY KEY NOT ENFORCED, cnt INT)"
                        + " WITH ( 'num-sorted-run.compaction-trigger' = '9999' )");
        FileStoreTable table = paimonTable("word_count");
        SnapshotManager snapshotManager = table.snapshotManager();

        // initially prepare 6 snapshots, expected snapshots (1, 2, 3, 4, 5, 6)
        for (int i = 0; i < 6; ++i) {
            sql("INSERT INTO word_count VALUES ('" + String.valueOf(i) + "', " + i + ")");
        }
        checkSnapshots(snapshotManager, 1, 6);

        // retain_max => 5, expected snapshots (2, 3, 4, 5, 6)
        sql("CALL sys.expire_snapshots(`table` => 'default.word_count', retain_max => 5)");
        checkSnapshots(snapshotManager, 2, 6);

        // older_than => timestamp of snapshot 6, max_deletes => 1, expected snapshots (3, 4, 5, 6)
        Timestamp ts6 = new Timestamp(snapshotManager.latestSnapshot().timeMillis());
        sql(
                "CALL sys.expire_snapshots(`table` => 'default.word_count', older_than => '"
                        + ts6.toString()
                        + "', max_deletes => 1)");
        checkSnapshots(snapshotManager, 3, 6);

        // older_than => timestamp of snapshot 6, retain_min => 3, expected snapshots (4, 5, 6)
        sql(
                "CALL sys.expire_snapshots(`table` => 'default.word_count', older_than => '"
                        + ts6.toString()
                        + "', retain_min => 3)");
        checkSnapshots(snapshotManager, 4, 6);

        // older_than => timestamp of snapshot 6, expected snapshots (6)
        sql(
                "CALL sys.expire_snapshots(`table`  => 'default.word_count', older_than => '"
                        + ts6.toString()
                        + "')");
        checkSnapshots(snapshotManager, 6, 6);
    }

    @Test
    public void testExpireSnapshotsAction() throws Exception {
        sql(
                "CREATE TABLE word_count ( word STRING PRIMARY KEY NOT ENFORCED, cnt INT)"
                        + " WITH ( 'num-sorted-run.compaction-trigger' = '9999' )");
        FileStoreTable table = paimonTable("word_count");
        StreamExecutionEnvironment env =
                streamExecutionEnvironmentBuilder().streamingMode().build();
        SnapshotManager snapshotManager = table.snapshotManager();

        // initially prepare 6 snapshots, expected snapshots (1, 2, 3, 4, 5, 6)
        for (int i = 0; i < 6; ++i) {
            sql("INSERT INTO word_count VALUES ('" + String.valueOf(i) + "', " + i + ")");
        }
        checkSnapshots(snapshotManager, 1, 6);

        // retain_max => 5, expected snapshots (2, 3, 4, 5, 6)
        createAction(
                        ExpireSnapshotsAction.class,
                        "expire_snapshots",
                        "--warehouse",
                        path,
                        "--database",
                        "default",
                        "--table",
                        "word_count",
                        "--retain_max",
                        "5")
                .withStreamExecutionEnvironment(env)
                .run();
        checkSnapshots(snapshotManager, 2, 6);

        // older_than => timestamp of snapshot 6, max_deletes => 1, expected snapshots (3, 4, 5, 6)
        Timestamp ts6 = new Timestamp(snapshotManager.latestSnapshot().timeMillis());
        createAction(
                        ExpireSnapshotsAction.class,
                        "expire_snapshots",
                        "--warehouse",
                        path,
                        "--database",
                        "default",
                        "--table",
                        "word_count",
                        "--older_than",
                        ts6.toString(),
                        "--max_deletes",
                        "1")
                .withStreamExecutionEnvironment(env)
                .run();
        checkSnapshots(snapshotManager, 3, 6);

        createAction(
                        ExpireSnapshotsAction.class,
                        "expire_snapshots",
                        "--warehouse",
                        path,
                        "--database",
                        "default",
                        "--table",
                        "word_count",
                        "--older_than",
                        ts6.toString(),
                        "--retain_min",
                        "3")
                .withStreamExecutionEnvironment(env)
                .run();
        checkSnapshots(snapshotManager, 4, 6);

        // older_than => timestamp of snapshot 6, expected snapshots (6)
        createAction(
                        ExpireSnapshotsAction.class,
                        "expire_snapshots",
                        "--warehouse",
                        path,
                        "--database",
                        "default",
                        "--table",
                        "word_count",
                        "--older_than",
                        ts6.toString())
                .withStreamExecutionEnvironment(env)
                .run();
        checkSnapshots(snapshotManager, 6, 6);
    }

    private void checkSnapshots(SnapshotManager sm, int earliest, int latest) throws IOException {
        assertThat(sm.snapshotCount()).isEqualTo(latest - earliest + 1);
        assertThat(sm.earliestSnapshotId()).isEqualTo(earliest);
        assertThat(sm.latestSnapshotId()).isEqualTo(latest);
    }

    private <T extends ActionBase> T createAction(Class<T> clazz, String... args) {
        return ActionFactory.createAction(args)
                .filter(clazz::isInstance)
                .map(clazz::cast)
                .orElseThrow(() -> new RuntimeException("Failed to create action"));
    }
}
