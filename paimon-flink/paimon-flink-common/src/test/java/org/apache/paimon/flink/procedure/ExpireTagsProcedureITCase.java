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

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** IT Case for {@link ExpireTagsProcedure}. */
public class ExpireTagsProcedureITCase extends CatalogITCaseBase {

    @Test
    public void testExpireTagsByTagCreateTimeAndTagTimeRetained() throws Exception {
        sql(
                "CREATE TABLE T (id STRING, name STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED)"
                        + " WITH ('bucket'='1', 'write-only'='true')");

        FileStoreTable table = paimonTable("T");
        SnapshotManager snapshotManager = table.snapshotManager();

        // generate 5 snapshots
        for (int i = 1; i <= 5; i++) {
            sql("INSERT INTO T VALUES ('" + i + "', '" + i + "')");
        }
        checkSnapshots(snapshotManager, 1, 5);

        sql("CALL sys.create_tag(`table` => 'default.T', tag => 'tag-1', snapshot_id => 1)");
        sql(
                "CALL sys.create_tag(`table` => 'default.T', tag => 'tag-2', snapshot_id => 2, time_retained => '1h')");

        // no tags expired
        assertThat(sql("CALL sys.expire_tags(`table` => 'default.T')"))
                .containsExactly(Row.of("No expired tags."));

        sql(
                "CALL sys.create_tag(`table` => 'default.T', tag => 'tag-3', snapshot_id => 3, time_retained => '1s')");
        sql(
                "CALL sys.create_tag(`table` => 'default.T', tag => 'tag-4', snapshot_id => 4, time_retained => '1s')");

        Thread.sleep(2000);
        // tag-3,tag-4 expired
        assertThat(sql("CALL sys.expire_tags(`table` => 'default.T')"))
                .containsExactlyInAnyOrder(Row.of("tag-3"), Row.of("tag-4"));
    }

    @Test
    public void testExpireTagsByOlderThanTime() throws Exception {
        sql(
                "CREATE TABLE T (id STRING, name STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED)"
                        + " WITH ('bucket'='1', 'write-only'='true')");

        FileStoreTable table = paimonTable("T");
        SnapshotManager snapshotManager = table.snapshotManager();

        // generate 5 snapshots
        for (int i = 1; i <= 5; i++) {
            sql("INSERT INTO T VALUES ('" + i + "', '" + i + "')");
        }
        checkSnapshots(snapshotManager, 1, 5);

        sql("CALL sys.create_tag(`table` => 'default.T', tag => 'tag-1', snapshot_id => 1)");
        sql(
                "CALL sys.create_tag(`table` => 'default.T', tag => 'tag-2', snapshot_id => 2, time_retained => '1d')");
        sql(
                "CALL sys.create_tag(`table` => 'default.T', tag => 'tag-3', snapshot_id => 3, time_retained => '1d')");
        sql(
                "CALL sys.create_tag(`table` => 'default.T', tag => 'tag-4', snapshot_id => 4, time_retained => '1d')");
        List<Row> sql = sql("select count(tag_name) from `T$tags`");
        assertThat(sql("select count(tag_name) from `T$tags`")).containsExactly(Row.of(4L));

        // no tags expired
        assertThat(sql("CALL sys.expire_tags(`table` => 'default.T')"))
                .containsExactlyInAnyOrder(Row.of("No expired tags."));

        // tag-2 as the base older_than time.
        // tag-1 expired by its file creation time.
        LocalDateTime olderThanTime1 = table.tagManager().getOrThrow("tag-2").getTagCreateTime();
        java.sql.Timestamp timestamp1 =
                new java.sql.Timestamp(
                        Timestamp.fromLocalDateTime(olderThanTime1).getMillisecond());
        assertThat(
                        sql(
                                "CALL sys.expire_tags(`table` => 'default.T', older_than => '"
                                        + timestamp1.toString()
                                        + "')"))
                .containsExactlyInAnyOrder(Row.of("tag-1"));

        sql(
                "CALL sys.create_tag(`table` => 'default.T', tag => 'tag-5', snapshot_id => 5, time_retained => '1s')");
        Thread.sleep(1000);

        // tag-4 as the base older_than time.
        // tag-2,tag-3,tag-5 expired, tag-5 reached its tagTimeRetained.
        LocalDateTime olderThanTime2 = table.tagManager().getOrThrow("tag-4").getTagCreateTime();
        java.sql.Timestamp timestamp2 =
                new java.sql.Timestamp(
                        Timestamp.fromLocalDateTime(olderThanTime2).getMillisecond());
        assertThat(
                        sql(
                                "CALL sys.expire_tags(`table` => 'default.T', older_than => '"
                                        + timestamp2.toString()
                                        + "')"))
                .containsExactlyInAnyOrder(Row.of("tag-2"), Row.of("tag-3"), Row.of("tag-5"));

        assertThat(sql("select tag_name from `T$tags`")).containsExactly(Row.of("tag-4"));
    }

    private void checkSnapshots(SnapshotManager sm, int earliest, int latest) throws IOException {
        assertThat(sm.snapshotCount()).isEqualTo(latest - earliest + 1);
        assertThat(sm.earliestSnapshotId()).isEqualTo(earliest);
        assertThat(sm.latestSnapshotId()).isEqualTo(latest);
    }
}
