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
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotNotExistException;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;

/** IT Case for {@link CreateTagFromTimestampProcedure}. */
public class CreateTagFromTimestampProcedureITCase extends CatalogITCaseBase {

    @Test
    public void testCreateTagsFromSnapshotsCommitTime() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k STRING,"
                        + " dt STRING,"
                        + " PRIMARY KEY (k, dt) NOT ENFORCED"
                        + ") PARTITIONED BY (dt) WITH ("
                        + " 'bucket' = '1'"
                        + ")");

        for (int i = 1; i <= 4; i++) {
            sql("insert into T values('%s', '2024-01-01')", i);
            Thread.sleep(100L);
        }

        FileStoreTable table = paimonTable("T");
        long earliestCommitTime = table.snapshotManager().earliestSnapshot().timeMillis();
        long commitTime3 = table.snapshotManager().snapshot(3).timeMillis();
        long commitTime4 = table.snapshotManager().snapshot(4).timeMillis();

        // create tag from timestamp that earlier than the earliest snapshot commit time.
        assertThat(
                        sql(
                                        "CALL sys.create_tag_from_timestamp("
                                                + "`table` => 'default.T',"
                                                + "`tag` => 'tag1',"
                                                + "`timestamp` => %s)",
                                        earliestCommitTime - 1)
                                .stream()
                                .map(Row::toString))
                .containsExactlyInAnyOrder(
                        String.format("+I[tag1, 1, %s, %s]", earliestCommitTime, Long.MIN_VALUE));

        // create tag from timestamp that equals to snapshot-3 commit time.
        assertThat(
                        sql(
                                        "CALL sys.create_tag_from_timestamp("
                                                + "`table` => 'default.T',"
                                                + "`tag` => 'tag2',"
                                                + "`timestamp` => %s)",
                                        commitTime3)
                                .stream()
                                .map(Row::toString))
                .containsExactlyInAnyOrder(
                        String.format("+I[tag2, 3, %s, %s]", commitTime3, Long.MIN_VALUE));

        // create tag from timestamp that later than snapshot-3 commit time.
        assertThat(
                        sql(
                                        "CALL sys.create_tag_from_timestamp("
                                                + "`table` => 'default.T',"
                                                + "`tag` => 'tag3',"
                                                + "`timestamp` => %s)",
                                        commitTime3 + 1)
                                .stream()
                                .map(Row::toString))
                .containsExactlyInAnyOrder(
                        String.format("+I[tag3, 4, %s, %s]", commitTime4, Long.MIN_VALUE));

        // create tag from timestamp that later than the latest snapshot commit time and throw
        // SnapshotNotExistException.
        assertThatException()
                .isThrownBy(
                        () ->
                                sql(
                                        "CALL sys.create_tag_from_timestamp("
                                                + "`table` => 'default.T',"
                                                + "`tag` => 'tag4',"
                                                + "`timestamp` => %s)",
                                        Long.MAX_VALUE))
                .withRootCauseInstanceOf(SnapshotNotExistException.class)
                .withMessageContaining(
                        "Could not find any snapshot whose commit-time later than %s.",
                        Long.MAX_VALUE);
    }

    @Test
    public void testCreateTagsFromTagsCommitTime() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k STRING,"
                        + " dt STRING,"
                        + " PRIMARY KEY (k, dt) NOT ENFORCED"
                        + ") PARTITIONED BY (dt) WITH ("
                        + " 'bucket' = '1'"
                        + ")");

        sql("insert into T values('1', '2024-01-01')");
        Thread.sleep(100L);

        sql("CALL sys.create_tag('default.T', 'tag1', 1)");

        // make snapshot-1 expire.
        sql(
                "insert into T/*+ OPTIONS("
                        + " 'snapshot.num-retained.max' = '1',"
                        + " 'snapshot.num-retained.min' = '1') */"
                        + " values('2', '2024-01-01')");

        FileStoreTable table = paimonTable("T");
        long earliestCommitTime = table.snapshotManager().earliestSnapshot().timeMillis();
        long tagSnapshotCommitTime = table.tagManager().getOrThrow("tag1").timeMillis();

        assertThat(tagSnapshotCommitTime < earliestCommitTime).isTrue();

        // create tag from timestamp that earlier than the expired snapshot 1.
        assertThat(
                        sql(
                                        "CALL sys.create_tag_from_timestamp("
                                                + "`table` => 'default.T',"
                                                + "`tag` => 'tag2',"
                                                + "`timestamp` => %s)",
                                        tagSnapshotCommitTime - 1)
                                .stream()
                                .map(Row::toString))
                .containsExactlyInAnyOrder(
                        String.format(
                                "+I[tag2, 1, %s, %s]", tagSnapshotCommitTime, Long.MIN_VALUE));

        // create tag from timestamp that later than the expired snapshot 1.
        assertThat(
                        sql(
                                        "CALL sys.create_tag_from_timestamp("
                                                + "`table` => 'default.T',"
                                                + "`tag` => 'tag3',"
                                                + "`timestamp` => %s)",
                                        earliestCommitTime - 1)
                                .stream()
                                .map(Row::toString))
                .containsExactlyInAnyOrder(
                        String.format("+I[tag3, 2, %s, %s]", earliestCommitTime, Long.MIN_VALUE));
    }
}
