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

import org.apache.paimon.Snapshot;
import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotNotExistException;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;

/** IT Case for {@link CreateTagFromWatermarkProcedure}. */
public class CreateTagFromWatermarkProcedureITCase extends CatalogITCaseBase {

    @Test
    public void testCreateTagsFromSnapshotsWatermark() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k STRING,"
                        + " dt STRING,"
                        + " PRIMARY KEY (k, dt) NOT ENFORCED"
                        + ") PARTITIONED BY (dt) WITH ("
                        + " 'bucket' = '1'"
                        + ")");
        // create snapshot 1 with watermark Long.MIN_VALUE.
        sql("insert into T values('k1', '2024-01-02')");

        assertThatException()
                .isThrownBy(
                        () ->
                                sql(
                                        "CALL sys.create_tag_from_watermark("
                                                + "`table` => 'default.T',"
                                                + "`tag` => 'tag1',"
                                                + "`watermark` => %s )",
                                        1000))
                .withRootCauseInstanceOf(SnapshotNotExistException.class)
                .withMessageContaining(
                        "Could not find any snapshot whose watermark later than %s.", 1000);

        // create snapshot 2 with watermark 1000.
        sql(
                "insert into T/*+ OPTIONS('end-input.watermark'= '1000') */ values('k2', '2024-01-02')");
        // create snapshot 3 with watermark 2000.
        sql(
                "insert into T/*+ OPTIONS('end-input.watermark'= '2000') */ values('k3', '2024-01-02')");

        FileStoreTable table = paimonTable("T");

        long watermark1 = table.snapshotManager().snapshot(1).watermark();

        Snapshot snapshot2 = table.snapshotManager().snapshot(2);
        long commitTime2 = snapshot2.timeMillis();
        long watermark2 = snapshot2.watermark();

        Snapshot snapshot3 = table.snapshotManager().snapshot(3);
        long commitTime3 = snapshot3.timeMillis();
        long watermark3 = snapshot3.watermark();

        assertThat(watermark1 == Long.MIN_VALUE).isTrue();
        assertThat(watermark2 == 1000).isTrue();
        assertThat(watermark3 == 2000).isTrue();

        assertThat(
                        sql(
                                        "CALL sys.create_tag_from_watermark("
                                                + "`table` => 'default.T',"
                                                + "`tag` => 'tag2',"
                                                + "`watermark` => %s)",
                                        watermark2 - 1)
                                .stream()
                                .map(Row::toString))
                .containsExactlyInAnyOrder(
                        String.format("+I[tag2, 2, %s, %s]", commitTime2, watermark2));

        assertThat(
                        sql(
                                        "CALL sys.create_tag_from_watermark("
                                                + "`table` => 'default.T',"
                                                + "`tag` => 'tag3',"
                                                + "`watermark` => %s)",
                                        watermark2 + 1)
                                .stream()
                                .map(Row::toString))
                .containsExactlyInAnyOrder(
                        String.format("+I[tag3, 3, %s, %s]", commitTime3, watermark3));

        assertThatException()
                .isThrownBy(
                        () ->
                                sql(
                                        "CALL sys.create_tag_from_watermark("
                                                + "`table` => 'default.T',"
                                                + "`tag` => 'tag4',"
                                                + "`watermark` => %s )",
                                        watermark3 + 1))
                .withRootCauseInstanceOf(SnapshotNotExistException.class)
                .withMessageContaining(
                        "Could not find any snapshot whose watermark later than %s.",
                        watermark3 + 1);
    }

    @Test
    public void testCreateTagsFromTagsWatermark() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k STRING,"
                        + " dt STRING,"
                        + " PRIMARY KEY (k, dt) NOT ENFORCED"
                        + ") PARTITIONED BY (dt) WITH ("
                        + " 'bucket' = '1'"
                        + ")");

        sql(
                "insert into T/*+ OPTIONS('end-input.watermark'= '1000') */ values('k2', '2024-01-02')");

        sql("CALL sys.create_tag('default.T', 'tag1', 1)");

        // make snapshot-1 expire.
        sql(
                "insert into T/*+ OPTIONS('end-input.watermark'= '2000',"
                        + " 'snapshot.num-retained.max' = '1',"
                        + " 'snapshot.num-retained.min' = '1') */"
                        + " values('k2', '2024-01-02')");

        FileStoreTable table = paimonTable("T");

        assertThat(table.snapshotManager().snapshotExists(1)).isFalse();

        Snapshot tagSnapshot1 = table.tagManager().taggedSnapshot("tag1");

        long tagsCommitTime = tagSnapshot1.timeMillis();
        long tagsWatermark = tagSnapshot1.watermark();

        Snapshot snapshot2 = table.snapshotManager().snapshot(2);
        long commitTime2 = snapshot2.timeMillis();
        long watermark2 = snapshot2.watermark();

        assertThat(tagsWatermark == 1000).isTrue();
        assertThat(watermark2 == 2000).isTrue();

        // create tag from tag1 that snapshot is 1.
        assertThat(
                        sql(
                                        "CALL sys.create_tag_from_watermark("
                                                + "`table` => 'default.T',"
                                                + "`tag` => 'tag2',"
                                                + "`watermark` => %s)",
                                        tagsWatermark - 1)
                                .stream()
                                .map(Row::toString))
                .containsExactlyInAnyOrder(
                        String.format("+I[tag2, 1, %s, %s]", tagsCommitTime, tagsWatermark));

        assertThat(
                        sql(
                                        "CALL sys.create_tag_from_watermark("
                                                + "`table` => 'default.T',"
                                                + "`tag` => 'tag3',"
                                                + "`watermark` => %s)",
                                        watermark2 - 1)
                                .stream()
                                .map(Row::toString))
                .containsExactlyInAnyOrder(
                        String.format("+I[tag3, 2, %s, %s]", commitTime2, watermark2));
    }
}
