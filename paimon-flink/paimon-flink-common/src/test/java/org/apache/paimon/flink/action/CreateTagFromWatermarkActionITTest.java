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

package org.apache.paimon.flink.action;

import org.apache.paimon.Snapshot;
import org.apache.paimon.table.FileStoreTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.bEnv;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.init;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link CreateTagFromTimestampAction}. */
public class CreateTagFromWatermarkActionITTest extends ActionITCaseBase {

    @BeforeEach
    public void setUp() {
        init(warehouse);
    }

    @Test
    public void testCreateTagsFromSnapshotsWatermark() throws Exception {
        bEnv.executeSql(
                "CREATE TABLE T ("
                        + " k STRING,"
                        + " dt STRING,"
                        + " PRIMARY KEY (k, dt) NOT ENFORCED"
                        + ") PARTITIONED BY (dt) WITH ("
                        + " 'bucket' = '1'"
                        + ")");

        bEnv.executeSql("insert into T values('k1', '2024-01-02')").await();
        // create snapshot 2 with watermark 1000.
        bEnv.executeSql(
                        "insert into T/*+ OPTIONS('end-input.watermark'= '1000') */ values('k2', '2024-01-02')")
                .await();
        // create snapshot 3 with watermark 2000.
        bEnv.executeSql(
                        "insert into T/*+ OPTIONS('end-input.watermark'= '2000') */ values('k3', '2024-01-02')")
                .await();
        FileStoreTable table = getFileStoreTable("T");

        Snapshot snapshot2 = table.snapshotManager().snapshot(2);
        long commitTime2 = snapshot2.timeMillis();
        long watermark2 = snapshot2.watermark();

        Snapshot snapshot3 = table.snapshotManager().snapshot(3);
        long commitTime3 = snapshot3.timeMillis();
        long watermark3 = snapshot3.watermark();
        createAction(
                        CreateTagFromWatermarkAction.class,
                        "create_tag_from_watermark",
                        "--warehouse",
                        warehouse,
                        "--table",
                        database + ".T",
                        "--tag",
                        "tag2",
                        "--watermark",
                        Long.toString(watermark2 - 1))
                .run();
        assertThat(table.tagManager().tagExists("tag2")).isTrue();
        assertThat(table.tagManager().taggedSnapshot("tag2").watermark()).isEqualTo(watermark2);
        assertThat(table.tagManager().taggedSnapshot("tag2").timeMillis()).isEqualTo(commitTime2);

        createAction(
                        CreateTagFromWatermarkAction.class,
                        "create_tag_from_watermark",
                        "--warehouse",
                        warehouse,
                        "--table",
                        database + ".T",
                        "--tag",
                        "tag3",
                        "--watermark",
                        Long.toString(watermark2 + 1))
                .run();
        assertThat(table.tagManager().tagExists("tag3")).isTrue();
        assertThat(table.tagManager().taggedSnapshot("tag3").watermark()).isEqualTo(watermark3);
        assertThat(table.tagManager().taggedSnapshot("tag3").timeMillis()).isEqualTo(commitTime3);
    }

    @Test
    public void testCreateTagsFromTagsWatermark() throws Exception {
        bEnv.executeSql(
                "CREATE TABLE T ("
                        + " k STRING,"
                        + " dt STRING,"
                        + " PRIMARY KEY (k, dt) NOT ENFORCED"
                        + ") PARTITIONED BY (dt) WITH ("
                        + " 'bucket' = '1'"
                        + ")");

        bEnv.executeSql(
                        "insert into T/*+ OPTIONS('end-input.watermark'= '1000') */ values('k2', '2024-01-02')")
                .await();

        bEnv.executeSql("CALL sys.create_tag('default.T', 'tag1', 1)").await();

        // make snapshot-1 expire.
        bEnv.executeSql(
                        "insert into T/*+ OPTIONS('end-input.watermark'= '2000',"
                                + " 'snapshot.num-retained.max' = '1',"
                                + " 'snapshot.num-retained.min' = '1') */"
                                + " values('k2', '2024-01-02')")
                .await();

        FileStoreTable table = getFileStoreTable("T");

        assertThat(table.snapshotManager().snapshotExists(1)).isFalse();

        Snapshot tagSnapshot1 = table.tagManager().taggedSnapshot("tag1");

        long tagsCommitTime = tagSnapshot1.timeMillis();
        long tagsWatermark = tagSnapshot1.watermark();

        Snapshot snapshot2 = table.snapshotManager().snapshot(2);
        long commitTime2 = snapshot2.timeMillis();
        long watermark2 = snapshot2.watermark();

        assertThat(tagsWatermark == 1000).isTrue();
        assertThat(watermark2 == 2000).isTrue();

        createAction(
                        CreateTagFromWatermarkAction.class,
                        "create_tag_from_watermark",
                        "--warehouse",
                        warehouse,
                        "--table",
                        database + ".T",
                        "--tag",
                        "tag2",
                        "--watermark",
                        Long.toString(tagsWatermark - 1))
                .run();
        assertThat(table.tagManager().tagExists("tag2")).isTrue();
        assertThat(table.tagManager().taggedSnapshot("tag2").watermark()).isEqualTo(tagsWatermark);
        assertThat(table.tagManager().taggedSnapshot("tag2").timeMillis())
                .isEqualTo(tagsCommitTime);

        createAction(
                        CreateTagFromWatermarkAction.class,
                        "create_tag_from_watermark",
                        "--warehouse",
                        warehouse,
                        "--table",
                        database + ".T",
                        "--tag",
                        "tag3",
                        "--watermark",
                        Long.toString(watermark2 - 1))
                .run();
        assertThat(table.tagManager().tagExists("tag3")).isTrue();
        assertThat(table.tagManager().taggedSnapshot("tag3").watermark()).isEqualTo(watermark2);
        assertThat(table.tagManager().taggedSnapshot("tag3").timeMillis()).isEqualTo(commitTime2);
    }
}
