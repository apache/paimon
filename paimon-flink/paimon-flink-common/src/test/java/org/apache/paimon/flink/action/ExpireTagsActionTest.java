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

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.table.FileStoreTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.bEnv;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.init;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link ExpireTagsAction}. */
public class ExpireTagsActionTest extends ActionITCaseBase {

    @BeforeEach
    public void setUp() {
        init(warehouse);
    }

    @Test
    public void testExpireTags() throws Exception {
        bEnv.executeSql(
                "CREATE TABLE T (id STRING, name STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED)"
                        + " WITH ('bucket'='1', 'write-only'='true')");

        FileStoreTable table = getFileStoreTable("T");

        // generate 5 snapshots
        for (int i = 1; i <= 5; i++) {
            bEnv.executeSql("INSERT INTO T VALUES ('" + i + "', '" + i + "')").await();
        }
        assertThat(table.snapshotManager().snapshotCount()).isEqualTo(5);

        bEnv.executeSql("CALL sys.create_tag('default.T', 'tag-1', 1)").await();
        bEnv.executeSql("CALL sys.create_tag('default.T', 'tag-2', 2, '1h')").await();
        bEnv.executeSql("CALL sys.create_tag('default.T', 'tag-3', 3, '1h')").await();
        assertThat(table.tagManager().tags().size()).isEqualTo(3);

        createAction(
                        ExpireTagsAction.class,
                        "expire_tags",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        "T")
                .run();
        // no tags expired
        assertThat(table.tagManager().tags().size()).isEqualTo(3);

        bEnv.executeSql("CALL sys.create_tag('default.T', 'tag-4', 4, '1s')").await();
        bEnv.executeSql("CALL sys.create_tag('default.T', 'tag-5', 5, '1s')").await();
        assertThat(table.tagManager().tags().size()).isEqualTo(5);

        Thread.sleep(2000);
        createAction(
                        ExpireTagsAction.class,
                        "expire_tags",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        "T")
                .run();
        // tag-4,tag-5 expires
        assertThat(table.tagManager().tags().size()).isEqualTo(3);
        assertThat(table.tagManager().tagExists("tag-4")).isFalse();
        assertThat(table.tagManager().tagExists("tag-5")).isFalse();

        // tag-3 as the base older_than time
        LocalDateTime olderThanTime = table.tagManager().tag("tag-3").getTagCreateTime();
        java.sql.Timestamp timestamp =
                new java.sql.Timestamp(Timestamp.fromLocalDateTime(olderThanTime).getMillisecond());
        createAction(
                        ExpireTagsAction.class,
                        "expire_tags",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        "T",
                        "--older_than",
                        timestamp.toString())
                .run();
        // tag-1,tag-2 expires. tag-1 expired by its file creation time.
        assertThat(table.tagManager().tags().size()).isEqualTo(1);
        assertThat(table.tagManager().tagExists("tag-3")).isTrue();
    }
}
