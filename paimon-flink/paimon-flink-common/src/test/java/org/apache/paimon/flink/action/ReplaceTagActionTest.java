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

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.TagManager;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.bEnv;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.init;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link ReplaceTagAction}. */
public class ReplaceTagActionTest extends ActionITCaseBase {

    @BeforeEach
    public void setUp() {
        init(warehouse);
    }

    @Test
    public void testReplaceTag() throws Exception {
        bEnv.executeSql(
                "CREATE TABLE T (id INT, name STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED)"
                        + " WITH ('bucket'='1')");

        FileStoreTable table = getFileStoreTable("T");
        TagManager tagManager = table.tagManager();

        bEnv.executeSql("INSERT INTO T VALUES (1, 'a')").await();
        bEnv.executeSql("INSERT INTO T VALUES (2, 'b')").await();
        assertThat(table.snapshotManager().snapshotCount()).isEqualTo(2);

        Assertions.assertThatThrownBy(
                        () ->
                                bEnv.executeSql(
                                        "CALL sys.replace_tag(`table` => 'default.T', tag => 'test_tag')"))
                .hasMessageContaining("Tag name 'test_tag' does not exist.");

        bEnv.executeSql("CALL sys.create_tag(`table` => 'default.T', tag => 'test_tag')");
        assertThat(tagManager.tagExists("test_tag")).isEqualTo(true);
        assertThat(tagManager.tag("test_tag").trimToSnapshot().id()).isEqualTo(2);
        assertThat(tagManager.tag("test_tag").getTagTimeRetained()).isEqualTo(null);

        // replace tag with new time_retained
        createAction(
                        ReplaceTagAction.class,
                        "replace_tag",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        "T",
                        "--tag_name",
                        "test_tag",
                        "--time_retained",
                        "1 d")
                .run();
        assertThat(tagManager.tag("test_tag").getTagTimeRetained().toHours()).isEqualTo(24);

        // replace tag with new snapshot and time_retained
        createAction(
                        ReplaceTagAction.class,
                        "replace_tag",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        "T",
                        "--tag_name",
                        "test_tag",
                        "--snapshot",
                        "1",
                        "--time_retained",
                        "2 d")
                .run();
        assertThat(tagManager.tag("test_tag").trimToSnapshot().id()).isEqualTo(1);
        assertThat(tagManager.tag("test_tag").getTagTimeRetained().toHours()).isEqualTo(48);
    }
}
