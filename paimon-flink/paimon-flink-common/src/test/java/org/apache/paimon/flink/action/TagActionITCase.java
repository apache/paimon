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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TagManager;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.init;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.testBatchRead;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for tag management actions. */
public class TagActionITCase extends ActionITCaseBase {

    @Test
    public void testCreateAndDeleteTag() throws Exception {
        init(warehouse);

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()},
                        new String[] {"k", "v"});
        FileStoreTable table =
                createFileStoreTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        Collections.emptyMap());

        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();

        // 3 snapshots
        writeData(rowData(1L, BinaryString.fromString("Hi")));
        writeData(rowData(2L, BinaryString.fromString("Hello")));
        writeData(rowData(3L, BinaryString.fromString("Paimon")));

        TagManager tagManager = new TagManager(table.fileIO(), table.location());

        CreateTagAction createTagAction =
                new CreateTagAction(
                        warehouse, database, tableName, Collections.emptyMap(), "tag2", 2);
        createTagAction.run();
        assertThat(tagManager.tagExists("tag2")).isTrue();

        // read tag2
        testBatchRead(
                "SELECT * FROM `" + tableName + "` /*+ OPTIONS('scan.tag-name'='tag2') */",
                Arrays.asList(Row.of(1L, "Hi"), Row.of(2L, "Hello")));

        DeleteTagAction deleteTagAction =
                new DeleteTagAction(warehouse, database, tableName, Collections.emptyMap(), "tag2");
        deleteTagAction.run();
        assertThat(tagManager.tagExists("tag2")).isFalse();
    }
}
