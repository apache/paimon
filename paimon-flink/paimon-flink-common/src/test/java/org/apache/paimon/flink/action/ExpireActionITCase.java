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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.init;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link ExpireAction}. */
public class ExpireActionITCase extends ActionITCaseBase {
    private static final DataType[] FIELD_TYPES =
            new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()};

    private static final RowType ROW_TYPE = RowType.of(FIELD_TYPES, new String[] {"k", "v"});

    @BeforeEach
    public void setUp() {
        init(warehouse);
    }

    @Test
    public void testExpireWithNumber() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.WRITE_ONLY.key(), "true");
        FileStoreTable fileStoreTable = prepareExpireTable(options);

        ExpireAction action =
                new ExpireAction(warehouse, database, tableName, 1, 3, Long.MAX_VALUE, options);
        action.run();

        int latestSnapshotId = fileStoreTable.snapshotManager().latestSnapshotId().intValue();
        for (int i = 1; i <= latestSnapshotId; i++) {
            if (i > latestSnapshotId - 3) {
                assertThat(fileStoreTable.snapshotManager().snapshotExists(i)).isTrue();
            } else {
                assertThat(fileStoreTable.snapshotManager().snapshotExists(i)).isFalse();
            }
        }

        Path snapshotDir = fileStoreTable.snapshotManager().snapshotDirectory();
        Path earliest = new Path(snapshotDir, fileStoreTable.snapshotManager().EARLIEST);

        assertThat(fileStoreTable.fileIO().exists(earliest)).isTrue();

        Long earliestId = fileStoreTable.snapshotManager().earliestSnapshotId();

        // remove earliest hint file
        fileStoreTable.fileIO().delete(earliest, false);

        assertThat(fileStoreTable.snapshotManager().earliestSnapshotId()).isEqualTo(earliestId);
    }

    @Test
    public void testExpireWithTime() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.WRITE_ONLY.key(), "true");
        FileStoreTable fileStoreTable = prepareExpireTable(options);
        ExpireAction action =
                new ExpireAction(
                        warehouse, database, tableName, 1, Integer.MAX_VALUE, 1000, options);
        Thread.sleep(1500);
        writeData(rowData(5L, BinaryString.fromString("hi2")));
        writeData(rowData(6L, BinaryString.fromString("flink2")));
        writeData(rowData(7L, BinaryString.fromString("paimon2")));
        writeData(rowData(8L, BinaryString.fromString("apache2")));
        long expireMillis = System.currentTimeMillis();

        action.run();
        action.run();

        int latestSnapshotId = fileStoreTable.snapshotManager().latestSnapshotId().intValue();
        for (int i = 1; i <= latestSnapshotId; i++) {
            if (fileStoreTable.snapshotManager().snapshotExists(i)) {
                assertThat(fileStoreTable.snapshotManager().snapshot(i).timeMillis())
                        .isBetween(expireMillis - 1000, expireMillis);
            }
        }
    }

    @Test
    public void testNumRetainedMin() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.WRITE_ONLY.key(), "true");
        FileStoreTable fileStoreTable = prepareExpireTable(options);
        ExpireAction action =
                new ExpireAction(warehouse, database, tableName, 2, Integer.MAX_VALUE, 1, options);

        int lastestSnapshotId = fileStoreTable.snapshotManager().latestSnapshotId().intValue();
        Thread.sleep(100);
        action.run();

        for (int i = 1; i <= lastestSnapshotId - 2; i++) {
            assertThat(fileStoreTable.snapshotManager().snapshotExists(i)).isFalse();
        }
        for (int i = lastestSnapshotId - 2 + 1; i <= lastestSnapshotId; i++) {
            assertThat(fileStoreTable.snapshotManager().snapshotExists(i)).isTrue();
        }
    }

    private FileStoreTable prepareExpireTable(Map<String, String> options) throws Exception {

        FileStoreTable fileStoreTable =
                createFileStoreTable(
                        ROW_TYPE, Collections.emptyList(), Collections.singletonList("k"), options);
        StreamWriteBuilder writeBuilder =
                fileStoreTable.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();

        writeData(rowData(1L, BinaryString.fromString("hi")));
        writeData(rowData(2L, BinaryString.fromString("flink")));
        writeData(rowData(3L, BinaryString.fromString("paimon")));
        writeData(rowData(4L, BinaryString.fromString("apache")));
        return fileStoreTable;
    }
}
