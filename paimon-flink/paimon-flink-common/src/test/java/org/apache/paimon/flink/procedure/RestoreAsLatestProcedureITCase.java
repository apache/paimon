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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** IT cases for restore_as_latest procedure. */
public class RestoreAsLatestProcedureITCase extends CatalogITCaseBase {

    @Test
    public void testRestoreSnapshotAsLatest() throws Exception {
        sql("CREATE TABLE T (id INT, name STRING)");

        FileStoreTable table = paimonTable("T");
        SnapshotManager snapshotManager = table.snapshotManager();

        commitRow(table, 1, "a");
        commitRow(table, 2, "b");
        commitRow(table, 3, "c");
        assertEquals(3, snapshotManager.latestSnapshotId());

        assertThat(sql("CALL sys.restore_as_latest(`table` => 'default.T', snapshot_id => 1)"))
                .containsExactly(Row.of(3L, 1L, 4L));

        assertEquals(4, snapshotManager.latestSnapshotId());
        assertTrue(snapshotManager.snapshotExists(2));
        assertTrue(snapshotManager.snapshotExists(3));
        assertThat(sql("SELECT * FROM T")).containsExactly(Row.of(1, "a"));

        commitRow(table, 4, "d");
        assertEquals(5, snapshotManager.latestSnapshotId());
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(Row.of(1, "a"), Row.of(4, "d"));
    }

    @Test
    public void testRestoreTagAsLatest() throws Exception {
        sql("CREATE TABLE T (id INT, name STRING)");

        FileStoreTable table = paimonTable("T");
        SnapshotManager snapshotManager = table.snapshotManager();

        commitRow(table, 1, "a");
        commitRow(table, 2, "b");
        commitRow(table, 3, "c");
        assertEquals(3, snapshotManager.latestSnapshotId());

        sql("CALL sys.create_tag(`table` => 'default.T', tag => 'tag-1', snapshot_id => 1)");

        assertThat(sql("CALL sys.restore_as_latest(`table` => 'default.T', tag => 'tag-1')"))
                .containsExactly(Row.of(3L, 1L, 4L));

        assertEquals(4, snapshotManager.latestSnapshotId());
        assertTrue(snapshotManager.snapshotExists(2));
        assertTrue(snapshotManager.snapshotExists(3));
        assertThat(sql("SELECT * FROM T")).containsExactly(Row.of(1, "a"));
    }

    private void commitRow(FileStoreTable table, int id, String name) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(id, BinaryString.fromString(name)));
            commit.commit(write.prepareCommit());
        }
    }
}
