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
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link RepairEarliestSnapshotProcedure}. */
public class RepairEarliestSnapshotProcedureITCase extends CatalogITCaseBase {

    @Test
    public void testRepairEarliestSnapshot() throws Exception {
        sql("CREATE TABLE T (k INT)");
        for (int i = 1; i <= 5; i++) {
            sql("INSERT INTO T VALUES (" + i + ")");
        }

        FileStoreTable table = paimonTable("T");
        SnapshotManager snapshotManager = table.snapshotManager();
        snapshotManager.deleteSnapshot(2);
        snapshotManager.deleteSnapshot(3);
        snapshotManager.deleteSnapshot(4);

        assertThat(
                        sql(
                                "CALL sys.repair_earliest_snapshot("
                                        + "`table` => 'default.T', snapshot_id => 5)"))
                .containsExactly(Row.of(1L, 5L));
        assertThat(snapshotManager.earliestSnapshotId()).isEqualTo(5);
    }
}
