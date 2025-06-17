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
import static org.junit.jupiter.api.Assertions.assertEquals;

/** IT Case for {@link RollbackToProcedure} and {@link RollbackToTimestampProcedure}. */
public class RollbackProcedureITCase extends CatalogITCaseBase {

    @Test
    public void testRollbackTo() throws Exception {
        sql(
                "CREATE TABLE T (id STRING, name STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED)"
                        + " WITH ('bucket'='1', 'write-only'='true')");

        FileStoreTable table = paimonTable("T");
        SnapshotManager snapshotManager = table.snapshotManager();

        for (int i = 1; i <= 5; i++) {
            sql("INSERT INTO T VALUES ('" + i + "', '" + i + "')");
        }
        assertEquals(5, snapshotManager.latestSnapshotId());

        sql("CALL sys.create_tag(`table` => 'default.T', tag => 'tag-2', snapshot_id => 2)");

        // rollback to snapshot_id
        long latestSnapshotId = snapshotManager.latestSnapshot().id();
        assertThat(sql("CALL sys.rollback_to(`table` => 'default.T', snapshot_id => 4)"))
                .containsExactly(Row.of(latestSnapshotId, 4L));

        // rollback to tag
        latestSnapshotId = snapshotManager.latestSnapshot().id();
        assertThat(sql("CALL sys.rollback_to(`table` => 'default.T', tag => 'tag-2')"))
                .containsExactly(Row.of(latestSnapshotId, 2L));
    }

    @Test
    public void testRollbackToTimestamp() throws Exception {
        sql(
                "CREATE TABLE T (id STRING, name STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED)"
                        + " WITH ('bucket'='1', 'write-only'='true')");

        FileStoreTable table = paimonTable("T");
        SnapshotManager snapshotManager = table.snapshotManager();

        sql("INSERT INTO T VALUES ('1', 'a')");
        sql("INSERT INTO T VALUES ('1', 'b')");
        long timestamp = System.currentTimeMillis();

        sql("INSERT INTO T VALUES ('3', 'c')");
        assertEquals(3, snapshotManager.latestSnapshotId());

        // rollback to timestamp
        long latestSnapshotId = snapshotManager.latestSnapshot().id();
        assertThat(
                        sql(
                                String.format(
                                        "CALL sys.rollback_to_timestamp(`table` => 'default.T', `timestamp` => %s)",
                                        timestamp)))
                .containsExactly(Row.of(latestSnapshotId, 2L));
    }
}
