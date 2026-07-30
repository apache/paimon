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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

/** A procedure to repair the earliest snapshot. */
public class RepairEarliestSnapshotProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "repair_earliest_snapshot";

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "snapshot_id", type = @DataTypeHint("BIGINT"))
            })
    public @DataTypeHint(
            "ROW<previous_earliest_snapshot_id BIGINT, current_earliest_snapshot_id BIGINT>") Row[]
            call(ProcedureContext procedureContext, String tableId, Long snapshotId)
                    throws Catalog.TableNotExistException {
        SnapshotManager snapshotManager = ((FileStoreTable) table(tableId)).snapshotManager();
        long previous = snapshotManager.repairEarliestSnapshot(snapshotId);
        return new Row[] {Row.of(previous, snapshotId)};
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
