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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

/**
 * Rollback to timestamp procedure. Usage:
 *
 * <pre><code>
 *  -- rollback to the snapshot which earlier or equal than timestamp.
 *  CALL sys.rollback_to_timestamp(`table` => 'tableId', timestamp => timestamp)
 * </code></pre>
 */
public class RollbackToTimestampProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "rollback_to_timestamp";

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "timestamp", type = @DataTypeHint("BIGINT"))
            })
    public @DataTypeHint("ROW<previous_snapshot_id BIGINT, current_snapshot_id BIGINT>") Row[] call(
            ProcedureContext procedureContext, String tableId, Long timestamp)
            throws Catalog.TableNotExistException {
        Table table = catalog.getTable(Identifier.fromString(tableId));
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        SnapshotManager snapshotManager = fileStoreTable.snapshotManager();
        Snapshot latestSnapshot = snapshotManager.latestSnapshot();
        Preconditions.checkNotNull(latestSnapshot, "Latest snapshot is null, can not rollback.");

        Snapshot snapshot = snapshotManager.earlierOrEqualTimeMills(timestamp);
        Preconditions.checkNotNull(
                snapshot, String.format("count not find snapshot earlier than %s", timestamp));
        long snapshotId = snapshot.id();
        fileStoreTable.rollbackTo(snapshotId);
        return new Row[] {Row.of(latestSnapshot.id(), snapshotId)};
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
