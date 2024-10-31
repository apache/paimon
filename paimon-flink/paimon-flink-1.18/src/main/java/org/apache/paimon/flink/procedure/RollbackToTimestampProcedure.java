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

import org.apache.flink.table.procedure.ProcedureContext;

/**
 * Rollback to timestamp procedure. Usage:
 *
 * <pre><code>
 *  -- rollback to the snapshot which earlier or equal than timestamp.
 *  CALL sys.rollback_to_timestamp('tableId', timestamp)
 * </code></pre>
 */
public class RollbackToTimestampProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "rollback_to_timestamp";

    public String[] call(ProcedureContext procedureContext, String tableId, long timestamp)
            throws Catalog.TableNotExistException {
        Preconditions.checkNotNull(tableId, "table can not be empty");
        Table table = catalog.getTable(Identifier.fromString(tableId));
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        Snapshot snapshot = fileStoreTable.snapshotManager().earlierOrEqualTimeMills(timestamp);
        Preconditions.checkNotNull(
                snapshot, String.format("count not find snapshot earlier than %s", timestamp));
        long snapshotId = snapshot.id();
        fileStoreTable.rollbackTo(snapshotId);
        return new String[] {String.format("Success roll back to snapshot: %s .", snapshotId)};
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
