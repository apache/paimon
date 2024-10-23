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
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

/**
 * Rollback procedure. Usage:
 *
 * <pre><code>
 *  -- rollback to a snapshot
 *  CALL sys.rollback_to(`table` => 'tableId', snapshot_id => snapshotId)
 *
 *  -- rollback to a tag
 *  CALL sys.rollback_to(`table` => 'tableId', tag => 'tagName')
 * </code></pre>
 */
public class RollbackToProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "rollback_to";

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "tag", type = @DataTypeHint("STRING"), isOptional = true),
                @ArgumentHint(
                        name = "snapshot_id",
                        type = @DataTypeHint("BIGINT"),
                        isOptional = true),
                @ArgumentHint(name = "timestamp", type = @DataTypeHint("BIGINT"), isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            String tagName,
            Long snapshotId,
            Long timestamp)
            throws Catalog.TableNotExistException {
        Table table = catalog.getTable(Identifier.fromString(tableId));
        if (!StringUtils.isNullOrWhitespaceOnly(tagName)) {
            table.rollbackTo(tagName);
        } else if (snapshotId != null) {
            table.rollbackTo(snapshotId);
        } else {
            FileStoreTable fileStoreTable = (FileStoreTable) table;
            Snapshot snapshot = fileStoreTable.snapshotManager().earlierOrEqualTimeMills(timestamp);
            Preconditions.checkNotNull(
                    snapshot, String.format("count not find snapshot earlier than %s", timestamp));
            fileStoreTable.rollbackTo(snapshot.id());
        }
        return new String[] {"Success"};
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
