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

import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.UUID;

/**
 * Rollback to as latest procedure. Usage:
 *
 * <pre><code>
 *  -- roll back to a snapshot as the latest snapshot
 *  CALL sys.rollback_to_as_latest(`table` => 'tableId', snapshot_id => snapshotId)
 *
 *  -- roll back to a tag as the latest snapshot
 *  CALL sys.rollback_to_as_latest(`table` => 'tableId', tag => 'tagName')
 * </code></pre>
 */
public class RollbackToAsLatestProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "rollback_to_as_latest";

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "tag", type = @DataTypeHint("STRING"), isOptional = true),
                @ArgumentHint(
                        name = "snapshot_id",
                        type = @DataTypeHint("BIGINT"),
                        isOptional = true)
            })
    public @DataTypeHint(
            "ROW<previous_snapshot_id BIGINT, rolled_back_snapshot_id BIGINT, current_snapshot_id BIGINT>")
    Row[] call(ProcedureContext procedureContext, String tableId, String tagName, Long snapshotId)
            throws Catalog.TableNotExistException {
        Table table = catalog.getTable(Identifier.fromString(tableId));
        FileStoreTable fileStoreTable = (FileStoreTable) table;

        FileStore<?> store = fileStoreTable.store();
        Snapshot latestSnapshot = store.snapshotManager().latestSnapshot();
        Preconditions.checkNotNull(latestSnapshot, "Latest snapshot is null, can not roll back.");

        boolean hasTag = !StringUtils.isNullOrWhitespaceOnly(tagName);
        boolean hasSnapshot = snapshotId != null;
        Preconditions.checkArgument(
                hasTag != hasSnapshot, "Must specify exactly one of tag and snapshot_id.");

        Snapshot targetSnapshot;
        if (hasTag) {
            targetSnapshot = store.newTagManager().getOrThrow(tagName).trimToSnapshot();
        } else {
            targetSnapshot = findSnapshot(store, snapshotId);
        }

        try (TableCommitImpl commit =
                fileStoreTable.newCommit("rollback-to-as-latest-" + UUID.randomUUID().toString())) {
            Preconditions.checkState(
                    commit.rollbackToAsLatest(targetSnapshot),
                    "Failed to roll back to snapshot %s as latest.",
                    targetSnapshot.id());
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to roll back to snapshot %s as latest.", targetSnapshot.id()),
                    e);
        }

        return new Row[] {
            Row.of(
                    latestSnapshot.id(),
                    targetSnapshot.id(),
                    store.snapshotManager().latestSnapshotId())
        };
    }

    private Snapshot findSnapshot(FileStore<?> store, long snapshotId) {
        SnapshotManager snapshotManager = store.snapshotManager();
        if (snapshotManager.snapshotExists(snapshotId)) {
            return snapshotManager.snapshot(snapshotId);
        }

        SortedMap<Snapshot, List<String>> tags = store.newTagManager().tags();
        for (Map.Entry<Snapshot, List<String>> entry : tags.entrySet()) {
            if (entry.getKey().id() == snapshotId) {
                return entry.getKey();
            } else if (entry.getKey().id() > snapshotId) {
                break;
            }
        }

        throw new IllegalArgumentException(
                String.format("Snapshot '%s' to roll back to doesn't exist.", snapshotId));
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
