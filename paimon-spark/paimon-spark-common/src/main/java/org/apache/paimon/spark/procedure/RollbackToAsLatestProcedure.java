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

package org.apache.paimon.spark.procedure;

import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.spark.SparkTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.StringUtils;
import org.apache.paimon.utils.TagManager;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.UUID;
import java.util.function.Function;

import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Rollback to a snapshot or tag as the latest snapshot, without dropping the snapshots and tags
 * created after it (unlike {@link RollbackProcedure}). Mirrors Flink's {@code
 * rollback_to_as_latest} procedure.
 */
public class RollbackToAsLatestProcedure extends BaseProcedure {

    private static final String ROLLBACK_TO_AS_LATEST_TAG_PREFIX = "rollback-to-as-latest-";

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("tag", StringType),
                ProcedureParameter.optional("snapshot_id", LongType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField(
                                "previous_snapshot_id",
                                DataTypes.LongType,
                                false,
                                Metadata.empty()),
                        new StructField(
                                "rolled_back_snapshot_id",
                                DataTypes.LongType,
                                false,
                                Metadata.empty()),
                        new StructField(
                                "current_snapshot_id", DataTypes.LongType, false, Metadata.empty())
                    });

    protected RollbackToAsLatestProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
        String tagName = args.isNullAt(1) ? null : args.getString(1);
        Long snapshotId = args.isNullAt(2) ? null : args.getLong(2);

        return modifyPaimonTableRefreshingCacheOnFailure(
                tableIdent,
                table -> {
                    FileStoreTable fileStoreTable = (FileStoreTable) table;
                    FileStore<?> store = fileStoreTable.store();
                    Snapshot latestSnapshot = store.snapshotManager().latestSnapshot();
                    Preconditions.checkNotNull(
                            latestSnapshot, "Latest snapshot is null, can not roll back.");

                    boolean hasTag = !StringUtils.isNullOrWhitespaceOnly(tagName);
                    boolean hasSnapshot = snapshotId != null;
                    Preconditions.checkArgument(
                            hasTag != hasSnapshot,
                            "Must specify exactly one of tag and snapshot_id.");

                    TagManager tagManager = store.newTagManager();
                    Tag targetTag;
                    Snapshot targetSnapshot;
                    if (hasTag) {
                        targetTag = tagManager.getOrThrow(tagName);
                        targetSnapshot = targetTag.trimToSnapshot();
                    } else {
                        targetTag = null;
                        targetSnapshot = findSnapshot(store, tagManager, snapshotId);
                    }

                    String createdRollbackTag = null;
                    boolean canDeleteCreatedTag = true;
                    String commitUser = ROLLBACK_TO_AS_LATEST_TAG_PREFIX + UUID.randomUUID();
                    try {
                        if (!hasTag) {
                            createdRollbackTag =
                                    createRollbackToAsLatestTag(tagManager, targetSnapshot);
                            targetTag = tagManager.getOrThrow(createdRollbackTag);
                        }
                        try (TableCommitImpl commit = fileStoreTable.newCommit(commitUser)) {
                            // The core rollback reads latest again, so another writer may change
                            // the rollback snapshot ID. An exception may also come after a
                            // successful commit (for example, from a callback). Keep the
                            // protection tag unless the commit returns false.
                            canDeleteCreatedTag = false;
                            boolean success = commit.rollbackToAsLatest(targetTag);
                            canDeleteCreatedTag = !success;
                            Preconditions.checkState(
                                    success,
                                    "Failed to roll back to snapshot %s as latest.",
                                    targetSnapshot.id());
                        }
                    } catch (Exception e) {
                        try {
                            if (createdRollbackTag != null && canDeleteCreatedTag) {
                                tagManager.deleteTag(
                                        createdRollbackTag,
                                        store.newTagDeletion(),
                                        store.snapshotManager(),
                                        Collections.emptyList());
                            }
                        } catch (Exception cleanupException) {
                            e.addSuppressed(cleanupException);
                        }
                        throw new RuntimeException(
                                String.format(
                                        "Failed to roll back to snapshot %s as latest.",
                                        targetSnapshot.id()),
                                e);
                    }

                    InternalRow outputRow =
                            newInternalRow(
                                    latestSnapshot.id(),
                                    targetSnapshot.id(),
                                    store.snapshotManager().latestSnapshotId());
                    return new InternalRow[] {outputRow};
                });
    }

    /**
     * Like {@link #modifyPaimonTable} but also refreshes Spark's cached plans when {@code func}
     * throws. {@code rollback_to_as_latest} can publish the rollback snapshot and then fail (for
     * example a post-commit callback throws), so the table state is already durable; the shared
     * success-only refresh would otherwise leave {@code CACHE TABLE} serving the pre-rollback data.
     * The original failure is preserved; a refresh error is only added as suppressed.
     */
    private InternalRow[] modifyPaimonTableRefreshingCacheOnFailure(
            Identifier ident, Function<org.apache.paimon.table.Table, InternalRow[]> func) {
        SparkTable sparkTable = loadSparkTable(ident);
        try {
            InternalRow[] result = func.apply(sparkTable.getTable());
            refreshSparkCache(ident, sparkTable);
            return result;
        } catch (RuntimeException e) {
            try {
                refreshSparkCache(ident, sparkTable);
            } catch (RuntimeException refreshError) {
                e.addSuppressed(refreshError);
            }
            throw e;
        }
    }

    private String createRollbackToAsLatestTag(TagManager tagManager, Snapshot targetSnapshot) {
        String tagName =
                ROLLBACK_TO_AS_LATEST_TAG_PREFIX + targetSnapshot.id() + "-" + UUID.randomUUID();
        tagManager.createTag(targetSnapshot, tagName, null, Collections.emptyList(), false);
        return tagName;
    }

    private Snapshot findSnapshot(FileStore<?> store, TagManager tagManager, long snapshotId) {
        SnapshotManager snapshotManager = store.snapshotManager();
        if (snapshotManager.snapshotExists(snapshotId)) {
            return snapshotManager.snapshot(snapshotId);
        }

        SortedMap<Snapshot, List<String>> tags = tagManager.tags();
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

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<RollbackToAsLatestProcedure>() {
            @Override
            public RollbackToAsLatestProcedure doBuild() {
                return new RollbackToAsLatestProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "RollbackToAsLatestProcedure";
    }
}
