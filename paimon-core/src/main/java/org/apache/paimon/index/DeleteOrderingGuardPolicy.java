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

package org.apache.paimon.index;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.operation.commit.DeleteOrderingGuardGeneration;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaValidation;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyTableUtils;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.SnapshotManager;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.index.DeleteOrderingGuardFile.DELETE_ORDERING_GUARD;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** The ordering and deletion semantics under which retained bounds were captured. */
public class DeleteOrderingGuardPolicy {

    private DeleteOrderingGuardPolicy() {}

    public static void validateRescale(FileStoreTable table, Snapshot snapshot) {
        checkArgument(
                !table.coreOptions().deleteOrderingGuardsEnabled()
                        && (!DeleteOrderingGuardFile.mayContainGuards(snapshot)
                                || table.store()
                                        .newIndexFileHandler()
                                        .scan(snapshot, DELETE_ORDERING_GUARD)
                                        .isEmpty()),
                "Rescaling tables with delete ordering guards is not supported.");
    }

    /** Rewinding snapshot metadata must not revive writes fenced by an ordering-history reset. */
    public static void validateHistoryRewrite(
            CoreOptions options, SnapshotManager snapshotManager, String operation) {
        Snapshot latest;
        try {
            latest = snapshotManager.latestSnapshot();
        } catch (RuntimeException e) {
            if (!(e.getCause() instanceof FileNotFoundException)) {
                throw e;
            }
            // A missing latest hint can be recovered, but retained history still fences writes.
            try {
                latest =
                        snapshotManager.safelyGetAllSnapshots().stream()
                                .max(Comparator.comparingLong(Snapshot::id))
                                .orElse(null);
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }
        if (options.deleteOrderingGuardsEnabled()
                || DeleteOrderingGuardGeneration.isTracked(latest)
                || DeleteOrderingGuardFile.mayContainGuards(latest)) {
            throw new UnsupportedOperationException(
                    operation
                            + " is not supported for delete ordering guard histories. "
                            + "Use rollbackToAsLatest or sys.rollback_to_as_latest to restore a snapshot "
                            + "while preserving writer fencing.");
        }
    }

    public static void validateFastForward(
            CoreOptions options,
            SnapshotManager snapshotManager,
            SchemaManager schemaManager,
            String branchName) {
        BranchManager.fastForwardValidate(branchName, snapshotManager.branch());
        validateHistoryRewrite(options, snapshotManager, "Fast-forward");
        schemaManager
                .copyWithBranch(branchName)
                .latest()
                .ifPresent(
                        schema ->
                                validateHistoryRewrite(
                                        new CoreOptions(schema.options()),
                                        snapshotManager.copyWithBranch(branchName),
                                        "Fast-forward"));
    }

    /** Payload schema evolution and capture toggles do not change the interpretation of a bound. */
    public static void validateCompatible(TableSchema source, TableSchema target) {
        CoreOptions before = new CoreOptions(source.options());
        CoreOptions after = new CoreOptions(target.options());
        checkArgument(
                orderingOptions(before).equals(orderingOptions(after)),
                "Delete ordering guard ordering/deletion policy changed since schema %s; "
                        + "an ordering migration is required. Reset the complete table history "
                        + "before altering the policy, then reopen writers and reload authoritative data.",
                source.id());
        RowType sourceKey = keyType(source);
        RowType targetKey = keyType(target);
        checkArgument(
                sourceKey.equals(targetKey),
                "Delete ordering guard primary key schema changed; an ordering migration is required.");
        checkArgument(
                orderingType(source, before).equals(orderingType(target, after)),
                "Delete ordering guard ordering field schema changed; an ordering migration is required.");
    }

    /** Validate actual segment headers rather than a snapshot's possibly newer schema id. */
    public static boolean validateGuards(
            FileIO fileIO,
            SchemaManager schemas,
            List<IndexManifestEntry> entries,
            Function<IndexManifestEntry, Path> paths,
            TableSchema target)
            throws IOException {
        Set<Long> checked = new HashSet<>();
        boolean retained = false;
        for (IndexManifestEntry entry : entries) {
            if (DELETE_ORDERING_GUARD.equals(entry.indexFile().indexType())) {
                if (!retained) {
                    SchemaValidation.validateRetainedDeleteOrderingGuards(target);
                    retained = true;
                }
                long schemaId = DeleteOrderingGuardFile.readSchemaId(fileIO, paths.apply(entry));
                if (checked.add(schemaId)) {
                    validateCompatible(schemas.schema(schemaId), target);
                }
            }
        }
        return retained;
    }

    private static RowType orderingType(TableSchema schema, CoreOptions options) {
        Set<String> fields = DeleteOrderingGuardSerializer.orderingFields(options);
        return new RowType(
                schema.fields().stream()
                        .filter(field -> fields.contains(field.name()))
                        .collect(Collectors.toList()));
    }

    private static RowType keyType(TableSchema schema) {
        return new RowType(
                PrimaryKeyTableUtils.PrimaryKeyFieldsExtractor.EXTRACTOR.keyFields(schema));
    }

    /** Use the retained snapshot's policy unless the caller explicitly overrides that option. */
    public static void restoreHistoricalOptions(
            TableSchema historical, Map<String, String> options, Set<String> explicit) {
        Set<String> keys = new HashSet<>(options.keySet());
        keys.addAll(historical.options().keySet());
        Set<String> fixed =
                new HashSet<>(
                        Arrays.asList(
                                CoreOptions.MERGE_ENGINE.key(),
                                CoreOptions.SEQUENCE_FIELD.key(),
                                CoreOptions.SEQUENCE_FIELD_SORT_ORDER.key(),
                                CoreOptions.SEQUENCE_SNAPSHOT_ORDERING.key(),
                                CoreOptions.IGNORE_DELETE.key()));
        for (String key : keys) {
            if (!explicit.contains(key) && fixed.contains(key)) {
                if (historical.options().containsKey(key)) {
                    options.put(key, historical.options().get(key));
                } else {
                    options.remove(key);
                }
            }
        }
    }

    public static Map<String, Object> orderingOptions(CoreOptions options) {
        Map<String, Object> result = new HashMap<>();
        result.put("engine", options.mergeEngine());
        result.put("sequence", options.sequenceField());
        result.put("direction", options.sequenceFieldSortOrderIsAscending());
        result.put("ignore-delete", options.ignoreDelete());
        result.put("snapshot-ordering", options.snapshotSequenceOrdering());
        return result;
    }
}
