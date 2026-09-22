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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.FileStoreCommitImpl;
import org.apache.paimon.rest.RESTCatalog;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaValidation;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.RetryWaiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * Enables data evolution on an existing append table without rewriting its data files.
 *
 * <p>{@code row-tracking.enabled} and {@code data-evolution.enabled} are immutable for {@code ALTER
 * TABLE} because a data-evolution table derives every row id from the first row id of its file, and
 * only the commit that adds a file assigns one. Files that are already in the table would therefore
 * never get a row id. This class closes that gap in three steps:
 *
 * <ol>
 *   <li>Assign a first row id to every live data file that has none, by rewriting the manifests of
 *       the latest snapshot and committing them as a metadata-only snapshot. Ids are contiguous per
 *       partition, in the order {@code sys.reassign_row_id} would produce, so the converted table
 *       needs no reassignment afterwards.
 *   <li>Commit a schema with both options enabled through the catalog, so that catalog metadata
 *       stays in sync.
 *   <li>Assign ids to any file that a writer on the previous schema committed in between, now that
 *       every later commit assigns ids on its own. A writer that loaded the table before the switch
 *       is refused from then on, see {@code FileStoreCommitImpl}.
 * </ol>
 *
 * <p>The procedure is idempotent: on a table that already has data evolution enabled it only
 * assigns ids to files that still lack one, and reports {@code skipped} when there are none.
 */
public class DataEvolutionEnabler {

    private static final Logger LOG = LoggerFactory.getLogger(DataEvolutionEnabler.class);
    private static final String COMMIT_USER_PREFIX = "enable-data-evolution";
    private static final int MAX_REPAIR_ROUNDS = 5;

    private final Catalog catalog;
    private final Identifier identifier;
    private final Runnable beforeRowIdCommit;
    private final Runnable beforeSchemaChange;

    public DataEvolutionEnabler(Catalog catalog, Identifier identifier) {
        this(catalog, identifier, () -> {}, () -> {});
    }

    /** Hooks for tests to inject concurrent activity between the steps. */
    DataEvolutionEnabler(
            Catalog catalog,
            Identifier identifier,
            Runnable beforeRowIdCommit,
            Runnable beforeSchemaChange) {
        this.catalog = catalog;
        this.identifier = identifier;
        this.beforeRowIdCommit = beforeRowIdCommit;
        this.beforeSchemaChange = beforeSchemaChange;
    }

    /** Validates and, unless {@code dryRun}, converts the table. */
    public Result run(boolean dryRun) throws Exception {
        FileStoreTable table = loadTable();
        CoreOptions options = table.coreOptions();
        boolean enabled = options.rowTrackingEnabled() && options.dataEvolutionEnabled();
        validate(table, enabled);

        long schemaBefore = table.schema().id();
        Long snapshotBefore = table.snapshotManager().latestSnapshotId();
        Assignment planned = plan(table);
        if (enabled && planned.files.isEmpty()) {
            return Result.skipped(
                    schemaBefore, snapshotBefore, "data evolution is already enabled");
        }
        if (dryRun) {
            return Result.dryRun(schemaBefore, snapshotBefore, enabled, planned);
        }

        long assignedFiles = 0;
        long assignedRows = 0;
        Long nextRowId = planned.files.isEmpty() ? null : planned.nextRowId;
        if (!planned.files.isEmpty()) {
            Committed committed = assignRowIdsWithRetry(table, planned);
            assignedFiles += committed.assignment.files.size();
            assignedRows += committed.assignment.rowCount;
            nextRowId = committed.assignment.nextRowId;
        }

        if (!enabled) {
            beforeSchemaChange.run();
            catalog.alterTable(identifier, SchemaChange.enableDataEvolution(), false);
            table = loadTable();
            checkState(
                    table.coreOptions().rowTrackingEnabled()
                            && table.coreOptions().dataEvolutionEnabled(),
                    "Schema change did not enable data evolution on table %s.",
                    identifier.getFullName());

            // Repair what a writer on the previous schema committed between the two steps.
            for (int round = 0; round < MAX_REPAIR_ROUNDS; round++) {
                Assignment remaining = plan(table);
                if (remaining.files.isEmpty()) {
                    break;
                }
                LOG.info(
                        "Assigning row ids to {} file(s) committed to table {} while data evolution was being enabled.",
                        remaining.files.size(),
                        identifier.getFullName());
                Committed committed = assignRowIdsWithRetry(table, remaining);
                assignedFiles += committed.assignment.files.size();
                assignedRows += committed.assignment.rowCount;
                nextRowId = committed.assignment.nextRowId;
            }
            checkState(
                    plan(table).files.isEmpty(),
                    "Table %s still has data files without a row id after %s repair rounds; "
                            + "stop the writers that predate the schema change and run the "
                            + "procedure again.",
                    identifier.getFullName(),
                    MAX_REPAIR_ROUNDS);
        }

        return new Result(
                schemaBefore,
                table.schema().id(),
                snapshotBefore,
                table.snapshotManager().latestSnapshotId(),
                assignedFiles,
                assignedRows,
                nextRowId,
                false,
                false,
                null);
    }

    private FileStoreTable loadTable() throws Exception {
        Table table = catalog.getTable(identifier);
        checkArgument(
                table instanceof FileStoreTable,
                "Only a FileStoreTable can enable data evolution, but table %s is a %s.",
                identifier.getFullName(),
                table.getClass().getSimpleName());
        return (FileStoreTable) table;
    }

    private void validate(FileStoreTable table, boolean enabled) {
        checkArgument(
                !(DelegateCatalog.rootCatalog(catalog) instanceof RESTCatalog),
                "Enabling data evolution on table %s of a REST catalog is not supported yet.",
                identifier.getFullName());
        if (enabled) {
            return;
        }
        // The constraints of a row-tracking table are the ones of the schema this will create.
        TableSchema current = table.schema();
        Map<String, String> options = new HashMap<>(current.options());
        options.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        options.put(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        try {
            SchemaValidation.validateTableSchema(
                    new TableSchema(
                            current.id() + 1,
                            current.fields(),
                            current.highestFieldId(),
                            current.partitionKeys(),
                            current.primaryKeys(),
                            options,
                            current.comment()));
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot enable data evolution on table %s: %s",
                            identifier.getFullName(), e.getMessage()),
                    e);
        }
    }

    /** Plans a first row id for every live data file of the latest snapshot that has none. */
    private Assignment plan(FileStoreTable table) {
        Snapshot latest = table.snapshotManager().latestSnapshot();
        if (latest == null) {
            return Assignment.empty(null, 0L);
        }
        ManifestFile manifestFile = table.store().manifestFileFactory().create();
        ManifestList manifestList = table.store().manifestListFactory().create();
        List<ManifestFileMeta> manifests = manifestList.readDataManifests(latest);

        Map<FileEntry.Identifier, ManifestEntry> live = new LinkedHashMap<>();
        FileEntry.mergeEntries(
                manifestFile, manifests, live, table.coreOptions().scanManifestParallelism());

        List<ManifestEntry> withoutRowId = new ArrayList<>();
        for (ManifestEntry entry : live.values()) {
            if (entry.kind() == FileKind.ADD && entry.file().firstRowId() == null) {
                withoutRowId.add(entry);
            }
        }
        long start = latest.nextRowId() == null ? 0L : latest.nextRowId();
        if (withoutRowId.isEmpty()) {
            return Assignment.empty(latest, start);
        }

        // Contiguous per partition, partitions in order: the layout reassign_row_id produces.
        // Within a partition the files keep the order they have in the manifests, which is the
        // order they were committed in (the sort is stable).
        RecordComparator partitionComparator =
                CodeGenUtils.newRecordComparator(
                        table.schema().logicalPartitionType().getFieldTypes());
        withoutRowId.sort(
                (left, right) -> partitionComparator.compare(left.partition(), right.partition()));

        Map<FileEntry.Identifier, Long> firstRowIds = new HashMap<>();
        long next = start;
        long rowCount = 0;
        for (ManifestEntry entry : withoutRowId) {
            firstRowIds.put(entry.identifier(), next);
            next += entry.file().rowCount();
            rowCount += entry.file().rowCount();
        }
        return new Assignment(latest, manifests, withoutRowId, firstRowIds, rowCount, next);
    }

    private Committed assignRowIdsWithRetry(FileStoreTable table, Assignment initial)
            throws Exception {
        CoreOptions options = table.coreOptions();
        RetryWaiter retryWaiter =
                new RetryWaiter(options.commitMinRetryWait(), options.commitMaxRetryWait());
        long startMillis = System.currentTimeMillis();
        Assignment assignment = initial;
        int retryCount = 0;
        while (true) {
            if (commitAssignment(table, assignment)) {
                return new Committed(assignment);
            }
            if (System.currentTimeMillis() - startMillis > options.commitTimeout()
                    || retryCount >= options.commitMaxRetries()) {
                throw new RuntimeException(
                        String.format(
                                "Failed to assign row ids to table %s after %s millis and %s "
                                        + "retries because newer snapshots kept being committed.",
                                identifier.getFullName(),
                                System.currentTimeMillis() - startMillis,
                                retryCount));
            }
            retryWaiter.retryWait(retryCount);
            retryCount++;
            // Another commit landed: plan again from the new latest snapshot. Files that already
            // received an id in it (written by a writer on the new schema) keep it.
            assignment = plan(table);
            if (assignment.files.isEmpty()) {
                return new Committed(assignment);
            }
            LOG.info(
                    "Retrying row id assignment for table {} on snapshot {} ({}/{}).",
                    identifier.getFullName(),
                    assignment.snapshot.id(),
                    retryCount,
                    options.commitMaxRetries());
        }
    }

    /**
     * Rewrites the manifests holding the planned files and commits them, referencing the table's
     * current schema. Returns false when the snapshot moved on in the meantime.
     */
    private boolean commitAssignment(FileStoreTable table, Assignment assignment) {
        ManifestFile manifestFile = table.store().manifestFileFactory().create();
        ManifestList manifestList = table.store().manifestListFactory().create();

        List<ManifestFileMeta> baseManifests = new ArrayList<>();
        for (ManifestFileMeta manifest : assignment.manifests) {
            List<ManifestEntry> entries =
                    manifestFile.read(manifest.fileName(), manifest.fileSize());
            List<ManifestEntry> rewritten = new ArrayList<>(entries.size());
            boolean changed = false;
            for (ManifestEntry entry : entries) {
                Long firstRowId = assignment.firstRowIds.get(entry.identifier());
                if (firstRowId != null && entry.file().firstRowId() == null) {
                    rewritten.add(entry.assignFirstRowId(firstRowId));
                    changed = true;
                } else {
                    rewritten.add(entry);
                }
            }
            if (changed) {
                baseManifests.addAll(manifestFile.write(rewritten));
            } else {
                baseManifests.add(manifest);
            }
        }

        Pair<String, Long> baseManifestList = manifestList.write(baseManifests);
        Pair<String, Long> deltaManifestList = manifestList.write(Collections.emptyList());
        String commitUser = COMMIT_USER_PREFIX + "-" + UUID.randomUUID();
        try (FileStoreCommitImpl commit =
                (FileStoreCommitImpl) table.store().newCommit(commitUser, table)) {
            beforeRowIdCommit.run();
            return commit.replaceManifestList(
                    assignment.snapshot,
                    table.schema().id(),
                    assignment.snapshot.totalRecordCount(),
                    baseManifestList,
                    deltaManifestList,
                    assignment.snapshot.indexManifest(),
                    assignment.nextRowId,
                    assignment.snapshot.properties());
        }
    }

    private static class Assignment {
        @Nullable final Snapshot snapshot;
        final List<ManifestFileMeta> manifests;
        final List<ManifestEntry> files;
        final Map<FileEntry.Identifier, Long> firstRowIds;
        final long rowCount;
        final long nextRowId;

        Assignment(
                @Nullable Snapshot snapshot,
                List<ManifestFileMeta> manifests,
                List<ManifestEntry> files,
                Map<FileEntry.Identifier, Long> firstRowIds,
                long rowCount,
                long nextRowId) {
            this.snapshot = snapshot;
            this.manifests = manifests;
            this.files = files;
            this.firstRowIds = firstRowIds;
            this.rowCount = rowCount;
            this.nextRowId = nextRowId;
        }

        static Assignment empty(@Nullable Snapshot snapshot, long nextRowId) {
            return new Assignment(
                    snapshot,
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyMap(),
                    0L,
                    nextRowId);
        }
    }

    private static class Committed {
        final Assignment assignment;

        Committed(Assignment assignment) {
            this.assignment = assignment;
        }
    }

    /** What the procedure did, or would do. */
    public static class Result {
        public final long schemaBefore;
        public final long schemaAfter;
        @Nullable public final Long snapshotBefore;
        @Nullable public final Long snapshotAfter;
        public final long assignedFileCount;
        public final long assignedRowCount;
        @Nullable public final Long nextRowId;
        public final boolean skipped;
        public final boolean dryRun;
        @Nullable public final String skipReason;

        Result(
                long schemaBefore,
                long schemaAfter,
                @Nullable Long snapshotBefore,
                @Nullable Long snapshotAfter,
                long assignedFileCount,
                long assignedRowCount,
                @Nullable Long nextRowId,
                boolean skipped,
                boolean dryRun,
                @Nullable String skipReason) {
            this.schemaBefore = schemaBefore;
            this.schemaAfter = schemaAfter;
            this.snapshotBefore = snapshotBefore;
            this.snapshotAfter = snapshotAfter;
            this.assignedFileCount = assignedFileCount;
            this.assignedRowCount = assignedRowCount;
            this.nextRowId = nextRowId;
            this.skipped = skipped;
            this.dryRun = dryRun;
            this.skipReason = skipReason;
        }

        static Result skipped(long schema, @Nullable Long snapshot, String reason) {
            return new Result(schema, schema, snapshot, snapshot, 0, 0, null, true, false, reason);
        }

        static Result dryRun(
                long schema, @Nullable Long snapshot, boolean enabled, Assignment planned) {
            return new Result(
                    schema,
                    enabled ? schema : schema + 1,
                    snapshot,
                    snapshot,
                    planned.files.size(),
                    planned.rowCount,
                    planned.files.isEmpty() ? null : planned.nextRowId,
                    false,
                    true,
                    null);
        }

        /** One-line summary for procedure output. */
        public String describe(Identifier identifier) {
            if (skipped) {
                return String.format(
                        "Skipped. Table '%s' was not changed: %s.",
                        identifier.getFullName(), skipReason);
            }
            String work =
                    String.format(
                            "schema %d -> %d, snapshot %s -> %s, %d file(s) with %d row(s) assigned "
                                    + "row ids, nextRowId=%s",
                            schemaBefore,
                            schemaAfter,
                            snapshotBefore,
                            snapshotAfter,
                            assignedFileCount,
                            assignedRowCount,
                            nextRowId);
            return dryRun
                    ? String.format(
                            "Dry run. Enabling data evolution on table '%s' would do: %s.",
                            identifier.getFullName(), work)
                    : String.format(
                            "Success. Enabled data evolution on table '%s': %s.",
                            identifier.getFullName(), work);
        }
    }
}
