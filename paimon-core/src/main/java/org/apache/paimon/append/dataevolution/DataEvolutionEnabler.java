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
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.operation.FileStoreCommitImpl;
import org.apache.paimon.rest.RESTCatalog;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaValidation;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchWriteBuilder;
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
 * never get a row id. This class closes that gap in four steps:
 *
 * <ol>
 *   <li>Assign a first row id to every live data file that has none, by rewriting the manifests of
 *       the latest snapshot and committing them as a metadata-only snapshot. Ids are contiguous per
 *       partition, in the order {@code sys.reassign_row_id} would produce, so the converted table
 *       needs no reassignment afterwards. The snapshot is marked as one whose live files all have a
 *       row id, see {@link #rowIdsAssigned(Snapshot)}.
 *   <li>Commit a schema with both options enabled through the catalog, so that catalog metadata
 *       stays in sync. The schema change is accepted only while the latest snapshot is such a
 *       marked one (or the table has none): when a writer committed in between, steps 1 and 2 are
 *       repeated.
 *   <li>Commit a fence: an empty snapshot on the new schema. A writer that checked the previous
 *       schema read its base snapshot before that check, so it either committed before the fence,
 *       or its commit loses the race for the next snapshot id and is refused on retry, see {@code
 *       FileStoreCommitImpl}.
 *   <li>Assign ids to the files that such writers committed before the fence.
 * </ol>
 *
 * <p>The procedure is idempotent: on a table that already has data evolution enabled it only
 * assigns ids to files that still lack one, and reports {@code skipped} when there are none.
 */
public class DataEvolutionEnabler {

    private static final Logger LOG = LoggerFactory.getLogger(DataEvolutionEnabler.class);
    private static final String COMMIT_USER_PREFIX = "enable-data-evolution";

    /**
     * Snapshot property set by the row id commits of this class. Its value is the id of the
     * snapshot it was written to, so that a later snapshot which copies the properties of its base
     * does not carry the mark.
     */
    public static final String ROW_IDS_ASSIGNED_SNAPSHOT_ID =
            "data-evolution.row-ids-assigned-snapshot-id";

    private final Catalog catalog;
    private final Identifier identifier;
    private final Runnable beforeRowIdCommit;
    private final Runnable beforeSchemaChange;
    private final Runnable beforeFence;

    public DataEvolutionEnabler(Catalog catalog, Identifier identifier) {
        this(catalog, identifier, () -> {}, () -> {}, () -> {});
    }

    DataEvolutionEnabler(
            Catalog catalog,
            Identifier identifier,
            Runnable beforeRowIdCommit,
            Runnable beforeSchemaChange) {
        this(catalog, identifier, beforeRowIdCommit, beforeSchemaChange, () -> {});
    }

    /** Hooks for tests to inject concurrent activity between the steps. */
    DataEvolutionEnabler(
            Catalog catalog,
            Identifier identifier,
            Runnable beforeRowIdCommit,
            Runnable beforeSchemaChange,
            Runnable beforeFence) {
        this.catalog = catalog;
        this.identifier = identifier;
        this.beforeRowIdCommit = beforeRowIdCommit;
        this.beforeSchemaChange = beforeSchemaChange;
        this.beforeFence = beforeFence;
    }

    /**
     * Whether {@code snapshot} was committed by this class, which guarantees that every live data
     * file in it has a first row id.
     */
    public static boolean rowIdsAssigned(Snapshot snapshot) {
        Map<String, String> properties = snapshot.properties();
        return properties != null
                && Long.toString(snapshot.id())
                        .equals(properties.get(ROW_IDS_ASSIGNED_SNAPSHOT_ID));
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

        Totals totals = new Totals();
        if (!enabled) {
            assignRowIdsAndEnable(table, planned, totals);
            table = loadTable();
            checkState(
                    table.coreOptions().rowTrackingEnabled()
                            && table.coreOptions().dataEvolutionEnabled(),
                    "Schema change did not enable data evolution on table %s.",
                    identifier.getFullName());
        }

        // A writer that checked the previous schema may still be on its way to commit. After the
        // fence it can no longer succeed, so the files that need a row id are final.
        beforeFence.run();
        commitFence(table);
        Assignment remaining = plan(table);
        if (!remaining.files.isEmpty()) {
            LOG.info(
                    "Assigning row ids to {} file(s) of table {} that were committed without one.",
                    remaining.files.size(),
                    identifier.getFullName());
            totals.add(assignRowIdsWithRetry(table, remaining, false));
        }
        checkState(
                plan(table).files.isEmpty(),
                "Table %s still has data files without a row id. A writer of an older Paimon "
                        + "version may still be writing to it; stop it and run the procedure "
                        + "again.",
                identifier.getFullName());

        Snapshot latest = table.snapshotManager().latestSnapshot();
        return new Result(
                schemaBefore,
                table.schema().id(),
                snapshotBefore,
                latest == null ? null : latest.id(),
                totals.files,
                totals.rows,
                latest == null ? null : latest.nextRowId(),
                false,
                false,
                null);
    }

    /**
     * Assigns row ids to the files of the latest snapshot and switches the schema. The schema
     * change is only accepted on a snapshot committed by the assignment, so a commit that lands in
     * between makes both steps run again.
     */
    private void assignRowIdsAndEnable(FileStoreTable table, Assignment planned, Totals totals)
            throws Exception {
        CoreOptions options = table.coreOptions();
        RetryWaiter retryWaiter =
                new RetryWaiter(options.commitMinRetryWait(), options.commitMaxRetryWait());
        long startMillis = System.currentTimeMillis();
        Assignment assignment = planned;
        int retryCount = 0;
        while (true) {
            if (assignment.snapshot != null) {
                // also when no file needs an id: the commit marks the snapshot as fully assigned
                totals.add(assignRowIdsWithRetry(table, assignment, true));
            }
            beforeSchemaChange.run();
            if (dataEvolutionEnabled(table)) {
                // a concurrent run switched the schema already
                return;
            }
            try {
                catalog.alterTable(identifier, SchemaChange.enableDataEvolution(), false);
                return;
            } catch (RuntimeException e) {
                Snapshot latest = table.snapshotManager().latestSnapshot();
                if (latest == null || rowIdsAssigned(latest)) {
                    throw e;
                }
                if (System.currentTimeMillis() - startMillis > options.commitTimeout()
                        || retryCount >= options.commitMaxRetries()) {
                    throw new RuntimeException(
                            String.format(
                                    "Failed to enable data evolution on table %s after %s millis "
                                            + "and %s retries because newer snapshots kept being "
                                            + "committed between the row id assignment and the "
                                            + "schema change.",
                                    identifier.getFullName(),
                                    System.currentTimeMillis() - startMillis,
                                    retryCount),
                            e);
                }
                LOG.info(
                        "Snapshot {} of table {} was committed after the row id assignment, "
                                + "assigning again ({}/{}).",
                        latest.id(),
                        identifier.getFullName(),
                        retryCount + 1,
                        options.commitMaxRetries());
                retryWaiter.retryWait(retryCount);
                retryCount++;
                assignment = plan(table);
            }
        }
    }

    private boolean dataEvolutionEnabled(FileStoreTable table) {
        CoreOptions latest =
                CoreOptions.fromMap(
                        table.schemaManager()
                                .latestOrThrow(
                                        "Cannot get latest schema for table "
                                                + identifier.getFullName())
                                .options());
        return latest.rowTrackingEnabled() && latest.dataEvolutionEnabled();
    }

    /**
     * Commits an empty snapshot through the normal commit path, which refuses writers on a schema
     * without row tracking.
     */
    private void commitFence(FileStoreTable table) throws Exception {
        String commitUser = COMMIT_USER_PREFIX + "-" + UUID.randomUUID();
        try (FileStoreCommit commit = table.store().newCommit(commitUser, table)) {
            commit.ignoreEmptyCommit(false);
            commit.commit(new ManifestCommittable(BatchWriteBuilder.COMMIT_IDENTIFIER), false);
        }
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
            return new Assignment(
                    latest, manifests, Collections.emptyList(), Collections.emptyMap(), 0L, start);
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

    private Assignment assignRowIdsWithRetry(
            FileStoreTable table, Assignment initial, boolean commitIfNothingToAssign)
            throws Exception {
        CoreOptions options = table.coreOptions();
        RetryWaiter retryWaiter =
                new RetryWaiter(options.commitMinRetryWait(), options.commitMaxRetryWait());
        long startMillis = System.currentTimeMillis();
        Assignment assignment = initial;
        int retryCount = 0;
        while (true) {
            if (commitAssignment(table, assignment)) {
                return assignment;
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
            if (assignment.files.isEmpty() && !commitIfNothingToAssign) {
                return assignment;
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
     * Rewrites the manifests holding the planned files and commits them, referencing the latest
     * schema and marking the new snapshot, see {@link #rowIdsAssigned}. Returns false when the
     * snapshot moved on in the meantime.
     */
    private boolean commitAssignment(FileStoreTable table, Assignment assignment) {
        ManifestFile manifestFile = table.store().manifestFileFactory().create();
        ManifestList manifestList = table.store().manifestListFactory().create();

        // A plain append writer numbers its rows, so a file's sequence numbers can exceed the
        // snapshot ids of later commits, while a row-tracking commit stamps its files with its
        // snapshot id and a data-evolution read takes the column of the file with the highest one.
        // Stamp the converted files like the commit that gives them their row ids.
        long sequenceNumber = assignment.snapshot.id() + 1;
        List<ManifestFileMeta> baseManifests = new ArrayList<>();
        for (ManifestFileMeta manifest : assignment.manifests) {
            List<ManifestEntry> entries =
                    manifestFile.read(manifest.fileName(), manifest.fileSize());
            List<ManifestEntry> rewritten = new ArrayList<>(entries.size());
            boolean changed = false;
            for (ManifestEntry entry : entries) {
                Long firstRowId = assignment.firstRowIds.get(entry.identifier());
                if (firstRowId != null && entry.file().firstRowId() == null) {
                    rewritten.add(
                            entry.assignFirstRowId(firstRowId)
                                    .assignSequenceNumber(sequenceNumber, sequenceNumber));
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
            // A schema change creates no snapshot, so it does not fail the snapshot CAS below:
            // read the latest schema for every attempt, as the normal commit path does.
            long schemaId =
                    table.schemaManager()
                            .latestOrThrow(
                                    "Cannot get latest schema for table "
                                            + identifier.getFullName())
                            .id();
            Map<String, String> properties = new HashMap<>();
            if (assignment.snapshot.properties() != null) {
                properties.putAll(assignment.snapshot.properties());
            }
            properties.put(
                    ROW_IDS_ASSIGNED_SNAPSHOT_ID, Long.toString(assignment.snapshot.id() + 1));
            return commit.replaceManifestList(
                    assignment.snapshot,
                    schemaId,
                    assignment.snapshot.totalRecordCount(),
                    baseManifestList,
                    deltaManifestList,
                    assignment.snapshot.indexManifest(),
                    assignment.nextRowId,
                    properties);
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

    private static class Totals {
        long files;
        long rows;

        void add(Assignment committed) {
            files += committed.files.size();
            rows += committed.rowCount;
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
                    planned.snapshot == null ? null : planned.nextRowId,
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
