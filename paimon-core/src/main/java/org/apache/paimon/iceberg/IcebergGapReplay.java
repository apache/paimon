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

package org.apache.paimon.iceberg;

import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.Path;
import org.apache.paimon.iceberg.IcebergCommitCallback.BuildResult;
import org.apache.paimon.iceberg.IcebergCommitCallback.FileChangesCollector;
import org.apache.paimon.iceberg.IcebergCommitCallback.MetadataNotWritten;
import org.apache.paimon.iceberg.IcebergCommitCallback.Publication;
import org.apache.paimon.iceberg.IcebergCommitCallback.RebuildRequest;
import org.apache.paimon.iceberg.IcebergCommitCallback.ReplayPoint;
import org.apache.paimon.iceberg.IcebergCommitCallback.SchemaCache;
import org.apache.paimon.iceberg.manifest.IcebergManifestFile;
import org.apache.paimon.iceberg.manifest.IcebergManifestFileMeta;
import org.apache.paimon.iceberg.manifest.IcebergManifestList;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.iceberg.metadata.IcebergPartitionField;
import org.apache.paimon.iceberg.metadata.IcebergPartitionSpec;
import org.apache.paimon.iceberg.metadata.IcebergSchema;
import org.apache.paimon.iceberg.metadata.IcebergSnapshot;
import org.apache.paimon.iceberg.metadata.IcebergSnapshotSummary;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.paimon.iceberg.IcebergCommitCallback.SNAPSHOT_SUMMARY_PAIMON_COMMIT_IDENTITY;
import static org.apache.paimon.iceberg.IcebergCommitCallback.SNAPSHOT_SUMMARY_REPLAY_ORIGINALS;
import static org.apache.paimon.iceberg.IcebergCommitCallback.SNAPSHOT_SUMMARY_TOTAL_RECORDS;
import static org.apache.paimon.iceberg.IcebergCommitCallback.commitIdentity;
import static org.apache.paimon.iceberg.IcebergCommitCallback.generationOf;
import static org.apache.paimon.iceberg.IcebergCommitCallback.isMissingFile;
import static org.apache.paimon.iceberg.IcebergCommitCallback.lastPartitionIdOf;
import static org.apache.paimon.iceberg.IcebergCommitCallback.metadataVersionOf;
import static org.apache.paimon.iceberg.IcebergCommitCallback.replayedOriginals;

/**
 * The replay of the Iceberg snapshots that a missing base left out, for one snapshot of Paimon: it
 * extends the newest older metadata that is valid, instead of rebuilding from every data file. See
 * {@link IcebergCommitCallback}.
 */
final class IcebergGapReplay {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergGapReplay.class);

    private static final int MAX_CANDIDATES = 5;

    private final IcebergCommitCallback callback;
    private final FileStoreTable table;
    private final IcebergPathFactory pathFactory;
    private final IcebergManifestList manifestList;
    private final IcebergManifestFile manifestFile;

    private final Snapshot snapshot;
    private final long snapshotId;
    private final FileChangesCollector collector;
    private final RebuildRequest defaultRebuild;

    private final int maxSnapshots;
    private final long maxDeltaEntries;
    private final long maxBaseManifestBytes;
    private final long maxDurationNanos;
    private final long startNanos = System.nanoTime();

    private final List<Long> candidates = new ArrayList<>();
    private int nextCandidate = 0;
    private long chargedBytes = 0;

    // what a rebuild after a failed replay has to inherit
    @Nullable private String baseUuid;
    private int maxLastColumnId;
    private long maxNextRowId;

    // true once the metadata file may exist: from then on no file of the replay is deleted
    private boolean exposing = false;

    IcebergGapReplay(
            IcebergCommitCallback callback,
            Snapshot snapshot,
            FileChangesCollector collector,
            RebuildRequest rebuild) {
        this.callback = callback;
        this.table = callback.table;
        this.pathFactory = callback.pathFactory;
        this.manifestList = callback.manifestList;
        this.manifestFile = callback.manifestFile;
        this.snapshot = snapshot;
        this.snapshotId = snapshot.id();
        this.collector = collector;
        this.defaultRebuild = rebuild;
        this.maxLastColumnId = rebuild.lastColumnIdFloor;
        this.maxNextRowId = rebuild.nextRowIdFloor;

        Options options = table.coreOptions().toConfiguration();
        this.maxSnapshots = options.get(IcebergOptions.REPLAY_MAX_SNAPSHOTS);
        this.maxDeltaEntries = options.get(IcebergOptions.REPLAY_MAX_DELTA_ENTRIES);
        this.maxBaseManifestBytes =
                options.get(IcebergOptions.REPLAY_MAX_BASE_MANIFEST_BYTES).getBytes();
        this.maxDurationNanos = options.get(IcebergOptions.REPLAY_MAX_DURATION).toNanos();
    }

    /**
     * Replays the snapshot, and returns what a rebuild has to inherit when no replay was possible,
     * or it failed before it exposed anything, and the snapshot is still the head. Returns null
     * when nothing is left to do.
     */
    @Nullable
    RebuildRequest run() throws IOException {
        listCandidates();
        ReplayBase base = nextBase();
        if (base == null) {
            return rebuildIfHead("there is no usable older Iceberg metadata");
        }
        pathFactory.startRecording();
        try {
            return replay(base);
        } finally {
            pathFactory.stopRecording();
        }
    }

    @Nullable
    private RebuildRequest replay(ReplayBase first) throws IOException {
        try {
            ReplayBase base = first;
            List<Snapshot> steps;
            Acquired acquired = null;
            for (int attempt = 0; ; attempt++) {
                try {
                    steps = readSteps(base);
                    acquired = acquire(base, steps);
                    break;
                } catch (BrokenHistory e) {
                    LOG.info(
                            "The history of Iceberg metadata v{} of table {} is broken: {}",
                            base.version,
                            table.fullName(),
                            e.getMessage());
                    deleteCreatedFiles();
                    callback.replayPoint(ReplayPoint.CANDIDATE_REJECTED, base.version);
                    base = attempt == 0 ? nextBase() : null;
                    if (base == null) {
                        throw e;
                    }
                }
            }

            IcebergMetadata result = runSteps(acquired, steps);
            finish(result, acquired);
            LOG.info(
                    "Replayed the Iceberg metadata of snapshots {} to {} of table {} on top "
                            + "of v{}, and copied {} manifests.",
                    base.version + 1,
                    snapshotId,
                    table.fullName(),
                    base.version,
                    acquired.copied.size());
            return null;
        } catch (ReplaySourceReplaced e) {
            LOG.info(
                    "Snapshot {} of table {} was replaced while its Iceberg metadata was "
                            + "replayed, so nothing is written: {}",
                    snapshotId,
                    table.fullName(),
                    e.getMessage());
            deleteCreatedFiles();
            callback.replayPoint(ReplayPoint.ABANDONED, snapshotId);
            return null;
        } catch (ReplayAbort e) {
            deleteCreatedFiles();
            return rebuildIfHead(e.getMessage());
        } catch (IOException | RuntimeException | Error e) {
            if (!exposing) {
                deleteCreatedFiles();
            }
            throw e;
        }
    }

    // ---------------------------------------------------------------------------------
    // Base selection
    // ---------------------------------------------------------------------------------

    private void listCandidates() throws IOException {
        // the newest first, each version once, whatever else the directory holds
        Set<Long> versions = new TreeSet<>(Comparator.reverseOrder());
        Iterator<Path> it =
                pathFactory.getAllMetadataPathBefore(table.fileIO(), snapshotId).iterator();
        while (it.hasNext()) {
            long version = metadataVersionOf(it.next());
            if (version >= 0) {
                versions.add(version);
            }
        }
        for (long version : versions) {
            if (candidates.size() == MAX_CANDIDATES) {
                break;
            }
            candidates.add(version);
        }
    }

    @Nullable
    private ReplayBase nextBase() throws IOException {
        while (nextCandidate < candidates.size()) {
            long version = candidates.get(nextCandidate++);
            String problem = null;
            ReplayBase base = null;
            try {
                base = checkBase(version);
            } catch (ReplayAbort e) {
                problem = e.getMessage();
            }
            if (base != null) {
                baseUuid = base.metadata.tableUuid();
                maxLastColumnId = Math.max(maxLastColumnId, base.metadata.lastColumnId());
                if (base.metadata.nextRowId() != null) {
                    maxNextRowId = Math.max(maxNextRowId, base.metadata.nextRowId());
                }
                callback.replayPoint(ReplayPoint.BASE_SELECTED, version);
                return base;
            }
            LOG.info(
                    "Iceberg metadata v{} of table {} is not a base for snapshot {}: {}",
                    version,
                    table.fullName(),
                    snapshotId,
                    problem);
            callback.replayPoint(ReplayPoint.CANDIDATE_REJECTED, version);
        }
        return null;
    }

    private ReplayBase checkBase(long version) throws IOException {
        if (snapshotId - version > maxSnapshots) {
            throw new ReplayAbort(
                    "it is "
                            + (snapshotId - version)
                            + " snapshots behind, more than "
                            + IcebergOptions.REPLAY_MAX_SNAPSHOTS.key());
        }
        Snapshot paimonSnapshot = readSnapshot(version);
        if (paimonSnapshot == null) {
            throw new ReplayAbort("there is no Paimon snapshot " + version);
        }
        IcebergMetadata metadata = callback.readUsableMetadata(pathFactory.toMetadataPath(version));
        if (metadata == null) {
            throw new ReplayAbort("it is missing or malformed");
        }
        if (metadata.formatVersion() != callback.formatVersion) {
            throw new ReplayAbort("its format version is " + metadata.formatVersion());
        }
        IcebergSnapshot current;
        try {
            current = metadata.currentSnapshot();
        } catch (RuntimeException e) {
            throw new ReplayAbort("its current snapshot is missing", e);
        }
        if (current.snapshotId() != version) {
            throw new ReplayAbort("its current snapshot is " + current.snapshotId());
        }
        String identity =
                current.summary() == null
                        ? null
                        : current.summary().get(SNAPSHOT_SUMMARY_PAIMON_COMMIT_IDENTITY);
        if (identity == null || !identity.equals(commitIdentity(paimonSnapshot))) {
            throw new ReplayAbort("it was not built from Paimon snapshot " + version);
        }
        checkFields(metadata);
        return new ReplayBase(version, metadata);
    }

    /**
     * What the replay carries over from the base as it is, and what its snapshots point at, is what
     * a rebuild would write for the table, or the base is not used. A field that is missing would
     * fail every replay for good, and the same base is picked every time.
     */
    private void checkFields(IcebergMetadata metadata) {
        if (metadata.tableUuid() == null
                || metadata.schemas() == null
                || metadata.partitionSpecs() == null
                || metadata.snapshots() == null) {
            throw new ReplayAbort("it lacks the table uuid, the schemas or the partition specs");
        }
        if (!Objects.equals(metadata.location(), table.location().toString())) {
            throw new ReplayAbort("it is for the location " + metadata.location());
        }
        SchemaCache schemaCache = callback.new SchemaCache();
        List<IcebergPartitionField> fields =
                callback.partitionFieldsOf(schemaCache.get(schemaCache.getLatestSchemaId()));
        if (!metadata.partitionSpecs()
                        .equals(Collections.singletonList(new IcebergPartitionSpec(fields)))
                || metadata.lastPartitionId() != lastPartitionIdOf(fields)) {
            throw new ReplayAbort("its partition spec is not that of the table");
        }
        Set<Integer> schemaIds = new HashSet<>();
        for (IcebergSchema schema : metadata.schemas()) {
            schemaIds.add(schema.schemaId());
        }
        if (!schemaIds.contains(metadata.currentSchemaId())) {
            throw new ReplayAbort("it lacks its current schema");
        }
        for (IcebergSnapshot s : metadata.snapshots()) {
            if (s.summary() == null
                    || s.manifestList() == null
                    || !schemaIds.contains(s.schemaId())) {
                throw new ReplayAbort(
                        "its snapshot "
                                + s.snapshotId()
                                + " lacks a summary, a list or its schema");
            }
        }
    }

    // ---------------------------------------------------------------------------------
    // Pre-flight and acquisition of the history
    // ---------------------------------------------------------------------------------

    /** The Paimon snapshots after the base up to N, and the check that they are replayable. */
    private List<Snapshot> readSteps(ReplayBase base) {
        checkDuration();
        ManifestList deltaLists = table.store().manifestListFactory().create();
        List<Snapshot> steps = new ArrayList<>();
        long entries = 0;
        for (long id = base.version + 1; id <= snapshotId; id++) {
            Snapshot step = id == snapshotId ? snapshot : readSnapshot(id);
            if (step == null) {
                throw new ReplayAbort("Paimon snapshot " + id + " does not exist");
            }
            try {
                for (ManifestFileMeta meta : deltaLists.readDeltaManifests(step)) {
                    entries += meta.numAddedFiles() + meta.numDeletedFiles();
                }
            } catch (RuntimeException e) {
                if (isMissingFile(e)) {
                    throw new ReplayAbort(
                            "the delta of Paimon snapshot " + id + " cannot be read", e);
                }
                throw e;
            }
            if (entries > maxDeltaEntries) {
                throw new ReplayAbort(
                        "the snapshots after v"
                                + base.version
                                + " change more than "
                                + maxDeltaEntries
                                + " files");
            }
            steps.add(step);
        }
        return steps;
    }

    /** Index of the first snapshot of the base that retention keeps for snapshot N. */
    private int firstRetained(IcebergMetadata base, List<Snapshot> steps) {
        List<IcebergSnapshot> baseSnapshots = base.snapshots();
        int total = baseSnapshots.size() + steps.size();
        for (int i = 0; i + 1 < total; i++) {
            boolean expire;
            if (i < baseSnapshots.size()) {
                expire = callback.shouldExpire(baseSnapshots.get(i), snapshotId);
            } else {
                Snapshot step = steps.get(i - baseSnapshots.size());
                expire = callback.shouldExpire(step.id(), step.timeMillis(), snapshotId);
            }
            if (!expire) {
                return Math.min(i, baseSnapshots.size());
            }
        }
        // like the build helper, nothing is dropped when every older snapshot expires
        return 0;
    }

    /**
     * Copies the manifests and lists the output keeps, and those of the working base, so the replay
     * owns everything it reads and publishes.
     */
    private Acquired acquire(ReplayBase base, List<Snapshot> steps) throws IOException {
        List<IcebergSnapshot> baseSnapshots = base.metadata.snapshots();
        int first = firstRetained(base.metadata, steps);
        List<IcebergSnapshot> retained =
                new ArrayList<>(baseSnapshots.subList(first, baseSnapshots.size()));
        IcebergSnapshot working = base.metadata.currentSnapshot();
        List<IcebergSnapshot> toCopy = new ArrayList<>(retained);
        boolean workingRetained = first < baseSnapshots.size();
        if (!workingRetained) {
            // the working base is always acquired, even when the output drops its own entry
            toCopy.add(working);
        }

        // a manifest is known by its name, the directory is that of the table
        Map<String, Long> unique = new LinkedHashMap<>();
        for (IcebergSnapshot s : toCopy) {
            checkDuration();
            for (IcebergManifestFileMeta meta : readBaseList(s)) {
                unique.putIfAbsent(manifestNameOf(meta), meta.manifestLength());
            }
        }
        long bytes = 0;
        for (long length : unique.values()) {
            bytes += length;
        }
        // every manifest is read twice, to copy it and to validate the copy
        charge(2 * bytes);
        callback.replayPoint(ReplayPoint.LISTS_READ, base.version);

        Map<String, String> copied = new HashMap<>();
        Map<String, IcebergManifestFile.ManifestFacts> facts = new HashMap<>();
        Map<Long, String> originalLists = new HashMap<>();
        List<IcebergSnapshot> retainedCopies = new ArrayList<>();
        IcebergSnapshot workingCopy = null;
        for (IcebergSnapshot s : toCopy) {
            checkDuration();
            List<IcebergManifestFileMeta> owned = new ArrayList<>();
            long liveRows = 0;
            for (IcebergManifestFileMeta meta : readBaseList(s)) {
                if (meta.content() == IcebergManifestFileMeta.Content.DATA) {
                    liveRows += meta.liveRowsCount();
                }
                String name = manifestNameOf(meta);
                String target = copied.get(name);
                if (target == null) {
                    checkDuration();
                    CopiedManifest copy = copyManifest(meta, s);
                    target = copy.target;
                    copied.put(name, target);
                    facts.put(name, copy.facts);
                } else {
                    // a manifest that several lists name is decoded once, and every list
                    // that names it is held to what it holds
                    checkAgreement(facts.get(name), meta, s);
                }
                owned.add(meta.withManifestPath(target));
            }
            checkKept(s, liveRows);
            String listName = manifestList.writeWithoutRolling(owned);
            IcebergSnapshot copy =
                    recordingOriginal(
                            s.withManifestList(pathFactory.toManifestListPath(listName).toString()),
                            s);
            originalLists.put(s.snapshotId(), new Path(s.manifestList()).getName());
            if (s.snapshotId() == working.snapshotId()) {
                workingCopy = copy;
            }
            if (retained.contains(s)) {
                retainedCopies.add(copy);
            }
        }
        callback.replayPoint(ReplayPoint.COPIED, copied.size());

        List<IcebergSnapshot> ownedSnapshots = new ArrayList<>(retainedCopies);
        if (!workingRetained) {
            ownedSnapshots.add(workingCopy);
        }
        return new Acquired(
                base, base.metadata.withSnapshots(ownedSnapshots), originalLists, copied);
    }

    /** The copy names the list it replaces, which only the cleanup of its snapshot deletes. */
    private IcebergSnapshot recordingOriginal(IcebergSnapshot copy, IcebergSnapshot original) {
        List<String> originals = new ArrayList<>(replayedOriginals(original));
        originals.add(new Path(original.manifestList()).getName());
        Map<String, String> summary = original.summary().getSummary();
        summary.put(SNAPSHOT_SUMMARY_REPLAY_ORIGINALS, JsonSerdeUtil.toFlatJson(originals));
        return copy.withSummary(new IcebergSnapshotSummary(summary));
    }

    private String manifestNameOf(IcebergManifestFileMeta meta) {
        return new Path(meta.manifestPath()).getName();
    }

    private List<IcebergManifestFileMeta> readBaseList(IcebergSnapshot icebergSnapshot)
            throws IOException {
        String name = new Path(icebergSnapshot.manifestList()).getName();
        try {
            return manifestList.decode(
                    name, table.fileIO().getFileSize(pathFactory.toManifestListPath(name)));
        } catch (IcebergManifestFile.CorruptManifestException e) {
            throw new BrokenHistory(
                    "the manifest list of snapshot "
                            + icebergSnapshot.snapshotId()
                            + " is corrupt: "
                            + e.getMessage(),
                    e);
        } catch (IOException e) {
            if (isMissingFile(e)) {
                throw new BrokenHistory(
                        "the manifest list of snapshot "
                                + icebergSnapshot.snapshotId()
                                + " cannot be read",
                        e);
            }
            throw e;
        }
    }

    /**
     * What a snapshot that the output keeps has to be, whatever its lists say about their
     * manifests: its live rows are what it records, and what Paimon has for it while Paimon still
     * has the snapshot. A list that lost a manifest shows here, and a reader that travels to the
     * snapshot would miss its rows.
     */
    private void checkKept(IcebergSnapshot kept, long liveRows) {
        Long recorded = callback.getSummaryLong(kept, SNAPSHOT_SUMMARY_TOTAL_RECORDS);
        if (recorded != null && recorded != liveRows) {
            throw new BrokenHistory(
                    "snapshot "
                            + kept.snapshotId()
                            + " records "
                            + recorded
                            + " rows and its list holds "
                            + liveRows);
        }
        Snapshot paimon = readSnapshot(kept.snapshotId());
        if (paimon == null) {
            return;
        }
        String identity = kept.summary().get(SNAPSHOT_SUMMARY_PAIMON_COMMIT_IDENTITY);
        if (identity != null && !identity.equals(commitIdentity(paimon))) {
            throw new BrokenHistory(
                    "snapshot " + kept.snapshotId() + " is not the snapshot of Paimon");
        }
        if (paimon.totalRecordCount() != liveRows) {
            throw new BrokenHistory(
                    "snapshot "
                            + kept.snapshotId()
                            + " has "
                            + paimon.totalRecordCount()
                            + " rows in Paimon and its list holds "
                            + liveRows);
        }
    }

    /** Copies a manifest, and returns the copy with what it holds. */
    private CopiedManifest copyManifest(IcebergManifestFileMeta meta, IcebergSnapshot listedBy)
            throws IOException {
        // the manifests of the table are in its metadata directory, whatever a list says
        Path source = pathFactory.toManifestFilePath(manifestNameOf(meta));
        Path target = pathFactory.newManifestFile();
        long length;
        try {
            length = table.fileIO().getFileSize(source);
            if (length != meta.manifestLength()) {
                throw new BrokenHistory(
                        "manifest "
                                + source
                                + " has "
                                + length
                                + " bytes, its list records "
                                + meta.manifestLength());
            }
            table.fileIO().copyFile(source, target, false);
        } catch (IOException e) {
            if (isMissingFile(e)) {
                throw new BrokenHistory("manifest " + source + " cannot be copied", e);
            }
            throw e;
        }
        long copyLength = table.fileIO().getFileSize(target);
        if (copyLength != length) {
            // the source has the length its list records, so the copy was cut short
            throw new IOException(
                    "The copy of manifest "
                            + source
                            + " has "
                            + copyLength
                            + " bytes, the manifest has "
                            + length);
        }
        // the length says nothing about the contents, and a step that only adds files
        // trusts the counters of the list, so the copy is decoded before it is relied on
        IcebergManifestFile.ManifestFacts decoded;
        try {
            decoded =
                    manifestFile.decode(
                            meta.withManifestPath(target.toString()), length, this::checkDuration);
        } catch (IcebergManifestFile.CorruptManifestException e) {
            throw new BrokenHistory(
                    "the copy of manifest " + source + " is corrupt: " + e.getMessage(), e);
        }
        checkAgreement(decoded, meta, listedBy);
        return new CopiedManifest(target.toString(), decoded);
    }

    /** A list that records a manifest differently from what it holds is a broken history. */
    private void checkAgreement(
            IcebergManifestFile.ManifestFacts decoded,
            IcebergManifestFileMeta entry,
            IcebergSnapshot listedBy) {
        String disagreement = decoded.disagreement(entry, listedBy.snapshotId());
        if (disagreement != null) {
            throw new BrokenHistory(
                    "the list of snapshot "
                            + listedBy.snapshotId()
                            + " records manifest "
                            + entry.manifestPath()
                            + " wrongly: "
                            + disagreement);
        }
    }

    // ---------------------------------------------------------------------------------
    // Steps
    // ---------------------------------------------------------------------------------

    private IcebergMetadata runSteps(Acquired acquired, List<Snapshot> steps) throws IOException {
        IcebergMetadata current = acquired.working;
        for (Snapshot step : steps) {
            long id = step.id();
            checkDuration();
            table.snapshotManager().invalidateCache();
            verifySource(step);

            FileChangesCollector stepCollector =
                    id == snapshotId
                            ? collector
                            : (removed, added) -> callback.collectFileChanges(id, removed, added);
            // deletion vectors make a table ineligible, so a step has no index files
            BuildResult result =
                    callback.buildMetadataWithBase(
                            chargingCollector(guardingDelta(stepCollector, id), current),
                            Collections.emptyList(),
                            step,
                            current,
                            defaultRebuild.lastColumnIdFloor,
                            defaultRebuild.nextRowIdFloor,
                            false);
            switch (result.kind) {
                case STALE_SOURCE:
                    throw new ReplaySourceReplaced(
                            "the base of snapshot " + id + " is on another timeline");
                case NEEDS_REBUILD:
                    maxLastColumnId = Math.max(maxLastColumnId, result.lastColumnIdFloor);
                    maxNextRowId = Math.max(maxNextRowId, result.nextRowIdFloor);
                    throw new ReplayAbort(
                            "snapshot " + id + " cannot extend its base: " + result.reason);
                case COMPLETED:
                    current = result.metadata;
                    break;
                default:
                    throw new IllegalStateException("Unknown build result " + result.kind);
            }
            assertRowCount(step, current);
            maxLastColumnId = Math.max(maxLastColumnId, current.lastColumnId());
            if (current.nextRowId() != null) {
                maxNextRowId = Math.max(maxNextRowId, current.nextRowId());
            }
            callback.replayPoint(ReplayPoint.STEP_DONE, id);
        }
        return current;
    }

    /**
     * Reports a delta that is missing as a failed replay. The preflight reads only the delta lists,
     * so a missing list or manifest can still show up here. Other failures propagate.
     */
    private FileChangesCollector guardingDelta(FileChangesCollector inner, long id) {
        return (removedFiles, addedFiles) -> {
            try {
                return inner.collect(removedFiles, addedFiles);
            } catch (IOException | RuntimeException e) {
                if (isMissingFile(e)) {
                    throw new ReplayAbort(
                            "the delta of Paimon snapshot " + id + " cannot be read", e);
                }
                throw e;
            }
        };
    }

    /** The snapshot on disk is still the one this step replays. */
    private void verifySource(Snapshot step) {
        Snapshot onDisk = readSnapshot(step.id());
        if (onDisk == null) {
            if (step.id() == snapshotId) {
                throw new ReplaySourceReplaced("snapshot " + snapshotId + " is gone");
            }
            throw new ReplayAbort("Paimon snapshot " + step.id() + " is gone");
        }
        if (!commitIdentity(onDisk).equals(commitIdentity(step))) {
            throw new ReplaySourceReplaced(
                    "snapshot " + step.id() + " is not the snapshot that was committed");
        }
    }

    /** Charges the manifests a step reads from its base, when it reads more than its delta. */
    private FileChangesCollector chargingCollector(
            FileChangesCollector inner, IcebergMetadata base) {
        return (removedFiles, addedFiles) -> {
            boolean addOnly = inner.collect(removedFiles, addedFiles);
            List<IcebergManifestFileMeta> baseMetas =
                    manifestList.read(new Path(base.currentSnapshot().manifestList()).getName());
            List<IcebergManifestFileMeta> baseData =
                    baseMetas.stream()
                            .filter(meta -> meta.content() == IcebergManifestFileMeta.Content.DATA)
                            .collect(Collectors.toList());
            if (!addOnly || !callback.baseTotalsUsable(base.currentSnapshot(), baseData)) {
                charge(baseMetas.stream().mapToLong(IcebergManifestFileMeta::manifestLength).sum());
            }
            return addOnly;
        };
    }

    private void assertRowCount(Snapshot step, IcebergMetadata metadata) {
        Long actual =
                callback.getSummaryLong(metadata.currentSnapshot(), SNAPSHOT_SUMMARY_TOTAL_RECORDS);
        if (actual == null || actual != step.totalRecordCount()) {
            throw new ReplayAbort(
                    "snapshot "
                            + step.id()
                            + " has "
                            + step.totalRecordCount()
                            + " records in Paimon and "
                            + actual
                            + " in the replayed Iceberg metadata");
        }
    }

    // ---------------------------------------------------------------------------------
    // Exposure
    // ---------------------------------------------------------------------------------

    private void finish(IcebergMetadata result, Acquired acquired) throws IOException {
        keepOnlyReachable(result);

        // The last check of the source, the version file, the hint and the pointer of the
        // external catalog are done under the lock that the commits and the rollbacks of the
        // table take, like every other publication. The copying and the checks of the
        // history stay outside.
        Publication publication =
                callback.publishUnderLock(snapshot, lock -> publish(lock, result));
        if (publication == null) {
            throw new ReplaySourceReplaced(
                    "snapshot " + snapshotId + " is not the snapshot that was committed");
        }
        if (!publication.ours) {
            // another writer's metadata won; it owns its files, and ours are not reachable
            exposing = false;
            deleteCreatedFiles();
        }
        if (publication.head) {
            // chosen here, and deleted under the lock while the publication stands. What
            // depends on the files the metadata lists is chosen from the replay's own
            // metadata, and only when that is what is durable
            List<Path> files = callback.applicableMetadataFiles(snapshotId);
            if (publication.ours) {
                files.addAll(droppedOriginalFiles(result, acquired));
            }
            callback.deleteUnderLock(snapshot, publication.generation, files);
        }
    }

    private Publication publish(Lock lock, IcebergMetadata result) throws IOException {
        // from the write on the file may exist, unless the write is known to have left
        // nothing: a failed write of unknown outcome may still have landed
        exposing = true;
        IcebergMetadata durable;
        try {
            durable = callback.writeMetadataFile(snapshot, result);
        } catch (MetadataNotWritten e) {
            exposing = false;
            throw e;
        }
        // when another callback's metadata won, it owns its files, and the replay's are not
        // reachable, whatever fails after that
        exposing = generationOf(durable).equals(generationOf(result));
        callback.replayPoint(ReplayPoint.EXPOSED, snapshotId);
        return callback.pointTo(lock, snapshotId, result, durable, null, null);
    }

    /** Deletes the files the replay created that the final metadata does not reach. */
    private void keepOnlyReachable(IcebergMetadata result) {
        List<Path> created = pathFactory.recordedPaths();
        Set<String> createdNames = new HashSet<>();
        for (Path path : created) {
            createdNames.add(path.getName());
        }
        Set<String> reachable = new HashSet<>();
        for (IcebergSnapshot s : result.snapshots()) {
            String listName = new Path(s.manifestList()).getName();
            if (!createdNames.contains(listName)) {
                throw new ReplayAbort(
                        "snapshot "
                                + s.snapshotId()
                                + " references the list "
                                + listName
                                + ", which the replay did not create");
            }
            reachable.add(listName);
            for (IcebergManifestFileMeta meta : manifestList.read(listName)) {
                String name = new Path(meta.manifestPath()).getName();
                if (!createdNames.contains(name)) {
                    throw new ReplayAbort(
                            "snapshot "
                                    + s.snapshotId()
                                    + " references the manifest "
                                    + name
                                    + ", which the replay did not create");
                }
                reachable.add(name);
            }
        }
        for (Path path : created) {
            if (!reachable.contains(path.getName())) {
                table.fileIO().deleteQuietly(path);
            }
        }
    }

    /**
     * The files that the usual retention cleanup deletes for the base snapshots that the output
     * dropped: their lists, the lists that earlier replays left for them, and the manifests that no
     * kept original list still lists. The copies are never touched. Nothing is deleted here.
     */
    private List<Path> droppedOriginalFiles(IcebergMetadata result, Acquired acquired) {
        List<Path> files = new ArrayList<>();
        try {
            Set<Long> keptIds = new HashSet<>();
            for (IcebergSnapshot s : result.snapshots()) {
                keptIds.add(s.snapshotId());
            }
            List<IcebergSnapshot> dropped = new ArrayList<>();
            for (IcebergSnapshot s : acquired.base.metadata.snapshots()) {
                if (!keptIds.contains(s.snapshotId())) {
                    dropped.add(s);
                }
            }
            if (dropped.isEmpty()) {
                return files;
            }
            IcebergSnapshot firstKept = result.snapshots().get(0);
            // A kept snapshot that is a copy still has its original list, with the manifests
            // it lists. A kept snapshot that a step built lists only files of the replay, so
            // no original stays in use when every base snapshot is dropped.
            Set<String> inUse = new HashSet<>();
            String keptOriginal = acquired.originalLists.get(firstKept.snapshotId());
            if (keptOriginal != null) {
                inUse.addAll(callback.manifestPathsIn(keptOriginal));
            }
            // what the kept snapshot still keeps of the older replays
            callback.addManifestPathsOfOriginals(firstKept, inUse);
            for (int i = 0; i < dropped.size(); i++) {
                Set<String> next =
                        i + 1 < dropped.size()
                                ? callback.manifestPathsReachableFrom(dropped.get(i + 1))
                                : inUse;
                callback.collectExpiredList(
                        files, new Path(dropped.get(i).manifestList()).getName(), next);
                callback.collectExpiredOriginals(files, dropped.get(i), next);
            }
            return files;
        } catch (RuntimeException e) {
            LOG.warn(
                    "Failed to clean the metadata files that retention dropped for table {}. "
                            + "They stay on storage.",
                    table.fullName(),
                    e);
            return Collections.emptyList();
        }
    }

    // ---------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------

    /**
     * What a rebuild has to inherit when the replay cannot be done, or null when the snapshot is
     * not the head any more, or was replaced. A callback of such a snapshot cannot move the
     * pointer, and a rebuild reads every data file of the table, so it is not done for it.
     */
    @Nullable
    private RebuildRequest rebuildIfHead(String reason) {
        Long latest = table.snapshotManager().latestSnapshotId();
        if (latest == null || latest != snapshotId) {
            LOG.info(
                    "The Iceberg metadata of snapshot {} of table {} cannot be replayed and "
                            + "the head is {}, so it is not rebuilt: {}",
                    snapshotId,
                    table.fullName(),
                    latest,
                    reason);
            callback.replayPoint(ReplayPoint.ABANDONED, snapshotId);
            return null;
        }
        try {
            // the failure may have come before any step checked the source
            verifySource(snapshot);
        } catch (ReplaySourceReplaced replaced) {
            LOG.info(
                    "The Iceberg metadata of snapshot {} of table {} cannot be replayed and "
                            + "the snapshot was replaced, so nothing is written: {}",
                    snapshotId,
                    table.fullName(),
                    replaced.getMessage());
            callback.replayPoint(ReplayPoint.ABANDONED, snapshotId);
            return null;
        }
        LOG.info(
                "The Iceberg metadata of snapshot {} of table {} cannot be replayed, so it is "
                        + "rebuilt: {}",
                snapshotId,
                table.fullName(),
                reason);
        return rebuildRequest();
    }

    private RebuildRequest rebuildRequest() {
        return new RebuildRequest(
                defaultRebuild.inheritUuid != null ? defaultRebuild.inheritUuid : baseUuid,
                maxLastColumnId,
                maxNextRowId);
    }

    private void deleteCreatedFiles() {
        for (Path path : pathFactory.recordedPaths()) {
            table.fileIO().deleteQuietly(path);
        }
    }

    @Nullable
    private Snapshot readSnapshot(long id) {
        try {
            return SnapshotManager.tryFromPath(
                    table.fileIO(), table.snapshotManager().snapshotPath(id));
        } catch (FileNotFoundException e) {
            return null;
        }
    }

    private void checkDuration() {
        if (System.nanoTime() - startNanos > maxDurationNanos) {
            throw new ReplayAbort("it ran longer than " + IcebergOptions.REPLAY_MAX_DURATION.key());
        }
    }

    private void charge(long bytes) {
        chargedBytes += bytes;
        if (chargedBytes > maxBaseManifestBytes) {
            throw new ReplayAbort(
                    "it reads more than "
                            + IcebergOptions.REPLAY_MAX_BASE_MANIFEST_BYTES.key()
                            + " of manifests");
        }
    }

    /** An expected failure of a replay: the replay is dropped, the caller decides what is next. */
    private static class ReplayAbort extends RuntimeException {

        private ReplayAbort(String message) {
            super(message);
        }

        private ReplayAbort(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** A snapshot of the retained history is missing or damaged, so the base cannot be used. */
    private static final class BrokenHistory extends ReplayAbort {

        private BrokenHistory(String message, Throwable cause) {
            super(message, cause);
        }

        private BrokenHistory(String message) {
            super(message);
        }
    }

    /** The Paimon snapshot was replaced by a rollback or a recreation while it was replayed. */
    private static final class ReplaySourceReplaced extends RuntimeException {

        private ReplaySourceReplaced(String message) {
            super(message);
        }
    }

    /** A valid older metadata version. */
    private static final class ReplayBase {

        private final long version;
        private final IcebergMetadata metadata;

        private ReplayBase(long version, IcebergMetadata metadata) {
            this.version = version;
            this.metadata = metadata;
        }
    }

    /** A manifest that the replay copied, and what the copy holds. */
    private static final class CopiedManifest {

        private final String target;
        private final IcebergManifestFile.ManifestFacts facts;

        private CopiedManifest(String target, IcebergManifestFile.ManifestFacts facts) {
            this.target = target;
            this.facts = facts;
        }
    }

    /** The base metadata whose snapshots point at lists and manifests the replay owns. */
    private static final class Acquired {

        private final ReplayBase base;
        private final IcebergMetadata working;
        /** Snapshot id to the name of its original list, for the snapshots that were copied. */
        private final Map<Long, String> originalLists;
        /** Name of an original manifest to the path of its copy. */
        private final Map<String, String> copied;

        private Acquired(
                ReplayBase base,
                IcebergMetadata working,
                Map<Long, String> originalLists,
                Map<String, String> copied) {
            this.base = base;
            this.working = working;
            this.originalLists = originalLists;
            this.copied = copied;
        }
    }
}
