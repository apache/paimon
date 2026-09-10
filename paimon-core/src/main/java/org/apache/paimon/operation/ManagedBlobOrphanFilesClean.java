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

package org.apache.paimon.operation;

import org.apache.paimon.Snapshot;
import org.apache.paimon.blob.ManagedBlobReachabilityCollector;
import org.apache.paimon.blob.ManagedBlobReachabilityCollector.Result;
import org.apache.paimon.blob.ManagedBlobReferenceFile;
import org.apache.paimon.blob.ManagedBlobReferenceFile.Reference;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.DataFilePathFactories;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Cleans unreferenced primary-key {@code .managed.blob} packs.
 *
 * <p>Unlike {@link OrphanFilesClean}, this cleaner only lists and deletes managed BLOB packs. Pack
 * reachability is collected from live {@link FileKind#ADD} data-file {@code .blobref} sidecars.
 * Missing manifest lists or unreadable sidecars on a still-existing data file abort pack deletion
 * for the rest of the run.
 *
 * <p>Used packs are collected twice. If the snapshot topology or the used-pack set changes between
 * those collections, this run deletes nothing. That shrinks the race with compaction reuse; it is
 * not a commit lease.
 */
public abstract class ManagedBlobOrphanFilesClean extends OrphanFilesClean {

    /**
     * Marker emitted into the used-pack set when a {@code .blobref} sidecar or a required manifest
     * cannot be trusted. Callers must skip deleting every {@code .managed.blob} pack.
     */
    public static final String SKIP_MANAGED_BLOB_GC = "__paimon_skip_managed_blob_gc__";

    public ManagedBlobOrphanFilesClean(FileStoreTable table, long olderThanMillis, boolean dryRun) {
        super(table, olderThanMillis, dryRun);
    }

    /**
     * Join key for a managed pack. File-system qualification is omitted so a reference written as
     * {@code hdfs:///warehouse/...} still matches a listing returned as {@code
     * hdfs://namenode:8020/warehouse/...}. A collision between distinct storage authorities can
     * only retain an orphan: a candidate sharing its URI path with any live pack is conservatively
     * treated as used. Relative paths are intentionally left unchanged because resolving them
     * requires the semantics of the table's {@link FileIO}.
     */
    public static String packIdentity(Path packPath) {
        return packPath.toUri().getPath();
    }

    public static String packIdentity(Reference reference) {
        return packIdentity(reference.toPath());
    }

    /**
     * Sorted {@code branch:snapshotId} pairs over every valid branch. Used to abort pack GC when
     * the snapshot set changes between the two used-pack collections.
     */
    protected List<String> snapshotTopology() throws IOException {
        List<String> topology = new ArrayList<>();
        for (String branch : validBranches()) {
            for (Snapshot snapshot : safelyGetAllSnapshots(branch)) {
                topology.add(branch + ":" + snapshot.id());
            }
        }
        Collections.sort(topology);
        return topology;
    }

    /**
     * Collects used pack identities from every valid branch. Subclasses may override to
     * parallelize.
     */
    protected Set<String> collectUsedPacks() throws IOException {
        Set<String> used = new HashSet<>();
        ReachabilityScan scan = newReachabilityScan();
        for (String branch : validBranches()) {
            for (Snapshot snapshot : safelyGetAllSnapshots(branch)) {
                emitUsedPacks(branch, snapshot, scan, used::add);
            }
        }
        return used;
    }

    /** Creates independent deduplication state for one complete reachability scan. */
    protected final ReachabilityScan newReachabilityScan() {
        return new ReachabilityScan();
    }

    /** Test hook between the two used-pack collections. Production cleaners leave this empty. */
    protected void betweenUsedCollections() {}

    /**
     * Aborts this run when sidecars are untrusted, the snapshot topology changed, or the two
     * used-pack collections disagree. Callers must not delete any pack when this returns true.
     */
    protected boolean shouldAbortPackGc(
            List<String> topologyBefore, Set<String> used, Set<String> used2) throws IOException {
        if (used.contains(SKIP_MANAGED_BLOB_GC) || used2.contains(SKIP_MANAGED_BLOB_GC)) {
            LOG.warn(
                    "Skip managed blob pack GC for table {} because some sidecars or manifests cannot be trusted.",
                    table.fullName());
            return true;
        }
        List<String> topologyAfter = snapshotTopology();
        if (!topologyBefore.equals(topologyAfter)) {
            LOG.warn(
                    "Skip managed blob pack GC for table {} because snapshot topology changed during used-pack collection.",
                    table.fullName());
            return true;
        }
        if (!used.equals(used2)) {
            LOG.warn(
                    "Skip managed blob pack GC for table {} because the used pack set changed during used-pack collection.",
                    table.fullName());
            return true;
        }
        return false;
    }

    /**
     * Emits referenced pack identities from {@code entry}. Pack reachability is collected only from
     * {@link FileKind#ADD} files: {@link FileKind#DELETE} entries remain in delta manifests after
     * compaction, while snapshot expire may already have removed their {@code .blobref} sidecars.
     */
    protected void emitUsedPacks(
            ManifestEntry entry, DataFilePathFactory pathFactory, Consumer<String> used) {
        emitUsedPacks(entry, pathFactory, newReachabilityScan(), used);
    }

    /** Emits referenced packs while reading each sidecar at most once in this scan. */
    protected void emitUsedPacks(
            ManifestEntry entry,
            DataFilePathFactory pathFactory,
            ReachabilityScan scan,
            Consumer<String> used) {
        for (SidecarWorkItem workItem : createSidecarWorkItems(entry, pathFactory)) {
            if (scan.markSidecar(workItem.dedupIdentity())) {
                emitUsedPacks(workItem, scan, used);
            }
        }
    }

    /**
     * Creates one serializable work item per managed BLOB reference sidecar of an {@link
     * FileKind#ADD} entry. No sidecar is read by this method.
     */
    protected final List<SidecarWorkItem> createSidecarWorkItems(
            ManifestEntry entry, DataFilePathFactory pathFactory) {
        if (entry.kind() != FileKind.ADD) {
            return Collections.emptyList();
        }
        List<String> extraFiles = entry.file().extraFiles();
        if (extraFiles == null || extraFiles.isEmpty()) {
            return Collections.emptyList();
        }
        Path dataFile = pathFactory.toPath(entry);
        List<SidecarWorkItem> workItems = new ArrayList<>();
        for (String extraFile : extraFiles) {
            if (extraFile != null
                    && extraFile.endsWith(ManagedBlobReferenceFile.REFERENCE_FILE_SUFFIX)) {
                Path sidecar = new Path(dataFile.getParent(), extraFile);
                workItems.add(
                        new SidecarWorkItem(
                                dataFile, sidecar, extraFile, sidecarDedupIdentity(sidecar)));
            }
        }
        return workItems;
    }

    /**
     * Reads one globally deduplicated sidecar work item and emits canonical pack identities.
     * Callers are responsible for deduplicating {@link SidecarWorkItem#dedupIdentity()} within one
     * reachability pass.
     */
    protected final void emitUsedPacks(
            SidecarWorkItem workItem, ReachabilityScan scan, Consumer<String> used) {
        Result reachability =
                new ManagedBlobReachabilityCollector(fileIO)
                        .fromSidecar(workItem.dataFile(), workItem.sidecar());
        if (reachability.isUnsafe()) {
            used.accept(SKIP_MANAGED_BLOB_GC);
            return;
        }
        for (Reference reference : reachability.referenced()) {
            Optional<String> identity = packIdentity(reference, scan);
            used.accept(identity.orElse(SKIP_MANAGED_BLOB_GC));
        }
    }

    private String sidecarDedupIdentity(Path sidecar) {
        String path = sidecar.toUri().getPath();
        if (fileIO instanceof LocalFileIO && path != null && !new File(path).isAbsolute()) {
            return new File(path).toPath().toAbsolutePath().normalize().toUri().toString();
        }
        return sidecar.toUri().normalize().toString();
    }

    /** Serializable unit of work for one managed BLOB reference sidecar. */
    public static final class SidecarWorkItem implements Serializable {

        private static final long serialVersionUID = 1L;

        private final Path dataFile;
        private final Path sidecar;
        private final String extraFile;
        private final String dedupIdentity;

        private SidecarWorkItem(
                Path dataFile, Path sidecar, String extraFile, String dedupIdentity) {
            this.dataFile = dataFile;
            this.sidecar = sidecar;
            this.extraFile = extraFile;
            this.dedupIdentity = dedupIdentity;
        }

        public Path dataFile() {
            return dataFile;
        }

        public Path sidecar() {
            return sidecar;
        }

        public String extraFile() {
            return extraFile;
        }

        public String dedupIdentity() {
            return dedupIdentity;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SidecarWorkItem that = (SidecarWorkItem) o;
            return Objects.equals(dedupIdentity, that.dedupIdentity);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dedupIdentity);
        }

        @Override
        public String toString() {
            return dedupIdentity;
        }
    }

    /** Returns a canonical identity or empty when a relative path cannot be resolved safely. */
    protected final Optional<String> packIdentityForCleanup(Path packPath) {
        if (isAbsolute(packPath)) {
            return Optional.of(packIdentity(packPath));
        }
        if (fileIO instanceof LocalFileIO) {
            String path = packPath.toUri().getPath();
            return Optional.of(
                    new File(path).toPath().toAbsolutePath().normalize().toUri().getPath());
        }
        try {
            Path canonical = fileIO.getFileStatus(packPath).getPath();
            return isAbsolute(canonical) ? Optional.of(packIdentity(canonical)) : Optional.empty();
        } catch (IOException e) {
            LOG.warn("Cannot safely resolve relative managed blob path {}.", packPath, e);
            return Optional.empty();
        }
    }

    private Optional<String> packIdentity(Reference reference, ReachabilityScan scan) {
        Path packPath = reference.toPath();
        if (isAbsolute(packPath) || fileIO instanceof LocalFileIO) {
            return packIdentityForCleanup(packPath);
        }
        Optional<Path> canonicalRoot = scan.canonicalStorageRoot(reference.storageRootId(), fileIO);
        if (!canonicalRoot.isPresent()) {
            LOG.warn(
                    "Cannot safely resolve relative managed blob storage root {}. Skip pack GC this run.",
                    reference.storageRootId());
            return Optional.empty();
        }
        return Optional.of(packIdentity(new Path(canonicalRoot.get(), reference.relativePath())));
    }

    private static boolean isAbsolute(Path path) {
        String uriPath = path.toUri().getPath();
        return uriPath != null && new File(uriPath).isAbsolute();
    }

    /**
     * Reads {@code manifestName} and emits used packs. A missing manifest is treated as unsafe:
     * {@link java.io.FileNotFoundException} would otherwise look like an empty used set.
     */
    protected void emitUsedPacks(
            String manifestName,
            ManifestFile manifestFile,
            DataFilePathFactories pathFactories,
            Consumer<String> used)
            throws IOException {
        emitUsedPacks("", manifestName, manifestFile, pathFactories, newReachabilityScan(), used);
    }

    private void emitUsedPacks(
            String branch,
            String manifestName,
            ManifestFile manifestFile,
            DataFilePathFactories pathFactories,
            ReachabilityScan scan,
            Consumer<String> used)
            throws IOException {
        if (!scan.markManifest(branch, manifestName)) {
            return;
        }
        List<ManifestEntry> entries =
                retryReadingFiles(() -> manifestFile.readWithIOException(manifestName), null);
        if (entries == null) {
            LOG.warn(
                    "Manifest {} is missing while collecting used managed blob packs. Skip pack GC this run.",
                    manifestName);
            used.accept(SKIP_MANAGED_BLOB_GC);
            return;
        }
        for (ManifestEntry entry : entries) {
            emitUsedPacks(entry, pathFactories.get(entry.partition(), entry.bucket()), scan, used);
        }
    }

    /**
     * Reads data manifests of {@code snapshot} and emits used packs. A missing manifest list is
     * treated as unsafe for the same reason as a missing manifest.
     */
    protected void emitUsedPacks(String branch, Snapshot snapshot, Consumer<String> used)
            throws IOException {
        emitUsedPacks(branch, snapshot, newReachabilityScan(), used);
    }

    /** Reads a snapshot while deduplicating manifests and sidecars across the whole scan. */
    protected void emitUsedPacks(
            String branch, Snapshot snapshot, ReachabilityScan scan, Consumer<String> used)
            throws IOException {
        FileStoreTable branchTable = table.switchToBranch(branch);
        ManifestList manifestList = branchTable.store().manifestListFactory().create();
        ManifestFile manifestFile = branchTable.store().manifestFileFactory().create();
        DataFilePathFactories pathFactories =
                new DataFilePathFactories(branchTable.store().pathFactory());
        List<ManifestFileMeta> metas = new ArrayList<>();
        if (!addManifestList(manifestList, snapshot.changelogManifestList(), metas, used)
                || !addManifestList(manifestList, snapshot.deltaManifestList(), metas, used)
                || !addManifestList(manifestList, snapshot.baseManifestList(), metas, used)) {
            return;
        }
        for (ManifestFileMeta meta : metas) {
            emitUsedPacks(branch, meta.fileName(), manifestFile, pathFactories, scan, used);
        }
    }

    /** Thread-safe read-deduplication state scoped to one reachability scan. */
    protected static final class ReachabilityScan {

        private final Set<String> manifests = ConcurrentHashMap.newKeySet();
        private final Set<String> sidecars = ConcurrentHashMap.newKeySet();
        private final ConcurrentHashMap<String, Optional<Path>> canonicalStorageRoots =
                new ConcurrentHashMap<>();

        private boolean markManifest(String branch, String manifestName) {
            return manifests.add(branch + '\0' + manifestName);
        }

        private boolean markSidecar(String sidecarIdentity) {
            return sidecars.add(sidecarIdentity);
        }

        private Optional<Path> canonicalStorageRoot(String storageRootId, FileIO fileIO) {
            return canonicalStorageRoots.computeIfAbsent(
                    storageRootId,
                    root -> {
                        try {
                            Path canonical = fileIO.getFileStatus(new Path(root)).getPath();
                            return isAbsolute(canonical)
                                    ? Optional.of(canonical)
                                    : Optional.empty();
                        } catch (IOException e) {
                            return Optional.empty();
                        }
                    });
        }
    }

    private boolean addManifestList(
            ManifestList manifestList,
            String listFileName,
            List<ManifestFileMeta> metas,
            Consumer<String> used)
            throws IOException {
        if (listFileName == null) {
            return true;
        }
        List<ManifestFileMeta> listed =
                retryReadingFiles(() -> manifestList.readWithIOException(listFileName), null);
        if (listed == null) {
            LOG.warn(
                    "Manifest list {} is missing while collecting used managed blob packs. Skip pack GC this run.",
                    listFileName);
            used.accept(SKIP_MANAGED_BLOB_GC);
            return false;
        }
        metas.addAll(listed);
        return true;
    }

    /** Deletes a managed BLOB pack and returns whether this invocation deleted it. */
    protected boolean cleanManagedBlobFile(Path path) {
        return cleanManagedBlobFile(path, false);
    }

    /**
     * Idempotently completes deletion of a managed BLOB pack.
     *
     * <p>Distributed attempts use this method so a pack deleted by a failed earlier attempt is
     * still included in the successful attempt's logical cleanup result.
     */
    protected boolean cleanManagedBlobFileIdempotently(Path path) {
        return cleanManagedBlobFile(path, true);
    }

    private boolean cleanManagedBlobFile(Path path, boolean missingIsSuccess) {
        if (dryRun) {
            return true;
        }
        try {
            if (fileIO.isDir(path)) {
                LOG.error(
                        "Refusing to delete directory {} in managed blob orphan cleanup. "
                                + "This indicates a bug in candidate collection.",
                        path);
                return false;
            }
        } catch (FileNotFoundException e) {
            return missingIsSuccess;
        } catch (IOException e) {
            LOG.warn("Failed to check whether managed blob pack {} is a directory.", path, e);
            return false;
        }
        try {
            if (fileIO.delete(path, false)) {
                return true;
            }
            if (missingIsSuccess && !fileIO.exists(path)) {
                return true;
            }
            LOG.warn("Failed to delete managed blob pack {}.", path);
            return false;
        } catch (FileNotFoundException e) {
            return missingIsSuccess;
        } catch (IOException e) {
            LOG.warn("Failed to delete managed blob pack {}.", path, e);
            return false;
        }
    }

    public static boolean isManagedBlobPackName(String fileName) {
        return fileName.endsWith(ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX);
    }
}
