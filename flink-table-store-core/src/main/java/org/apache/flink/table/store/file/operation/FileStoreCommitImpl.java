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

package org.apache.flink.table.store.file.operation;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.manifest.ManifestCommittable;
import org.apache.flink.table.store.file.manifest.ManifestEntry;
import org.apache.flink.table.store.file.manifest.ManifestFile;
import org.apache.flink.table.store.file.manifest.ManifestFileMeta;
import org.apache.flink.table.store.file.manifest.ManifestList;
import org.apache.flink.table.store.file.mergetree.sst.SstFileMeta;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.file.utils.TypeUtils;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Default implementation of {@link FileStoreCommit}.
 *
 * <p>This class provides an atomic commit method to the user.
 *
 * <ol>
 *   <li>Before calling {@link FileStoreCommitImpl#commit}, if user cannot determine if this commit
 *       is done before, user should first call {@link FileStoreCommitImpl#filterCommitted}.
 *   <li>Before committing, it will first check for conflicts by checking if all files to be removed
 *       currently exists.
 *   <li>After that it use the external {@link FileStoreCommitImpl#lock} (if provided) or the atomic
 *       rename of the file system to ensure atomicity.
 *   <li>If commit fails due to conflicts or exception it tries its best to clean up and aborts.
 *   <li>If atomic rename fails it tries again after reading the latest snapshot from step 2.
 * </ol>
 */
public class FileStoreCommitImpl implements FileStoreCommit {

    private static final Logger LOG = LoggerFactory.getLogger(FileStoreCommitImpl.class);

    private final String commitUser;
    private final RowType partitionType;
    private final FileStorePathFactory pathFactory;
    private final ManifestFile manifestFile;
    private final ManifestList manifestList;
    private final FileStoreScan scan;
    private final FileStoreOptions fileStoreOptions;

    @Nullable private Lock lock;

    public FileStoreCommitImpl(
            String commitUser,
            RowType partitionType,
            FileStorePathFactory pathFactory,
            ManifestFile.Factory manifestFileFactory,
            ManifestList.Factory manifestListFactory,
            FileStoreScan scan,
            FileStoreOptions fileStoreOptions) {
        this.commitUser = commitUser;
        this.partitionType = partitionType;
        this.pathFactory = pathFactory;
        this.manifestFile = manifestFileFactory.create();
        this.manifestList = manifestListFactory.create();
        this.scan = scan;
        this.fileStoreOptions = fileStoreOptions;

        this.lock = null;
    }

    @Override
    public FileStoreCommit withLock(Lock lock) {
        this.lock = lock;
        return this;
    }

    @Override
    public List<ManifestCommittable> filterCommitted(List<ManifestCommittable> committableList) {
        // if there is no previous snapshots then nothing should be filtered
        Long latestSnapshotId = pathFactory.latestSnapshotId();
        if (latestSnapshotId == null) {
            return committableList;
        }

        // check if a committable is already committed by its uuid
        Map<String, ManifestCommittable> uuids = new LinkedHashMap<>();
        for (ManifestCommittable committable : committableList) {
            uuids.put(committable.uuid(), committable);
        }

        for (long id = latestSnapshotId; id >= Snapshot.FIRST_SNAPSHOT_ID; id--) {
            Path snapshotPath = pathFactory.toSnapshotPath(id);
            Snapshot snapshot = Snapshot.fromPath(snapshotPath);
            if (commitUser.equals(snapshot.commitUser())) {
                if (uuids.containsKey(snapshot.commitUuid())) {
                    uuids.remove(snapshot.commitUuid());
                } else {
                    // early exit, because committableList must be the latest commits by this
                    // commit user
                    break;
                }
            }
        }

        return new ArrayList<>(uuids.values());
    }

    @Override
    public void commit(ManifestCommittable committable, Map<String, String> properties) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Ready to commit\n" + committable.toString());
        }

        List<ManifestEntry> appendChanges = collectChanges(committable.newFiles(), ValueKind.ADD);
        tryCommit(appendChanges, committable.uuid(), Snapshot.CommitKind.APPEND);

        List<ManifestEntry> compactChanges = new ArrayList<>();
        compactChanges.addAll(collectChanges(committable.compactBefore(), ValueKind.DELETE));
        compactChanges.addAll(collectChanges(committable.compactAfter(), ValueKind.ADD));
        tryCommit(compactChanges, committable.uuid(), Snapshot.CommitKind.COMPACT);
    }

    @Override
    public void overwrite(
            Map<String, String> partition,
            ManifestCommittable committable,
            Map<String, String> properties) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Ready to overwrite partition "
                            + partition.toString()
                            + "\n"
                            + committable.toString());
        }

        BinaryRowData partitionRowData =
                TypeUtils.partitionMapToBinaryRowData(partition, partitionType);

        List<ManifestEntry> appendChanges = collectChanges(committable.newFiles(), ValueKind.ADD);
        tryOverwrite(
                partitionRowData, appendChanges, committable.uuid(), Snapshot.CommitKind.APPEND);

        List<ManifestEntry> compactChanges = new ArrayList<>();
        compactChanges.addAll(collectChanges(committable.compactBefore(), ValueKind.DELETE));
        compactChanges.addAll(collectChanges(committable.compactAfter(), ValueKind.ADD));
        tryCommit(compactChanges, committable.uuid(), Snapshot.CommitKind.COMPACT);
    }

    private void tryCommit(
            List<ManifestEntry> changes, String hash, Snapshot.CommitKind commitKind) {
        while (true) {
            Long latestSnapshotId = pathFactory.latestSnapshotId();
            if (tryCommitOnce(changes, hash, commitKind, latestSnapshotId)) {
                break;
            }
        }
    }

    private void tryOverwrite(
            BinaryRowData partition,
            List<ManifestEntry> changes,
            String hash,
            Snapshot.CommitKind commitKind) {
        while (true) {
            Long latestSnapshotId = pathFactory.latestSnapshotId();

            List<ManifestEntry> changesWithOverwrite = new ArrayList<>();
            if (latestSnapshotId != null) {
                List<ManifestEntry> currentEntries =
                        scan.withSnapshot(latestSnapshotId)
                                .withPartitionFilter(Collections.singletonList(partition))
                                .plan()
                                .files();
                for (ManifestEntry entry : currentEntries) {
                    changesWithOverwrite.add(
                            new ManifestEntry(
                                    ValueKind.DELETE,
                                    entry.partition(),
                                    entry.bucket(),
                                    entry.totalBuckets(),
                                    entry.file()));
                }
            }
            changesWithOverwrite.addAll(changes);

            if (tryCommitOnce(changesWithOverwrite, hash, commitKind, latestSnapshotId)) {
                break;
            }
        }
    }

    private List<ManifestEntry> collectChanges(
            Map<BinaryRowData, Map<Integer, List<SstFileMeta>>> map, ValueKind kind) {
        List<ManifestEntry> changes = new ArrayList<>();
        for (Map.Entry<BinaryRowData, Map<Integer, List<SstFileMeta>>> entryWithPartition :
                map.entrySet()) {
            for (Map.Entry<Integer, List<SstFileMeta>> entryWithBucket :
                    entryWithPartition.getValue().entrySet()) {
                changes.addAll(
                        entryWithBucket.getValue().stream()
                                .map(
                                        file ->
                                                new ManifestEntry(
                                                        kind,
                                                        entryWithPartition.getKey(),
                                                        entryWithBucket.getKey(),
                                                        fileStoreOptions.bucket,
                                                        file))
                                .collect(Collectors.toList()));
            }
        }
        return changes;
    }

    private boolean tryCommitOnce(
            List<ManifestEntry> changes,
            String hash,
            Snapshot.CommitKind commitKind,
            Long latestSnapshotId) {
        long newSnapshotId =
                latestSnapshotId == null ? Snapshot.FIRST_SNAPSHOT_ID : latestSnapshotId + 1;
        Path newSnapshotPath = pathFactory.toSnapshotPath(newSnapshotId);
        Path tmpSnapshotPath = pathFactory.toTmpSnapshotPath(newSnapshotId);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Ready to commit changes to snapshot #" + newSnapshotId);
            for (ManifestEntry entry : changes) {
                LOG.debug("  * " + entry.toString());
            }
        }

        Snapshot latestSnapshot = null;
        if (latestSnapshotId != null) {
            noConflictsOrFail(latestSnapshotId, changes);
            latestSnapshot = Snapshot.fromPath(pathFactory.toSnapshotPath(latestSnapshotId));
        }

        Snapshot newSnapshot;
        String manifestListName = null;
        List<ManifestFileMeta> oldMetas = new ArrayList<>();
        List<ManifestFileMeta> newMetas = new ArrayList<>();
        try {
            if (latestSnapshot != null) {
                // read all previous manifest files
                oldMetas.addAll(manifestList.read(latestSnapshot.manifestList()));
            }
            // merge manifest files with changes
            newMetas.addAll(
                    ManifestFileMeta.merge(
                            oldMetas,
                            changes,
                            manifestFile,
                            fileStoreOptions.manifestSuggestedSize.getBytes()));
            // prepare snapshot file
            manifestListName = manifestList.write(newMetas);
            newSnapshot =
                    new Snapshot(
                            newSnapshotId,
                            manifestListName,
                            commitUser,
                            hash,
                            commitKind,
                            System.currentTimeMillis());
            FileUtils.writeFileUtf8(tmpSnapshotPath, newSnapshot.toJson());
        } catch (Throwable e) {
            // fails when preparing for commit, we should clean up
            cleanUpTmpSnapshot(tmpSnapshotPath, manifestListName, oldMetas, newMetas);
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when preparing snapshot #%d (path %s) by user %s "
                                    + "with hash %s and kind %s. Clean up.",
                            newSnapshotId,
                            newSnapshotPath.toString(),
                            commitUser,
                            hash,
                            commitKind.name()),
                    e);
        }

        boolean success;
        try {
            FileSystem fs = tmpSnapshotPath.getFileSystem();
            // atomic rename
            if (lock != null) {
                success =
                        lock.runWithLock(
                                () ->
                                        // fs.rename may not returns false if target file
                                        // already exists, or even not atomic
                                        // as we're relying on external locking, we can first
                                        // check if file exist then rename to work around this
                                        // case
                                        !fs.exists(newSnapshotPath)
                                                && fs.rename(tmpSnapshotPath, newSnapshotPath));
            } else {
                success = fs.rename(tmpSnapshotPath, newSnapshotPath);
            }
        } catch (Throwable e) {
            // exception when performing the atomic rename,
            // we cannot clean up because we can't determine the success
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when committing snapshot #%d (path %s) by user %s "
                                    + "with hash %s and kind %s. "
                                    + "Cannot clean up because we can't determine the success.",
                            newSnapshotId,
                            newSnapshotPath.toString(),
                            commitUser,
                            hash,
                            commitKind.name()),
                    e);
        }

        if (success) {
            return true;
        }

        // atomic rename fails, clean up and try again
        LOG.warn(
                String.format(
                        "Atomic rename failed for snapshot #%d (path %s) by user %s "
                                + "with hash %s and kind %s. "
                                + "Clean up and try again.",
                        newSnapshotId,
                        newSnapshotPath.toString(),
                        commitUser,
                        hash,
                        commitKind.name()));
        cleanUpTmpSnapshot(tmpSnapshotPath, manifestListName, oldMetas, newMetas);
        return false;
    }

    private void noConflictsOrFail(long snapshotId, List<ManifestEntry> changes) {
        Set<ManifestEntry.Identifier> removedFiles =
                changes.stream()
                        .filter(e -> e.kind().equals(ValueKind.DELETE))
                        .map(ManifestEntry::identifier)
                        .collect(Collectors.toSet());
        if (removedFiles.isEmpty()) {
            // early exit for append only changes
            return;
        }

        List<BinaryRowData> changedPartitions =
                changes.stream()
                        .map(ManifestEntry::partition)
                        .distinct()
                        .collect(Collectors.toList());
        try {
            for (ManifestEntry entry :
                    scan.withSnapshot(snapshotId)
                            .withPartitionFilter(changedPartitions)
                            .plan()
                            .files()) {
                removedFiles.remove(entry.identifier());
            }
        } catch (Throwable e) {
            throw new RuntimeException("Cannot determine if conflicts exist.", e);
        }

        if (!removedFiles.isEmpty()) {
            throw new RuntimeException(
                    "Conflicts detected on:\n"
                            + removedFiles.stream()
                                    .map(
                                            i ->
                                                    pathFactory.getPartitionString(i.partition)
                                                            + ", bucket "
                                                            + i.bucket
                                                            + ", level "
                                                            + i.level
                                                            + ", file "
                                                            + i.fileName)
                                    .collect(Collectors.joining("\n")));
        }
    }

    private void cleanUpTmpSnapshot(
            Path tmpSnapshotPath,
            String manifestListName,
            List<ManifestFileMeta> oldMetas,
            List<ManifestFileMeta> newMetas) {
        // clean up tmp snapshot file
        FileUtils.deleteOrWarn(tmpSnapshotPath);
        // clean up newly created manifest list
        if (manifestListName != null) {
            manifestList.delete(manifestListName);
        }
        // clean up newly merged manifest files
        Set<ManifestFileMeta> oldMetaSet = new HashSet<>(oldMetas); // for faster searching
        for (ManifestFileMeta suspect : newMetas) {
            if (!oldMetaSet.contains(suspect)) {
                manifestList.delete(suspect.fileName());
            }
        }
    }
}
