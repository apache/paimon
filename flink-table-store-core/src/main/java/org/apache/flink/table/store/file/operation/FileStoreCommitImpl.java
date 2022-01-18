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
import org.apache.flink.table.store.file.manifest.ManifestCommittableSerializer;
import org.apache.flink.table.store.file.manifest.ManifestEntry;
import org.apache.flink.table.store.file.manifest.ManifestFile;
import org.apache.flink.table.store.file.manifest.ManifestFileMeta;
import org.apache.flink.table.store.file.manifest.ManifestList;
import org.apache.flink.table.store.file.mergetree.sst.SstFileMeta;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Default implementation of {@link FileStoreCommit}.
 *
 * <p>This class provides an atomic commit method to the user.
 *
 * <ol>
 *   <li>Before calling {@link FileStoreCommitImpl#commit}, user should first call {@link
 *       FileStoreCommitImpl#filterCommitted} to make sure this commit is not done before.
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

    private final String committer;
    private final ManifestCommittableSerializer committableSerializer;

    private final FileStorePathFactory pathFactory;
    private final ManifestFile manifestFile;
    private final ManifestList manifestList;
    private final FileStoreOptions fileStoreOptions;
    private final FileStoreScan scan;

    @Nullable private Lock lock;

    public FileStoreCommitImpl(
            String committer,
            ManifestCommittableSerializer committableSerializer,
            FileStorePathFactory pathFactory,
            ManifestFile manifestFile,
            ManifestList manifestList,
            FileStoreOptions fileStoreOptions,
            FileStoreScan scan) {
        this.committer = committer;
        this.committableSerializer = committableSerializer;

        this.pathFactory = pathFactory;
        this.manifestFile = manifestFile;
        this.manifestList = manifestList;
        this.fileStoreOptions = fileStoreOptions;
        this.scan = scan;

        this.lock = null;
    }

    @Override
    public FileStoreCommit withLock(Lock lock) {
        this.lock = lock;
        return this;
    }

    @Override
    public List<ManifestCommittable> filterCommitted(List<ManifestCommittable> committableList) {
        committableList = new ArrayList<>(committableList);

        // filter out commits with no new files
        committableList.removeIf(committable -> committable.newFiles().isEmpty());

        // if there is no previous snapshots then nothing should be filtered
        Long latestSnapshotId = pathFactory.latestSnapshotId();
        if (latestSnapshotId == null) {
            return committableList;
        }

        // check if a committable is already committed by its hash
        Map<String, ManifestCommittable> hashes = new LinkedHashMap<>();
        for (ManifestCommittable committable : committableList) {
            hashes.put(digestManifestCommittable(committable), committable);
        }

        for (long id = latestSnapshotId; id >= Snapshot.FIRST_SNAPSHOT_ID; id--) {
            Path snapshotPath = pathFactory.toSnapshotPath(id);
            Snapshot snapshot = Snapshot.fromPath(snapshotPath);
            if (committer.equals(snapshot.committer())) {
                if (hashes.containsKey(snapshot.hash())) {
                    hashes.remove(snapshot.hash());
                } else {
                    // early exit, because committableList must be the latest commits by this
                    // committer
                    break;
                }
            }
        }

        return new ArrayList<>(hashes.values());
    }

    @Override
    public void commit(ManifestCommittable committable, Map<String, String> properties) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Ready to commit\n" + committable.toString());
        }

        String hash = digestManifestCommittable(committable);

        List<ManifestEntry> appendChanges = collectChanges(committable.newFiles(), ValueKind.ADD);
        if (!appendChanges.isEmpty()) {
            tryCommit(appendChanges, hash, Snapshot.Type.APPEND);
        }

        List<ManifestEntry> compactChanges = new ArrayList<>();
        compactChanges.addAll(collectChanges(committable.compactBefore(), ValueKind.DELETE));
        compactChanges.addAll(collectChanges(committable.compactAfter(), ValueKind.ADD));
        if (!compactChanges.isEmpty()) {
            tryCommit(compactChanges, hash, Snapshot.Type.COMPACT);
        }
    }

    @Override
    public void overwrite(
            Map<String, String> partition,
            ManifestCommittable committable,
            Map<String, String> properties) {
        throw new UnsupportedOperationException();
    }

    private String digestManifestCommittable(ManifestCommittable committable) {
        try {
            return new String(
                    Base64.getEncoder()
                            .encode(
                                    MessageDigest.getInstance("MD5")
                                            .digest(committableSerializer.serialize(committable))));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 algorithm not found. This is impossible.", e);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to serialize ManifestCommittable. This is unexpected.", e);
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

    private void tryCommit(List<ManifestEntry> changes, String hash, Snapshot.Type type) {
        while (true) {
            Long latestSnapshotId = pathFactory.latestSnapshotId();
            long newSnapshotId =
                    latestSnapshotId == null ? Snapshot.FIRST_SNAPSHOT_ID : latestSnapshotId + 1;
            Path newSnapshotPath = pathFactory.toSnapshotPath(newSnapshotId);
            Path tmpSnapshotPath =
                    new Path(
                            newSnapshotPath.getParent()
                                    + "/."
                                    + newSnapshotPath.getName()
                                    + UUID.randomUUID());

            Snapshot latestSnapshot = null;
            if (latestSnapshotId != null) {
                detectConflicts(latestSnapshotId, changes);
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
                    // merge manifest files
                    newMetas.addAll(
                            ManifestFileMeta.merge(
                                    oldMetas,
                                    manifestFile,
                                    fileStoreOptions.manifestSuggestedSize.getBytes()));
                }
                // write all changes to manifest file
                newMetas.add(manifestFile.write(changes));
                // prepare snapshot file
                manifestListName = manifestList.write(newMetas);
                newSnapshot = new Snapshot(newSnapshotId, manifestListName, committer, hash, type);
                FileUtils.writeFileUtf8(tmpSnapshotPath, newSnapshot.toJson());
            } catch (Throwable e) {
                // fails when preparing for commit, we should clean up
                cleanUpManifests(tmpSnapshotPath, manifestListName, oldMetas, newMetas);
                throw new RuntimeException(
                        String.format(
                                "Exception occurs when preparing snapshot #%d (path %s) by committer %s "
                                        + "with hash %s and type %s. Clean up.",
                                newSnapshotId,
                                newSnapshotPath.toString(),
                                committer,
                                hash,
                                type.name()),
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
                                "Exception occurs when committing snapshot #%d (path %s) by committer %s "
                                        + "with hash %s and type %s. "
                                        + "Cannot clean up because we can't determine the success.",
                                newSnapshotId,
                                newSnapshotPath.toString(),
                                committer,
                                hash,
                                type.name()),
                        e);
            }

            if (success) {
                return;
            }

            // atomic rename fails, clean up and try again
            LOG.warn(
                    String.format(
                            "Atomic rename failed for snapshot #%d (path %s) by committer %s "
                                    + "with hash %s and type %s. "
                                    + "Clean up and try again.",
                            newSnapshotId,
                            newSnapshotPath.toString(),
                            committer,
                            hash,
                            type.name()));
            cleanUpManifests(tmpSnapshotPath, manifestListName, oldMetas, newMetas);
        }
    }

    private void detectConflicts(long snapshotId, List<ManifestEntry> changes) {
        Set<ManifestEntry.Identifier> removedFiles =
                changes.stream()
                        .filter(e -> e.kind().equals(ValueKind.DELETE))
                        .map(ManifestEntry::identifier)
                        .collect(Collectors.toSet());
        if (removedFiles.isEmpty()) {
            // early exit for append only changes
            return;
        }

        try {
            for (ManifestEntry entry : scan.withSnapshot(snapshotId).plan().files()) {
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
                                                            + ", file "
                                                            + i.fileName)
                                    .collect(Collectors.joining("\n")));
        }
    }

    private void cleanUpManifests(
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
