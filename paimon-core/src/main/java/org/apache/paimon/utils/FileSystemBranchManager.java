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

package org.apache.paimon.utils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.tag.Tag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.paimon.utils.FileUtils.listVersionedDirectories;
import static org.apache.paimon.utils.FileUtils.listVersionedFileStatus;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.SnapshotManager.SNAPSHOT_PREFIX;

/** A {@link BranchManager} implementation to manage branches via file system. */
public class FileSystemBranchManager implements BranchManager {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemBranchManager.class);

    private final FileIO fileIO;
    private final Path tablePath;
    private final SnapshotManager snapshotManager;
    private final TagManager tagManager;
    private final SchemaManager schemaManager;
    private final BranchMergeHandler mergeHandler;
    private final Supplier<Lock> lockSupplier;

    public FileSystemBranchManager(
            FileIO fileIO,
            Path path,
            SnapshotManager snapshotManager,
            TagManager tagManager,
            SchemaManager schemaManager,
            BranchMergeHandler mergeHandler) {
        this(fileIO, path, snapshotManager, tagManager, schemaManager, mergeHandler, Lock::empty);
    }

    /**
     * Create a branch manager whose fast-forward holds the lock from {@code lockSupplier}, which
     * must be the lock that commits to the current branch use. Fast-forward closes it after use.
     */
    public FileSystemBranchManager(
            FileIO fileIO,
            Path path,
            SnapshotManager snapshotManager,
            TagManager tagManager,
            SchemaManager schemaManager,
            BranchMergeHandler mergeHandler,
            Supplier<Lock> lockSupplier) {
        this.fileIO = fileIO;
        this.tablePath = path;
        this.snapshotManager = snapshotManager;
        this.tagManager = tagManager;
        this.schemaManager = schemaManager;
        this.mergeHandler = mergeHandler;
        this.lockSupplier = lockSupplier;
    }

    /** Return the path of a branch. */
    public Path branchPath(String branchName) {
        return new Path(BranchManager.branchPath(tablePath, branchName));
    }

    @Override
    public void createBranch(String branchName) {
        createBranch(branchName, false);
    }

    @Override
    public void createBranch(String branchName, boolean ignoreIfExists) {
        if (ignoreIfExists && branchExists(branchName)) {
            return;
        }
        validateBranch(branchName);
        try {
            TableSchema latestSchema = schemaManager.latest().get();
            copySchemasToBranch(branchName, latestSchema.id());
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when create branch '%s' (directory in %s).",
                            branchName, BranchManager.branchPath(tablePath, branchName)),
                    e);
        }
    }

    @Override
    public void createBranch(String branchName, String tagName) {
        createBranch(branchName, tagName, false);
    }

    @Override
    public void createBranch(String branchName, String tagName, boolean ignoreIfExists) {
        if (ignoreIfExists && branchExists(branchName)) {
            return;
        }
        validateBranch(branchName);
        Snapshot snapshot = tagManager.getOrThrow(tagName).trimToSnapshot();

        try {
            // Copy the corresponding tag, snapshot and schema files into the branch directory
            fileIO.copyFile(
                    tagManager.tagPath(tagName),
                    tagManager.copyWithBranch(branchName).tagPath(tagName),
                    true);
            Path branchSnapshotPath =
                    snapshotManager.copyWithBranch(branchName).snapshotPath(snapshot.id());
            if (snapshotManager.snapshotExists(snapshot.id())) {
                fileIO.copyFile(
                        snapshotManager.snapshotPath(snapshot.id()), branchSnapshotPath, true);
            } else {
                // The snapshot may have expired while the tag preserved its content, so write the
                // snapshot from the tag instead of copying a file that no longer exists.
                fileIO.writeFile(branchSnapshotPath, snapshot.toJson(), true);
            }
            copySchemasToBranch(branchName, snapshot.schemaId());
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when create branch '%s' (directory in %s).",
                            branchName, BranchManager.branchPath(tablePath, branchName)),
                    e);
        }
    }

    @Override
    public void dropBranch(String branchName) {
        checkArgument(
                !BranchManager.isMainBranch(branchName), "The main branch cannot be deleted.");
        checkArgument(branchExists(branchName), "Branch name '%s' doesn't exist.", branchName);
        try {
            // Delete branch directory
            fileIO.delete(branchPath(branchName), true);
        } catch (IOException e) {
            LOG.info(
                    String.format(
                            "Deleting the branch failed due to an exception in deleting the directory %s. Please try again.",
                            BranchManager.branchPath(tablePath, branchName)),
                    e);
        }
    }

    /** Check if path exists. */
    private boolean fileExists(Path path) {
        try {
            return fileIO.exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Failed to determine if path '%s' exists.", path), e);
        }
    }

    @Override
    public void fastForward(String branchName) {
        BranchManager.fastForwardValidate(branchName, snapshotManager.branch());
        checkArgument(branchExists(branchName), "Branch name '%s' doesn't exist.", branchName);

        // Hold the lock that commits to the current branch use, so a fast-forward and a commit
        // never interleave their snapshot writes.
        Lock lock = lockSupplier.get();
        try {
            lock.runWithLock(
                    () -> {
                        fastForwardWithLock(branchName, lock);
                        return null;
                    });
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Failed to fast forward branch '%s'.", branchName), e);
        } finally {
            // Like a commit, do not report a finished fast-forward as failed because the lock
            // could not be closed.
            IOUtils.closeQuietly(lock);
        }
    }

    private void fastForwardWithLock(String branchName, Lock lock) {
        Long earliestSnapshotId = snapshotManager.copyWithBranch(branchName).earliestSnapshotId();
        if (earliestSnapshotId == null) {
            throw new RuntimeException(
                    "Cannot fast forward branch "
                            + branchName
                            + ", because it does not have snapshot.");
        }

        try {
            if (!tryAppendFromBranch(branchName, earliestSnapshotId, lock)) {
                replaceFromBranch(branchName, earliestSnapshotId, lock);
            }
            snapshotManager.invalidateCache();
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when fast forward '%s' (directory in %s).",
                            branchName, BranchManager.branchPath(tablePath, branchName)),
                    e);
        }
    }

    /**
     * Fast-forwards by appending when the current branch is a prefix of the branch: its snapshots
     * from the branch's earliest one (or its own earliest, if later) to its latest are the
     * branch's, and so are its schemas. Nothing is deleted and files are written in ascending id
     * order, so a failure leaves the current branch at one of the branch's snapshots and a retry
     * continues from there.
     *
     * @return false if the current branch is not a prefix of the branch
     */
    private boolean tryAppendFromBranch(String branchName, long branchEarliestId, Lock lock)
            throws IOException {
        SnapshotManager branchSnapshotManager = snapshotManager.copyWithBranch(branchName);
        SchemaManager branchSchemaManager = schemaManager.copyWithBranch(branchName);
        // List rather than trust the LATEST hint: an interrupted replace by an older version copies
        // the branch's hint before the snapshots it names.
        Map<Long, Long> sizes = snapshotSizes(snapshotManager);
        Map<Long, Long> branchSizes = snapshotSizes(branchSnapshotManager);
        if (sizes.isEmpty() || branchSizes.isEmpty()) {
            return false;
        }
        long latestId = Collections.max(sizes.keySet());
        long branchLatestId = Collections.max(branchSizes.keySet());
        if (latestId < branchEarliestId
                || latestId > branchLatestId
                || !samePrefix(
                        sizes, branchSizes, branchSnapshotManager, branchEarliestId, latestId)
                || !sameSchemas(branchSchemaManager)) {
            return false;
        }

        long latestSchemaId = Collections.max(schemaManager.listAllIds());
        for (long schemaId : sorted(branchSchemaManager.listAllIds())) {
            if (schemaId > latestSchemaId) {
                copyAtomically(
                        lock,
                        branchSchemaManager.toSchemaPath(schemaId),
                        schemaManager.toSchemaPath(schemaId));
            }
        }
        copySnapshots(branchSnapshotManager, latestId + 1, branchLatestId, lock);
        copyBranchTags(branchName, lock);
        lock.ensureValid();
        snapshotManager.commitLatestHint(branchLatestId);
        return true;
    }

    private Map<Long, Long> snapshotSizes(SnapshotManager manager) throws IOException {
        Map<Long, Long> sizes = new HashMap<>();
        for (FileStatus status :
                listVersionedFileStatus(fileIO, manager.snapshotDirectory(), SNAPSHOT_PREFIX)
                        .collect(Collectors.toList())) {
            String version = status.getPath().getName().substring(SNAPSHOT_PREFIX.length());
            try {
                sizes.put(Long.parseLong(version), status.getLen());
            } catch (NumberFormatException ignored) {
                // not a snapshot file, for example a temporary file of a Python writer
            }
        }
        return sizes;
    }

    /**
     * Whether the current branch has every snapshot from {@code from} (or its earliest, if later)
     * to {@code to}, each with the branch's content. Sizes are compared first because they come
     * with the listing; equal sizes do not mean equal snapshots.
     */
    private boolean samePrefix(
            Map<Long, Long> sizes,
            Map<Long, Long> branchSizes,
            SnapshotManager branchSnapshotManager,
            long from,
            long to)
            throws IOException {
        long start = Math.max(from, Collections.min(sizes.keySet()));
        for (long id = start; id <= to; id++) {
            Long size = sizes.get(id);
            if (size == null || !size.equals(branchSizes.get(id))) {
                return false;
            }
        }
        for (long id = start; id <= to; id++) {
            if (!sameContent(
                    snapshotManager.snapshotPath(id),
                    branchSnapshotManager.snapshotPath(id),
                    Snapshot::fromJson)) {
                return false;
            }
        }
        return true;
    }

    /** Whether the current branch's schemas are the branch's schemas up to the current latest. */
    private boolean sameSchemas(SchemaManager branchSchemaManager) throws IOException {
        Set<Long> ids = new HashSet<>(schemaManager.listAllIds());
        if (ids.isEmpty()) {
            return false;
        }
        long latestId = Collections.max(ids);
        Set<Long> branchIds =
                branchSchemaManager.listAllIds().stream()
                        .filter(id -> id <= latestId)
                        .collect(Collectors.toSet());
        if (!ids.equals(branchIds)) {
            return false;
        }
        for (long id : ids) {
            if (!sameContent(
                    schemaManager.toSchemaPath(id),
                    branchSchemaManager.toSchemaPath(id),
                    TableSchema::fromJson)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Replaces the current branch's snapshots and tags from the branch's earliest snapshot onwards,
     * and its schemas, with the branch's. Schemas are replaced file by file instead of deleted up
     * front, so a failure never leaves the remaining snapshots without their schemas.
     */
    private void replaceFromBranch(String branchName, long earliestSnapshotId, Lock lock)
            throws IOException {
        SnapshotManager branchSnapshotManager = snapshotManager.copyWithBranch(branchName);
        SchemaManager branchSchemaManager = schemaManager.copyWithBranch(branchName);
        long earliestSchemaId = branchSnapshotManager.snapshot(earliestSnapshotId).schemaId();

        // Delete tags first: a retry decides from snapshots and schemas alone whether to append,
        // which keeps main's tags, so the tags must be gone before either changes.
        List<Path> deletePaths =
                new ArrayList<>(
                        tagManager.tagPaths(
                                path -> Tag.fromPath(fileIO, path).id() >= earliestSnapshotId));
        deletePaths.addAll(snapshotManager.snapshotPaths(id -> id >= earliestSnapshotId));
        lock.ensureValid();
        snapshotManager.deleteLatestHint();
        for (Path path : deletePaths) {
            delete(lock, path);
        }
        // Main expired its own snapshots up to its EARLIEST hint; the branch's snapshots below
        // that hint become main's. Move the hint before copying them, so that a retry after a
        // failed copy, which appends the rest, finds it already moved.
        Long earliestHint =
                HintFileUtils.readHint(
                        fileIO, HintFileUtils.EARLIEST, snapshotManager.snapshotDirectory());
        if (earliestHint != null && earliestHint > earliestSnapshotId) {
            lock.ensureValid();
            snapshotManager.commitEarliestHint(earliestSnapshotId);
        }

        List<Long> branchSchemaIds = sorted(branchSchemaManager.listAllIds());
        for (long id : branchSchemaIds) {
            replaceAtomically(
                    lock, branchSchemaManager.toSchemaPath(id), schemaManager.toSchemaPath(id));
        }
        for (long id : schemaManager.listAllIds()) {
            if (id >= earliestSchemaId && !branchSchemaIds.contains(id)) {
                delete(lock, schemaManager.toSchemaPath(id));
            }
        }

        // List rather than trust the branch's LATEST hint, as the append path does.
        long branchLatestId = Collections.max(snapshotSizes(branchSnapshotManager).keySet());
        copySnapshots(branchSnapshotManager, earliestSnapshotId, branchLatestId, lock);
        copyBranchTags(branchName, lock);
        lock.ensureValid();
        snapshotManager.commitLatestHint(branchLatestId);
    }

    private void copySnapshots(
            SnapshotManager branchSnapshotManager, long fromId, long toId, Lock lock)
            throws IOException {
        for (long id = fromId; id <= toId; id++) {
            copyAtomically(
                    lock, branchSnapshotManager.snapshotPath(id), snapshotManager.snapshotPath(id));
        }
    }

    private void copyBranchTags(String branchName, Lock lock) throws IOException {
        // Continue fast-forward even without tags.
        Path branchTagDirectory = tagManager.copyWithBranch(branchName).tagDirectory();
        if (!fileIO.exists(branchTagDirectory)) {
            return;
        }
        for (FileStatus status : fileIO.listStatus(branchTagDirectory)) {
            if (!status.isDir()) {
                Path tag = status.getPath();
                replaceAtomically(lock, tag, new Path(tagManager.tagDirectory(), tag.getName()));
            }
        }
    }

    /** Copies a file without exposing partial content, accepting a target already copied. */
    private void copyAtomically(Lock lock, Path source, Path target) throws IOException {
        String content = readExactly(source);
        // Reading the source may outlast the lease, so check it right before writing.
        lock.ensureValid();
        if ((fileIO.exists(target) || !fileIO.tryToWriteAtomic(target, content))
                && !content.equals(readExactly(target))) {
            throw new IOException(
                    String.format(
                            "Cannot copy %s to %s, because the target holds other content.",
                            source, target));
        }
    }

    /** Like {@link #copyAtomically}, but first deletes a target that holds other content. */
    private void replaceAtomically(Lock lock, Path source, Path target) throws IOException {
        if (fileIO.exists(target) && !readExactly(source).equals(readExactly(target))) {
            delete(lock, target);
        }
        copyAtomically(lock, source, target);
    }

    /** Deletes a file while the lease is valid, failing if it remains. */
    private void delete(Lock lock, Path path) throws IOException {
        lock.ensureValid();
        if (!fileIO.delete(path, false) && fileIO.exists(path)) {
            throw new IOException("Failed to delete " + path);
        }
    }

    /** Whether two files hold the same content, comparing parsed values when the bytes differ. */
    private boolean sameContent(Path path, Path branchPath, Function<String, ?> parser)
            throws IOException {
        String content = readExactly(path);
        String branchContent = readExactly(branchPath);
        if (content.equals(branchContent)) {
            return true;
        }
        try {
            return parser.apply(content).equals(parser.apply(branchContent));
        } catch (RuntimeException e) {
            // A file cut short by an interrupted copy does not parse.
            return false;
        }
    }

    /** Reads a file with its line breaks, unlike {@link FileIO#readFileUtf8}. */
    private String readExactly(Path path) throws IOException {
        return new String(IOUtils.readFully(fileIO.newInputStream(path), true), UTF_8);
    }

    private static List<Long> sorted(List<Long> ids) {
        return ids.stream().sorted().collect(Collectors.toList());
    }

    @Override
    public void mergeBranch(String sourceBranch, String targetBranch) {
        BranchManager.mergeValidate(sourceBranch, targetBranch);
        validateMergeBranches(sourceBranch, targetBranch);
        validateAppendOnlyHistory(sourceBranch, targetBranch);
        validateAppendOnly(sourceBranch, targetBranch);
        validateNoDataEvolution(sourceBranch, targetBranch);
        validateRowTrackingConsistent(sourceBranch, targetBranch);
        validateLatestSchema(sourceBranch, targetBranch);

        List<ManifestEntry> filesToMerge = computeMergeDiff(sourceBranch, targetBranch);
        if (filesToMerge.isEmpty()) {
            return;
        }

        validateMergeFileSchemas(sourceBranch, targetBranch, filesToMerge);
        mergeHandler.commit(targetBranch, filesToMerge);
    }

    private void validateMergeBranches(String sourceBranch, String targetBranch) {
        if (!BranchManager.isMainBranch(sourceBranch)) {
            checkArgument(branchExists(sourceBranch), "Branch '%s' doesn't exist.", sourceBranch);
        }
        if (!BranchManager.isMainBranch(targetBranch)) {
            checkArgument(branchExists(targetBranch), "Branch '%s' doesn't exist.", targetBranch);
        }

        SnapshotManager sourceSm = snapshotManager.copyWithBranch(sourceBranch);
        checkArgument(
                sourceSm.latestSnapshotId() != null,
                "Cannot merge branch '%s', because it does not have snapshot.",
                sourceBranch);

        SnapshotManager targetSm = snapshotManager.copyWithBranch(targetBranch);
        checkArgument(
                targetSm.latestSnapshotId() != null,
                "Cannot merge into branch '%s', because it does not have snapshot.",
                targetBranch);
    }

    private void validateLatestSchema(String sourceBranch, String targetBranch) {
        SchemaManager sourceSchemaMgr =
                new FileSystemSchemaManager(fileIO, tablePath, sourceBranch);
        SchemaManager targetSchemaMgr =
                new FileSystemSchemaManager(fileIO, tablePath, targetBranch);
        TableSchema sourceSchema = sourceSchemaMgr.latest().get();
        TableSchema targetSchema = targetSchemaMgr.latest().get();
        checkArgument(
                sourceSchema.fields().equals(targetSchema.fields()),
                "Cannot merge branch '%s' into '%s', schema mismatch.",
                sourceBranch,
                targetBranch);
    }

    private void validateMergeFileSchemas(
            String sourceBranch, String targetBranch, List<ManifestEntry> filesToMerge) {
        SchemaManager sourceSchemaMgr =
                new FileSystemSchemaManager(fileIO, tablePath, sourceBranch);
        SchemaManager targetSchemaMgr =
                new FileSystemSchemaManager(fileIO, tablePath, targetBranch);

        for (Long schemaId :
                filesToMerge.stream()
                        .map(entry -> entry.file().schemaId())
                        .collect(Collectors.toSet())) {
            checkArgument(
                    targetSchemaMgr.schemaExists(schemaId),
                    "Cannot merge branch '%s' into '%s', because target branch does not contain "
                            + "schema id %s required by source files.",
                    sourceBranch,
                    targetBranch,
                    schemaId);

            TableSchema sourceSchema = sourceSchemaMgr.schema(schemaId);
            TableSchema targetSchema = targetSchemaMgr.schema(schemaId);
            checkArgument(
                    sourceSchema.equals(targetSchema),
                    "Cannot merge branch '%s' into '%s', schema history mismatch for schema id %s.",
                    sourceBranch,
                    targetBranch,
                    schemaId);
        }
    }

    private void validateAppendOnly(String sourceBranch, String targetBranch) {
        SchemaManager sourceSchemaMgr =
                new FileSystemSchemaManager(fileIO, tablePath, sourceBranch);
        TableSchema sourceSchema = sourceSchemaMgr.latest().get();
        checkArgument(
                sourceSchema.primaryKeys().isEmpty(),
                "Branch merge is only supported for append-only tables, "
                        + "but branch '%s' has primary keys.",
                sourceBranch);

        SchemaManager targetSchemaMgr =
                new FileSystemSchemaManager(fileIO, tablePath, targetBranch);
        TableSchema targetSchema = targetSchemaMgr.latest().get();
        checkArgument(
                targetSchema.primaryKeys().isEmpty(),
                "Branch merge is only supported for append-only tables, "
                        + "but branch '%s' has primary keys.",
                targetBranch);
    }

    // Branch merge is implemented as a conservative file-level merge. Without persisted branch
    // lineage metadata, we cannot reliably infer a fork point after snapshots expire. To preserve
    // correctness, both branches must retain complete append-only history from the first snapshot.
    // This restriction can be relaxed in the future if branch fork metadata is introduced.
    private void validateAppendOnlyHistory(String sourceBranch, String targetBranch) {
        validateCompleteAppendOnly(snapshotManager.copyWithBranch(sourceBranch), sourceBranch);
        validateCompleteAppendOnly(snapshotManager.copyWithBranch(targetBranch), targetBranch);
    }

    private void validateCompleteAppendOnly(SnapshotManager sm, String branch) {
        Long earliest = sm.earliestSnapshotId();
        Long latest = sm.latestSnapshotId();
        if (earliest == null || latest == null) {
            return;
        }
        if (earliest != Snapshot.FIRST_SNAPSHOT_ID) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot merge: branch '%s' does not start at snapshot %d "
                                    + "(earliest is %d). "
                                    + "Branch merge requires complete append-only snapshot history.",
                            branch, Snapshot.FIRST_SNAPSHOT_ID, earliest));
        }
        for (long id = Snapshot.FIRST_SNAPSHOT_ID; id <= latest; id++) {
            if (!sm.snapshotExists(id)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot merge: snapshot %d is missing on branch '%s'. "
                                        + "Branch merge requires complete append-only snapshot history.",
                                id, branch));
            }
            Snapshot snapshot = sm.snapshot(id);
            Snapshot.CommitKind kind = snapshot.commitKind();
            if (kind != Snapshot.CommitKind.APPEND && kind != Snapshot.CommitKind.ANALYZE) {
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot merge: snapshot %d on branch '%s' has commit kind '%s'. "
                                        + "Branch merge requires complete append-only snapshot history.",
                                id, branch, kind));
            }
        }
    }

    private void validateNoDataEvolution(String sourceBranch, String targetBranch) {
        for (String branch : new String[] {sourceBranch, targetBranch}) {
            SchemaManager sm = new FileSystemSchemaManager(fileIO, tablePath, branch);
            TableSchema schema = sm.latest().get();
            CoreOptions opts = new CoreOptions(schema.options());
            checkArgument(
                    !opts.dataEvolutionEnabled(),
                    "Branch merge is not supported for data-evolution tables (branch '%s').",
                    branch);
        }
    }

    private void validateRowTrackingConsistent(String sourceBranch, String targetBranch) {
        boolean sourceEnabled = isRowTrackingEnabled(sourceBranch);
        boolean targetEnabled = isRowTrackingEnabled(targetBranch);
        checkArgument(
                sourceEnabled == targetEnabled,
                "Cannot merge branch '%s' into '%s': row-tracking settings must match "
                        + "(source=%s, target=%s).",
                sourceBranch,
                targetBranch,
                sourceEnabled,
                targetEnabled);
    }

    private boolean isRowTrackingEnabled(String branch) {
        SchemaManager schemaManager = new FileSystemSchemaManager(fileIO, tablePath, branch);
        TableSchema schema = schemaManager.latest().get();
        return new CoreOptions(schema.options()).rowTrackingEnabled();
    }

    private List<ManifestEntry> computeMergeDiff(String sourceBranch, String targetBranch) {
        Set<FileEntry.Identifier> targetFileIds =
                mergeHandler.readBranchFiles(targetBranch).keySet();

        Map<FileEntry.Identifier, ManifestEntry> sourceFiles =
                mergeHandler.readBranchFiles(sourceBranch);

        List<ManifestEntry> filesToMerge = new ArrayList<>();
        for (Map.Entry<FileEntry.Identifier, ManifestEntry> entry : sourceFiles.entrySet()) {
            if (!targetFileIds.contains(entry.getKey())) {
                ManifestEntry manifestEntry = entry.getValue();
                if (manifestEntry.kind() == FileKind.ADD) {
                    filesToMerge.add(manifestEntry);
                }
            }
        }
        return filesToMerge;
    }

    /** Check if a branch exists. */
    public boolean branchExists(String branchName) {
        Path branchPath = branchPath(branchName);
        return fileExists(branchPath);
    }

    public void validateBranch(String branchName) {
        BranchManager.validateBranch(branchName);
        checkArgument(!branchExists(branchName), "Branch name '%s' already exists.", branchName);
    }

    @Override
    public void renameBranch(String fromBranch, String toBranch) {
        checkArgument(!BranchManager.isMainBranch(fromBranch), "Cannot rename the main branch.");
        checkArgument(branchExists(fromBranch), "Branch name '%s' doesn't exist.", fromBranch);
        checkArgument(!branchExists(toBranch), "Branch name '%s' already exists.", toBranch);
        BranchManager.validateBranch(toBranch);

        try {
            // Use rename for atomic operation and better performance
            fileIO.rename(branchPath(fromBranch), branchPath(toBranch));
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when rename branch from '%s' to '%s'.",
                            fromBranch, toBranch),
                    e);
        }
    }

    @Override
    public List<String> branches() {
        return branches(fileIO, tablePath);
    }

    /** List branches without constructing the table's runtime managers. */
    public static List<String> branches(FileIO fileIO, Path tablePath) {
        try {
            return listVersionedDirectories(fileIO, new Path(tablePath, "branch"), BRANCH_PREFIX)
                    .map(status -> status.getPath().getName().substring(BRANCH_PREFIX.length()))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void copySchemasToBranch(String branchName, long schemaId) throws IOException {
        for (int i = 0; i <= schemaId; i++) {
            if (schemaManager.schemaExists(i)) {
                fileIO.copyFile(
                        schemaManager.toSchemaPath(i),
                        schemaManager.copyWithBranch(branchName).toSchemaPath(i),
                        true);
            }
        }
    }
}
