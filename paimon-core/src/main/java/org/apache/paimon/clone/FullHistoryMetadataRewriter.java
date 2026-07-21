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

package org.apache.paimon.clone;

import org.apache.paimon.Changelog;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.DATA_FILE_EXTERNAL_PATHS;
import static org.apache.paimon.CoreOptions.DATA_FILE_EXTERNAL_PATHS_SPECIFIC_FS;
import static org.apache.paimon.CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY;
import static org.apache.paimon.CoreOptions.DATA_FILE_PATH_DIRECTORY;
import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_EXTERNAL_PATH;
import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Rewrites the complete metadata graph of a full-history clone. */
public class FullHistoryMetadataRewriter {

    private final FileStoreTable sourceTable;
    private final FileIO targetFileIO;
    private final Path targetRoot;
    private final PathMapping pathMapping;

    public FullHistoryMetadataRewriter(
            FileStoreTable sourceTable,
            FileIO targetFileIO,
            Path targetRoot,
            PathMapping pathMapping) {
        this.sourceTable = sourceTable;
        this.targetFileIO = targetFileIO;
        this.targetRoot = targetRoot;
        this.pathMapping = pathMapping;
    }

    public void rewrite() throws Exception {
        List<String> branches = new ArrayList<>(sourceTable.branchManager().branches());
        branches.add(DEFAULT_MAIN_BRANCH);

        for (String branch : branches) {
            rewriteSchemas(sourceTable.switchToBranch(branch), branch);
        }

        FileStoreTable targetTable = FileStoreTableFactory.create(targetFileIO, targetRoot);
        for (String branch : branches) {
            new BranchRewriter(
                            sourceTable.switchToBranch(branch),
                            targetTable.switchToBranch(branch),
                            pathMapping)
                    .rewrite();
        }
    }

    private void rewriteSchemas(FileStoreTable sourceBranchTable, String branch) throws Exception {
        SchemaManager sourceSchemaManager = sourceBranchTable.schemaManager();
        SchemaManager targetSchemaManager = new SchemaManager(targetFileIO, targetRoot, branch);
        for (TableSchema sourceSchema : sourceSchemaManager.listAll()) {
            FullHistoryClonePlanner.validateSupportedSchema(sourceSchema);
            TableSchema targetSchema =
                    sourceSchema.copy(
                            rewriteOptions(
                                    sourceSchema.options(),
                                    pathMapping,
                                    sourceBranchTable.location()));
            if (targetSchemaManager.schemaExists(targetSchema.id())) {
                checkState(
                        Objects.equals(targetSchemaManager.schema(targetSchema.id()), targetSchema),
                        "Target schema %s in branch %s is different from the planned schema.",
                        targetSchema.id(),
                        branch);
            } else {
                Path schemaPath =
                        new Path(
                                targetSchemaManager.schemaDirectory(),
                                SchemaManager.SCHEMA_PREFIX + targetSchema.id());
                checkState(
                        targetFileIO.tryToWriteAtomic(schemaPath, targetSchema.toString()),
                        "Failed to write target schema %s in branch %s.",
                        targetSchema.id(),
                        branch);
            }
        }
    }

    static Map<String, String> rewriteOptions(
            Map<String, String> sourceOptions, PathMapping pathMapping) {
        String path = sourceOptions.get(PATH.key());
        return rewriteOptions(sourceOptions, pathMapping, path == null ? null : new Path(path));
    }

    static Map<String, String> rewriteOptions(
            Map<String, String> sourceOptions,
            PathMapping pathMapping,
            @Nullable Path sourceTableRoot) {
        Map<String, String> targetOptions = new HashMap<>(sourceOptions);
        rewritePathOption(targetOptions, PATH.key(), pathMapping);
        rewritePathOption(targetOptions, GLOBAL_INDEX_EXTERNAL_PATH.key(), pathMapping);
        rewriteDataFilePathDirectory(sourceOptions, targetOptions, pathMapping, sourceTableRoot);

        String externalPaths = targetOptions.get(DATA_FILE_EXTERNAL_PATHS.key());
        if (externalPaths != null) {
            List<String> sourcePaths =
                    Arrays.stream(externalPaths.split(","))
                            .map(String::trim)
                            .collect(Collectors.toList());
            List<String> targetPaths =
                    sourcePaths.stream()
                            .map(pathMapping::rewriteRequired)
                            .collect(Collectors.toList());
            targetOptions.put(DATA_FILE_EXTERNAL_PATHS.key(), String.join(",", targetPaths));
            rewriteSpecificFileSystem(sourceOptions, targetOptions, sourcePaths, targetPaths);
        }
        return targetOptions;
    }

    private static void rewriteSpecificFileSystem(
            Map<String, String> sourceOptions,
            Map<String, String> targetOptions,
            List<String> sourcePaths,
            List<String> targetPaths) {
        CoreOptions sourceCoreOptions = CoreOptions.fromMap(sourceOptions);
        if (sourceCoreOptions.externalPathStrategy()
                != CoreOptions.ExternalPathStrategy.SPECIFIC_FS) {
            return;
        }

        String sourceSpecificFileSystem = sourceCoreOptions.externalSpecificFS();
        checkArgument(
                sourceSpecificFileSystem != null,
                "%s must be set when %s is specific-fs.",
                DATA_FILE_EXTERNAL_PATHS_SPECIFIC_FS.key(),
                DATA_FILE_EXTERNAL_PATHS_STRATEGY.key());
        Set<String> targetSchemes = new HashSet<>();
        for (int i = 0; i < sourcePaths.size(); i++) {
            String sourceScheme = new Path(sourcePaths.get(i)).toUri().getScheme();
            if (sourceSpecificFileSystem.equalsIgnoreCase(sourceScheme)) {
                String targetScheme = new Path(targetPaths.get(i)).toUri().getScheme();
                checkArgument(
                        targetScheme != null,
                        "Mapped external path must have a scheme: %s",
                        targetPaths.get(i));
                targetSchemes.add(targetScheme.toLowerCase(Locale.ROOT));
            }
        }
        checkArgument(
                targetSchemes.size() == 1,
                "External paths selected by %s=%s must map to exactly one target file system, but found %s.",
                DATA_FILE_EXTERNAL_PATHS_SPECIFIC_FS.key(),
                sourceSpecificFileSystem,
                targetSchemes);
        String targetSpecificFileSystem = targetSchemes.iterator().next();
        for (int i = 0; i < sourcePaths.size(); i++) {
            String sourceScheme = new Path(sourcePaths.get(i)).toUri().getScheme();
            String targetScheme = new Path(targetPaths.get(i)).toUri().getScheme();
            if (!sourceSpecificFileSystem.equalsIgnoreCase(sourceScheme)) {
                checkArgument(
                        !targetSpecificFileSystem.equalsIgnoreCase(targetScheme),
                        "Mapping external path at index %s to %s would select additional external paths for %s.",
                        i,
                        targetSpecificFileSystem,
                        DATA_FILE_EXTERNAL_PATHS_SPECIFIC_FS.key());
            }
        }
        targetOptions.put(DATA_FILE_EXTERNAL_PATHS_SPECIFIC_FS.key(), targetSpecificFileSystem);
    }

    private static void rewritePathOption(
            Map<String, String> options, String key, PathMapping pathMapping) {
        String path = options.get(key);
        if (path != null) {
            options.put(key, pathMapping.rewriteRequired(path));
        }
    }

    private static void rewriteDataFilePathDirectory(
            Map<String, String> sourceOptions,
            Map<String, String> targetOptions,
            PathMapping pathMapping,
            @Nullable Path sourceTableRoot) {
        String path = sourceOptions.get(DATA_FILE_PATH_DIRECTORY.key());
        if (path != null && isAbsolutePath(path)) {
            Path sourcePath = new Path(path);
            if (sourcePath.toUri().getScheme() == null) {
                checkArgument(
                        sourceTableRoot != null,
                        "The source table root is required to resolve %s.",
                        DATA_FILE_PATH_DIRECTORY.key());
                sourcePath = new Path(sourceTableRoot, sourcePath);
            }
            targetOptions.put(
                    DATA_FILE_PATH_DIRECTORY.key(),
                    pathMapping.rewriteRequired(sourcePath.toString()));
        }
    }

    private static boolean isAbsolutePath(String path) {
        java.net.URI uri = new Path(path).toUri();
        return uri.getScheme() != null || uri.getPath().startsWith("/");
    }

    private static class BranchRewriter {

        private final FileStoreTable source;
        private final FileStoreTable target;
        private final PathMapping mapping;
        private final ManifestFile sourceManifestFile;
        private final ManifestFile targetManifestFile;
        private final ManifestList sourceManifestList;
        private final ManifestList targetManifestList;
        private final IndexFileHandler sourceIndexHandler;
        private final IndexManifestFile targetIndexManifestFile;
        private final Map<String, List<ManifestFileMeta>> rewrittenManifests = new HashMap<>();
        private final Map<String, Pair<String, Long>> rewrittenManifestLists = new HashMap<>();
        private final Map<String, String> rewrittenIndexManifests = new HashMap<>();
        private final Map<String, Boolean> copiedStatistics = new HashMap<>();

        private BranchRewriter(FileStoreTable source, FileStoreTable target, PathMapping mapping) {
            this.source = source;
            this.target = target;
            this.mapping = mapping;
            this.sourceManifestFile = source.store().manifestFileFactory().create();
            this.targetManifestFile = target.store().manifestFileFactory().create();
            this.sourceManifestList = source.store().manifestListFactory().create();
            this.targetManifestList = target.store().manifestListFactory().create();
            this.sourceIndexHandler = source.store().newIndexFileHandler();
            this.targetIndexManifestFile = target.store().indexManifestFileFactory().create();
        }

        private void rewrite() throws Exception {
            rewriteSnapshots();
            rewriteTags();
            rewriteChangelogs();
        }

        private void rewriteSnapshots() throws IOException {
            SnapshotManager sourceManager = source.snapshotManager();
            SnapshotManager targetManager = target.snapshotManager();
            List<Snapshot> snapshots = sourceManager.safelyGetAllSnapshots();
            for (Snapshot snapshot : snapshots) {
                Snapshot rewritten = rewriteSnapshot(snapshot);
                target.fileIO()
                        .overwriteFileUtf8(
                                targetManager.snapshotPath(rewritten.id()), rewritten.toJson());
            }
            commitSnapshotHints(targetManager, snapshots);
        }

        private void rewriteTags() throws IOException {
            TagManager targetManager = target.tagManager();
            for (Pair<Tag, String> tagAndName : source.tagManager().tagObjects()) {
                Tag sourceTag = tagAndName.getLeft();
                Snapshot rewritten = rewriteSnapshot(sourceTag);
                String content = rewritten.toJson();
                if (sourceTag.getTagTimeRetained() != null) {
                    LocalDateTime createTime =
                            sourceTag.getTagCreateTime() == null
                                    ? DateTimeUtils.toLocalDateTime(
                                            source.fileIO()
                                                    .getFileStatus(
                                                            source.tagManager()
                                                                    .tagPath(tagAndName.getRight()))
                                                    .getModificationTime())
                                    : sourceTag.getTagCreateTime();
                    content =
                            Tag.fromSnapshotAndTagTtl(
                                            rewritten, sourceTag.getTagTimeRetained(), createTime)
                                    .toJson();
                }
                target.fileIO()
                        .overwriteFileUtf8(targetManager.tagPath(tagAndName.getRight()), content);
            }
        }

        private void rewriteChangelogs() throws IOException {
            ChangelogManager sourceManager = source.changelogManager();
            ChangelogManager targetManager = target.changelogManager();
            List<Changelog> changelogs = sourceManager.safelyGetAllChangelogs();
            for (Changelog changelog : changelogs) {
                targetManager.commitChangelog(
                        new Changelog(rewriteChangelog(changelog)), changelog.id());
            }
            if (!changelogs.isEmpty()) {
                long earliest = changelogs.stream().mapToLong(Changelog::id).min().getAsLong();
                long latest = changelogs.stream().mapToLong(Changelog::id).max().getAsLong();
                targetManager.commitLongLivedChangelogEarliestHint(earliest);
                targetManager.commitLongLivedChangelogLatestHint(latest);
            }
        }

        private Snapshot rewriteSnapshot(Snapshot snapshot) throws IOException {
            Pair<String, Long> baseManifestList = rewriteManifestList(snapshot.baseManifestList());
            Pair<String, Long> deltaManifestList =
                    rewriteManifestList(snapshot.deltaManifestList());
            Pair<String, Long> changelogManifestList =
                    snapshot.changelogManifestList() == null
                            ? null
                            : rewriteManifestList(snapshot.changelogManifestList());
            String indexManifest =
                    snapshot.indexManifest() == null
                            ? null
                            : rewriteIndexManifest(snapshot.indexManifest());
            copyStatistics(snapshot.statistics());

            return copySnapshot(
                    snapshot,
                    baseManifestList,
                    deltaManifestList,
                    changelogManifestList,
                    indexManifest,
                    snapshot.statistics());
        }

        private Snapshot rewriteChangelog(Changelog changelog) throws IOException {
            Pair<String, Long> baseManifestList;
            Pair<String, Long> deltaManifestList;
            Pair<String, Long> changelogManifestList;
            if (changelog.changelogManifestList() != null) {
                baseManifestList =
                        Pair.of(changelog.baseManifestList(), changelog.baseManifestListSize());
                deltaManifestList =
                        Pair.of(changelog.deltaManifestList(), changelog.deltaManifestListSize());
                changelogManifestList = rewriteManifestList(changelog.changelogManifestList());
            } else {
                baseManifestList = rewriteManifestList(changelog.baseManifestList());
                deltaManifestList = rewriteManifestList(changelog.deltaManifestList());
                changelogManifestList = null;
            }

            // Index and statistics are owned by snapshot expiration, not long-lived changelogs.
            return copySnapshot(
                    changelog,
                    baseManifestList,
                    deltaManifestList,
                    changelogManifestList,
                    changelog.indexManifest(),
                    changelog.statistics());
        }

        private Snapshot copySnapshot(
                Snapshot snapshot,
                Pair<String, Long> baseManifestList,
                Pair<String, Long> deltaManifestList,
                Pair<String, Long> changelogManifestList,
                String indexManifest,
                String statistics) {
            return new Snapshot(
                    snapshot.version(),
                    snapshot.id(),
                    snapshot.schemaId(),
                    baseManifestList.getLeft(),
                    baseManifestList.getRight(),
                    deltaManifestList.getLeft(),
                    deltaManifestList.getRight(),
                    changelogManifestList == null ? null : changelogManifestList.getLeft(),
                    changelogManifestList == null ? null : changelogManifestList.getRight(),
                    indexManifest,
                    snapshot.commitUser(),
                    snapshot.commitIdentifier(),
                    snapshot.commitKind(),
                    snapshot.timeMillis(),
                    snapshot.totalRecordCount(),
                    snapshot.deltaRecordCount(),
                    snapshot.changelogRecordCount(),
                    snapshot.watermark(),
                    statistics,
                    snapshot.properties(),
                    snapshot.nextRowId(),
                    snapshot.operation());
        }

        private Pair<String, Long> rewriteManifestList(String fileName) throws IOException {
            Pair<String, Long> rewritten = rewrittenManifestLists.get(fileName);
            if (rewritten != null) {
                return rewritten;
            }

            List<ManifestFileMeta> targetMetas = new ArrayList<>();
            for (ManifestFileMeta sourceMeta : sourceManifestList.readWithIOException(fileName)) {
                targetMetas.addAll(rewriteManifest(sourceMeta));
            }
            rewritten = targetManifestList.write(targetMetas);
            rewrittenManifestLists.put(fileName, rewritten);
            return rewritten;
        }

        private List<ManifestFileMeta> rewriteManifest(ManifestFileMeta sourceMeta)
                throws IOException {
            List<ManifestFileMeta> rewritten = rewrittenManifests.get(sourceMeta.fileName());
            if (rewritten != null) {
                return rewritten;
            }

            List<ManifestEntry> targetEntries = new ArrayList<>();
            for (ManifestEntry sourceEntry :
                    sourceManifestFile.readWithIOException(
                            sourceMeta.fileName(), sourceMeta.fileSize())) {
                DataFileMeta sourceFile = sourceEntry.file();
                DataFileMeta targetFile =
                        sourceFile
                                .externalPath()
                                .map(mapping::rewriteRequired)
                                .map(sourceFile::newExternalPath)
                                .orElse(sourceFile);
                targetEntries.add(
                        ManifestEntry.create(
                                sourceEntry.kind(),
                                sourceEntry.partition(),
                                sourceEntry.bucket(),
                                sourceEntry.totalBuckets(),
                                targetFile));
            }
            rewritten = targetManifestFile.write(targetEntries);
            rewrittenManifests.put(sourceMeta.fileName(), rewritten);
            return rewritten;
        }

        private String rewriteIndexManifest(String fileName) throws IOException {
            String rewritten = rewrittenIndexManifests.get(fileName);
            if (rewritten != null) {
                return rewritten;
            }

            List<IndexManifestEntry> targetEntries = new ArrayList<>();
            for (IndexManifestEntry sourceEntry :
                    sourceIndexHandler.readManifestWithIOException(fileName)) {
                IndexFileMeta sourceFile = sourceEntry.indexFile();
                String externalPath =
                        sourceFile.externalPath() == null
                                ? null
                                : mapping.rewriteRequired(sourceFile.externalPath());
                IndexFileMeta targetFile =
                        new IndexFileMeta(
                                sourceFile.indexType(),
                                sourceFile.fileName(),
                                sourceFile.fileSize(),
                                sourceFile.rowCount(),
                                sourceFile.dvRanges(),
                                externalPath,
                                sourceFile.globalIndexMeta());
                targetEntries.add(
                        new IndexManifestEntry(
                                sourceEntry.kind(),
                                sourceEntry.partition(),
                                sourceEntry.bucket(),
                                targetFile));
            }
            rewritten = targetIndexManifestFile.writeWithoutRolling(targetEntries);
            rewrittenIndexManifests.put(fileName, rewritten);
            return rewritten;
        }

        private void copyStatistics(String fileName) throws IOException {
            if (fileName == null || copiedStatistics.put(fileName, true) != null) {
                return;
            }
            Path sourcePath = source.store().pathFactory().statsFileFactory().toPath(fileName);
            Path targetPath = target.store().pathFactory().statsFileFactory().toPath(fileName);
            long sourceSize = source.fileIO().getFileSize(sourcePath);
            FullHistoryFileCopier.copyFile(
                    source.fileIO(),
                    target.fileIO(),
                    new FullHistoryCopyPlan.FileCopy(
                            sourcePath,
                            targetPath,
                            FullHistoryCopyPlan.FileKind.METADATA,
                            sourceSize),
                    false);
        }

        private static void commitSnapshotHints(
                SnapshotManager targetManager, List<Snapshot> snapshots) throws IOException {
            if (snapshots.isEmpty()) {
                return;
            }
            long earliest = snapshots.stream().mapToLong(Snapshot::id).min().getAsLong();
            long latest = snapshots.stream().mapToLong(Snapshot::id).max().getAsLong();
            targetManager.commitEarliestHint(earliest);
            targetManager.commitLatestHint(latest);
        }
    }
}
