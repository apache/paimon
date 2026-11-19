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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.ExternalPathStrategy;
import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.utils.DataFilePathFactories;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadPoolExecutor;

import static org.apache.paimon.utils.ThreadPoolUtils.createCachedThreadPool;
import static org.apache.paimon.utils.ThreadPoolUtils.randomlyOnlyExecute;

/**
 * Deletes the data files of a single table branch using the {@link
 * ExternalPathStrategy#ENTROPY_INJECT_TEMPLATE} strategy, whose external paths are shared with
 * other tables.
 *
 * <p>For every other strategy the configured external paths belong to a single table, so dropping a
 * table can simply delete these directories recursively. Templates can place hash directories
 * before the database and table directories, allowing multiple tables to share a root. Files
 * referenced by this branch are therefore collected from manifests and deleted one by one.
 *
 * <p>Collection takes every entry of every surviving manifest, both {@code ADD} and {@code DELETE},
 * without merging them. A file added by an older retained snapshot and removed by a newer one is
 * still on disk when snapshot expiration has not caught up with it, and once the older snapshot is
 * gone the file is only recorded as a {@code DELETE} entry. The merged view offered by {@link
 * FileDeletionBase} contains only live files and would omit these pending deletions. Files absent
 * from all retained manifests are not discovered by this cleanup.
 *
 * <p>Collection and deletion are two separate steps: the paths of a branch are read from its
 * manifests first, and {@link #deleteCollectedFiles()} runs only once the catalog has accepted the
 * drop. A catalog can reject the drop with an authorization, transport or SQL error while leaving
 * the table registered, and data of a table that still exists must stay in place. Holding the paths
 * of a branch in memory is the price of that ordering.
 *
 * <p>File paths are deduplicated per branch; a file referenced by more than one branch may be
 * deleted repeatedly, and missing files are ignored.
 */
public class EntropyInjectTemplateFilesDeletion {

    private static final Logger LOG =
            LoggerFactory.getLogger(EntropyInjectTemplateFilesDeletion.class);

    private static final String DROPPED_BRANCH_FILES = "dropped-branch-template-files";
    private static final String DROP_FILE_PREFIX = "drop-";

    private final FileStoreTable table;
    private final Set<Path> files;

    private EntropyInjectTemplateFilesDeletion(FileStoreTable table, Set<Path> files) {
        this.table = table;
        this.files = files;
    }

    /** Collects branch files if a historical schema used the template strategy. */
    public static Optional<EntropyInjectTemplateFilesDeletion> collectIfTemplate(
            FileIO fileIO, Path tablePath, String branch, List<TableSchema> schemas) {
        if (schemas == null
                || schemas.stream().noneMatch(EntropyInjectTemplateFilesDeletion::isTemplate)) {
            return Optional.empty();
        }

        TableSchema latest = schemas.stream().max(Comparator.comparingLong(TableSchema::id)).get();
        FileStoreTable table =
                FileStoreTableFactory.createWithoutFallbackBranch(
                        fileIO,
                        tablePath,
                        latest,
                        new Options().set(CoreOptions.BRANCH, branch),
                        CatalogEnvironment.empty());
        return Optional.of(collect(table));
    }

    /**
     * Collects the data files of the supplied branch. Manifest read failures abort the drop and
     * retain metadata for retry.
     */
    private static EntropyInjectTemplateFilesDeletion collect(FileStoreTable table) {
        try {
            return new EntropyInjectTemplateFilesDeletion(table, collectFiles(table));
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to read the data files of table "
                            + table.fullName()
                            + ", the table is kept so that dropping it can be retried.",
                    e);
        }
    }

    /**
     * Deletes the collected files. Call it only after the catalog has deleted the table. Deletion
     * is best-effort: failures are logged and do not undo the metadata deletion.
     */
    public void deleteCollectedFiles() {
        if (files.isEmpty()) {
            return;
        }

        ThreadPoolExecutor executor =
                createCachedThreadPool(
                        table.coreOptions().fileOperationThreadNum(), "ENTROPY_TEMPLATE_DELETE");
        try {
            randomlyOnlyExecute(executor, table.fileIO()::deleteQuietly, files);
            LOG.info(
                    "Attempted to delete {} files of table branch {}.",
                    files.size(),
                    table.fullName());
        } finally {
            executor.shutdownNow();
        }
    }

    /** Retains the branch's file references outside its metadata directory before it is dropped. */
    public void retainForTableDrop() {
        if (files.isEmpty()) {
            return;
        }
        Path directory = new Path(table.location(), DROPPED_BRANCH_FILES);
        Path path = new Path(directory, DROP_FILE_PREFIX + UUID.randomUUID());
        try {
            if (!table.fileIO()
                    .tryToWriteAtomic(
                            path,
                            JsonSerdeUtil.toJson(
                                    files.stream().map(Path::toString).toArray(String[]::new)))) {
                throw new IOException("Could not publish " + path);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to retain template files for " + table.fullName(), e);
        }
    }

    /** Reads files retained by previous branch drops while the table still exists. */
    public static List<Path> retainedBranchFiles(FileIO fileIO, Path tablePath) {
        Path directory = new Path(tablePath, DROPPED_BRANCH_FILES);
        Set<Path> retained = new LinkedHashSet<>();
        try {
            if (fileIO.exists(directory)) {
                for (FileStatus status : fileIO.listStatus(directory)) {
                    Path path = status.getPath();
                    if (!status.isDir() && path.getName().startsWith(DROP_FILE_PREFIX)) {
                        String[] paths =
                                JsonSerdeUtil.fromJson(fileIO.readFileUtf8(path), String[].class);
                        for (String file : paths) {
                            retained.add(new Path(file));
                        }
                    }
                }
            }
            return new ArrayList<>(retained);
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to read retained branch files in " + directory, e);
        }
    }

    public static boolean isTemplate(TableSchema schema) {
        // compare the raw value instead of parsing the enum, so that a schema written by a newer
        // version with an unknown strategy does not fail the whole drop
        String strategy = schema.options().get(CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key());
        return ExternalPathStrategy.ENTROPY_INJECT_TEMPLATE.toString().equalsIgnoreCase(strategy);
    }

    /** Reads each referenced manifest once; a file may occur in more than one manifest. */
    private static Set<Path> collectFiles(FileStoreTable table) throws IOException {
        ManifestList manifestList = table.store().manifestListFactory().create();
        Set<String> dataManifests = new LinkedHashSet<>();
        Set<String> indexManifests = new LinkedHashSet<>();
        for (Snapshot snapshot : allSnapshots(table)) {
            for (ManifestFileMeta meta : manifestList.readAllManifests(snapshot)) {
                dataManifests.add(meta.fileName());
            }
            if (snapshot.indexManifest() != null) {
                indexManifests.add(snapshot.indexManifest());
            }
        }

        Set<Path> files = new LinkedHashSet<>();
        files.addAll(collectDataFiles(table, dataManifests));
        files.addAll(collectIndexFiles(table, indexManifests));
        return files;
    }

    private static Set<Snapshot> allSnapshots(FileStoreTable table) throws IOException {
        Set<Snapshot> snapshots =
                new LinkedHashSet<>(table.snapshotManager().safelyGetAllSnapshots());
        snapshots.addAll(table.tagManager().taggedSnapshots());
        snapshots.addAll(table.changelogManager().safelyGetAllChangelogs());
        return snapshots;
    }

    private static Set<Path> collectDataFiles(FileStoreTable table, Set<String> manifestNames)
            throws IOException {
        ManifestFile manifestFile = table.store().manifestFileFactory().create();
        DataFilePathFactories factories = new DataFilePathFactories(table.store().pathFactory());

        Set<Path> files = new LinkedHashSet<>();
        for (String manifestName : manifestNames) {
            for (ManifestEntry entry : manifestFile.readWithIOException(manifestName)) {
                files.addAll(
                        entry.file()
                                .collectFiles(factories.get(entry.partition(), entry.bucket())));
            }
        }
        return files;
    }

    private static Set<Path> collectIndexFiles(FileStoreTable table, Set<String> manifestNames)
            throws IOException {
        IndexFileHandler indexFileHandler = table.store().newIndexFileHandler();

        Set<Path> files = new LinkedHashSet<>();
        for (String manifestName : manifestNames) {
            if (!indexFileHandler.existsManifest(manifestName)) {
                continue;
            }
            for (IndexManifestEntry entry :
                    indexFileHandler.readManifestWithIOException(manifestName)) {
                files.add(indexFileHandler.filePath(entry));
            }
        }
        return files;
    }
}
