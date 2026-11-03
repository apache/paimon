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
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.DataFilePathFactories;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashSet;
import java.util.Set;
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
 * <p>Deletion collects file paths one manifest at a time instead of keeping a table-wide file set.
 * Snapshot and manifest references are still collected for the branch. File paths are deduplicated
 * within each manifest; references in different manifests may cause repeated deletion attempts, and
 * missing files are ignored.
 */
public class EntropyInjectTemplateFilesDeletion {

    private static final Logger LOG =
            LoggerFactory.getLogger(EntropyInjectTemplateFilesDeletion.class);

    private final FileStoreTable table;
    private final ThreadPoolExecutor executor;

    private EntropyInjectTemplateFilesDeletion(FileStoreTable table) {
        this.table = table;
        this.executor =
                createCachedThreadPool(
                        table.coreOptions().fileOperationThreadNum(), "ENTROPY_TEMPLATE_DELETE");
    }

    /**
     * Deletes the data files of the supplied branch. Call it only for a branch with template paths
     * in its current schema, and only from a catalog that deletes the data of a dropped table.
     *
     * <p>Manifest read failures abort the drop and retain metadata for retry. File deletion is
     * best-effort: failures are logged and do not prevent metadata deletion.
     */
    public static void delete(FileStoreTable table) {
        EntropyInjectTemplateFilesDeletion deletion = new EntropyInjectTemplateFilesDeletion(table);
        try {
            long deleted = deletion.deleteFiles();
            LOG.info("Attempted to delete {} files of table branch {}.", deleted, table.fullName());
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to delete the external data files of table "
                            + table.fullName()
                            + ", the table is kept so that dropping it can be retried.",
                    e);
        } finally {
            deletion.executor.shutdownNow();
        }
    }

    public static boolean isTemplate(TableSchema schema) {
        // compare the raw value instead of parsing the enum, so that a schema written by a newer
        // version with an unknown strategy does not fail the whole drop
        String strategy = schema.options().get(CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key());
        return ExternalPathStrategy.ENTROPY_INJECT_TEMPLATE.toString().equalsIgnoreCase(strategy);
    }

    /** Reads each referenced manifest once; a file may occur in more than one manifest. */
    private long deleteFiles() throws IOException {
        ManifestList manifestList = table.store().manifestListFactory().create();
        Set<String> dataManifests = new LinkedHashSet<>();
        Set<String> indexManifests = new LinkedHashSet<>();
        for (Snapshot snapshot : allSnapshots()) {
            for (ManifestFileMeta meta : manifestList.readAllManifests(snapshot)) {
                dataManifests.add(meta.fileName());
            }
            if (snapshot.indexManifest() != null) {
                indexManifests.add(snapshot.indexManifest());
            }
        }

        return deleteDataFiles(dataManifests) + deleteIndexFiles(indexManifests);
    }

    private Set<Snapshot> allSnapshots() throws IOException {
        Set<Snapshot> snapshots =
                new LinkedHashSet<>(table.snapshotManager().safelyGetAllSnapshots());
        snapshots.addAll(table.tagManager().taggedSnapshots());
        snapshots.addAll(table.changelogManager().safelyGetAllChangelogs());
        return snapshots;
    }

    private long deleteDataFiles(Set<String> manifestNames) throws IOException {
        ManifestFile manifestFile = table.store().manifestFileFactory().create();
        DataFilePathFactories factories = new DataFilePathFactories(table.store().pathFactory());

        long deleted = 0;
        for (String manifestName : manifestNames) {
            Set<Path> files = new LinkedHashSet<>();
            for (ManifestEntry entry : manifestFile.readWithIOException(manifestName)) {
                files.addAll(
                        entry.file()
                                .collectFiles(factories.get(entry.partition(), entry.bucket())));
            }
            deleted += deleteAll(files);
        }
        return deleted;
    }

    private long deleteIndexFiles(Set<String> manifestNames) throws IOException {
        IndexFileHandler indexFileHandler = table.store().newIndexFileHandler();

        long deleted = 0;
        for (String manifestName : manifestNames) {
            if (!indexFileHandler.existsManifest(manifestName)) {
                continue;
            }
            Set<Path> files = new LinkedHashSet<>();
            for (IndexManifestEntry entry :
                    indexFileHandler.readManifestWithIOException(manifestName)) {
                files.add(indexFileHandler.filePath(entry));
            }
            deleted += deleteAll(files);
        }
        return deleted;
    }

    private long deleteAll(Set<Path> files) {
        if (files.isEmpty()) {
            return 0;
        }
        randomlyOnlyExecute(executor, table.fileIO()::deleteQuietly, files);
        return files.size();
    }
}
