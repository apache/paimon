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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.avro.AvroFileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.io.RollingFileWriterImpl;
import org.apache.paimon.io.SingleFileWriter;
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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DataFilePathFactories;
import org.apache.paimon.utils.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.ThreadPoolUtils.createCachedThreadPool;
import static org.apache.paimon.utils.ThreadPoolUtils.randomlyOnlyExecute;

/** Persists exact template file paths in branch cleanup manifests under the table directory. */
public final class TemplateFileDeletionPlan {

    private static final Logger LOG = LoggerFactory.getLogger(TemplateFileDeletionPlan.class);
    private static final String CLEANUP_DIRECTORY = ".paimon-template-drops";
    private static final String BRANCH_PREFIX = "branch-";
    private static final String MANIFEST_PREFIX = "manifest-";
    private static final RowType PATH_ROW =
            RowType.of(new DataField(0, "path", DataTypes.STRING()));

    private final FileIO fileIO;
    private final Path tablePath;

    private TemplateFileDeletionPlan(FileIO fileIO, Path tablePath) {
        this.fileIO = fileIO;
        this.tablePath = tablePath;
    }

    public static boolean isTemplate(TableSchema schema) {
        String strategy = schema.options().get(CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key());
        return ExternalPathStrategy.ENTROPY_INJECT_TEMPLATE.toString().equalsIgnoreCase(strategy);
    }

    /** Archives dropped branches separately so reusing a branch name preserves its old files. */
    public static void save(
            FileIO fileIO, Path tablePath, String branch, TableSchema schema, boolean archive) {
        if (!isTemplate(schema)) {
            return;
        }
        Path root = cleanupPath(tablePath);
        String prefix =
                archive
                        ? BRANCH_PREFIX
                        : BRANCH_PREFIX
                                + UUID.nameUUIDFromBytes(branch.getBytes(StandardCharsets.UTF_8))
                                + "-";
        try {
            List<Path> previous =
                    archive
                            ? Collections.emptyList()
                            : FileUtils.listVersionedDirectories(fileIO, root, prefix)
                                    .map(FileStatus::getPath)
                                    .collect(Collectors.toList());
            writePlan(
                    fileIO, tablePath, branch, schema, new Path(root, prefix + UUID.randomUUID()));
            for (Path path : previous) {
                fileIO.delete(path, true);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot replace cleanup plan for branch " + branch, e);
        }
    }

    private static void writePlan(
            FileIO fileIO, Path tablePath, String branch, TableSchema schema, Path plan) {
        RollingFileWriter<Path, Path> writer =
                newManifestWriter(fileIO, plan, CoreOptions.fromMap(schema.options()));
        boolean saved = false;
        try {
            FileStoreTable table =
                    FileStoreTableFactory.createWithoutFallbackBranch(
                            fileIO,
                            tablePath,
                            schema,
                            new Options().set(CoreOptions.BRANCH, branch),
                            CatalogEnvironment.empty());
            collectFiles(table, writer);
            writer.close();
            saved = true;
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot write template cleanup plan " + plan, e);
        } finally {
            if (!saved) {
                writer.abort();
                fileIO.deleteDirectoryQuietly(plan);
            }
        }
    }

    public static TemplateFileDeletionPlan init(FileIO fileIO, Path tablePath) {
        return new TemplateFileDeletionPlan(fileIO, tablePath);
    }

    public static void checkCanCreateTable(FileIO fileIO, Path tablePath) {
        try {
            if (fileIO.exists(cleanupPath(tablePath))) {
                throw new IllegalStateException(
                        "Resume the saved cleanup plan for "
                                + tablePath
                                + " before recreating the table.");
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot check cleanup directory for " + tablePath, e);
        }
    }

    /** Deletes the files recorded in each cleanup manifest, then the manifest itself. */
    public void execute() {
        Path root = cleanupPath(tablePath);
        try {
            if (fileIO.exists(root)) {
                reclaimFiles(root);
                deleteIfEmpty(root);
            }
        } catch (IOException | RuntimeException e) {
            throw new IllegalStateException(
                    "Template file cleanup is incomplete for "
                            + tablePath
                            + ". Resume the saved cleanup plan at the table path.",
                    e);
        }
    }

    private void deleteIfEmpty(Path directory) throws IOException {
        if (fileIO.exists(directory)
                && fileIO.listStatus(directory).length == 0
                && !fileIO.delete(directory, false)
                && fileIO.exists(directory)) {
            throw new IOException("Cannot remove empty cleanup directory " + directory);
        }
    }

    private void reclaimFiles(Path root) throws IOException {
        FormatReaderFactory reader =
                FileFormat.manifestFormat(new CoreOptions(new Options()))
                        .createReaderFactory(PATH_ROW, PATH_ROW, null);
        ThreadPoolExecutor executor =
                createCachedThreadPool(
                        Runtime.getRuntime().availableProcessors(), "TEMPLATE_DROP_FILES");
        boolean complete = true;
        try {
            for (FileStatus branch : fileIO.listDirectories(root)) {
                Path plan = branch.getPath();
                if (!plan.getName().startsWith(BRANCH_PREFIX)) {
                    continue;
                }
                for (FileStatus manifest : fileIO.listStatus(plan)) {
                    if (!manifest.isDir()
                            && manifest.getPath().getName().startsWith(MANIFEST_PREFIX)
                            && !reclaimManifest(reader, executor, manifest)) {
                        complete = false;
                    }
                }
                deleteIfEmpty(plan);
            }
        } finally {
            executor.shutdownNow();
        }
        if (!complete) {
            throw new IOException("Some template files could not be deleted.");
        }
    }

    private boolean reclaimManifest(
            FormatReaderFactory reader, ThreadPoolExecutor executor, FileStatus manifest)
            throws IOException {
        List<Path> files = new ArrayList<>();
        try {
            FileUtils.createFormatReader(fileIO, reader, manifest.getPath(), manifest.getLen())
                    .forEachRemaining(row -> files.add(new Path(row.getString(0).toString())));
        } catch (IOException | RuntimeException e) {
            if (AvroFileFormat.isIncompleteFile(e)) {
                LOG.warn(
                        "Skipping incomplete cleanup manifest {}. Its files may remain after DROP.",
                        manifest.getPath(),
                        e);
                return true;
            }
            throw new IOException("Cannot read cleanup manifest " + manifest.getPath(), e);
        }
        AtomicBoolean deleted = new AtomicBoolean(true);
        randomlyOnlyExecute(
                executor,
                path -> {
                    if (!deleteFile(path)) {
                        deleted.set(false);
                    }
                },
                files);
        return deleted.get() && deleteFile(manifest.getPath());
    }

    private boolean deleteFile(Path path) {
        try {
            if (!fileIO.exists(path) || fileIO.delete(path, false) || !fileIO.exists(path)) {
                return true;
            }
            LOG.warn("Cannot remove template file {}", path);
        } catch (FileNotFoundException | NoSuchFileException e) {
            return true;
        } catch (IOException e) {
            LOG.warn("Cannot remove template file {}", path, e);
        }
        return false;
    }

    private static Path cleanupPath(Path tablePath) {
        return new Path(tablePath, CLEANUP_DIRECTORY);
    }

    static RollingFileWriter<Path, Path> newManifestWriter(
            FileIO fileIO, Path plan, CoreOptions options) {
        FormatWriterFactory factory =
                FileFormat.manifestFormat(options).createWriterFactory(PATH_ROW);
        return new RollingFileWriterImpl<>(
                () ->
                        new SingleFileWriter<Path, Path>(
                                fileIO,
                                factory,
                                new Path(plan, MANIFEST_PREFIX + UUID.randomUUID() + ".avro"),
                                path -> GenericRow.of(BinaryString.fromString(path.toString())),
                                options.manifestCompression(),
                                false) {
                            @Override
                            public Path result() {
                                return path;
                            }
                        },
                options.manifestTargetSize().getBytes(),
                Long.MAX_VALUE);
    }

    // Include ADD and DELETE entries, even if a file is absent from the latest snapshot.
    private static void collectFiles(FileStoreTable table, RollingFileWriter<Path, Path> writer)
            throws IOException {
        ManifestList manifestList = table.store().manifestListFactory().create();
        Set<String> dataManifests = new LinkedHashSet<>();
        Set<String> indexManifests = new LinkedHashSet<>();
        Set<Snapshot> snapshots =
                new LinkedHashSet<>(table.snapshotManager().safelyGetAllSnapshots());
        snapshots.addAll(table.tagManager().taggedSnapshots());
        snapshots.addAll(table.changelogManager().safelyGetAllChangelogs());
        for (Snapshot snapshot : snapshots) {
            for (ManifestFileMeta meta : manifestList.readAllManifests(snapshot)) {
                dataManifests.add(meta.fileName());
            }
            if (snapshot.indexManifest() != null) {
                indexManifests.add(snapshot.indexManifest());
            }
        }

        ManifestFile manifestFile = table.store().manifestFileFactory().create();
        DataFilePathFactories factories = new DataFilePathFactories(table.store().pathFactory());
        for (String name : dataManifests) {
            for (ManifestEntry entry : manifestFile.readWithIOException(name)) {
                for (Path file :
                        entry.file()
                                .collectFiles(factories.get(entry.partition(), entry.bucket()))) {
                    writer.write(file);
                }
            }
        }

        IndexFileHandler indexes = table.store().newIndexFileHandler();
        for (String name : indexManifests) {
            if (indexes.existsManifest(name)) {
                for (IndexManifestEntry entry : indexes.readManifestWithIOException(name)) {
                    writer.write(indexes.filePath(entry));
                }
            }
        }
    }
}
