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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashSet;
import java.util.Set;

/** Collects exact file paths for tables with shared hash prefixes. */
public final class EntropyInjectTemplateFilesDeletion {

    private EntropyInjectTemplateFilesDeletion() {}

    /** Reads one branch's manifests into the drop plan before metadata is removed. */
    public static void collect(
            FileIO fileIO,
            Path tablePath,
            String branch,
            TableSchema schema,
            TemplateFileDeletionPlan plan) {
        try {
            forEachFile(branchTable(fileIO, tablePath, branch, schema), plan::add);
        } catch (IOException e) {
            plan.discard();
            throw new UncheckedIOException("Cannot collect template files for " + tablePath, e);
        } catch (RuntimeException e) {
            plan.discard();
            throw e;
        }
    }

    public static boolean isTemplate(TableSchema schema) {
        String strategy = schema.options().get(CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key());
        return ExternalPathStrategy.ENTROPY_INJECT_TEMPLATE.toString().equalsIgnoreCase(strategy);
    }

    private static FileStoreTable branchTable(
            FileIO fileIO, Path tablePath, String branch, TableSchema schema) {
        return FileStoreTableFactory.createWithoutFallbackBranch(
                fileIO,
                tablePath,
                schema,
                new Options().set(CoreOptions.BRANCH, branch),
                CatalogEnvironment.empty());
    }

    // Include ADD and DELETE entries, even if a file is absent from the latest snapshot.
    private static void forEachFile(FileStoreTable table, FileConsumer consumer)
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
                    consumer.accept(file);
                }
            }
        }

        IndexFileHandler indexes = table.store().newIndexFileHandler();
        for (String name : indexManifests) {
            if (indexes.existsManifest(name)) {
                for (IndexManifestEntry entry : indexes.readManifestWithIOException(name)) {
                    consumer.accept(indexes.filePath(entry));
                }
            }
        }
    }

    @FunctionalInterface
    private interface FileConsumer {
        void accept(Path path) throws IOException;
    }
}
