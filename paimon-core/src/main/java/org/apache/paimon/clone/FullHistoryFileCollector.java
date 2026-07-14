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
import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;

/** Collects all files reachable from snapshots, tags and branches of a Paimon table. */
public class FullHistoryFileCollector {

    private final FileStoreTable table;

    public FullHistoryFileCollector(FileStoreTable table) {
        this.table = table;
    }

    public FullHistoryFileSet collect() throws IOException {
        FullHistoryFileSet.Builder builder = FullHistoryFileSet.builder();
        for (String branch : branches()) {
            collectBranch(table.switchToBranch(branch), builder);
        }
        new FullHistoryPayloadFileVisitor(table)
                .visit(
                        (path, kind, size) -> {
                            if (kind == FullHistoryCopyPlan.FileKind.DATA) {
                                builder.addDataFile(path);
                            } else if (kind == FullHistoryCopyPlan.FileKind.INDEX) {
                                builder.addIndexFile(path);
                            }
                        });
        return builder.build();
    }

    private List<String> branches() {
        List<String> branches = new ArrayList<>(table.branchManager().branches());
        branches.add(DEFAULT_MAIN_BRANCH);
        return branches;
    }

    private void collectBranch(FileStoreTable branchTable, FullHistoryFileSet.Builder builder)
            throws IOException {
        MetadataContext context = new MetadataContext(branchTable, builder);
        context.collectSchemaFiles();
        context.collectSnapshots();
        context.collectTags();
        context.collectChangelogs();
    }

    private static class MetadataContext {

        private final FileStoreTable table;
        private final FullHistoryFileSet.Builder builder;
        private final FileStorePathFactory pathFactory;
        private final ManifestList manifestList;
        private final Set<String> collectedManifestLists = new HashSet<>();
        private final Set<String> collectedIndexManifests = new HashSet<>();
        private final Set<String> collectedStatistics = new HashSet<>();

        private MetadataContext(FileStoreTable table, FullHistoryFileSet.Builder builder) {
            this.table = table;
            this.builder = builder;
            this.pathFactory = table.store().pathFactory();
            this.manifestList = table.store().manifestListFactory().create();
        }

        private void collectSchemaFiles() throws IOException {
            for (Path schemaPath : table.schemaManager().schemaPaths(id -> true)) {
                builder.addMetadataFile(schemaPath);
            }
        }

        private void collectSnapshots() throws IOException {
            SnapshotManager manager = table.snapshotManager();
            for (Snapshot snapshot : manager.safelyGetAllSnapshots()) {
                builder.addMetadataFile(manager.snapshotPath(snapshot.id()));
                collectSnapshotMetadata(snapshot);
            }
        }

        private void collectTags() throws IOException {
            TagManager manager = table.tagManager();
            for (Path tagPath : manager.tagPaths(path -> true)) {
                builder.addMetadataFile(tagPath);
            }
            for (Snapshot snapshot : manager.taggedSnapshots()) {
                collectSnapshotMetadata(snapshot);
            }
        }

        private void collectChangelogs() throws IOException {
            ChangelogManager manager = table.changelogManager();
            for (Changelog changelog : manager.safelyGetAllChangelogs()) {
                builder.addMetadataFile(manager.longLivedChangelogPath(changelog.id()));
                if (changelog.changelogManifestList() != null) {
                    collectManifestList(changelog.changelogManifestList());
                } else {
                    collectManifestList(changelog.baseManifestList());
                    collectManifestList(changelog.deltaManifestList());
                }
            }
        }

        private void collectSnapshotMetadata(Snapshot snapshot) throws IOException {
            collectManifestList(snapshot.baseManifestList());
            collectManifestList(snapshot.deltaManifestList());
            collectManifestList(snapshot.changelogManifestList());
            collectIndexManifest(snapshot.indexManifest());
            collectStatistics(snapshot.statistics());
        }

        private void collectManifestList(String fileName) throws IOException {
            if (fileName == null || !collectedManifestLists.add(fileName)) {
                return;
            }
            builder.addMetadataFile(pathFactory.manifestListFactory().toPath(fileName));
            for (ManifestFileMeta meta : manifestList.readWithIOException(fileName)) {
                builder.addMetadataFile(pathFactory.manifestFileFactory().toPath(meta.fileName()));
            }
        }

        private void collectIndexManifest(String fileName) throws IOException {
            if (fileName == null || !collectedIndexManifests.add(fileName)) {
                return;
            }
            IndexFileHandler handler = table.store().newIndexFileHandler();
            if (handler.existsManifest(fileName)) {
                builder.addMetadataFile(handler.indexManifestFilePath(fileName));
            }
        }

        private void collectStatistics(String fileName) {
            if (fileName != null && collectedStatistics.add(fileName)) {
                builder.addMetadataFile(pathFactory.statsFileFactory().toPath(fileName));
            }
        }
    }
}
