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
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.FileStorePathFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;

/** Streams every payload file reachable from snapshots, tags and branches. */
public class FullHistoryPayloadFileVisitor {

    private final FileStoreTable table;

    public FullHistoryPayloadFileVisitor(FileStoreTable table) {
        this.table = table;
    }

    public void visit(Visitor visitor) throws IOException {
        List<String> branches = new ArrayList<>(table.branchManager().branches());
        branches.add(DEFAULT_MAIN_BRANCH);
        for (String branch : branches) {
            visitBranch(table.switchToBranch(branch), visitor);
        }
    }

    private void visitBranch(FileStoreTable branchTable, Visitor visitor) throws IOException {
        VisitContext context = new VisitContext(branchTable, visitor);
        List<Snapshot> snapshots = new ArrayList<>();
        snapshots.addAll(branchTable.snapshotManager().safelyGetAllSnapshots());
        snapshots.addAll(branchTable.tagManager().taggedSnapshots());
        snapshots.sort(Comparator.comparingLong(Snapshot::id));
        for (Snapshot snapshot : snapshots) {
            context.visitSnapshot(snapshot);
        }
        for (Changelog changelog : branchTable.changelogManager().safelyGetAllChangelogs()) {
            context.visitChangelog(changelog);
        }
    }

    private static class VisitContext {

        private final FileStoreTable table;
        private final Visitor visitor;
        private final FileStorePathFactory pathFactory;
        private final ManifestList manifestList;
        private final ManifestFile manifestFile;
        private final Set<String> visitedSnapshotData = new HashSet<>();
        private final Set<String> visitedChangelogLists = new HashSet<>();
        private final Set<String> visitedAppendDeltaLists = new HashSet<>();
        private final Set<String> visitedIndexManifests = new HashSet<>();
        private final Set<String> visitedLiveManifests = new HashSet<>();
        private final Set<String> visitedChangelogManifests = new HashSet<>();
        private final Set<String> visitedAppendDeltaManifests = new HashSet<>();

        private VisitContext(FileStoreTable table, Visitor visitor) {
            this.table = table;
            this.visitor = visitor;
            this.pathFactory = table.store().pathFactory();
            this.manifestList = table.store().manifestListFactory().create();
            this.manifestFile = table.store().manifestFileFactory().create();
        }

        private void visitSnapshot(Snapshot snapshot) throws IOException {
            String dataKey = snapshot.baseManifestList() + '\n' + snapshot.deltaManifestList();
            if (visitedSnapshotData.add(dataKey)) {
                visitLiveDataFiles(snapshot);
            }

            if (snapshot.changelogManifestList() != null) {
                visitChangelogManifest(snapshot.changelogManifestList());
            } else {
                visitAppendDeltaFiles(snapshot.deltaManifestList());
            }
            visitIndexManifest(snapshot.indexManifest());
        }

        private void visitChangelog(Changelog changelog) throws IOException {
            if (changelog.changelogManifestList() != null) {
                visitChangelogManifest(changelog.changelogManifestList());
            } else {
                visitAppendDeltaFiles(changelog.deltaManifestList());
            }
        }

        private void visitLiveDataFiles(Snapshot snapshot) throws IOException {
            List<ManifestFileMeta> manifests = new ArrayList<>();
            manifests.addAll(readManifestList(snapshot.baseManifestList()));
            manifests.addAll(readManifestList(snapshot.deltaManifestList()));

            Set<FileEntry.Identifier> deletedFiles = new HashSet<>();
            for (ManifestFileMeta meta : manifests) {
                if (meta.numDeletedFiles() == 0) {
                    continue;
                }
                for (ManifestEntry entry : readManifest(meta)) {
                    if (entry.kind() == FileKind.DELETE) {
                        deletedFiles.add(entry.identifier());
                    }
                }
            }

            for (ManifestFileMeta meta : manifests) {
                if (meta.numAddedFiles() == 0 || !visitedLiveManifests.add(meta.fileName())) {
                    continue;
                }
                for (ManifestEntry entry : readManifest(meta)) {
                    if (entry.kind() == FileKind.ADD
                            && !deletedFiles.contains(entry.identifier())) {
                        visitDataFile(entry);
                    }
                }
            }
        }

        private void visitChangelogManifest(String manifestListName) throws IOException {
            if (!visitedChangelogLists.add(manifestListName)) {
                return;
            }
            for (ManifestFileMeta meta : readManifestList(manifestListName)) {
                if (!visitedChangelogManifests.add(meta.fileName())) {
                    continue;
                }
                for (ManifestEntry entry : readManifest(meta)) {
                    if (entry.kind() == FileKind.ADD) {
                        visitDataFile(entry);
                    }
                }
            }
        }

        private void visitAppendDeltaFiles(String manifestListName) throws IOException {
            if (!visitedAppendDeltaLists.add(manifestListName)) {
                return;
            }
            for (ManifestFileMeta meta : readManifestList(manifestListName)) {
                if (!visitedAppendDeltaManifests.add(meta.fileName())) {
                    continue;
                }
                for (ManifestEntry entry : readManifest(meta)) {
                    if (entry.file().fileSource().orElse(FileSource.APPEND) == FileSource.APPEND) {
                        visitDataFile(entry);
                    }
                }
            }
        }

        private List<ManifestFileMeta> readManifestList(String fileName) throws IOException {
            return manifestList.readWithIOException(fileName);
        }

        private List<ManifestEntry> readManifest(ManifestFileMeta meta) throws IOException {
            return manifestFile.readWithIOException(meta.fileName(), meta.fileSize());
        }

        private void visitDataFile(ManifestEntry entry) throws IOException {
            DataFilePathFactory dataPathFactory =
                    pathFactory.createDataFilePathFactory(entry.partition(), entry.bucket());
            visitor.accept(
                    dataPathFactory.toPath(entry),
                    FullHistoryCopyPlan.FileKind.DATA,
                    entry.file().fileSize());
            for (String extraFile : entry.extraFiles()) {
                visitor.accept(
                        dataPathFactory.toAlignedPath(extraFile, entry),
                        FullHistoryCopyPlan.FileKind.DATA,
                        -1L);
            }
        }

        private void visitIndexManifest(String indexManifest) throws IOException {
            if (indexManifest == null || !visitedIndexManifests.add(indexManifest)) {
                return;
            }
            IndexFileHandler indexFileHandler = table.store().newIndexFileHandler();
            for (IndexManifestEntry entry :
                    indexFileHandler.readManifestWithIOException(indexManifest)) {
                if (entry.kind() == FileKind.ADD) {
                    visitor.accept(
                            indexFileHandler.filePath(entry),
                            FullHistoryCopyPlan.FileKind.INDEX,
                            entry.indexFile().fileSize());
                }
            }
        }
    }

    /** Receives one reachable payload path at a time. Duplicate paths are allowed. */
    @FunctionalInterface
    public interface Visitor {
        void accept(Path path, FullHistoryCopyPlan.FileKind kind, long expectedSize)
                throws IOException;
    }
}
