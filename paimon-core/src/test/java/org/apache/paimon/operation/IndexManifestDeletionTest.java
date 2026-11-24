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
import org.apache.paimon.Snapshot;
import org.apache.paimon.TestAppendFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.BinaryIndexManifestEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.IndexFilePathFactories;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.paimon.data.BinaryString.fromString;
import static org.apache.paimon.index.HashIndexFile.HASH_INDEX;
import static org.apache.paimon.index.IndexFileMetaSerializerTest.randomDeletionVectorIndexFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/** Tests for planning index deletion with projected, reusable manifest entries. */
class IndexManifestDeletionTest {

    @TempDir java.nio.file.Path tempDir;

    private TestAppendFileStore store;
    private String emptyManifestList;

    @BeforeEach
    void setUp() throws Exception {
        store = TestAppendFileStore.createAppendStore(tempDir, new HashMap<>());
        emptyManifestList =
                store.manifestListFactory().create().write(Collections.emptyList()).getLeft();
    }

    @ParameterizedTest
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void testPlansPathsAndKeepsProtectedIndexes(boolean inDataDirectory, boolean externalRoot)
            throws Exception {
        store.options()
                .toConfiguration()
                .set(CoreOptions.INDEX_FILE_IN_DATA_FILE_DIR, inDataDirectory);
        if (externalRoot) {
            store.options()
                    .toConfiguration()
                    .set(
                            CoreOptions.GLOBAL_INDEX_EXTERNAL_PATH,
                            new Path(new Path(tempDir.toUri()), "new-index-root").toString());
        }
        IndexManifestFile manifest = store.indexManifestFileFactory().create();
        IndexFileHandler handler = store.newIndexFileHandler();
        SnapshotDeletion deletion = store.newSnapshotDeletion();
        List<IndexManifestEntry> entries =
                new ArrayList<>(
                        Arrays.asList(
                                hashEntry(0),
                                hashEntry(1),
                                hashEntry(2),
                                entry(3, randomDeletionVectorIndexFile()),
                                entry(
                                        4,
                                        new IndexFileMeta(
                                                "btree",
                                                "external-index",
                                                100,
                                                10,
                                                new GlobalIndexMeta(0, 9, 1, null, new byte[1024]),
                                                new Path(
                                                                new Path(tempDir.toUri()),
                                                                "old-index-root/external-index")
                                                        .toString()))));
        String protectedManifest =
                manifest.writeWithoutRolling(Collections.singletonList(entries.get(1)));
        String expiredManifest = manifest.writeWithoutRolling(entries);
        String duplicateManifest = manifest.writeWithoutRolling(entries);
        for (IndexManifestEntry entry : entries) {
            store.fileIO().tryToWriteAtomic(handler.filePath(entry), "index");
        }
        Path localPath = handler.filePath(entries.get(0));
        if (externalRoot && !inDataDirectory) {
            assertThat(localPath.getParent()).isEqualTo(store.pathFactory().indexPath());
        }

        Snapshot protectedSnapshot = snapshot(protectedManifest);
        Set<String> skippingSet =
                deletion.manifestSkippingSet(Collections.singletonList(protectedSnapshot));
        assertThat(skippingSet).contains(protectedManifest, entries.get(1).indexFile().fileName());
        assertThat(
                        store.newChangelogDeletion()
                                .manifestSkippingSet(Collections.singletonList(protectedSnapshot)))
                .contains(protectedManifest, entries.get(1).indexFile().fileName());
        List<Runnable> tasks =
                deletion.planManifestsCleaner(snapshot(expiredManifest), skippingSet, false, false);
        assertThat(tasks).hasSize(entries.size());
        tasks.addAll(
                deletion.planManifestsCleaner(
                        snapshot(duplicateManifest), skippingSet, false, false));
        assertThat(tasks).hasSize(entries.size() + 1);
        assertThat(
                        deletion.planManifestsCleaner(
                                snapshot(expiredManifest), skippingSet, false, false))
                .isEmpty();
        for (IndexManifestEntry entry : entries) {
            assertThat(store.fileIO().exists(handler.filePath(entry))).isTrue();
        }
        assertThat(
                        TraceableFileIO.openInputStreams(
                                path -> path.toString().contains(tempDir.toString())))
                .isEmpty();

        deletion.executeAll(tasks);

        for (int i = 0; i < entries.size(); i++) {
            assertThat(store.fileIO().exists(handler.filePath(entries.get(i))))
                    .as("index %s", i)
                    .isEqualTo(i == 1);
        }
        assertThat(handler.existsManifest(protectedManifest)).isTrue();
        assertThat(handler.existsManifest(expiredManifest)).isFalse();
        assertThat(handler.existsManifest(duplicateManifest)).isFalse();
    }

    @Test
    void testMissingManifestIsIgnoredOnlyForDeletion() {
        SnapshotDeletion deletion = store.newSnapshotDeletion();
        assertThat(
                        deletion.planManifestsCleaner(
                                snapshot("missing"), new HashSet<>(), false, false))
                .isEmpty();
        assertThatThrownBy(
                        () ->
                                deletion.manifestSkippingSet(
                                        Collections.singletonList(snapshot("missing"))))
                .hasRootCauseInstanceOf(FileNotFoundException.class);
    }

    @ParameterizedTest
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void testScanFailureDoesNotExecutePartialPlan(boolean failOnClose, boolean buildingSkippingSet)
            throws Exception {
        IndexManifestFile manifest = spy(store.indexManifestFileFactory().create());
        IndexFileHandler handler = spy(newHandler(manifest));
        SnapshotDeletion deletion = newDeletion(handler);
        IndexManifestEntry entry = hashEntry(0);
        String fileName = manifest.writeWithoutRolling(Collections.singletonList(entry));
        Path indexPath = handler.filePath(entry);
        Path manifestPath = manifest.indexManifestFilePath(fileName);
        store.fileIO().tryToWriteAtomic(indexPath, "index");
        BinaryIndexManifestEntry.Projection projection =
                buildingSkippingSet
                        ? BinaryIndexManifestEntry.FILE_NAME_PROJECTION
                        : BinaryIndexManifestEntry.FILE_PATH_PROJECTION;
        CloseableIterator<BinaryIndexManifestEntry> rows = manifest.scan(fileName, projection);
        FileNotFoundException failure = new FileNotFoundException("Failure during scanning");
        boolean[] pathPlanned = {false};
        Set<String> skippingSet = new HashSet<>();
        doAnswer(
                        invocation -> {
                            pathPlanned[0] = true;
                            return invocation.callRealMethod();
                        })
                .when(handler)
                .filePath(any(BinaryIndexManifestEntry.class));
        CloseableIterator<BinaryIndexManifestEntry> failing =
                new CloseableIterator<BinaryIndexManifestEntry>() {
                    private boolean consumed;

                    @Override
                    public boolean hasNext() {
                        if (consumed) {
                            assertThat(
                                            buildingSkippingSet
                                                    ? skippingSet.contains(
                                                            entry.indexFile().fileName())
                                                    : pathPlanned[0])
                                    .isTrue();
                            if (!failOnClose) {
                                throw new UncheckedIOException(failure);
                            }
                        }
                        return rows.hasNext();
                    }

                    @Override
                    public BinaryIndexManifestEntry next() {
                        consumed = true;
                        return rows.next();
                    }

                    @Override
                    public void close() throws Exception {
                        rows.close();
                        if (failOnClose) {
                            throw failure;
                        }
                    }
                };
        doReturn(failing).when(manifest).scan(fileName, projection);

        assertThatThrownBy(
                        () -> {
                            if (buildingSkippingSet) {
                                deletion.addIndexFilesToSkippingSet(
                                        snapshot(fileName), skippingSet);
                            } else {
                                deletion.cleanUnusedManifests(snapshot(fileName), skippingSet);
                            }
                        })
                .hasRootCause(failure);
        assertThat(store.fileIO().exists(indexPath)).isTrue();
        assertThat(store.fileIO().exists(manifestPath)).isTrue();
        assertThat(TraceableFileIO.openInputStreams(manifestPath::equals)).isEmpty();

        doCallRealMethod().when(manifest).scan(fileName, projection);
        deletion.cleanUnusedManifests(snapshot(fileName), new HashSet<>());
        assertThat(store.fileIO().exists(indexPath)).isFalse();
        assertThat(store.fileIO().exists(manifestPath)).isFalse();
    }

    @Test
    void testOpeningFailureIsNotIgnored() {
        IndexManifestFile manifest = mock(IndexManifestFile.class);
        IOException failure = new IOException("Unable to open manifest");
        doThrow(new UncheckedIOException(failure))
                .when(manifest)
                .scan("unavailable", BinaryIndexManifestEntry.FILE_PATH_PROJECTION);

        assertThatThrownBy(
                        () ->
                                newDeletion(newHandler(manifest))
                                        .planManifestsCleaner(
                                                snapshot("unavailable"),
                                                new HashSet<>(),
                                                false,
                                                false))
                .hasCause(failure);
    }

    private IndexFileHandler newHandler(IndexManifestFile manifest) {
        return new IndexFileHandler(
                store.fileIO(),
                store.snapshotManager(),
                manifest,
                new IndexFilePathFactories(store.pathFactory()),
                MemorySize.ofMebiBytes(2),
                false);
    }

    private SnapshotDeletion newDeletion(IndexFileHandler handler) {
        return new SnapshotDeletion(
                store.fileIO(),
                store.pathFactory(),
                store.manifestFileFactory().create(),
                store.manifestListFactory().create(),
                handler,
                store.newStatsFileHandler(),
                false,
                false,
                2,
                null);
    }

    private Snapshot snapshot(String indexManifest) {
        Snapshot snapshot = mock(Snapshot.class);
        when(snapshot.baseManifestList()).thenReturn(emptyManifestList);
        when(snapshot.baseManifestListSize()).thenReturn(null);
        when(snapshot.deltaManifestList()).thenReturn(emptyManifestList);
        when(snapshot.deltaManifestListSize()).thenReturn(null);
        when(snapshot.indexManifest()).thenReturn(indexManifest);
        return snapshot;
    }

    private IndexManifestEntry hashEntry(int partition) {
        return entry(
                partition,
                new IndexFileMeta(HASH_INDEX, "hash-index-" + partition, 16, 4, null, null, null));
    }

    private IndexManifestEntry entry(int partition, IndexFileMeta indexFile) {
        BinaryRow row = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeString(0, fromString("partition-" + partition));
        writer.writeString(1, fromString("0"));
        writer.complete();
        return new IndexManifestEntry(FileKind.ADD, row, partition % 2, indexFile);
    }
}
