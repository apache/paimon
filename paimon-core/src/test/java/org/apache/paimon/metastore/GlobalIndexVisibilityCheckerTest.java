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

package org.apache.paimon.metastore;

import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.globalindex.CentroidIndexMeta;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatchers;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Visibility checks for global index generations. */
class GlobalIndexVisibilityCheckerTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void addedCentroidBitmapFieldsPreserveCoverage(boolean centroid) {
        FileStoreTable table = mock(FileStoreTable.class);
        FileStore<?> store = mock(FileStore.class);
        IndexFileHandler files = mock(IndexFileHandler.class);
        doReturn(store).when(table).store();
        when(store.newIndexFileHandler()).thenReturn(files);

        Map<Snapshot, List<IndexManifestEntry>> entries = new HashMap<>();
        when(files.scan(any(Snapshot.class), ArgumentMatchers.<Filter<IndexManifestEntry>>any()))
                .thenAnswer(
                        invocation -> {
                            Filter<IndexManifestEntry> filter = invocation.getArgument(1);
                            return entries.get(invocation.getArgument(0)).stream()
                                    .filter(filter::test)
                                    .collect(Collectors.toList());
                        });
        Snapshot beforeCoverage = snapshot(entries, index(0, 9, centroid, 2));
        GlobalIndexVisibilityChecker checker =
                GlobalIndexVisibilityChecker.create(
                        table, beforeCoverage, Collections.singletonList(appendedRows()));
        assertThat(checker.noNeedToWait()).isFalse();
        assertThat(checker.visibleIn(beforeCoverage)).isFalse();

        assertThat(checker.visibleIn(snapshot(entries, index(0, 20, centroid, 2)))).isTrue();
        assertThat(checker.visibleIn(snapshot(entries, index(0, 20, centroid, 2, 3))))
                .isEqualTo(centroid);
        assertThat(checker.visibleIn(snapshot(entries, index(0, 20, centroid, 3)))).isFalse();
        assertThat(checker.visibleIn(snapshot(entries, index(0, 19, centroid, 2, 3)))).isFalse();
    }

    private static Snapshot snapshot(
            Map<Snapshot, List<IndexManifestEntry>> entries, IndexManifestEntry index) {
        Snapshot snapshot = mock(Snapshot.class);
        entries.put(snapshot, Collections.singletonList(index));
        return snapshot;
    }

    private static IndexManifestEntry index(
            long start, long end, boolean centroid, int... extraFields) {
        byte[] meta =
                centroid
                        ? CentroidIndexMeta.router(new ResultEntry("index", 0, null), 0).meta()
                        : null;
        return new IndexManifestEntry(
                FileKind.ADD,
                BinaryRow.EMPTY_ROW,
                0,
                new IndexFileMeta(
                        centroid ? "ivf-flat" : "btree",
                        "index",
                        0,
                        0,
                        new GlobalIndexMeta(start, end, 1, extraFields, meta),
                        null));
    }

    private static ManifestEntry appendedRows() {
        DataFileMeta file = mock(DataFileMeta.class);
        when(file.firstRowId()).thenReturn(10L);
        when(file.rowCount()).thenReturn(11L);
        when(file.fileSource()).thenReturn(Optional.of(FileSource.APPEND));
        when(file.fileName()).thenReturn("data.parquet");
        when(file.nonNullRowIdRange()).thenReturn(new Range(10, 20));
        ManifestEntry entry = mock(ManifestEntry.class);
        when(entry.file()).thenReturn(file);
        when(entry.kind()).thenReturn(FileKind.ADD);
        when(entry.partition()).thenReturn(BinaryRow.EMPTY_ROW);
        return entry;
    }
}
