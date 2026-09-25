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

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.apache.paimon.manifest.ManifestEntry.recordCount;
import static org.apache.paimon.manifest.ManifestEntry.recordCountAdd;
import static org.apache.paimon.manifest.ManifestEntry.recordCountDelete;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RemoveUnexistingManifests}. */
public class RemoveUnexistingManifestsTest {

    @TempDir java.nio.file.Path tempDir;

    private Catalog catalog;
    private FileStoreTable table;

    @BeforeEach
    public void beforeEach() throws Exception {
        Path warehouse = new Path(TraceableFileIO.SCHEME + "://" + tempDir.toString());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(warehouse));
        Identifier identifier = new Identifier("default", "T");
        catalog.createDatabase(identifier.getDatabaseName(), true);
        Schema schema =
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("v", DataTypes.STRING())
                        .primaryKey("k")
                        .option("bucket", "1")
                        .option("manifest.target-file-size", "1 B")
                        .build();
        catalog.createTable(identifier, schema, true);
        table = (FileStoreTable) catalog.getTable(identifier);
    }

    @AfterEach
    public void afterEach() throws IOException {
        Predicate<Path> pathPredicate = path -> path.toString().contains(tempDir.toString());
        assertThat(TraceableFileIO.openInputStreams(pathPredicate)).isEmpty();
        assertThat(TraceableFileIO.openOutputStreams(pathPredicate)).isEmpty();
    }

    @Test
    public void testNoSnapshotIsNoOp() {
        assertThat(new RemoveUnexistingManifests(table).execute()).isFalse();
        assertThat(table.snapshotManager().latestSnapshot()).isNull();
    }

    @Test
    public void testExistingManifestsAreNoOp() throws Exception {
        commit(GenericRow.of(1, BinaryString.fromString("a")));
        long snapshotId = table.snapshotManager().latestSnapshot().id();

        assertThat(new RemoveUnexistingManifests(table).execute()).isFalse();
        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(snapshotId);
        assertThat(readCount()).isEqualTo(1);
    }

    @Test
    public void testTotalRecordCountMatchesVisibleRowsAfterDroppingDeletedAdd() throws Exception {
        commit(GenericRow.of(1, BinaryString.fromString("a")));
        commit(GenericRow.of(2, BinaryString.fromString("b")));
        compact();

        List<ManifestFileMeta> manifests = manifests();
        List<List<ManifestEntry>> entries = new ArrayList<>();
        for (ManifestFileMeta meta : manifests) {
            entries.add(table.store().newScan().readManifest(meta));
        }
        int dropIndex = indexOfAddDeletedElsewhere(entries);
        assertThat(dropIndex).isGreaterThanOrEqualTo(0);

        List<ManifestEntry> remaining = new ArrayList<>();
        for (int i = 0; i < entries.size(); i++) {
            if (i != dropIndex) {
                remaining.addAll(entries.get(i));
            }
        }
        long summedRowCount = recordCount(remaining);
        long addedMinusDeleted = recordCountAdd(remaining) - recordCountDelete(remaining);

        Path path =
                table.store().pathFactory().toManifestFilePath(manifests.get(dropIndex).fileName());
        assertThat(table.fileIO().delete(path, false)).isTrue();

        long snapshotId = table.snapshotManager().latestSnapshot().id();
        assertThat(new RemoveUnexistingManifests(table).execute()).isTrue();

        table = (FileStoreTable) catalog.getTable(new Identifier("default", "T"));
        Snapshot latest = table.snapshotManager().latestSnapshot();
        long visibleRows = readCount();
        assertThat(latest.id()).isEqualTo(snapshotId + 1);
        assertThat(latest.totalRecordCount()).isEqualTo(visibleRows);
        assertThat(summedRowCount).isNotEqualTo(visibleRows);
        assertThat(addedMinusDeleted).isNotEqualTo(visibleRows);
        assertThat(manifestNames()).doesNotContain(manifests.get(dropIndex).fileName());
    }

    private int indexOfAddDeletedElsewhere(List<List<ManifestEntry>> entries) {
        for (int i = 0; i < entries.size(); i++) {
            for (ManifestEntry entry : entries.get(i)) {
                if (entry.kind() != FileKind.ADD) {
                    continue;
                }
                for (int j = 0; j < entries.size(); j++) {
                    if (i == j) {
                        continue;
                    }
                    for (ManifestEntry other : entries.get(j)) {
                        if (other.kind() == FileKind.DELETE
                                && other.identifier().equals(entry.identifier())) {
                            return i;
                        }
                    }
                }
            }
        }
        return -1;
    }

    private List<ManifestFileMeta> manifests() {
        return table.store()
                .newScan()
                .manifestsReader()
                .read(table.snapshotManager().latestSnapshot(), ScanMode.ALL)
                .allManifests;
    }

    private List<String> manifestNames() {
        List<String> names = new ArrayList<>();
        for (ManifestFileMeta meta : manifests()) {
            names.add(meta.fileName());
        }
        return names;
    }

    private long readCount() throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder();
        try (RecordReader<?> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            long[] count = new long[1];
            reader.forEachRemaining(row -> count[0]++);
            return count[0];
        }
    }

    private void commit(GenericRow row) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.write(row);
            commit.commit(write.prepareCommit());
        }
    }

    private void compact() throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.compact(BinaryRow.EMPTY_ROW, 0, true);
            commit.commit(write.prepareCommit());
        }
    }
}
