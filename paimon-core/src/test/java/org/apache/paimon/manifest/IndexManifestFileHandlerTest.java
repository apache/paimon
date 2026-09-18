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

package org.apache.paimon.manifest;

import org.apache.paimon.TestAppendFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.index.DataEvolutionIndexSourceMeta;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.SegmentsCache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.apache.paimon.index.IndexFileMetaSerializerTest.randomDeletionVectorIndexFile;
import static org.apache.paimon.utils.FileUtils.createFormatReader;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for IndexManifestFileHandler. */
public class IndexManifestFileHandlerTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testUnawareMode() throws Exception {
        TestAppendFileStore fileStore =
                TestAppendFileStore.createAppendStore(tempDir, new HashMap<>());

        IndexManifestFile indexManifestFile =
                new IndexManifestFile.Factory(
                                fileStore.fileIO(),
                                FileFormat.manifestFormat(fileStore.options()),
                                "zstd",
                                fileStore.pathFactory(),
                                null)
                        .create();
        IndexManifestFileHandler indexManifestFileHandler =
                new IndexManifestFileHandler(indexManifestFile, BucketMode.BUCKET_UNAWARE);

        IndexManifestEntry entry1 =
                new IndexManifestEntry(
                        FileKind.ADD, BinaryRow.EMPTY_ROW, 0, randomDeletionVectorIndexFile());
        String indexManifestFile1 = indexManifestFileHandler.write(null, Arrays.asList(entry1));

        IndexManifestEntry entry2 = entry1.toDeleteEntry();
        IndexManifestEntry entry3 =
                new IndexManifestEntry(
                        FileKind.ADD, BinaryRow.EMPTY_ROW, 0, randomDeletionVectorIndexFile());
        String indexManifestFile2 =
                indexManifestFileHandler.write(indexManifestFile1, Arrays.asList(entry2, entry3));

        List<IndexManifestEntry> entries = indexManifestFile.read(indexManifestFile2);
        assertThat(entries.size()).isEqualTo(1);
        assertThat(entries.contains(entry1)).isFalse();
        assertThat(entries.contains(entry2)).isFalse();
        assertThat(entries.contains(entry3)).isTrue();
    }

    @Test
    public void testHashFixedBucket() throws Exception {
        TestAppendFileStore fileStore =
                TestAppendFileStore.createAppendStore(tempDir, new HashMap<>());

        IndexManifestFile indexManifestFile =
                new IndexManifestFile.Factory(
                                fileStore.fileIO(),
                                FileFormat.manifestFormat(fileStore.options()),
                                "zstd",
                                fileStore.pathFactory(),
                                null)
                        .create();
        IndexManifestFileHandler indexManifestFileHandler =
                new IndexManifestFileHandler(indexManifestFile, BucketMode.HASH_FIXED);

        IndexManifestEntry entry1 =
                new IndexManifestEntry(
                        FileKind.ADD, BinaryRow.EMPTY_ROW, 0, randomDeletionVectorIndexFile());
        IndexManifestEntry entry2 =
                new IndexManifestEntry(
                        FileKind.ADD, BinaryRow.EMPTY_ROW, 1, randomDeletionVectorIndexFile());
        String indexManifestFile1 =
                indexManifestFileHandler.write(null, Arrays.asList(entry1, entry2));

        IndexManifestEntry entry3 =
                new IndexManifestEntry(
                        FileKind.ADD, BinaryRow.EMPTY_ROW, 1, randomDeletionVectorIndexFile());
        IndexManifestEntry entry4 =
                new IndexManifestEntry(
                        FileKind.ADD, BinaryRow.EMPTY_ROW, 2, randomDeletionVectorIndexFile());
        String indexManifestFile2 =
                indexManifestFileHandler.write(indexManifestFile1, Arrays.asList(entry3, entry4));

        List<IndexManifestEntry> entries = indexManifestFile.read(indexManifestFile2);
        assertThat(entries.size()).isEqualTo(3);
        assertThat(entries.contains(entry1)).isTrue();
        assertThat(entries.contains(entry2)).isFalse();
        assertThat(entries.contains(entry3)).isTrue();
        assertThat(entries.contains(entry4)).isTrue();
    }

    @Test
    public void testNewIndexManifestReadableWithLegacySchema() throws Exception {
        TestAppendFileStore fileStore =
                TestAppendFileStore.createAppendStore(tempDir, new HashMap<>());
        FileFormat fileFormat = FileFormat.manifestFormat(fileStore.options());
        IndexManifestFile indexManifestFile =
                new IndexManifestFile.Factory(
                                fileStore.fileIO(),
                                fileFormat,
                                "zstd",
                                fileStore.pathFactory(),
                                null)
                        .create();
        IndexManifestFileHandler handler =
                new IndexManifestFileHandler(indexManifestFile, BucketMode.HASH_FIXED);

        String manifestFile = handler.write(null, Arrays.asList(pkVectorEntry("btree", "index")));

        RowType legacyGlobalIndexSchema =
                GlobalIndexMeta.SCHEMA.copy(GlobalIndexMeta.SCHEMA.getFields().subList(0, 5));
        List<DataField> legacyEntryFields = new ArrayList<>(IndexManifestEntry.SCHEMA.getFields());
        legacyEntryFields.set(9, legacyEntryFields.get(9).newType(legacyGlobalIndexSchema));
        RowType legacySchema =
                ManifestSchemaUtils.withFormatIdentifier(new RowType(false, legacyEntryFields));
        FormatReaderFactory legacyReaderFactory =
                fileFormat.createReaderFactory(legacySchema, legacySchema, new ArrayList<>());
        Path path = fileStore.pathFactory().indexManifestFileFactory().toPath(manifestFile);

        try (CloseableIterator<InternalRow> iterator =
                createFormatReader(fileStore.fileIO(), legacyReaderFactory, path, null)
                        .toCloseableIterator()) {
            InternalRow row = iterator.next();
            assertThat(row.getInt(0)).isEqualTo(1);
            InternalRow globalIndex = row.getRow(10, 5);
            assertThat(globalIndex.getLong(0)).isEqualTo(0);
            assertThat(globalIndex.getLong(1)).isEqualTo(1);
            assertThat(globalIndex.getInt(2)).isEqualTo(1);
            assertThat(globalIndex.isNullAt(4)).isTrue();
            assertThat(iterator.hasNext()).isFalse();
        }
    }

    @Test
    public void testLegacyIndexManifestRewriteFallsBackToResolvedRows() throws Exception {
        TestAppendFileStore fileStore =
                TestAppendFileStore.createAppendStore(tempDir, new HashMap<>());
        FileFormat fileFormat = FileFormat.manifestFormat(fileStore.options());
        IndexManifestFile indexManifestFile =
                new IndexManifestFile.Factory(
                                fileStore.fileIO(),
                                fileFormat,
                                "zstd",
                                fileStore.pathFactory(),
                                null)
                        .create();
        IndexManifestFileHandler handler =
                new IndexManifestFileHandler(indexManifestFile, BucketMode.BUCKET_UNAWARE);

        IndexManifestEntry previous = globalIndexEntry("previous", 0, 99, 1);
        String legacyManifest = writeLegacyManifest(fileStore, fileFormat, previous);
        IndexManifestEntry added = globalIndexEntry("added", 100, 199, 1);

        String rewritten = handler.write(legacyManifest, Arrays.asList(added));

        assertThat(indexManifestFile.read(rewritten)).containsExactlyInAnyOrder(previous, added);
        try (IndexManifestAvroReader reader = indexManifestFile.scanAvroBlocks(rewritten)) {
            assertThat(reader.rawBlockCopySupported()).isTrue();
        }
    }

    @Test
    public void testWarmCachedManifestUsesMaterializedRewrite() throws Exception {
        TestAppendFileStore fileStore =
                TestAppendFileStore.createAppendStore(tempDir, new HashMap<>());
        SegmentsCache<Path> cache =
                new SegmentsCache<>(1024, MemorySize.ofMebiBytes(1), Long.MAX_VALUE, null, false);
        IndexManifestFile indexManifestFile =
                new IndexManifestFile.Factory(
                                fileStore.fileIO(),
                                FileFormat.manifestFormat(fileStore.options()),
                                "zstd",
                                fileStore.pathFactory(),
                                cache)
                        .create();
        IndexManifestFileHandler handler =
                new IndexManifestFileHandler(indexManifestFile, BucketMode.BUCKET_UNAWARE);
        IndexManifestEntry previous = globalIndexEntry("previous", 0, 99, 1);
        String manifest = handler.write(null, Arrays.asList(previous));

        assertThat(indexManifestFile.isCached(manifest)).isFalse();
        assertThat(indexManifestFile.read(manifest)).containsExactly(previous);
        assertThat(indexManifestFile.isCached(manifest)).isTrue();

        IndexManifestEntry added = globalIndexEntry("added", 100, 199, 1);
        String rewritten = handler.write(manifest, Arrays.asList(added));
        assertThat(indexManifestFile.read(rewritten)).containsExactlyInAnyOrder(previous, added);
    }

    @Test
    public void testGlobalIndexOverlappingRangeRejectedWhenPreviousFileKept() throws Exception {
        TestAppendFileStore fileStore =
                TestAppendFileStore.createAppendStore(tempDir, new HashMap<>());

        IndexManifestFile indexManifestFile = createIndexManifestFile(fileStore);
        IndexManifestFileHandler indexManifestFileHandler =
                new IndexManifestFileHandler(indexManifestFile, BucketMode.BUCKET_UNAWARE);

        IndexManifestEntry previous = globalIndexEntry("prev-index", 0, 99, 1);
        String manifestFileName = indexManifestFileHandler.write(null, Arrays.asList(previous));

        IndexManifestEntry added = globalIndexEntry("new-index", 50, 149, 1);
        assertThatThrownBy(
                        () ->
                                indexManifestFileHandler.write(
                                        manifestFileName, Arrays.asList(added)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("prev-index")
                .hasMessageContaining("new-index")
                .hasMessageContaining("overlapping row range");
    }

    @Test
    public void testFailedRewriteCleansNewIndexManifest() throws Exception {
        TestAppendFileStore fileStore =
                TestAppendFileStore.createAppendStore(tempDir, new HashMap<>());
        IndexManifestFile indexManifestFile = createIndexManifestFile(fileStore);
        IndexManifestFileHandler handler =
                new IndexManifestFileHandler(indexManifestFile, BucketMode.BUCKET_UNAWARE);

        IndexManifestEntry previous = globalIndexEntry("previous", 0, 99, 1);
        String manifest = handler.write(null, Arrays.asList(previous));
        int filesBefore =
                fileStore.fileIO().listStatus(fileStore.pathFactory().manifestPath()).length;

        assertThatThrownBy(
                        () ->
                                handler.write(
                                        manifest,
                                        Arrays.asList(globalIndexEntry("overlap", 50, 149, 1))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("overlapping row range");
        assertThat(fileStore.fileIO().listStatus(fileStore.pathFactory().manifestPath()))
                .hasSize(filesBefore);
    }

    @Test
    public void testGlobalIndexOverlappingRangeAllowedAfterDelete() throws Exception {
        TestAppendFileStore fileStore =
                TestAppendFileStore.createAppendStore(tempDir, new HashMap<>());

        IndexManifestFile indexManifestFile = createIndexManifestFile(fileStore);
        IndexManifestFileHandler indexManifestFileHandler =
                new IndexManifestFileHandler(indexManifestFile, BucketMode.BUCKET_UNAWARE);

        IndexManifestEntry previous = globalIndexEntry("prev-index", 0, 99, 1);
        String manifestFileName = indexManifestFileHandler.write(null, Arrays.asList(previous));

        IndexManifestEntry added = globalIndexEntry("new-index", 50, 149, 1);
        String newManifestFileName =
                indexManifestFileHandler.write(
                        manifestFileName, Arrays.asList(previous.toDeleteEntry(), added));

        List<IndexManifestEntry> entries = indexManifestFile.read(newManifestFileName);
        assertThat(entries).containsExactly(added);
    }

    @Test
    public void testMissingGlobalIndexDeleteRejected() throws Exception {
        TestAppendFileStore fileStore =
                TestAppendFileStore.createAppendStore(tempDir, new HashMap<>());
        IndexManifestFile indexManifestFile = createIndexManifestFile(fileStore);

        IndexManifestEntry previous = globalIndexEntry("prev-index", 0, 99, 1);
        String manifest =
                indexManifestFile.writeIndexFiles(
                        null, Arrays.asList(previous), BucketMode.BUCKET_UNAWARE);
        IndexManifestEntry missing = globalIndexEntry("missing-index", 100, 199, 1);

        assertThatThrownBy(
                        () ->
                                indexManifestFile.writeIndexFiles(
                                        manifest,
                                        Arrays.asList(missing.toDeleteEntry()),
                                        BucketMode.BUCKET_UNAWARE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Trying to delete global index file missing-index which does not exist.");
    }

    @Test
    public void testDataEvolutionSourceMetaDoesNotDisableRangeValidation() throws Exception {
        TestAppendFileStore fileStore =
                TestAppendFileStore.createAppendStore(tempDir, new HashMap<>());
        IndexManifestFile indexManifestFile = createIndexManifestFile(fileStore);
        IndexManifestFileHandler handler =
                new IndexManifestFileHandler(indexManifestFile, BucketMode.BUCKET_UNAWARE);

        IndexManifestEntry previous = dataEvolutionIndexEntry("old-index", 0, 99, 1, 1);
        String manifest = handler.write(null, Arrays.asList(previous));
        IndexManifestEntry replacement = dataEvolutionIndexEntry("new-index", 0, 99, 1, 2);

        assertThatThrownBy(() -> handler.write(manifest, Arrays.asList(replacement)))
                .hasMessageContaining("overlapping row range");

        String replaced =
                handler.write(manifest, Arrays.asList(previous.toDeleteEntry(), replacement));
        assertThat(indexManifestFile.read(replaced)).containsExactly(replacement);
    }

    @Test
    public void testGlobalIndexOverlappingRangeAllowedForDifferentFieldId() throws Exception {
        TestAppendFileStore fileStore =
                TestAppendFileStore.createAppendStore(tempDir, new HashMap<>());

        IndexManifestFile indexManifestFile = createIndexManifestFile(fileStore);
        IndexManifestFileHandler indexManifestFileHandler =
                new IndexManifestFileHandler(indexManifestFile, BucketMode.BUCKET_UNAWARE);

        IndexManifestEntry previous = globalIndexEntry("prev-index", 0, 99, 1);
        String manifestFileName = indexManifestFileHandler.write(null, Arrays.asList(previous));

        IndexManifestEntry added = globalIndexEntry("new-index", 50, 149, 2);
        String newManifestFileName =
                indexManifestFileHandler.write(manifestFileName, Arrays.asList(added));

        List<IndexManifestEntry> entries = indexManifestFile.read(newManifestFileName);
        assertThat(entries).containsExactlyInAnyOrder(previous, added);
    }

    @Test
    public void testGlobalIndexNonOverlappingRangeAllowedForSameFieldId() throws Exception {
        TestAppendFileStore fileStore =
                TestAppendFileStore.createAppendStore(tempDir, new HashMap<>());

        IndexManifestFile indexManifestFile = createIndexManifestFile(fileStore);
        IndexManifestFileHandler indexManifestFileHandler =
                new IndexManifestFileHandler(indexManifestFile, BucketMode.BUCKET_UNAWARE);

        IndexManifestEntry previous = globalIndexEntry("prev-index", 0, 99, 1);
        String manifestFileName = indexManifestFileHandler.write(null, Arrays.asList(previous));

        IndexManifestEntry added = globalIndexEntry("new-index", 100, 199, 1);
        String newManifestFileName =
                indexManifestFileHandler.write(manifestFileName, Arrays.asList(added));

        List<IndexManifestEntry> entries = indexManifestFile.read(newManifestFileName);
        assertThat(entries).containsExactlyInAnyOrder(previous, added);
    }

    @Test
    public void testPrimaryKeyVectorSegmentsAllowFileLocalOverlappingRanges() throws Exception {
        TestAppendFileStore fileStore =
                TestAppendFileStore.createAppendStore(tempDir, new HashMap<>());
        IndexManifestFile indexManifestFile = createIndexManifestFile(fileStore);
        IndexManifestFileHandler handler =
                new IndexManifestFileHandler(indexManifestFile, BucketMode.HASH_FIXED);
        IndexManifestEntry first = pkVectorEntry("test-vector-ann", "ann-1");
        String firstManifest = handler.write(null, Arrays.asList(first));
        IndexManifestEntry second = pkVectorEntry("test-vector-ann", "ann-2");

        String secondManifest = handler.write(firstManifest, Arrays.asList(second));

        assertThat(indexManifestFile.read(secondManifest)).containsExactlyInAnyOrder(first, second);

        IndexManifestEntry ann = pkVectorEntry("test-vector-ann", "ann-3");
        String annManifest =
                handler.write(
                        secondManifest,
                        Arrays.asList(first.toDeleteEntry(), second.toDeleteEntry(), ann));
        assertThat(indexManifestFile.read(annManifest)).containsExactly(ann);
    }

    @Test
    public void testSourceMetaDoesNotDisableRangeValidationForOtherFields() throws Exception {
        TestAppendFileStore fileStore =
                TestAppendFileStore.createAppendStore(tempDir, new HashMap<>());
        IndexManifestFile indexManifestFile = createIndexManifestFile(fileStore);
        IndexManifestFileHandler handler =
                new IndexManifestFileHandler(indexManifestFile, BucketMode.HASH_FIXED);
        IndexManifestEntry sourceBacked = pkVectorEntry("btree", "source-backed");
        IndexManifestEntry previousRange = globalIndexEntry("previous-range", 0, 99, 2);
        String manifest = handler.write(null, Arrays.asList(sourceBacked, previousRange));
        IndexManifestEntry overlappingRange = globalIndexEntry("overlapping-range", 50, 149, 2);

        assertThatThrownBy(() -> handler.write(manifest, Arrays.asList(overlappingRange)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("previous-range")
                .hasMessageContaining("overlapping-range")
                .hasMessageContaining("overlapping row range");
    }

    private IndexManifestFile createIndexManifestFile(TestAppendFileStore fileStore) {
        return new IndexManifestFile.Factory(
                        fileStore.fileIO(),
                        FileFormat.manifestFormat(fileStore.options()),
                        "zstd",
                        fileStore.pathFactory(),
                        null)
                .create();
    }

    private String writeLegacyManifest(
            TestAppendFileStore fileStore, FileFormat fileFormat, IndexManifestEntry entry)
            throws Exception {
        RowType legacyGlobalIndexSchema =
                GlobalIndexMeta.SCHEMA.copy(GlobalIndexMeta.SCHEMA.getFields().subList(0, 5));
        List<DataField> legacyEntryFields = new ArrayList<>(IndexManifestEntry.SCHEMA.getFields());
        legacyEntryFields.set(9, legacyEntryFields.get(9).newType(legacyGlobalIndexSchema));
        RowType legacySchema =
                ManifestSchemaUtils.withFormatIdentifier(new RowType(false, legacyEntryFields));

        InternalRow current = new IndexManifestEntrySerializer().toRow(entry);
        InternalRow currentGlobal = current.getRow(10, GlobalIndexMeta.SCHEMA.getFieldCount());
        GenericRow legacyGlobal =
                GenericRow.of(
                        currentGlobal.getLong(0),
                        currentGlobal.getLong(1),
                        currentGlobal.getInt(2),
                        currentGlobal.isNullAt(3) ? null : currentGlobal.getArray(3),
                        currentGlobal.isNullAt(4) ? null : currentGlobal.getBinary(4));
        GenericRow legacyRow =
                GenericRow.of(
                        current.getInt(0),
                        current.getByte(1),
                        current.getBinary(2),
                        current.getInt(3),
                        current.getString(4),
                        current.getString(5),
                        current.getLong(6),
                        current.getLong(7),
                        current.isNullAt(8) ? null : current.getArray(8),
                        current.isNullAt(9) ? null : current.getString(9),
                        legacyGlobal);

        Path path = fileStore.pathFactory().indexManifestFileFactory().newPath();
        try (PositionOutputStream out = fileStore.fileIO().newOutputStream(path, false);
                FormatWriter writer =
                        fileFormat.createWriterFactory(legacySchema).create(out, "zstd")) {
            writer.addElement(legacyRow);
        }
        return path.getName();
    }

    private IndexManifestEntry globalIndexEntry(
            String fileName, long rowRangeStart, long rowRangeEnd, int indexFieldId) {
        return new IndexManifestEntry(
                FileKind.ADD,
                BinaryRow.EMPTY_ROW,
                0,
                new IndexFileMeta(
                        "btree",
                        fileName,
                        1L,
                        rowRangeEnd - rowRangeStart + 1,
                        new GlobalIndexMeta(rowRangeStart, rowRangeEnd, indexFieldId, null, null),
                        null));
    }

    private IndexManifestEntry pkVectorEntry(String indexType, String fileName) {
        return new IndexManifestEntry(
                FileKind.ADD,
                BinaryRow.EMPTY_ROW,
                0,
                new IndexFileMeta(
                        indexType,
                        fileName,
                        1L,
                        1L,
                        new GlobalIndexMeta(0, 1, 1, null, null, new byte[] {1}),
                        null));
    }

    private IndexManifestEntry dataEvolutionIndexEntry(
            String fileName,
            long rowRangeStart,
            long rowRangeEnd,
            int indexFieldId,
            long scanSnapshotId) {
        return new IndexManifestEntry(
                FileKind.ADD,
                BinaryRow.EMPTY_ROW,
                0,
                new IndexFileMeta(
                        "lumina",
                        fileName,
                        1L,
                        rowRangeEnd - rowRangeStart + 1,
                        new GlobalIndexMeta(
                                rowRangeStart,
                                rowRangeEnd,
                                indexFieldId,
                                null,
                                null,
                                new DataEvolutionIndexSourceMeta(scanSnapshotId).serialize()),
                        null));
    }
}
