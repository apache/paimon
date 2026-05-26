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

package org.apache.paimon.append;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.BlobPlaceholder;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.blob.BlobFileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for blob update scenarios. */
public class BlobUpdateTest extends TableTestBase {

    private final byte[] blobBytes = randomBytes();

    @Test
    public void testReadBlobPlaceHolderFallback() throws Exception {
        createTableDefault();
        writeDataDefault(
                Arrays.asList(
                        GenericRow.of(1, BinaryString.fromString("first"), new BlobData(blobBytes)),
                        GenericRow.of(
                                2, BinaryString.fromString("second"), new BlobData(blobBytes)),
                        GenericRow.of(
                                3, BinaryString.fromString("third"), new BlobData(blobBytes))));

        FileStoreTable table = getTableDefault();
        byte[] updatedBytes = "updated-blob".getBytes();
        DataFilePathFactory pathFactory =
                table.store().pathFactory().createDataFilePathFactory(BinaryRow.EMPTY_ROW, 0);
        DataFileMeta newBlobFile =
                writeBlobFile(
                        table.fileIO(),
                        pathFactory.newBlobPath(),
                        Arrays.asList(BlobPlaceholder.INSTANCE, new BlobData(updatedBytes)),
                        0,
                        2,
                        table.schema().id(),
                        Collections.singletonList("f2"));
        commitBlobFiles(newBlobFile);

        List<byte[]> actual = new ArrayList<>();
        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        reader.forEachRemaining(row -> actual.add(row.getBlob(2).toData()));

        assertThat(actual.size()).isEqualTo(3);
        assertThat(actual.get(0)).isEqualTo(blobBytes);
        assertThat(actual.get(1)).isEqualTo(updatedBytes);
        assertThat(actual.get(2)).isEqualTo(blobBytes);
    }

    @Test
    public void testReadBlobPlaceHolderFallbackWithRowIdPushDown() throws Exception {
        createTableDefault();

        List<byte[]> originalBlobs = new ArrayList<>();
        List<InternalRow> rows = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            byte[] bytes = fixedBlobBytes(i);
            originalBlobs.add(bytes);
            rows.add(GenericRow.of(i, BinaryString.fromString("row-" + i), new BlobData(bytes)));
        }
        writeDataDefault(rows);

        FileStoreTable table = getTableDefault();
        assertBlobFileRowIdRanges(
                table,
                Arrays.asList(
                        new Range(0L, 2L),
                        new Range(3L, 5L),
                        new Range(6L, 8L),
                        new Range(9L, 9L)));

        DataFilePathFactory pathFactory =
                table.store().pathFactory().createDataFilePathFactory(BinaryRow.EMPTY_ROW, 0);
        byte[] updated4 = fixedBlobBytes(44);
        byte[] updated9 = fixedBlobBytes(99);
        DataFileMeta updateFile0 =
                writeBlobFile(
                        table.fileIO(),
                        pathFactory.newBlobPath(),
                        Arrays.asList(
                                BlobPlaceholder.INSTANCE,
                                BlobPlaceholder.INSTANCE,
                                BlobPlaceholder.INSTANCE,
                                BlobPlaceholder.INSTANCE,
                                new BlobData(updated4)),
                        0,
                        2,
                        table.schema().id(),
                        Collections.singletonList("f2"));
        DataFileMeta updateFile1 =
                writeBlobFile(
                        table.fileIO(),
                        pathFactory.newBlobPath(),
                        Arrays.asList(
                                BlobPlaceholder.INSTANCE,
                                BlobPlaceholder.INSTANCE,
                                BlobPlaceholder.INSTANCE,
                                BlobPlaceholder.INSTANCE,
                                new BlobData(updated9)),
                        5,
                        2,
                        table.schema().id(),
                        Collections.singletonList("f2"));
        commitBlobFiles(updateFile0, updateFile1);

        FileStoreTable readTable = getTableDefault();
        ReadBuilder readBuilder =
                readTable
                        .newReadBuilder()
                        .withReadType(readTable.rowType().project(Collections.singletonList("f2")))
                        .withRowRanges(Arrays.asList(new Range(5L, 5L), new Range(9L, 9L)));
        TableScan.Plan plan = readBuilder.newScan().plan();
        assertThat(plan.splits().size()).isEqualTo(1);
        DataSplit dataSplit = ((IndexedSplit) plan.splits().get(0)).dataSplit();
        assertThat(dataSplit.dataFiles().size()).isEqualTo(3);
        RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan);

        List<byte[]> actual = new ArrayList<>();
        reader.forEachRemaining(row -> actual.add(row.getBlob(0).toData()));

        assertThat(actual.size()).isEqualTo(2);
        assertThat(actual.get(0)).isEqualTo(originalBlobs.get(5));
        assertThat(actual.get(1)).isEqualTo(updated9);
    }

    /**
     * This test manually simulates the compacted layout described in BlobSequenceGroupRecordReader,
     * scaled down to row id [0, 9].
     *
     * <pre>
     * row id:      0    1    2    3    4    5    6    7    8    9
     * seq1:      [b0   b1   b2] [b3   b4]  .    .    .    .    .
     * seq2:      [u20  P]  [P    u23][u24] .    .    .    .    .
     * seq3:       .    .    .    .    .   [b5   b6   b7   b8   b9]
     * seq4:       .    .    .    .    .   [P    u46  P]  [u48   P]
     * seq6:      [P    u61  P    P    P    P    P    P    P   u69]
     *
     * RESULT:    u20   u61  b2   u23  u24  b5   u46  b7   u48  u69
     * </pre>
     */
    @Test
    public void testReadCompactedBlobSequenceGroups() throws Exception {
        createTableDefault();

        List<byte[]> baseBlobs = new ArrayList<>();
        List<InternalRow> rows = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            byte[] bytes = fixedBlobBytes(i);
            baseBlobs.add(bytes);
            rows.add(GenericRow.of(i, BinaryString.fromString("row-" + i), new BlobData(bytes)));
        }
        writeDataDefault(rows);

        FileStoreTable table = getTableDefault();
        List<DataFileMeta> oldBlobFiles = blobFiles(table);
        assertThat(oldBlobFiles.size()).isEqualTo(4);

        byte[] seq2Row0 = fixedBlobBytes(20);
        byte[] seq2Row3 = fixedBlobBytes(23);
        byte[] seq2Row4 = fixedBlobBytes(24);
        byte[] seq4Row6 = fixedBlobBytes(46);
        byte[] seq4Row8 = fixedBlobBytes(48);
        byte[] seq6Row1 = fixedBlobBytes(61);
        byte[] seq6Row9 = fixedBlobBytes(69);

        DataFilePathFactory pathFactory =
                table.store().pathFactory().createDataFilePathFactory(BinaryRow.EMPTY_ROW, 0);
        List<DataFileMeta> compactedLayout =
                Arrays.asList(
                        writeBlobFile(
                                table.fileIO(),
                                pathFactory.newBlobPath(),
                                Arrays.asList(
                                        new BlobData(baseBlobs.get(0)),
                                        new BlobData(baseBlobs.get(1)),
                                        new BlobData(baseBlobs.get(2))),
                                0,
                                1,
                                table.schema().id(),
                                Collections.singletonList("f2")),
                        writeBlobFile(
                                table.fileIO(),
                                pathFactory.newBlobPath(),
                                Arrays.asList(
                                        new BlobData(baseBlobs.get(3)),
                                        new BlobData(baseBlobs.get(4))),
                                3,
                                1,
                                table.schema().id(),
                                Collections.singletonList("f2")),
                        writeBlobFile(
                                table.fileIO(),
                                pathFactory.newBlobPath(),
                                Arrays.asList(new BlobData(seq2Row0), BlobPlaceholder.INSTANCE),
                                0,
                                2,
                                table.schema().id(),
                                Collections.singletonList("f2")),
                        writeBlobFile(
                                table.fileIO(),
                                pathFactory.newBlobPath(),
                                Arrays.asList(BlobPlaceholder.INSTANCE, new BlobData(seq2Row3)),
                                2,
                                2,
                                table.schema().id(),
                                Collections.singletonList("f2")),
                        writeBlobFile(
                                table.fileIO(),
                                pathFactory.newBlobPath(),
                                Collections.singletonList(new BlobData(seq2Row4)),
                                4,
                                2,
                                table.schema().id(),
                                Collections.singletonList("f2")),
                        writeBlobFile(
                                table.fileIO(),
                                pathFactory.newBlobPath(),
                                Arrays.asList(
                                        new BlobData(baseBlobs.get(5)),
                                        new BlobData(baseBlobs.get(6)),
                                        new BlobData(baseBlobs.get(7)),
                                        new BlobData(baseBlobs.get(8)),
                                        new BlobData(baseBlobs.get(9))),
                                5,
                                3,
                                table.schema().id(),
                                Collections.singletonList("f2")),
                        writeBlobFile(
                                table.fileIO(),
                                pathFactory.newBlobPath(),
                                Arrays.asList(
                                        BlobPlaceholder.INSTANCE,
                                        new BlobData(seq4Row6),
                                        BlobPlaceholder.INSTANCE),
                                5,
                                4,
                                table.schema().id(),
                                Collections.singletonList("f2")),
                        writeBlobFile(
                                table.fileIO(),
                                pathFactory.newBlobPath(),
                                Arrays.asList(new BlobData(seq4Row8), BlobPlaceholder.INSTANCE),
                                8,
                                4,
                                table.schema().id(),
                                Collections.singletonList("f2")),
                        writeBlobFile(
                                table.fileIO(),
                                pathFactory.newBlobPath(),
                                Arrays.asList(
                                        BlobPlaceholder.INSTANCE,
                                        new BlobData(seq6Row1),
                                        BlobPlaceholder.INSTANCE,
                                        BlobPlaceholder.INSTANCE,
                                        BlobPlaceholder.INSTANCE,
                                        BlobPlaceholder.INSTANCE,
                                        BlobPlaceholder.INSTANCE,
                                        BlobPlaceholder.INSTANCE,
                                        BlobPlaceholder.INSTANCE,
                                        new BlobData(seq6Row9)),
                                0,
                                6,
                                table.schema().id(),
                                Collections.singletonList("f2")));

        commitCompactFiles(oldBlobFiles, compactedLayout);

        FileStoreTable readTable = getTableDefault();
        assertBlobFileLayout(
                readTable,
                Arrays.asList(
                        "seq1:0-2",
                        "seq1:3-4",
                        "seq2:0-1",
                        "seq2:2-3",
                        "seq2:4-4",
                        "seq3:5-9",
                        "seq4:5-7",
                        "seq4:8-9",
                        "seq6:0-9"));

        ReadBuilder readBuilder = readTable.newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());

        List<byte[]> actual = new ArrayList<>();
        reader.forEachRemaining(row -> actual.add(row.getBlob(2).toData()));

        List<byte[]> expected =
                Arrays.asList(
                        seq2Row0,
                        seq6Row1,
                        baseBlobs.get(2),
                        seq2Row3,
                        seq2Row4,
                        baseBlobs.get(5),
                        seq4Row6,
                        baseBlobs.get(7),
                        seq4Row8,
                        seq6Row9);
        assertThat(actual.size()).isEqualTo(expected.size());
        for (int i = 0; i < expected.size(); i++) {
            assertThat(actual.get(i)).isEqualTo(expected.get(i));
        }

        // additionally, assert row id pushdown
        for (int i = 0; i < expected.size(); i++) {
            assertRowRangeRead(readTable, i, expected.get(i));
        }
    }

    private void commitBlobFiles(DataFileMeta... dataFiles) throws Exception {
        commitDataFiles(Arrays.asList(dataFiles), Collections.emptyList());
    }

    private void commitDataFiles(List<DataFileMeta> newFiles, List<DataFileMeta> deletedFiles)
            throws Exception {
        commitDefault(
                Collections.singletonList(
                        new CommitMessageImpl(
                                BinaryRow.EMPTY_ROW,
                                0,
                                null,
                                new DataIncrement(newFiles, deletedFiles, Collections.emptyList()),
                                CompactIncrement.emptyIncrement())));
    }

    private void commitCompactFiles(
            List<DataFileMeta> compactBefore, List<DataFileMeta> compactAfter) throws Exception {
        commitDefault(
                Collections.singletonList(
                        new CommitMessageImpl(
                                BinaryRow.EMPTY_ROW,
                                0,
                                null,
                                DataIncrement.emptyIncrement(),
                                new CompactIncrement(
                                        compactBefore, compactAfter, Collections.emptyList()))));
    }

    private static List<DataFileMeta> blobFiles(FileStoreTable table) {
        List<DataFileMeta> actual = new ArrayList<>();
        for (ManifestEntry entry : table.store().newScan().plan().files()) {
            DataFileMeta file = entry.file();
            if (BlobFileFormat.isBlobFile(file.fileName())) {
                actual.add(file);
            }
        }
        return actual;
    }

    private static void assertBlobFileRowIdRanges(FileStoreTable table, List<Range> expected) {
        List<Range> actual = new ArrayList<>();
        for (DataFileMeta file : blobFiles(table)) {
            actual.add(file.nonNullRowIdRange());
        }
        Collections.sort(actual, (left, right) -> Long.compare(left.from, right.from));
        assertThat(actual).isEqualTo(expected);
    }

    private static void assertBlobFileLayout(FileStoreTable table, List<String> expected) {
        List<DataFileMeta> files = blobFiles(table);
        Collections.sort(
                files,
                (left, right) -> {
                    int seqCompare =
                            Long.compare(left.maxSequenceNumber(), right.maxSequenceNumber());
                    if (seqCompare != 0) {
                        return seqCompare;
                    }
                    int fromCompare =
                            Long.compare(left.nonNullFirstRowId(), right.nonNullFirstRowId());
                    if (fromCompare != 0) {
                        return fromCompare;
                    }
                    return Long.compare(left.nonNullRowIdRange().to, right.nonNullRowIdRange().to);
                });
        List<String> actual = new ArrayList<>();
        for (DataFileMeta file : files) {
            Range range = file.nonNullRowIdRange();
            actual.add("seq" + file.maxSequenceNumber() + ":" + range.from + "-" + range.to);
        }
        assertThat(actual).isEqualTo(expected);
    }

    private static void assertRowRangeRead(FileStoreTable table, long rowId, byte[] expected)
            throws Exception {
        ReadBuilder readBuilder =
                table.newReadBuilder()
                        .withRowRanges(Collections.singletonList(new Range(rowId, rowId)));
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());

        List<byte[]> actual = new ArrayList<>();
        reader.forEachRemaining(row -> actual.add(row.getBlob(2).toData()));

        assertThat(actual.size()).isEqualTo(1);
        assertThat(actual.get(0)).isEqualTo(expected);
    }

    private static byte[] fixedBlobBytes(int value) {
        byte[] bytes = new byte[2 * 1024 * 124];
        Arrays.fill(bytes, (byte) value);
        return bytes;
    }

    private static DataFileMeta writeBlobFile(
            FileIO fileIO,
            Path path,
            List<Blob> blobs,
            long firstRowId,
            long maxSequenceNumber,
            long schemaId,
            List<String> writeCols)
            throws IOException {
        try (PositionOutputStream out = fileIO.newOutputStream(path, false)) {
            FormatWriter writer =
                    new BlobFileFormat()
                            .createWriterFactory(RowType.of(DataTypes.BLOB()))
                            .create(out, "none");
            for (Blob blob : blobs) {
                writer.addElement(GenericRow.of(blob));
            }
            writer.close();
        }
        return DataFileMeta.create(
                path.getName(),
                fileIO.getFileSize(path),
                blobs.size(),
                DataFileMeta.EMPTY_MIN_KEY,
                DataFileMeta.EMPTY_MAX_KEY,
                SimpleStats.EMPTY_STATS,
                SimpleStats.EMPTY_STATS,
                maxSequenceNumber,
                maxSequenceNumber,
                schemaId,
                DataFileMeta.DUMMY_LEVEL,
                Collections.emptyList(),
                Timestamp.fromEpochMillis(System.currentTimeMillis()),
                0L,
                null,
                FileSource.APPEND,
                null,
                null,
                firstRowId,
                writeCols);
    }

    @Override
    protected Schema schemaDefault() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.BLOB());
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "25 MB");
        schemaBuilder.option(CoreOptions.BLOB_TARGET_FILE_SIZE.key(), "700 KB");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        return schemaBuilder.build();
    }

    @Override
    protected InternalRow dataDefault(int time, int size) {
        return GenericRow.of(
                RANDOM.nextInt(), BinaryString.fromBytes(randomBytes()), new BlobData(blobBytes));
    }

    @Override
    protected byte[] randomBytes() {
        byte[] binary = new byte[2 * 1024 * 124];
        RANDOM.nextBytes(binary);
        return binary;
    }
}
