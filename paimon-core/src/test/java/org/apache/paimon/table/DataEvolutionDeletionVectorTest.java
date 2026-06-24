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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionFileKey;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.append.AppendDeleteFileMaintainer;
import org.apache.paimon.deletionvectors.append.BaseAppendDeleteFileMaintainer;
import org.apache.paimon.format.blob.BlobFileFormat;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
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
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.table.BucketMode.UNAWARE_BUCKET;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests row-range deletion vectors for data evolution tables. */
public class DataEvolutionDeletionVectorTest extends DataEvolutionTestBase {

    @Test
    public void testReadWithRowRangeDeletionVectors() throws Exception {
        FileStoreTable table = prepareTableWithStructuredUpdateAndDeletionVectors();

        ReadBuilder readBuilder =
                table.newReadBuilder().withRowRanges(Collections.singletonList(new Range(0, 14)));
        TableScan.Plan plan = readBuilder.newScan().plan();
        assertThat(plan.splits()).hasSize(1);

        DataSplit dataSplit = toDataSplit(plan.splits().get(0));
        assertRegularFileRowRanges(
                dataSplit.dataFiles(),
                Arrays.asList(
                        new Range(0, 4),
                        new Range(0, 4),
                        new Range(5, 9),
                        new Range(5, 9),
                        new Range(10, 14),
                        new Range(10, 14)));
        assertThat(dataSplit.dataEvolutionDeletionFiles()).isPresent();
        Map<Range, ?> deletionFiles = dataSplit.dataEvolutionDeletionFiles().get();
        assertThat(deletionFiles.keySet())
                .containsExactlyInAnyOrder(new Range(0, 3), new Range(4, 10), new Range(11, 12));

        assertThat(readRows(readBuilder, plan))
                .containsExactly(
                        "0|name-0|updated-0|0",
                        "2|name-2|updated-2|2",
                        "3|name-3|updated-3|3",
                        "5|name-5|updated-5|5",
                        "7|name-7|updated-7|7",
                        "8|name-8|updated-8|8",
                        "9|name-9|updated-9|9",
                        "11|name-11|updated-11|11",
                        "13|name-13|updated-13|13",
                        "14|name-14|updated-14|14");
    }

    @Test
    public void testReadSingleRowIdWithRowRangeDeletionVectors() throws Exception {
        FileStoreTable table = prepareTableWithStructuredUpdateAndDeletionVectors();

        assertThat(
                        readRows(
                                table.newReadBuilder()
                                        .withRowRanges(Collections.singletonList(new Range(4, 4)))))
                .isEmpty();
        assertThat(
                        readRows(
                                table.newReadBuilder()
                                        .withRowRanges(Collections.singletonList(new Range(7, 7)))))
                .containsExactly("7|name-7|updated-7|7");
    }

    @Test
    public void testReadProjectedColumnWithRowRangeDeletionVectors() throws Exception {
        FileStoreTable table = prepareTableWithStructuredUpdateAndDeletionVectors();

        ReadBuilder structuredColumnRead =
                table.newReadBuilder()
                        .withProjection(new int[] {2})
                        .withRowRanges(Collections.singletonList(new Range(0, 14)));
        assertThat(readProjectedStrings(structuredColumnRead))
                .containsExactly(
                        "updated-0",
                        "updated-2",
                        "updated-3",
                        "updated-5",
                        "updated-7",
                        "updated-8",
                        "updated-9",
                        "updated-11",
                        "updated-13",
                        "updated-14");

        ReadBuilder blobColumnRead =
                table.newReadBuilder()
                        .withProjection(new int[] {3})
                        .withRowRanges(Collections.singletonList(new Range(0, 14)));
        assertThat(readProjectedBlobValues(blobColumnRead))
                .containsExactly(0, 2, 3, 5, 7, 8, 9, 11, 13, 14);
    }

    @Test
    public void testMergedRowCountWithSpanningRowRangeDeletionVector() throws Exception {
        FileStoreTable table = prepareTableWithStructuredUpdateAndDeletionVectors();

        DataSplit dataSplit = planDataSplit(table, new Range(0, 14));

        assertThat(dataSplit.mergedRowCount()).hasValue(10L);
    }

    @Test
    public void testMergedRowCountIsEmptyWhenDeletionVectorExceedsPlannedDataFiles()
            throws Exception {
        FileStoreTable table = prepareTableWithStructuredUpdateAndDeletionVectors();

        DataSplit dataSplit = planDataSplit(table, new Range(0, 4));

        assertThat(dataSplit.mergedRowCount()).isEmpty();
    }

    @Test
    public void testMergedRowCountWithContainedRowRangeDeletionVectors() throws Exception {
        FileStoreTable table =
                prepareTableWithStructuredUpdateAndDeletionVectors(
                        Arrays.asList(
                                new DvSpec(new Range(0, 3), 1),
                                new DvSpec(new Range(5, 9), 6),
                                new DvSpec(new Range(11, 12), 12)));

        DataSplit dataSplit = planDataSplit(table, new Range(0, 14));

        assertThat(dataSplit.mergedRowCount()).hasValue(12L);
    }

    @Test
    public void testReadAfterRemovingAllRowRangeDeletionVectors() throws Exception {
        FileStoreTable table = prepareTableWithStructuredUpdateAndDeletionVectors();
        ReadBuilder readWithDvs =
                table.newReadBuilder().withRowRanges(Collections.singletonList(new Range(0, 14)));
        assertThat(readRows(readWithDvs)).hasSize(10);

        AppendDeleteFileMaintainer maintainer =
                BaseAppendDeleteFileMaintainer.forUnawareAppend(
                        table.store().newIndexFileHandler(),
                        table.latestSnapshot().get(),
                        BinaryRow.EMPTY_ROW);
        maintainer.notifyRemovedDeletionVector(DeletionFileKey.ofRange(new Range(0, 3)));
        maintainer.notifyRemovedDeletionVector(DeletionFileKey.ofRange(new Range(4, 10)));
        maintainer.notifyRemovedDeletionVector(DeletionFileKey.ofRange(new Range(11, 12)));

        List<IndexFileMeta> newIndexFiles = new ArrayList<>();
        List<IndexFileMeta> deletedIndexFiles = new ArrayList<>();
        for (IndexManifestEntry entry : maintainer.persist()) {
            if (entry.kind() == FileKind.ADD) {
                newIndexFiles.add(entry.indexFile());
            } else if (entry.kind() == FileKind.DELETE) {
                deletedIndexFiles.add(entry.indexFile());
            }
        }
        commitDefault(
                Collections.singletonList(
                        new CommitMessageImpl(
                                BinaryRow.EMPTY_ROW,
                                UNAWARE_BUCKET,
                                null,
                                new DataIncrement(
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        newIndexFiles,
                                        deletedIndexFiles),
                                CompactIncrement.emptyIncrement())));
        table = getTableDefault();

        ReadBuilder readBuilder =
                table.newReadBuilder().withRowRanges(Collections.singletonList(new Range(0, 14)));
        TableScan.Plan plan = readBuilder.newScan().plan();
        assertThat(plan.splits()).hasSize(1);
        DataSplit dataSplit = toDataSplit(plan.splits().get(0));

        assertThat(dataSplit.dataEvolutionDeletionFiles())
                .hasValueSatisfying(deletionFiles -> assertThat(deletionFiles).isEmpty());
        assertThat(readRows(readBuilder, plan))
                .containsExactly(
                        "0|name-0|updated-0|0",
                        "1|name-1|updated-1|1",
                        "2|name-2|updated-2|2",
                        "3|name-3|updated-3|3",
                        "4|name-4|updated-4|4",
                        "5|name-5|updated-5|5",
                        "6|name-6|updated-6|6",
                        "7|name-7|updated-7|7",
                        "8|name-8|updated-8|8",
                        "9|name-9|updated-9|9",
                        "10|name-10|updated-10|10",
                        "11|name-11|updated-11|11",
                        "12|name-12|updated-12|12",
                        "13|name-13|updated-13|13",
                        "14|name-14|updated-14|14");
    }

    @Override
    protected Schema schemaDefault() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.STRING());
        schemaBuilder.column("f3", DataTypes.BLOB());
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "128 MB");
        schemaBuilder.option(CoreOptions.BLOB_TARGET_FILE_SIZE.key(), "1 b");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        return schemaBuilder.build();
    }

    private FileStoreTable prepareTableWithStructuredUpdateAndDeletionVectors() throws Exception {
        return prepareTableWithStructuredUpdateAndDeletionVectors(
                Arrays.asList(
                        new DvSpec(new Range(0, 3), 1),
                        new DvSpec(new Range(4, 10), 4, 6, 10),
                        new DvSpec(new Range(11, 12), 12)));
    }

    private FileStoreTable prepareTableWithStructuredUpdateAndDeletionVectors(
            List<DvSpec> deletionVectorSpecs) throws Exception {
        createTableDefault();

        FileStoreTable table = getTableDefault();
        writeBaseRows(table);
        assertRegularFileRowRanges(
                table.store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .collect(Collectors.toList()),
                Arrays.asList(new Range(0, 4), new Range(5, 9), new Range(10, 14)));
        assertFirstBlobFileRowRanges(
                table, Arrays.asList(new Range(0, 0), new Range(1, 1), new Range(2, 2)), 15);

        updateStructuredColumn(table);
        commitDeletionVectors(table, deletionVectorSpecs);
        return getTableDefault();
    }

    private void writeBaseRows(FileStoreTable table) throws Exception {
        for (int batch = 0; batch < 3; batch++) {
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = builder.newWrite();
                    BatchTableCommit commit = builder.newCommit()) {
                for (int rowId = batch * 5; rowId < batch * 5 + 5; rowId++) {
                    write.write(
                            GenericRow.of(
                                    rowId,
                                    BinaryString.fromString("name-" + rowId),
                                    BinaryString.fromString("base-" + rowId),
                                    new BlobData(new byte[] {(byte) rowId})));
                }
                commit.commit(write.prepareCommit());
            }
        }
    }

    private void updateStructuredColumn(FileStoreTable table) throws Exception {
        RowType writeType = table.rowType().project(Collections.singletonList("f2"));
        for (int batch = 0; batch < 3; batch++) {
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = builder.newWrite().withWriteType(writeType);
                    BatchTableCommit commit = builder.newCommit()) {
                long firstRowId = batch * 5L;
                for (int rowId = batch * 5; rowId < batch * 5 + 5; rowId++) {
                    write.write(GenericRow.of(BinaryString.fromString("updated-" + rowId)));
                }
                List<CommitMessage> commitables = write.prepareCommit();
                setFirstRowId(commitables, firstRowId);
                commit.commit(commitables);
            }
        }
    }

    private void commitDeletionVectors(FileStoreTable table, List<DvSpec> deletionVectorSpecs)
            throws Exception {
        AppendDeleteFileMaintainer maintainer =
                BaseAppendDeleteFileMaintainer.forUnawareAppend(
                        table.store().newIndexFileHandler(),
                        table.latestSnapshot().get(),
                        BinaryRow.EMPTY_ROW);

        for (DvSpec spec : deletionVectorSpecs) {
            DeletionVector deletionVector = new BitmapDeletionVector();
            for (long rowId : spec.deletedRowIds) {
                deletionVector.delete(rowId - spec.range.from);
            }
            maintainer.notifyNewDeletionVector(DeletionFileKey.ofRange(spec.range), deletionVector);
        }

        List<IndexFileMeta> newIndexFiles = new ArrayList<>();
        List<IndexFileMeta> deletedIndexFiles = new ArrayList<>();
        for (IndexManifestEntry entry : maintainer.persist()) {
            if (entry.kind() == FileKind.ADD) {
                newIndexFiles.add(entry.indexFile());
            } else if (entry.kind() == FileKind.DELETE) {
                deletedIndexFiles.add(entry.indexFile());
            }
        }

        commitDefault(
                Collections.singletonList(
                        new CommitMessageImpl(
                                BinaryRow.EMPTY_ROW,
                                UNAWARE_BUCKET,
                                null,
                                new DataIncrement(
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        newIndexFiles,
                                        deletedIndexFiles),
                                CompactIncrement.emptyIncrement())));
    }

    private static DataSplit planDataSplit(FileStoreTable table, Range range) {
        ReadBuilder readBuilder =
                table.newReadBuilder().withRowRanges(Collections.singletonList(range));
        TableScan.Plan plan = readBuilder.newScan().plan();
        assertThat(plan.splits()).hasSize(1);
        return toDataSplit(plan.splits().get(0));
    }

    private static List<String> readRows(ReadBuilder readBuilder, TableScan.Plan plan)
            throws IOException {
        List<String> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(row -> rows.add(formatRow(row)));
        }
        rows.sort((left, right) -> Integer.compare(rowId(left), rowId(right)));
        return rows;
    }

    private static List<String> readRows(ReadBuilder readBuilder) throws IOException {
        return readRows(readBuilder, readBuilder.newScan().plan());
    }

    private static List<String> readProjectedStrings(ReadBuilder readBuilder) throws IOException {
        List<String> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(row -> rows.add(row.getString(0).toString()));
        }
        rows.sort((left, right) -> Integer.compare(projectedRowId(left), projectedRowId(right)));
        return rows;
    }

    private static List<Integer> readProjectedBlobValues(ReadBuilder readBuilder)
            throws IOException {
        List<Integer> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(row -> rows.add(row.getBlob(0).toData()[0] & 0xFF));
        }
        Collections.sort(rows);
        return rows;
    }

    private static int rowId(String row) {
        return Integer.parseInt(row.substring(0, row.indexOf('|')));
    }

    private static int projectedRowId(String row) {
        return Integer.parseInt(row.substring(row.lastIndexOf('-') + 1));
    }

    private static String formatRow(InternalRow row) {
        return row.getInt(0)
                + "|"
                + row.getString(1)
                + "|"
                + row.getString(2)
                + "|"
                + (row.getBlob(3).toData()[0] & 0xFF);
    }

    private static DataSplit toDataSplit(Split split) {
        if (split instanceof IndexedSplit) {
            return ((IndexedSplit) split).dataSplit();
        }
        return (DataSplit) split;
    }

    private static void assertRegularFileRowRanges(
            List<DataFileMeta> dataFiles, List<Range> expected) {
        List<Range> actual =
                dataFiles.stream()
                        .filter(file -> !BlobFileFormat.isBlobFile(file.fileName()))
                        .map(DataFileMeta::nonNullRowIdRange)
                        .sorted((left, right) -> Long.compare(left.from, right.from))
                        .collect(Collectors.toList());
        assertThat(actual).isEqualTo(expected);
    }

    private static void assertFirstBlobFileRowRanges(
            FileStoreTable table, List<Range> expectedFirstRanges, int expectedCount) {
        List<Range> actual =
                table.store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .filter(file -> BlobFileFormat.isBlobFile(file.fileName()))
                        .map(DataFileMeta::nonNullRowIdRange)
                        .sorted((left, right) -> Long.compare(left.from, right.from))
                        .collect(Collectors.toList());
        assertThat(actual).hasSize(expectedCount);
        assertThat(actual.subList(0, expectedFirstRanges.size())).isEqualTo(expectedFirstRanges);
    }

    private static class DvSpec {

        private final Range range;
        private final long[] deletedRowIds;

        private DvSpec(Range range, long... deletedRowIds) {
            this.range = range;
            this.deletedRowIds = deletedRowIds;
        }
    }
}
