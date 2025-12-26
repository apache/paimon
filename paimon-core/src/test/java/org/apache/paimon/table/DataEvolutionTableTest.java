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
import org.apache.paimon.append.dataevolution.DataEvolutionCompactCoordinator;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactTask;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.globalindex.DataEvolutionBatchScan;
import org.apache.paimon.globalindex.GlobalIndexFileReadWrite;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexScanBuilder;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.GlobalIndexerFactory;
import org.apache.paimon.globalindex.GlobalIndexerFactoryUtils;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.RowRangeGlobalIndexScanner;
import org.apache.paimon.globalindex.bitmap.BitmapGlobalIndexerFactory;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.DataEvolutionFileReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Test for table with data evolution. */
public class DataEvolutionTableTest extends TableTestBase {

    @Test
    public void testBasic() throws Exception {
        createTableDefault();
        Schema schema = schemaDefault();
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withWriteType(schema.rowType())) {
            write.write(
                    GenericRow.of(1, BinaryString.fromString("a"), BinaryString.fromString("b")));
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write.prepareCommit();
            commit.commit(commitables);
        }

        RowType writeType1 = schema.rowType().project(Collections.singletonList("f2"));
        try (BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {
            write1.write(GenericRow.of(BinaryString.fromString("c")));

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write1.prepareCommit();
            setFirstRowId(commitables, 0L);
            commit.commit(commitables);
        }

        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        assertThat(reader).isInstanceOf(DataEvolutionFileReader.class);
        reader.forEachRemaining(
                r -> {
                    assertThat(r.getInt(0)).isEqualTo(1);
                    assertThat(r.getString(1).toString()).isEqualTo("a");
                    assertThat(r.getString(2).toString()).isEqualTo("c");
                });

        // projection with only special fields.
        readBuilder = getTableDefault().newReadBuilder();
        reader =
                readBuilder
                        .withReadType(RowType.of(SpecialFields.ROW_ID))
                        .newRead()
                        .createReader(readBuilder.newScan().plan());
        AtomicInteger cnt = new AtomicInteger(0);
        reader.forEachRemaining(
                r -> {
                    cnt.incrementAndGet();
                });
        assertThat(cnt.get()).isEqualTo(1);

        // projection with an empty read type
        readBuilder = getTableDefault().newReadBuilder();
        reader =
                readBuilder
                        .withReadType(RowType.of())
                        .newRead()
                        .createReader(readBuilder.newScan().plan());
        AtomicInteger cnt1 = new AtomicInteger(0);
        reader.forEachRemaining(
                r -> {
                    cnt1.incrementAndGet();
                });
        assertThat(cnt1.get()).isEqualTo(1);
    }

    @Test
    public void testMultipleAppends() throws Exception {
        createTableDefault();

        Schema schema = schemaDefault();

        RowType writeType0 = schema.rowType().project(Arrays.asList("f0", "f1"));
        RowType writeType1 = schema.rowType().project(Collections.singletonList("f2"));
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();

        try (BatchTableWrite write = builder.newWrite().withWriteType(schema.rowType())) {
            for (int i = 0; i < 100; i++) {
                write.write(
                        GenericRow.of(
                                1, BinaryString.fromString("a"), BinaryString.fromString("b")));
            }
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write.prepareCommit();
            commit.commit(commitables);
        }

        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType0);
                BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {

            write0.write(GenericRow.of(1, BinaryString.fromString("a")));
            write1.write(GenericRow.of(BinaryString.fromString("b")));

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = new ArrayList<>();
            commitables.addAll(write0.prepareCommit());
            commitables.addAll(write1.prepareCommit());
            setFirstRowId(commitables, 100L);
            commit.commit(commitables);
        }

        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType0)) {
            write0.write(GenericRow.of(2, BinaryString.fromString("c")));

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write0.prepareCommit();
            setFirstRowId(commitables, 101L);
            commit.commit(commitables);
        }

        try (BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {
            write1.write(GenericRow.of(BinaryString.fromString("d")));

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write1.prepareCommit();
            setFirstRowId(commitables, 101L);
            commit.commit(commitables);
        }

        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());

        List<InternalRow> readRows = new ArrayList<>();
        reader.forEachRemaining(readRows::add);

        assertThat(readRows.size()).isEqualTo(102);

        AtomicInteger i = new AtomicInteger(0);
        reader.forEachRemaining(
                r -> {
                    if (i.get() < 101) {
                        assertThat(r.getInt(0)).isEqualTo(1);
                        assertThat(r.getString(1).toString()).isEqualTo("a");
                        assertThat(r.getString(2).toString()).isEqualTo("b");
                    } else {
                        assertThat(r.getInt(0)).isEqualTo(2);
                        assertThat(r.getString(1).toString()).isEqualTo("c");
                        assertThat(r.getString(2).toString()).isEqualTo("d"); // "d" from the append
                    }
                    i.incrementAndGet();
                });
    }

    @Test
    public void testOnlySomeColumns() throws Exception {
        createTableDefault();

        Schema schema = schemaDefault();
        RowType writeType0 = schema.rowType().project(Collections.singletonList("f0"));
        RowType writeType1 = schema.rowType().project(Collections.singletonList("f1"));
        RowType writeType2 = schema.rowType().project(Collections.singletonList("f2"));

        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();

        // Commit 1: f0
        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType0)) {
            write0.write(GenericRow.of(1));
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write0.prepareCommit();
            commit.commit(commitables);
        }

        // Commit 2: f1
        try (BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {
            write1.write(GenericRow.of(BinaryString.fromString("a")));
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write1.prepareCommit();
            setFirstRowId(commitables, 0L);
            commit.commit(commitables);
        }

        // Commit 3: f2
        try (BatchTableWrite write2 = builder.newWrite().withWriteType(writeType2)) {
            write2.write(GenericRow.of(BinaryString.fromString("b")));
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write2.prepareCommit();
            setFirstRowId(commitables, 0L);
            commit.commit(commitables);
        }

        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        assertThat(reader).isInstanceOf(DataEvolutionFileReader.class);

        reader.forEachRemaining(
                r -> {
                    assertThat(r.getInt(0)).isEqualTo(1);
                    assertThat(r.getString(1).toString()).isEqualTo("a");
                    assertThat(r.getString(2)).isEqualTo(BinaryString.fromString("b"));
                });
    }

    @Test
    public void testNullValues() throws Exception {
        createTableDefault();

        Schema schema = schemaDefault();
        RowType writeType0 = schema.rowType().project(Arrays.asList("f0", "f1"));
        RowType writeType1 = schema.rowType().project(Collections.singletonList("f2"));
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();

        // Commit 1: Some values are null
        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType0);
                BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {

            write0.write(GenericRow.of(1, null));
            write1.write(new GenericRow(1));

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = new ArrayList<>();
            commitables.addAll(write0.prepareCommit());
            commitables.addAll(write1.prepareCommit());
            setFirstRowId(commitables, 0L);
            commit.commit(commitables);
        }

        // Commit 2: Overwrite with non-null
        try (BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {
            write1.write(GenericRow.of(BinaryString.fromString("c")));

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write1.prepareCommit();
            setFirstRowId(commitables, 0L);
            commit.commit(commitables);
        }

        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());

        reader.forEachRemaining(
                r -> {
                    assertThat(r.getInt(0)).isEqualTo(1);
                    assertThat(r.isNullAt(1)).isTrue();
                    assertThat(r.getString(2).toString()).isEqualTo("c");
                });
    }

    @Test
    public void testMultipleAppendsDifferentFirstRowIds() throws Exception {
        createTableDefault();

        Schema schema = schemaDefault();
        RowType writeType0 = schema.rowType().project(Arrays.asList("f0", "f1"));
        RowType writeType1 = schema.rowType().project(Collections.singletonList("f2"));
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();

        // First commit, firstRowId = 0
        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType0);
                BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {

            write0.write(GenericRow.of(1, BinaryString.fromString("a")));
            write1.write(GenericRow.of(BinaryString.fromString("b")));

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = new ArrayList<>();
            commitables.addAll(write0.prepareCommit());
            commitables.addAll(write1.prepareCommit());
            setFirstRowId(commitables, 0L);
            commit.commit(commitables);
        }

        // Second commit, firstRowId = 1
        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType0)) {
            write0.write(GenericRow.of(2, BinaryString.fromString("c")));

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write0.prepareCommit();
            setFirstRowId(commitables, 1L); // Different firstRowId
            commit.commit(commitables);
        }

        // Third commit
        try (BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {
            write1.write(GenericRow.of(BinaryString.fromString("d")));

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write1.prepareCommit();
            setFirstRowId(commitables, 1L); // Different firstRowId
            commit.commit(commitables);
        }

        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());

        List<InternalRow> readRows = new ArrayList<>();
        reader.forEachRemaining(readRows::add);

        assertThat(readRows.size()).isEqualTo(2);

        assertThat(readRows.get(0).getInt(0)).isEqualTo(1);
        assertThat(readRows.get(0).getString(1).toString()).isEqualTo("a");
        assertThat(readRows.get(0).getString(2).toString()).isEqualTo("b");

        assertThat(readRows.get(1).getInt(0)).isEqualTo(2);
        assertThat(readRows.get(1).getString(1).toString()).isEqualTo("c");
        assertThat(readRows.get(1).getString(2).toString()).isEqualTo("d");
    }

    private void setFirstRowId(List<CommitMessage> commitables, long firstRowId) {
        commitables.forEach(
                c -> {
                    CommitMessageImpl commitMessage = (CommitMessageImpl) c;
                    List<DataFileMeta> newFiles =
                            new ArrayList<>(commitMessage.newFilesIncrement().newFiles());
                    commitMessage.newFilesIncrement().newFiles().clear();
                    commitMessage
                            .newFilesIncrement()
                            .newFiles()
                            .addAll(
                                    newFiles.stream()
                                            .map(s -> s.assignFirstRowId(firstRowId))
                                            .collect(Collectors.toList()));
                });
    }

    @Test
    public void testMoreData() throws Exception {
        createTableDefault();

        Schema schema = schemaDefault();
        RowType writeType0 = schema.rowType().project(Arrays.asList("f0", "f1"));
        RowType writeType1 = schema.rowType().project(Collections.singletonList("f2"));
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType0);
                BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {

            for (int i = 0; i < 10000; i++) {
                write0.write(GenericRow.of(i, BinaryString.fromString("a" + i)));
                write1.write(GenericRow.of(BinaryString.fromString("b" + i)));
            }

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = new ArrayList<>();
            commitables.addAll(write0.prepareCommit());
            commitables.addAll(write1.prepareCommit());
            setFirstRowId(commitables, 0L);
            commit.commit(commitables);
        }

        try (BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {
            for (int i = 0; i < 10000; i++) {
                write1.write(GenericRow.of(BinaryString.fromString("c" + i)));
            }

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write1.prepareCommit();
            commit.commit(commitables);
        }

        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        assertThat(reader).isInstanceOf(DataEvolutionFileReader.class);
        AtomicInteger i = new AtomicInteger(0);
        reader.forEachRemaining(
                r -> {
                    assertThat(r.getInt(0)).isEqualTo(i.get());
                    assertThat(r.getString(1).toString()).isEqualTo("a" + i.get());
                    assertThat(r.getString(2).toString()).isEqualTo("c" + i.get());
                    i.incrementAndGet();
                });
    }

    @Test
    public void testPredicate() throws Exception {
        createTableDefault();
        Schema schema = schemaDefault();
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withWriteType(schema.rowType())) {
            write.write(
                    GenericRow.of(1, BinaryString.fromString("a"), BinaryString.fromString("b")));
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write.prepareCommit();
            commit.commit(commitables);
        }

        RowType writeType1 = schema.rowType().project(Collections.singletonList("f2"));
        try (BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {
            write1.write(GenericRow.of(BinaryString.fromString("c")));

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write1.prepareCommit();
            setFirstRowId(commitables, 0L);
            commit.commit(commitables);
        }

        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        PredicateBuilder predicateBuilder = new PredicateBuilder(schema.rowType());
        Predicate predicate = predicateBuilder.notEqual(2, BinaryString.fromString("b"));
        readBuilder.withFilter(predicate);
        assertThat(((DataSplit) readBuilder.newScan().plan().splits().get(0)).dataFiles().size())
                .isEqualTo(2);

        predicate = predicateBuilder.notEqual(1, BinaryString.fromString("a"));
        readBuilder.withFilter(predicate);
        assertThat(readBuilder.newScan().plan().splits().isEmpty()).isTrue();

        predicate = predicateBuilder.notEqual(2, BinaryString.fromString("c"));
        readBuilder.withFilter(predicate);
        assertThat(readBuilder.newScan().plan().splits().isEmpty()).isTrue();
    }

    @Test
    public void testWithRowIdsFilterManifestEntries() throws Exception {
        innerTestWithRowIds(true);
    }

    @Test
    public void testWithRowIdsFilterManifests() throws Exception {
        innerTestWithRowIds(false);
    }

    public void innerTestWithRowIds(boolean compactManifests) throws Exception {
        createTableDefault();
        Schema schema = schemaDefault();
        FileStoreTable table = getTableDefault();
        BatchWriteBuilder builder = table.newBatchWriteBuilder();

        // Write first batch of data with firstRowId = 0
        RowType writeType0 = schema.rowType().project(Arrays.asList("f0", "f1"));
        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType0)) {
            write0.write(GenericRow.of(1, BinaryString.fromString("a")));
            write0.write(GenericRow.of(2, BinaryString.fromString("b")));

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write0.prepareCommit();
            setFirstRowId(commitables, 0L);
            commit.commit(commitables);
        }

        // Write second batch of data with firstRowId = 2
        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType0)) {
            write0.write(GenericRow.of(3, BinaryString.fromString("c")));
            write0.write(GenericRow.of(4, BinaryString.fromString("d")));

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write0.prepareCommit();
            setFirstRowId(commitables, 2L);
            commit.commit(commitables);
        }

        // Write third batch of data with firstRowId = 4
        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType0)) {
            write0.write(GenericRow.of(5, BinaryString.fromString("e")));
            write0.write(GenericRow.of(6, BinaryString.fromString("f")));

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write0.prepareCommit();
            setFirstRowId(commitables, 4L);
            commit.commit(commitables);
        }

        if (compactManifests) {
            try (BatchTableCommit commit = builder.newCommit()) {
                commit.compactManifests();
            }

            List<ManifestFileMeta> manifests =
                    table.store()
                            .manifestListFactory()
                            .create()
                            .readDataManifests(table.latestSnapshot().get());
            assertThat(manifests.size()).isEqualTo(1);
            assertThat(manifests.get(0).minRowId()).isEqualTo(0);
            assertThat(manifests.get(0).maxRowId()).isEqualTo(5);
        }

        ReadBuilder readBuilder = table.newReadBuilder();

        // Test 1: Filter by row IDs that exist in the first file (0, 1)
        List<Range> rowIds1 = Arrays.asList(new Range(0L, 1L));
        List<Split> splits1 = readBuilder.withRowRanges(rowIds1).newScan().plan().splits();
        assertThat(splits1.size())
                .isEqualTo(1); // Should return one split containing the first file

        // Verify the split contains only the first file (firstRowId=0, rowCount=2)
        DataSplit dataSplit1 = ((IndexedSplit) splits1.get(0)).dataSplit();
        assertThat(dataSplit1.dataFiles().size()).isEqualTo(1);
        DataFileMeta file1 = dataSplit1.dataFiles().get(0);
        assertThat(file1.firstRowId()).isEqualTo(0L);
        assertThat(file1.rowCount()).isEqualTo(2L);

        // Test 2: Filter by row IDs that exist in the second file (2, 3)
        List<Range> rowIds2 = Arrays.asList(new Range(2L, 3L));
        List<Split> splits2 = readBuilder.withRowRanges(rowIds2).newScan().plan().splits();
        assertThat(splits2.size())
                .isEqualTo(1); // Should return one split containing the second file

        // Verify the split contains only the second file (firstRowId=2, rowCount=2)
        DataSplit dataSplit2 = ((IndexedSplit) splits2.get(0)).dataSplit();
        assertThat(dataSplit2.dataFiles().size()).isEqualTo(1);
        DataFileMeta file2 = dataSplit2.dataFiles().get(0);
        assertThat(file2.firstRowId()).isEqualTo(2L);
        assertThat(file2.rowCount()).isEqualTo(2L);

        // Test 3: Filter by row IDs that exist in the third file (4, 5)
        List<Range> rowIds3 = Arrays.asList(new Range(4L, 5L));
        List<Split> splits3 = readBuilder.withRowRanges(rowIds3).newScan().plan().splits();
        assertThat(splits3.size())
                .isEqualTo(1); // Should return one split containing the third file

        // Verify the split contains only the third file (firstRowId=4, rowCount=2)
        DataSplit dataSplit3 = ((IndexedSplit) splits3.get(0)).dataSplit();
        assertThat(dataSplit3.dataFiles().size()).isEqualTo(1);
        DataFileMeta file3 = dataSplit3.dataFiles().get(0);
        assertThat(file3.firstRowId()).isEqualTo(4L);
        assertThat(file3.rowCount()).isEqualTo(2L);

        // Test 4: Filter by row IDs that span multiple files (1, 2, 4)
        List<Range> rowIds4 = Arrays.asList(new Range(0L, 1L), new Range(4L, 4L));
        List<Split> splits4 = readBuilder.withRowRanges(rowIds4).newScan().plan().splits();
        assertThat(splits4.size())
                .isEqualTo(1); // Should return one split containing all matching files

        // Verify the split contains all three files (firstRowId=0,2,4)
        DataSplit dataSplit4 = ((IndexedSplit) splits4.get(0)).dataSplit();
        assertThat(dataSplit4.dataFiles().size()).isEqualTo(2);

        // Check that all three files are present with correct firstRowIds
        List<Long> actualFirstRowIds =
                dataSplit4.dataFiles().stream()
                        .map(DataFileMeta::firstRowId)
                        .sorted()
                        .collect(Collectors.toList());
        assertThat(actualFirstRowIds.size()).isEqualTo(2);
        // Verify we have the expected firstRowIds: 0, 4
        boolean has0 = actualFirstRowIds.stream().anyMatch(id -> id == 0L);
        boolean has4 = actualFirstRowIds.stream().anyMatch(id -> id == 4L);
        assertThat(has0).isTrue();
        assertThat(has4).isTrue();

        // Verify each file has the correct row count
        for (DataFileMeta file : dataSplit4.dataFiles()) {
            assertThat(file.rowCount()).isEqualTo(2L);
        }

        // Test 5: Filter by row IDs that don't exist (10, 11)
        List<Range> rowIds5 = Arrays.asList(new Range(10L, 11L));
        List<Split> splits5 = readBuilder.withRowRanges(rowIds5).newScan().plan().splits();
        assertThat(splits5.size()).isEqualTo(0); // Should return no files

        // Test 6: Filter by null indices (should return all files)
        List<Split> splits6 = readBuilder.withRowRanges(null).newScan().plan().splits();
        assertThat(splits6.size()).isEqualTo(1); // Should return one split containing all files

        // Verify the split contains all three files
        DataSplit dataSplit6 = (DataSplit) splits6.get(0);
        assertThat(dataSplit6.dataFiles().size()).isEqualTo(3);

        // Check that all three files are present with correct firstRowIds
        List<Long> allFirstRowIds =
                dataSplit6.dataFiles().stream()
                        .map(DataFileMeta::firstRowId)
                        .sorted()
                        .collect(Collectors.toList());
        assertThat(allFirstRowIds.size()).isEqualTo(3);
        // Verify we have the expected firstRowIds: 0, 2, 4
        boolean has0All = allFirstRowIds.stream().anyMatch(id -> id == 0L);
        boolean has2All = allFirstRowIds.stream().anyMatch(id -> id == 2L);
        boolean has4All = allFirstRowIds.stream().anyMatch(id -> id == 4L);
        assertThat(has0All).isTrue();
        assertThat(has2All).isTrue();
        assertThat(has4All).isTrue();

        // Test 7: Filter by empty indices (should return no files)
        List<Split> splits7 =
                readBuilder.withRowRanges(Collections.emptyList()).newScan().plan().splits();
        assertThat(splits7.size()).isEqualTo(0); // Should return no files

        // Test 8: Filter by row IDs that partially exist (0, 1, 10)
        List<Range> rowIds8 = Arrays.asList(new Range(0L, 1L), new Range(10L, 10L));
        List<Split> splits8 = readBuilder.withRowRanges(rowIds8).newScan().plan().splits();
        assertThat(splits8.size())
                .isEqualTo(1); // Should return one split containing the first file

        // Verify the split contains only the first file (firstRowId=0)
        DataSplit dataSplit8 = ((IndexedSplit) splits8.get(0)).dataSplit();
        assertThat(dataSplit8.dataFiles().size()).isEqualTo(1);
        DataFileMeta file8 = dataSplit8.dataFiles().get(0);
        assertThat(file8.firstRowId()).isEqualTo(0L);
        assertThat(file8.rowCount()).isEqualTo(2L);

        List<Range> rowIds9 = Arrays.asList(new Range(0L, 0L), new Range(2L, 2L));
        List<Split> splits9 = readBuilder.withRowRanges(rowIds9).newScan().plan().splits();

        // Verify the actual data by reading from the filtered splits
        // Note: withRowIds filters at the file level, so we get all rows from matching files
        RecordReader<InternalRow> reader9 = readBuilder.newRead().createReader(splits9);
        AtomicInteger i = new AtomicInteger(0);
        reader9.forEachRemaining(
                row -> {
                    assertThat(row.getInt(0)).isEqualTo(1 + i.get() * 2);
                    assertThat(row.getString(1).toString())
                            .isEqualTo(new String(new char[] {(char) ('a' + i.get() * 2)}));
                    i.getAndIncrement();
                });
        assertThat(i.get()).isEqualTo(2);

        RowType writeType1 = schema.rowType().project(Collections.singletonList("f2"));
        try (BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {
            write1.write(GenericRow.of(BinaryString.fromString("a2")));
            write1.write(GenericRow.of(BinaryString.fromString("b2")));

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write1.prepareCommit();
            setFirstRowId(commitables, 0L);
            commit.commit(commitables);
        }

        List<Range> rowIds10 = Collections.singletonList(new Range(0L, 0L));
        List<Split> split10 = readBuilder.withRowRanges(rowIds10).newScan().plan().splits();

        // without projection， all datafiles needed to assemble a row should be scanned out
        List<DataFileMeta> fileMetas10 = (((IndexedSplit) split10.get(0)).dataSplit()).dataFiles();
        assertThat(fileMetas10.size()).isEqualTo(2);

        List<Range> rowIds11 = Collections.singletonList(new Range(0L, 0L));
        List<Split> split11 =
                readBuilder
                        .withRowRanges(rowIds11)
                        .withProjection(new int[] {0})
                        .newScan()
                        .plan()
                        .splits();

        // with projection, irrelevant datafiles should be filtered
        List<DataFileMeta> fileMetas11 = (((IndexedSplit) split11.get(0)).dataSplit()).dataFiles();
        assertThat(fileMetas11.size()).isEqualTo(1);
    }

    @Test
    public void testWithRowIdsFilterManifestsNonExistFile() throws Exception {
        createTableDefault();
        Schema schema = schemaDefault();
        FileStoreTable table = getTableDefault();
        BatchWriteBuilder builder = table.newBatchWriteBuilder();

        // Write first batch of data with firstRowId = 0
        RowType writeType0 = schema.rowType().project(Arrays.asList("f0", "f1"));
        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType0)) {
            write0.write(GenericRow.of(1, BinaryString.fromString("a")));
            write0.write(GenericRow.of(2, BinaryString.fromString("b")));

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write0.prepareCommit();
            setFirstRowId(commitables, 0L);
            commit.commit(commitables);
        }

        // Write second batch of data with firstRowId = 2
        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType0)) {
            write0.write(GenericRow.of(3, BinaryString.fromString("c")));
            write0.write(GenericRow.of(4, BinaryString.fromString("d")));

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write0.prepareCommit();
            setFirstRowId(commitables, 2L);
            commit.commit(commitables);
        }

        // Write third batch of data with firstRowId = 4
        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType0)) {
            write0.write(GenericRow.of(5, BinaryString.fromString("e")));
            write0.write(GenericRow.of(6, BinaryString.fromString("f")));

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write0.prepareCommit();
            setFirstRowId(commitables, 4L);
            commit.commit(commitables);
        }

        // assert manifest row id min max
        List<ManifestFileMeta> manifests =
                table.store()
                        .manifestListFactory()
                        .create()
                        .readDataManifests(table.latestSnapshot().get());
        assertThat(manifests.size()).isEqualTo(3);
        assertThat(manifests.get(0).minRowId()).isEqualTo(0);
        assertThat(manifests.get(0).maxRowId()).isEqualTo(1);
        assertThat(manifests.get(1).minRowId()).isEqualTo(2);
        assertThat(manifests.get(1).maxRowId()).isEqualTo(3);
        assertThat(manifests.get(2).minRowId()).isEqualTo(4);
        assertThat(manifests.get(2).maxRowId()).isEqualTo(5);

        // delete last manifest file, should never read it
        table.store().manifestFileFactory().create().delete(manifests.get(2).fileName());

        // assert file
        ReadBuilder readBuilder = table.newReadBuilder();
        List<Range> rowIds = Arrays.asList(new Range(0L, 0L), new Range(3L, 3L));
        List<Split> splits = readBuilder.withRowRanges(rowIds).newScan().plan().splits();
        assertThat(splits.size()).isEqualTo(1);
        DataSplit dataSplit = ((IndexedSplit) splits.get(0)).dataSplit();
        assertThat(dataSplit.dataFiles().size()).isEqualTo(2);
        DataFileMeta file1 = dataSplit.dataFiles().get(0);
        assertThat(file1.firstRowId()).isEqualTo(0L);
        assertThat(file1.rowCount()).isEqualTo(2L);
        DataFileMeta file2 = dataSplit.dataFiles().get(1);
        assertThat(file2.firstRowId()).isEqualTo(2L);
        assertThat(file2.rowCount()).isEqualTo(2L);
    }

    @Test
    public void testNonNullColumn() throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.STRING().copy(false));
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");

        Schema schema = schemaBuilder.build();

        catalog.createTable(identifier(), schema, true);
        Table table = catalog.getTable(identifier());
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        BatchTableWrite write = builder.newWrite();
        write.write(GenericRow.of(1, BinaryString.fromString("a"), BinaryString.fromString("b")));
        BatchTableCommit commit = builder.newCommit();
        List<CommitMessage> commitables = write.prepareCommit();
        commit.commit(commitables);

        write =
                builder.newWrite()
                        .withWriteType(schema.rowType().project(Collections.singletonList("f2")));
        write.write(GenericRow.of(BinaryString.fromString("c")));
        commit = builder.newCommit();
        commitables = write.prepareCommit();
        setFirstRowId(commitables, 0L);
        commit.commit(commitables);

        ReadBuilder readBuilder = table.newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        assertThat(reader).isInstanceOf(DataEvolutionFileReader.class);
        reader.forEachRemaining(
                r -> {
                    assertThat(r.getInt(0)).isEqualTo(1);
                    assertThat(r.getString(1).toString()).isEqualTo("a");
                    assertThat(r.getString(2).toString()).isEqualTo("c");
                });
    }

    @Test
    public void testGlobalIndex() throws Exception {
        write(100000L);

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());

        Predicate predicate =
                new PredicateBuilder(table.rowType()).equal(1, BinaryString.fromString("a100"));

        RoaringNavigableMap64 rowIds = globalIndexScan(table, predicate);
        assertNotNull(rowIds);
        Assertions.assertThat(rowIds.getLongCardinality()).isEqualTo(1);
        Assertions.assertThat(rowIds.toRangeList()).containsExactly(new Range(100L, 100L));

        Predicate predicate2 =
                new PredicateBuilder(table.rowType())
                        .in(
                                1,
                                Arrays.asList(
                                        BinaryString.fromString("a200"),
                                        BinaryString.fromString("a300"),
                                        BinaryString.fromString("a400")));

        rowIds = globalIndexScan(table, predicate2);
        assertNotNull(rowIds);
        Assertions.assertThat(rowIds.getLongCardinality()).isEqualTo(3);
        Assertions.assertThat(rowIds.toRangeList())
                .containsExactlyInAnyOrder(
                        new Range(200L, 200L), new Range(300L, 300L), new Range(400L, 400L));

        DataEvolutionBatchScan scan = (DataEvolutionBatchScan) table.newScan();
        RoaringNavigableMap64 finalRowIds = rowIds;
        scan.withGlobalIndexResult(GlobalIndexResult.create(() -> finalRowIds));

        List<String> readF1 = new ArrayList<>();
        table.newRead()
                .createReader(scan.plan())
                .forEachRemaining(
                        row -> {
                            readF1.add(row.getString(1).toString());
                        });

        Assertions.assertThat(readF1).containsExactly("a200", "a300", "a400");
    }

    @Test
    public void testGlobalIndexWithCoreScan() throws Exception {
        write(100000L);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());

        Predicate predicate =
                new PredicateBuilder(table.rowType())
                        .in(
                                1,
                                Arrays.asList(
                                        BinaryString.fromString("a200"),
                                        BinaryString.fromString("a300"),
                                        BinaryString.fromString("a400")));

        ReadBuilder readBuilder = table.newReadBuilder().withFilter(predicate);

        List<String> readF1 = new ArrayList<>();
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(
                        row -> {
                            readF1.add(row.getString(1).toString());
                        });

        Assertions.assertThat(readF1).containsExactly("a200", "a300", "a400");
    }

    @Test
    public void testCompactCoordinator() throws Exception {
        for (int i = 0; i < 10; i++) {
            write(100000L);
        }
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        // Create coordinator and call plan multiple times
        DataEvolutionCompactCoordinator coordinator =
                new DataEvolutionCompactCoordinator(table, false);

        // Each plan() call processes one manifest group
        List<DataEvolutionCompactTask> allTasks = new ArrayList<>();
        List<DataEvolutionCompactTask> tasks;
        try {
            while (!(tasks = coordinator.plan()).isEmpty() || allTasks.isEmpty()) {
                allTasks.addAll(tasks);
                if (tasks.isEmpty()) {
                    break;
                }
            }
        } catch (EndOfScanException ingore) {

        }

        // Verify no exceptions were thrown and tasks list is valid (may be empty)
        assertThat(allTasks).isNotNull();
        assertThat(allTasks.size()).isEqualTo(1);
        DataEvolutionCompactTask task = allTasks.get(0);
        assertThat(task.compactBefore().size()).isEqualTo(20);
    }

    @Test
    public void testCompact() throws Exception {
        for (int i = 0; i < 5; i++) {
            write(100000L);
        }
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        // Create coordinator and call plan multiple times
        DataEvolutionCompactCoordinator coordinator =
                new DataEvolutionCompactCoordinator(table, false);

        // Each plan() call processes one manifest group
        List<CommitMessage> commitMessages = new ArrayList<>();
        List<DataEvolutionCompactTask> tasks;
        try {
            while (!(tasks = coordinator.plan()).isEmpty()) {
                for (DataEvolutionCompactTask task : tasks) {
                    commitMessages.add(task.doCompact(table, "test-commit"));
                }
            }
        } catch (EndOfScanException ignore) {
        }

        table.newBatchWriteBuilder().newCommit().commit(commitMessages);

        List<ManifestEntry> entries = new ArrayList<>();
        Iterator<ManifestEntry> files = table.newSnapshotReader().readFileIterator();
        while (files.hasNext()) {
            entries.add(files.next());
        }

        assertThat(entries.size()).isEqualTo(1);
        assertThat(entries.get(0).file().nonNullFirstRowId()).isEqualTo(0);
        assertThat(entries.get(0).file().rowCount()).isEqualTo(500000L);
    }

    private void write(long count) throws Exception {
        createTableDefault();

        Schema schema = schemaDefault();
        RowType writeType0 = schema.rowType().project(Arrays.asList("f0", "f1"));
        RowType writeType1 = schema.rowType().project(Collections.singletonList("f2"));
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType0)) {
            for (int i = 0; i < count; i++) {
                write0.write(GenericRow.of(i, BinaryString.fromString("a" + i)));
            }
            BatchTableCommit commit = builder.newCommit();
            commit.commit(write0.prepareCommit());
        }

        long rowId = getTableDefault().snapshotManager().latestSnapshot().nextRowId() - count;
        builder = getTableDefault().newBatchWriteBuilder();
        try (BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {
            for (int i = 0; i < count; i++) {
                write1.write(GenericRow.of(BinaryString.fromString("b" + i)));
            }
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write1.prepareCommit();
            setFirstRowId(commitables, rowId);
            commit.commit(commitables);
        }

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        FileIO fileIO = table.fileIO();
        ReadBuilder readBuilder =
                table.newReadBuilder()
                        .withReadType(
                                SpecialFields.rowTypeWithRowTracking(
                                        table.rowType().project("f1")));
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());

        GlobalIndexFileReadWrite indexFileReadWrite =
                new GlobalIndexFileReadWrite(
                        fileIO,
                        table.store().pathFactory().indexFileFactory(BinaryRow.EMPTY_ROW, 0));

        DataField indexField = table.rowType().getField("f1");

        GlobalIndexerFactory globalIndexerFactory =
                GlobalIndexerFactoryUtils.load(BitmapGlobalIndexerFactory.IDENTIFIER);
        GlobalIndexer globalIndexer = globalIndexerFactory.create(indexField, new Options());
        GlobalIndexSingletonWriter globaIndexBuilder =
                (GlobalIndexSingletonWriter) globalIndexer.createWriter(indexFileReadWrite);

        reader.forEachRemaining(r -> globaIndexBuilder.write(r.getString(0)));

        List<ResultEntry> results = globaIndexBuilder.finish();

        List<IndexFileMeta> indexFileMetaList = new ArrayList<>();
        for (ResultEntry result : results) {
            String fileName = result.fileName();
            long fileSize = fileIO.getFileSize(indexFileReadWrite.filePath(fileName));
            GlobalIndexMeta globalIndexMeta =
                    new GlobalIndexMeta(
                            0, result.rowCount() - 1, indexField.id(), null, result.meta());
            indexFileMetaList.add(
                    new IndexFileMeta(
                            BitmapGlobalIndexerFactory.IDENTIFIER,
                            fileName,
                            fileSize,
                            count,
                            globalIndexMeta));
        }

        DataIncrement dataIncrement = DataIncrement.indexIncrement(indexFileMetaList);

        CommitMessage commitMessage =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        null,
                        dataIncrement,
                        CompactIncrement.emptyIncrement());

        table.newBatchWriteBuilder().newCommit().commit(Collections.singletonList(commitMessage));
    }

    private RoaringNavigableMap64 globalIndexScan(FileStoreTable table, Predicate predicate)
            throws Exception {
        GlobalIndexScanBuilder indexScanBuilder = table.store().newGlobalIndexScanBuilder();
        List<Range> ranges = indexScanBuilder.shardList();
        GlobalIndexResult globalFileIndexResult = GlobalIndexResult.createEmpty();
        for (Range range : ranges) {
            try (RowRangeGlobalIndexScanner scanner =
                    indexScanBuilder.withRowRange(range).build()) {
                Optional<GlobalIndexResult> globalIndexResult = scanner.scan(predicate, null);
                if (!globalIndexResult.isPresent()) {
                    throw new RuntimeException("Can't find index result by scan");
                }
                globalFileIndexResult = globalFileIndexResult.or(globalIndexResult.get());
            }
        }

        return globalFileIndexResult.results();
    }

    protected Schema schemaDefault() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.STRING());
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        return schemaBuilder.build();
    }
}
