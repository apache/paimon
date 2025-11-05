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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
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
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

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
    public void testWithRowIds() throws Exception {
        createTableDefault();
        Schema schema = schemaDefault();
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();

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

        ReadBuilder readBuilder = getTableDefault().newReadBuilder();

        // Test 1: Filter by row IDs that exist in the first file (0, 1)
        List<Long> rowIds1 = Arrays.asList(0L, 1L);
        List<Split> splits1 = readBuilder.withRowIds(rowIds1).newScan().plan().splits();
        assertThat(splits1.size())
                .isEqualTo(1); // Should return one split containing the first file

        // Verify the split contains only the first file (firstRowId=0, rowCount=2)
        DataSplit dataSplit1 = (DataSplit) splits1.get(0);
        assertThat(dataSplit1.dataFiles().size()).isEqualTo(1);
        DataFileMeta file1 = dataSplit1.dataFiles().get(0);
        assertThat(file1.firstRowId()).isEqualTo(0L);
        assertThat(file1.rowCount()).isEqualTo(2L);

        // Test 2: Filter by row IDs that exist in the second file (2, 3)
        List<Long> rowIds2 = Arrays.asList(2L, 3L);
        List<Split> splits2 = readBuilder.withRowIds(rowIds2).newScan().plan().splits();
        assertThat(splits2.size())
                .isEqualTo(1); // Should return one split containing the second file

        // Verify the split contains only the second file (firstRowId=2, rowCount=2)
        DataSplit dataSplit2 = (DataSplit) splits2.get(0);
        assertThat(dataSplit2.dataFiles().size()).isEqualTo(1);
        DataFileMeta file2 = dataSplit2.dataFiles().get(0);
        assertThat(file2.firstRowId()).isEqualTo(2L);
        assertThat(file2.rowCount()).isEqualTo(2L);

        // Test 3: Filter by row IDs that exist in the third file (4, 5)
        List<Long> rowIds3 = Arrays.asList(4L, 5L);
        List<Split> splits3 = readBuilder.withRowIds(rowIds3).newScan().plan().splits();
        assertThat(splits3.size())
                .isEqualTo(1); // Should return one split containing the third file

        // Verify the split contains only the third file (firstRowId=4, rowCount=2)
        DataSplit dataSplit3 = (DataSplit) splits3.get(0);
        assertThat(dataSplit3.dataFiles().size()).isEqualTo(1);
        DataFileMeta file3 = dataSplit3.dataFiles().get(0);
        assertThat(file3.firstRowId()).isEqualTo(4L);
        assertThat(file3.rowCount()).isEqualTo(2L);

        // Test 4: Filter by row IDs that span multiple files (1, 2, 4)
        List<Long> rowIds4 = Arrays.asList(0L, 1L, 4L);
        List<Split> splits4 = readBuilder.withRowIds(rowIds4).newScan().plan().splits();
        assertThat(splits4.size())
                .isEqualTo(1); // Should return one split containing all matching files

        // Verify the split contains all three files (firstRowId=0,2,4)
        DataSplit dataSplit4 = (DataSplit) splits4.get(0);
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
        List<Long> rowIds5 = Arrays.asList(10L, 11L);
        List<Split> splits5 = readBuilder.withRowIds(rowIds5).newScan().plan().splits();
        assertThat(splits5.size()).isEqualTo(0); // Should return no files

        // Test 6: Filter by null indices (should return all files)
        List<Split> splits6 = readBuilder.withRowIds(null).newScan().plan().splits();
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
                readBuilder.withRowIds(Collections.emptyList()).newScan().plan().splits();
        assertThat(splits7.size()).isEqualTo(0); // Should return no files

        // Test 8: Filter by row IDs that partially exist (0, 1, 10)
        List<Long> rowIds8 = Arrays.asList(0L, 1L, 10L);
        List<Split> splits8 = readBuilder.withRowIds(rowIds8).newScan().plan().splits();
        assertThat(splits8.size())
                .isEqualTo(1); // Should return one split containing the first file

        // Verify the split contains only the first file (firstRowId=0)
        DataSplit dataSplit8 = (DataSplit) splits8.get(0);
        assertThat(dataSplit8.dataFiles().size()).isEqualTo(1);
        DataFileMeta file8 = dataSplit8.dataFiles().get(0);
        assertThat(file8.firstRowId()).isEqualTo(0L);
        assertThat(file8.rowCount()).isEqualTo(2L);

        List<Long> rowIds9 = Arrays.asList(0L, 2L);
        List<Split> splits9 = readBuilder.withRowIds(rowIds9).newScan().plan().splits();

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
