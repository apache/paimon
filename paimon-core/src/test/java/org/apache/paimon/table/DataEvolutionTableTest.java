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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.DataEvolutionFileReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Test for table with data evolution. */
public class DataEvolutionTableTest extends DataEvolutionTestBase {

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
        TableScan.Plan plan = readBuilder.newScan().plan();

        // assert merged row count
        long noMergedRowCount = plan.splits().stream().mapToLong(Split::rowCount).sum();
        long mergedRowCount =
                plan.splits().stream()
                        .map(Split::mergedRowCount)
                        .filter(OptionalLong::isPresent)
                        .mapToLong(OptionalLong::getAsLong)
                        .sum();
        assertThat(noMergedRowCount).isEqualTo(2);
        assertThat(mergedRowCount).isEqualTo(1);

        RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan);
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
        reader.forEachRemaining(r -> cnt.incrementAndGet());
        assertThat(cnt.get()).isEqualTo(1);

        // projection with an empty read type
        readBuilder = getTableDefault().newReadBuilder();
        reader =
                readBuilder
                        .withReadType(RowType.of())
                        .newRead()
                        .createReader(readBuilder.newScan().plan());
        AtomicInteger cnt1 = new AtomicInteger(0);
        reader.forEachRemaining(r -> cnt1.incrementAndGet());
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
    public void testLimitPushDownWithoutFilter() throws Exception {
        createTableDefault();
        Schema schema = schemaDefault();
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        RowType writeType = schema.rowType().project(Arrays.asList("f0", "f1"));

        // Write three commits through normal path and verify limit pushdown can reduce scanned
        // files.
        for (int i = 0; i < 30; i++) {
            try (BatchTableWrite write = builder.newWrite().withWriteType(writeType)) {
                write.write(GenericRow.of(i * 2 + 1, BinaryString.fromString("v" + (i * 2 + 1))));
                write.write(GenericRow.of(i * 2 + 2, BinaryString.fromString("v" + (i * 2 + 2))));

                BatchTableCommit commit = builder.newCommit();
                commit.commit(write.prepareCommit());
            }
        }

        for (int i = 0; i < 2; i++) {
            try (BatchTableWrite write =
                    builder.newWrite().withWriteType(writeType.project("f1"))) {
                write.write(GenericRow.of(BinaryString.fromString("v" + (i * 2 + 1))));
                write.write(GenericRow.of(BinaryString.fromString("v" + (i * 2 + 2))));

                BatchTableCommit commit = builder.newCommit();
                List<CommitMessage> commitMessages = write.prepareCommit();
                setFirstRowId(commitMessages, 0L);
                commit.commit(commitMessages);
            }
        }

        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        TableScan.Plan fullPlan = readBuilder.newScan().plan();
        TableScan.Plan limitPlan = readBuilder.withLimit(3).newScan().plan();

        int fullFiles =
                fullPlan.splits().stream()
                        .map(split -> (DataSplit) split)
                        .mapToInt(split -> split.dataFiles().size())
                        .sum();
        int limitedFiles =
                limitPlan.splits().stream()
                        .map(split -> (DataSplit) split)
                        .mapToInt(split -> split.dataFiles().size())
                        .sum();

        assertThat(fullFiles).isEqualTo(32);
        assertThat(limitedFiles).isEqualTo(4);
        assertThat(limitedFiles).isLessThan(fullFiles);
    }

    @Test
    public void testLimitPushDownWithFilterShouldNotEarlyStop() throws Exception {
        createTableDefault();
        Schema schema = schemaDefault();
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        RowType writeType = schema.rowType().project(Arrays.asList("f0", "f1"));

        // Three manifests with different values on f1, only the last one matches filter.
        for (int i = 0; i < 3; i++) {
            String value = i == 0 ? "a" : (i == 1 ? "b" : "c");
            try (BatchTableWrite write = builder.newWrite().withWriteType(writeType)) {
                write.write(GenericRow.of(i * 2 + 1, BinaryString.fromString(value)));
                write.write(GenericRow.of(i * 2 + 2, BinaryString.fromString(value)));

                BatchTableCommit commit = builder.newCommit();
                commit.commit(write.prepareCommit());
            }
        }

        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        PredicateBuilder predicateBuilder = new PredicateBuilder(schema.rowType());
        Predicate predicate = predicateBuilder.equal(1, BinaryString.fromString("c"));

        TableScan.Plan plan = readBuilder.withFilter(predicate).withLimit(1).newScan().plan();
        assertThat(plan.splits().isEmpty()).isFalse();

        RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan);
        AtomicInteger rowCount = new AtomicInteger(0);
        reader.forEachRemaining(
                row -> {
                    assertThat(row.getString(1).toString()).isEqualTo("c");
                    rowCount.incrementAndGet();
                });
        assertThat(rowCount.get()).isGreaterThan(0);
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
    public void testPartitionGroupedRowIdsWithBlobFiles() throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("pt", DataTypes.STRING());
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.BLOB());
        schemaBuilder.partitionKeys("pt");
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "128 MB");
        schemaBuilder.option(CoreOptions.BLOB_TARGET_FILE_SIZE.key(), "1 b");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_PARTITION_GROUP_ON_COMMIT.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");

        catalog.createTable(identifier(), schemaBuilder.build(), true);
        FileStoreTable table = getTableDefault();
        BatchWriteBuilder builder = table.newBatchWriteBuilder();

        byte[] blobBytes = new byte[1];
        Arrays.fill(blobBytes, (byte) 1);
        String[] partitionOrder =
                new String[] {
                    "pt1", "pt0", "pt2", "pt1", "pt2", "pt0",
                    "pt0", "pt2", "pt1", "pt2", "pt0", "pt1",
                    "pt1", "pt2", "pt0", "pt0", "pt1", "pt2"
                };

        try (BatchTableWrite write = builder.newWrite()) {
            for (int i = 0; i < partitionOrder.length; i++) {
                write.write(
                        GenericRow.of(
                                BinaryString.fromString(partitionOrder[i]),
                                i,
                                BinaryString.fromString("v" + i),
                                new BlobData(blobBytes)));
            }

            try (BatchTableCommit commit = builder.newCommit()) {
                commit.commit(write.prepareCommit());
            }
        }

        Map<String, List<DataFileMeta>> filesByPartition = new HashMap<>();
        for (ManifestEntry entry : table.store().newScan().plan().files()) {
            String partition = entry.partition().getString(0).toString();
            filesByPartition.computeIfAbsent(partition, k -> new ArrayList<>()).add(entry.file());
        }

        assertThat(filesByPartition.size()).isEqualTo(3);
        for (List<DataFileMeta> files : filesByPartition.values()) {
            List<DataFileMeta> regularFiles =
                    files.stream()
                            .filter(file -> !"blob".equals(file.fileFormat()))
                            .collect(Collectors.toList());
            List<DataFileMeta> blobFiles =
                    files.stream()
                            .filter(file -> "blob".equals(file.fileFormat()))
                            .collect(Collectors.toList());

            assertThat(regularFiles.size()).isEqualTo(1);
            assertThat(blobFiles.size()).isGreaterThan(1);

            Range regularRange = assertContinuousRowIdRange(regularFiles);
            Range blobRange = assertContinuousRowIdRange(blobFiles);
            assertThat(regularRange.count()).isEqualTo(6L);
            assertThat(blobRange).isEqualTo(regularRange);
        }
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
    public void testCompactCoordinator() throws Exception {
        for (int i = 0; i < 10; i++) {
            write(100000L);
        }
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        // Create coordinator and call plan multiple times
        DataEvolutionCompactCoordinator coordinator =
                new DataEvolutionCompactCoordinator(table, false, false);

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
                new DataEvolutionCompactCoordinator(table, false, false);

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

    @Test
    public void testIndexPath() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        IndexPathFactory indexPathFactory = table.store().pathFactory().globalIndexFileFactory();

        Path path0 = indexPathFactory.toPath("test-file");
        assertThat(path0.toString().contains(warehouse.toString())).isTrue();
        String testExternalpath = "file:/external/path/test-file-dir";
        Path path1 =
                indexPathFactory.toPath(
                        new IndexFileMeta(
                                "test-type",
                                "test-file",
                                1024L,
                                100L,
                                null,
                                testExternalpath,
                                null));
        assertThat(path1.toString()).isEqualTo(testExternalpath);

        table =
                table.copy(
                        Collections.singletonMap(
                                CoreOptions.GLOBAL_INDEX_EXTERNAL_PATH.key(), testExternalpath));

        indexPathFactory = table.store().pathFactory().globalIndexFileFactory();
        Path path3 = indexPathFactory.toPath("test-file");
        assertThat(path3.toString()).isEqualTo(testExternalpath + "/test-file");

        String testExternalpath2 = "file:/external/path2/test-file";
        Path path4 =
                indexPathFactory.toPath(
                        new IndexFileMeta(
                                "test-type",
                                "test-file",
                                1024L,
                                100L,
                                null,
                                testExternalpath2,
                                null));
        assertThat(path4.toString()).isEqualTo(testExternalpath2);
    }

    @Test
    public void testProjectionPushdown() throws Exception {
        createTableDefault();
        Schema schema = schemaDefault();
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();

        // Write f0 and f1 together
        RowType writeType0 = schema.rowType().project(Arrays.asList("f0", "f1"));
        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType0)) {
            write0.write(GenericRow.of(1, BinaryString.fromString("a")));
            write0.write(GenericRow.of(2, BinaryString.fromString("b")));
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write0.prepareCommit();
            commit.commit(commitables);
        }

        // Write f2 separately with same firstRowId
        RowType writeType1 = schema.rowType().project(Collections.singletonList("f2"));
        try (BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {
            write1.write(GenericRow.of(BinaryString.fromString("x")));
            write1.write(GenericRow.of(BinaryString.fromString("y")));
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write1.prepareCommit();
            setFirstRowId(commitables, 0L);
            commit.commit(commitables);
        }

        // Project only f0 - should filter out the f2-only file
        ReadBuilder readBuilder = getTableDefault().newReadBuilder().withProjection(new int[] {0});
        TableScan.Plan plan = readBuilder.newScan().plan();
        DataSplit dataSplit = (DataSplit) plan.splits().get(0);
        assertThat(dataSplit.dataFiles().size()).isEqualTo(1);
        RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan);
        List<InternalRow> rows = new ArrayList<>();
        reader.forEachRemaining(rows::add);
        assertThat(rows.size()).isEqualTo(2);

        // Project only f2 - should filter out the f0,f1-only file
        readBuilder = getTableDefault().newReadBuilder().withProjection(new int[] {2});
        plan = readBuilder.newScan().plan();
        dataSplit = (DataSplit) plan.splits().get(0);
        assertThat(dataSplit.dataFiles().size()).isEqualTo(1);
        reader = readBuilder.newRead().createReader(plan);
        rows = new ArrayList<>();
        reader.forEachRemaining(rows::add);
        assertThat(rows.size()).isEqualTo(2);

        // Project f0 and f2 (skip f1) - needs both files
        readBuilder = getTableDefault().newReadBuilder().withProjection(new int[] {0, 2});
        plan = readBuilder.newScan().plan();
        dataSplit = (DataSplit) plan.splits().get(0);
        assertThat(dataSplit.dataFiles().size()).isEqualTo(2);
        reader = readBuilder.newRead().createReader(plan);
        RowType projectedType = schema.rowType().project(Arrays.asList("f0", "f2"));
        InternalRowSerializer serializer = new InternalRowSerializer(projectedType);
        List<InternalRow> projectedRows = new ArrayList<>();
        reader.forEachRemaining(r -> projectedRows.add(serializer.copy(r)));
        assertThat(projectedRows.size()).isEqualTo(2);
        projectedRows.sort(Comparator.comparingInt(r -> r.getInt(0)));
        assertThat(projectedRows.get(0).getInt(0)).isEqualTo(1);
        assertThat(projectedRows.get(0).getString(1).toString()).isEqualTo("x");
        assertThat(projectedRows.get(1).getInt(0)).isEqualTo(2);
        assertThat(projectedRows.get(1).getString(1).toString()).isEqualTo("y");
    }

    @Test
    public void testSequenceNumberConflictResolution() throws Exception {
        createTableDefault();
        Schema schema = schemaDefault();
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();

        // Write all columns first
        try (BatchTableWrite write = builder.newWrite().withWriteType(schema.rowType())) {
            write.write(
                    GenericRow.of(1, BinaryString.fromString("a"), BinaryString.fromString("old")));
            write.write(
                    GenericRow.of(2, BinaryString.fromString("b"), BinaryString.fromString("old")));
            BatchTableCommit commit = builder.newCommit();
            commit.commit(write.prepareCommit());
        }

        // Overwrite f2 with a new value (higher sequence number)
        RowType writeType1 = schema.rowType().project(Collections.singletonList("f2"));
        try (BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {
            write1.write(GenericRow.of(BinaryString.fromString("new")));
            write1.write(GenericRow.of(BinaryString.fromString("new")));
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write1.prepareCommit();
            setFirstRowId(commitables, 0L);
            commit.commit(commitables);
        }

        // Read and verify f2 shows the new value (higher seq number wins)
        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        InternalRowSerializer serializer = new InternalRowSerializer(schema.rowType());
        List<InternalRow> rows = new ArrayList<>();
        reader.forEachRemaining(r -> rows.add(serializer.copy(r)));
        assertThat(rows.size()).isEqualTo(2);
        rows.sort(Comparator.comparingInt(r -> r.getInt(0)));
        assertThat(rows.get(0).getInt(0)).isEqualTo(1);
        assertThat(rows.get(0).getString(1).toString()).isEqualTo("a");
        assertThat(rows.get(0).getString(2).toString()).isEqualTo("new");
        assertThat(rows.get(1).getInt(0)).isEqualTo(2);
        assertThat(rows.get(1).getString(1).toString()).isEqualTo("b");
        assertThat(rows.get(1).getString(2).toString()).isEqualTo("new");
    }

    @Test
    public void testMultipleOverwritesSameColumn() throws Exception {
        createTableDefault();
        Schema schema = schemaDefault();
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();

        // Write initial data with all columns
        try (BatchTableWrite write = builder.newWrite().withWriteType(schema.rowType())) {
            write.write(
                    GenericRow.of(
                            1, BinaryString.fromString("a"), BinaryString.fromString("version1")));
            BatchTableCommit commit = builder.newCommit();
            commit.commit(write.prepareCommit());
        }

        // Overwrite f2 - second time
        RowType writeType1 = schema.rowType().project(Collections.singletonList("f2"));
        try (BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {
            write1.write(GenericRow.of(BinaryString.fromString("version2")));
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write1.prepareCommit();
            setFirstRowId(commitables, 0L);
            commit.commit(commitables);
        }

        // Overwrite f2 - third time
        try (BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {
            write1.write(GenericRow.of(BinaryString.fromString("version3")));
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write1.prepareCommit();
            setFirstRowId(commitables, 0L);
            commit.commit(commitables);
        }

        // Read and verify only the latest version is visible
        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        List<InternalRow> rows = new ArrayList<>();
        reader.forEachRemaining(rows::add);
        assertThat(rows.size()).isEqualTo(1);
        assertThat(rows.get(0).getInt(0)).isEqualTo(1);
        assertThat(rows.get(0).getString(1).toString()).isEqualTo("a");
        assertThat(rows.get(0).getString(2).toString()).isEqualTo("version3");
    }

    @Test
    public void testCompactThenReadCorrectness() throws Exception {
        for (int i = 0; i < 5; i++) {
            write(100000L);
        }
        FileStoreTable table = getTableDefault();

        // Run compaction
        DataEvolutionCompactCoordinator coordinator =
                new DataEvolutionCompactCoordinator(table, false, false);
        List<CommitMessage> commitMessages = new ArrayList<>();
        List<DataEvolutionCompactTask> tasks;
        try {
            while (!(tasks = coordinator.plan()).isEmpty()) {
                for (DataEvolutionCompactTask task : tasks) {
                    commitMessages.add(task.doCompact(table, "test-compact"));
                }
            }
        } catch (EndOfScanException ignore) {
        }
        assertThat(commitMessages.isEmpty()).isFalse();
        table.newBatchWriteBuilder().newCommit().commit(commitMessages);

        // Verify data after compaction
        Schema schema = schemaDefault();
        table = getTableDefault();
        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().plan();
        RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan);
        InternalRowSerializer serializer = new InternalRowSerializer(schema.rowType());
        List<InternalRow> rowsAfter = new ArrayList<>();
        reader.forEachRemaining(r -> rowsAfter.add(serializer.copy(r)));
        assertThat(rowsAfter.size()).isEqualTo(500000);

        // Each write produces rows with f0=0..99999, so 5 writes gives 5 copies of each
        rowsAfter.sort(Comparator.comparingInt(r -> r.getInt(0)));
        // First 5 rows should all have f0=0 with correct f1 and f2
        for (int i = 0; i < 5; i++) {
            assertThat(rowsAfter.get(i).getInt(0)).isEqualTo(0);
            assertThat(rowsAfter.get(i).getString(1).toString()).isEqualTo("a0");
            assertThat(rowsAfter.get(i).getString(2).toString()).isEqualTo("b0");
        }
        // Spot check other values
        for (int i = 5; i < 10; i++) {
            assertThat(rowsAfter.get(i).getInt(0)).isEqualTo(1);
            assertThat(rowsAfter.get(i).getString(1).toString()).isEqualTo("a1");
            assertThat(rowsAfter.get(i).getString(2).toString()).isEqualTo("b1");
        }

        // After compaction, only 1 file should remain
        assertThat(plan.splits().size()).isEqualTo(1);
        DataSplit dataSplit = (DataSplit) plan.splits().get(0);
        assertThat(dataSplit.dataFiles().size()).isEqualTo(1);
    }

    @Test
    public void testStreamingRead() throws Exception {
        createTableDefault();
        Schema schema = schemaDefault();
        FileStoreTable table = getTableDefault();
        BatchWriteBuilder builder = table.newBatchWriteBuilder();

        ReadBuilder readBuilder = table.newReadBuilder();
        StreamTableScan streamScan = readBuilder.newStreamScan();

        // Initial plan should be empty (no snapshots yet), or return existing data
        // Write first batch - full row
        try (BatchTableWrite write = builder.newWrite().withWriteType(schema.rowType())) {
            write.write(
                    GenericRow.of(1, BinaryString.fromString("a"), BinaryString.fromString("b")));
            BatchTableCommit commit = builder.newCommit();
            commit.commit(write.prepareCommit());
        }

        // Streaming plan should return the new data
        TableScan.Plan plan = streamScan.plan();
        RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan);
        List<InternalRow> rows = new ArrayList<>();
        reader.forEachRemaining(rows::add);
        assertThat(rows.size()).isEqualTo(1);
        assertThat(rows.get(0).getInt(0)).isEqualTo(1);
        assertThat(rows.get(0).getString(1).toString()).isEqualTo("a");
        assertThat(rows.get(0).getString(2).toString()).isEqualTo("b");

        // Write a partial-column append for new row
        RowType writeType0 = schema.rowType().project(Arrays.asList("f0", "f1"));
        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType0)) {
            write0.write(GenericRow.of(2, BinaryString.fromString("c")));
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write0.prepareCommit();
            setFirstRowId(commitables, 1L);
            commit.commit(commitables);
        }

        // Streaming plan returns only the new incremental data
        plan = streamScan.plan();
        reader = readBuilder.newRead().createReader(plan);
        List<InternalRow> rows2 = new ArrayList<>();
        reader.forEachRemaining(rows2::add);
        assertThat(rows2.size()).isEqualTo(1);
        assertThat(rows2.get(0).getInt(0)).isEqualTo(2);
        assertThat(rows2.get(0).getString(1).toString()).isEqualTo("c");
        assertThat(rows2.get(0).isNullAt(2)).isTrue();
    }

    @Test
    public void testPartitionedTable() throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("pt", DataTypes.STRING());
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.STRING());
        schemaBuilder.partitionKeys("pt");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_PARTITION_GROUP_ON_COMMIT.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");

        catalog.createTable(identifier(), schemaBuilder.build(), true);
        FileStoreTable table = getTableDefault();
        BatchWriteBuilder builder = table.newBatchWriteBuilder();

        RowType fullType = table.rowType();
        RowType writeType0 = fullType.project(Arrays.asList("pt", "f0", "f1"));
        RowType writeType1 = fullType.project(Arrays.asList("pt", "f2"));

        // Write f0, f1 for partition p1 only
        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType0)) {
            write0.write(
                    GenericRow.of(BinaryString.fromString("p1"), 1, BinaryString.fromString("a")));
            write0.write(
                    GenericRow.of(BinaryString.fromString("p1"), 2, BinaryString.fromString("b")));
            BatchTableCommit commit = builder.newCommit();
            commit.commit(write0.prepareCommit());
        }

        long p1RowId =
                table.snapshotManager().latestSnapshot().nextRowId() - 2; // 2 rows written for p1

        // Write f2 for p1 with matching firstRowId
        try (BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {
            write1.write(
                    GenericRow.of(BinaryString.fromString("p1"), BinaryString.fromString("x")));
            write1.write(
                    GenericRow.of(BinaryString.fromString("p1"), BinaryString.fromString("y")));
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write1.prepareCommit();
            setFirstRowId(commitables, p1RowId);
            commit.commit(commitables);
        }

        // Write f0, f1 for partition p2
        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType0)) {
            write0.write(
                    GenericRow.of(BinaryString.fromString("p2"), 3, BinaryString.fromString("c")));
            BatchTableCommit commit = builder.newCommit();
            commit.commit(write0.prepareCommit());
        }

        long p2RowId =
                table.snapshotManager().latestSnapshot().nextRowId() - 1; // 1 row written to p2

        // Write f2 for p2 with matching firstRowId
        try (BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {
            write1.write(
                    GenericRow.of(BinaryString.fromString("p2"), BinaryString.fromString("z")));
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write1.prepareCommit();
            setFirstRowId(commitables, p2RowId);
            commit.commit(commitables);
        }

        // Read all and verify
        ReadBuilder readBuilder = table.newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        InternalRowSerializer serializer = new InternalRowSerializer(fullType);
        List<InternalRow> rows = new ArrayList<>();
        reader.forEachRemaining(r -> rows.add(serializer.copy(r)));
        rows.sort(Comparator.comparingInt(r -> r.getInt(1)));

        assertThat(rows.size()).isEqualTo(3);
        // p1 rows
        assertThat(rows.get(0).getString(0).toString()).isEqualTo("p1");
        assertThat(rows.get(0).getInt(1)).isEqualTo(1);
        assertThat(rows.get(0).getString(2).toString()).isEqualTo("a");
        assertThat(rows.get(0).getString(3).toString()).isEqualTo("x");

        assertThat(rows.get(1).getString(0).toString()).isEqualTo("p1");
        assertThat(rows.get(1).getInt(1)).isEqualTo(2);
        assertThat(rows.get(1).getString(2).toString()).isEqualTo("b");
        assertThat(rows.get(1).getString(3).toString()).isEqualTo("y");

        // p2 row
        assertThat(rows.get(2).getString(0).toString()).isEqualTo("p2");
        assertThat(rows.get(2).getInt(1)).isEqualTo(3);
        assertThat(rows.get(2).getString(2).toString()).isEqualTo("c");
        assertThat(rows.get(2).getString(3).toString()).isEqualTo("z");
    }

    @Test
    public void testSchemaEvolution() throws Exception {
        createTableDefault();
        Schema schema = schemaDefault();
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();

        // Write initial data with original schema (f0, f1, f2)
        try (BatchTableWrite write = builder.newWrite().withWriteType(schema.rowType())) {
            write.write(
                    GenericRow.of(1, BinaryString.fromString("a"), BinaryString.fromString("b")));
            BatchTableCommit commit = builder.newCommit();
            commit.commit(write.prepareCommit());
        }

        // Add a new column f3
        catalog.alterTable(identifier(), SchemaChange.addColumn("f3", DataTypes.STRING()), false);

        // Reload table to pick up new schema
        FileStoreTable table = getTableDefault();
        builder = table.newBatchWriteBuilder();

        // Write data with new schema (f0, f1, f2, f3)
        try (BatchTableWrite write = builder.newWrite()) {
            write.write(
                    GenericRow.of(
                            2,
                            BinaryString.fromString("c"),
                            BinaryString.fromString("d"),
                            BinaryString.fromString("e")));
            BatchTableCommit commit = builder.newCommit();
            commit.commit(write.prepareCommit());
        }

        // Read and verify - old rows should have null for f3
        ReadBuilder readBuilder = table.newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        List<InternalRow> rows = new ArrayList<>();
        reader.forEachRemaining(rows::add);
        rows.sort(Comparator.comparingInt(r -> r.getInt(0)));

        assertThat(rows.size()).isEqualTo(2);
        // Old row - f3 should be null
        assertThat(rows.get(0).getInt(0)).isEqualTo(1);
        assertThat(rows.get(0).getString(1).toString()).isEqualTo("a");
        assertThat(rows.get(0).getString(2).toString()).isEqualTo("b");
        assertThat(rows.get(0).isNullAt(3)).isTrue();

        // New row - all columns present
        assertThat(rows.get(1).getInt(0)).isEqualTo(2);
        assertThat(rows.get(1).getString(1).toString()).isEqualTo("c");
        assertThat(rows.get(1).getString(2).toString()).isEqualTo("d");
        assertThat(rows.get(1).getString(3).toString()).isEqualTo("e");
    }

    @Test
    public void testReadAfterMultipleAppendsToDifferentColumnSets() throws Exception {
        createTableDefault();
        Schema schema = schemaDefault();
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();

        // Commit 1: Write only f0 for row 0
        RowType writeType0 = schema.rowType().project(Collections.singletonList("f0"));
        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType0)) {
            write0.write(GenericRow.of(1));
            BatchTableCommit commit = builder.newCommit();
            commit.commit(write0.prepareCommit());
        }

        // Commit 2: Write only f1 for row 1 (different row)
        RowType writeType1 = schema.rowType().project(Collections.singletonList("f1"));
        try (BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {
            write1.write(GenericRow.of(BinaryString.fromString("a")));
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write1.prepareCommit();
            setFirstRowId(commitables, 1L);
            commit.commit(commitables);
        }

        // Commit 3: Write only f2 for row 2 (different row)
        RowType writeType2 = schema.rowType().project(Collections.singletonList("f2"));
        try (BatchTableWrite write2 = builder.newWrite().withWriteType(writeType2)) {
            write2.write(GenericRow.of(BinaryString.fromString("b")));
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write2.prepareCommit();
            setFirstRowId(commitables, 2L);
            commit.commit(commitables);
        }

        // Read all rows - each row should have only its written column with others null
        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        List<InternalRow> rows = new ArrayList<>();
        reader.forEachRemaining(rows::add);

        assertThat(rows.size()).isEqualTo(3);

        // Row 0: only f0 is set
        assertThat(rows.get(0).getInt(0)).isEqualTo(1);
        assertThat(rows.get(0).isNullAt(1)).isTrue();
        assertThat(rows.get(0).isNullAt(2)).isTrue();

        // Row 1: only f1 is set
        assertThat(rows.get(1).isNullAt(0)).isTrue();
        assertThat(rows.get(1).getString(1).toString()).isEqualTo("a");
        assertThat(rows.get(1).isNullAt(2)).isTrue();

        // Row 2: only f2 is set
        assertThat(rows.get(2).isNullAt(0)).isTrue();
        assertThat(rows.get(2).isNullAt(1)).isTrue();
        assertThat(rows.get(2).getString(2).toString()).isEqualTo("b");
    }

    /**
     * Central repro for the ADD COLUMN bug fixed in this change. Pre-ALTER files do not carry the
     * new column physically; {@code WHERE new_col IS NULL} must match every pre-ALTER row. Before
     * the fix, the single-entry filterByStats dropped pre-ALTER files at the manifest layer and the
     * predicate returned zero rows.
     */
    @Test
    public void testAddColumnIsNullKeepsPreAlterRows() throws Exception {
        createTableDefault();
        Schema schema = schemaDefault();

        // Pre-ALTER write: only (f0, f1).
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        RowType writeF0F1 = schema.rowType().project(Arrays.asList("f0", "f1"));
        try (BatchTableWrite write = builder.newWrite().withWriteType(writeF0F1)) {
            for (int i = 0; i < 5; i++) {
                write.write(GenericRow.of(i, BinaryString.fromString("a" + i)));
            }
            builder.newCommit().commit(write.prepareCommit());
        }

        // ADD COLUMN f3 (post-ALTER) and write a full-schema row at a fresh row id.
        catalog.alterTable(identifier(), SchemaChange.addColumn("f3", DataTypes.STRING()), false);
        FileStoreTable table = getTableDefault();
        builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite()) {
            for (int i = 5; i < 10; i++) {
                write.write(
                        GenericRow.of(
                                i,
                                BinaryString.fromString("a" + i),
                                BinaryString.fromString("c" + i),
                                BinaryString.fromString("e" + i)));
            }
            builder.newCommit().commit(write.prepareCommit());
        }

        // WHERE f3 IS NULL -> pre-ALTER rows (5 of them).
        PredicateBuilder pb = new PredicateBuilder(table.rowType());
        int f3Idx = table.rowType().getFieldIndex("f3");
        ReadBuilder rb = table.newReadBuilder().withFilter(pb.isNull(f3Idx));
        assertThat(countMatchingRows(rb)).isEqualTo(5);
    }

    /**
     * Predicate-aware stats pruning for ADD COLUMN: WHERE new_col = 'something' cannot match
     * pre-ALTER rows (their new_col is implicit NULL), so the pre-ALTER manifest must be pruned at
     * planning time. The all-NULL encoding in EvolutionStats / DataEvolutionArray makes
     * LeafPredicate.test drop the file via the leaf's normal decision instead of falling back to
     * "unknown stats -> keep".
     */
    @Test
    public void testAddColumnEqualityPredicatePrunesPreAlterFiles() throws Exception {
        createTableDefault();
        Schema schema = schemaDefault();

        // Pre-ALTER write: only (f0, f1).
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        RowType writeF0F1 = schema.rowType().project(Arrays.asList("f0", "f1"));
        try (BatchTableWrite write = builder.newWrite().withWriteType(writeF0F1)) {
            for (int i = 0; i < 5; i++) {
                write.write(GenericRow.of(i, BinaryString.fromString("a" + i)));
            }
            builder.newCommit().commit(write.prepareCommit());
        }

        catalog.alterTable(identifier(), SchemaChange.addColumn("f3", DataTypes.STRING()), false);
        FileStoreTable table = getTableDefault();
        builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite()) {
            for (int i = 5; i < 10; i++) {
                write.write(
                        GenericRow.of(
                                i,
                                BinaryString.fromString("a" + i),
                                BinaryString.fromString("c" + i),
                                BinaryString.fromString("e" + i)));
            }
            builder.newCommit().commit(write.prepareCommit());
        }

        // Total files on the table.
        assertThat(plannedFileCount(table, null, null)).isEqualTo(2);

        // WHERE f3 = 'e7' -> only the post-ALTER file can match. The pre-ALTER file is
        // pruned at planning because EvolutionStats encodes its missing f3 as all-NULL,
        // letting LeafPredicate.test evaluate Equal against (min=null, max=null,
        // nullCount=rowCount) and return false instead of falling through to
        // "unknown stats -> keep".
        PredicateBuilder pb = new PredicateBuilder(table.rowType());
        int f3Idx = table.rowType().getFieldIndex("f3");
        Predicate filter = pb.equal(f3Idx, BinaryString.fromString("e7"));
        assertThat(plannedFileCount(table, null, filter)).isEqualTo(1);
    }

    /**
     * Central repro for the RENAME COLUMN bug fixed in this change. The renamed field's id is
     * preserved across schemas, so a predicate on the latest name must still match rows in the
     * pre-rename file (whose physical writeCols carry the old name). Before the fix, the
     * single-entry filterByStats compared by name and dropped pre-rename files at the manifest
     * layer.
     */
    @Test
    public void testRenameColumnPredicateKeepsPreRenameRows() throws Exception {
        createTableDefault();
        Schema schema = schemaDefault();

        // Pre-rename write: f2 carries the values that will later be queried as f3.
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withWriteType(schema.rowType())) {
            for (int i = 0; i < 5; i++) {
                write.write(
                        GenericRow.of(
                                i,
                                BinaryString.fromString("a" + i),
                                BinaryString.fromString("preR_" + i)));
            }
            builder.newCommit().commit(write.prepareCommit());
        }

        catalog.alterTable(identifier(), SchemaChange.renameColumn("f2", "f3"), false);
        FileStoreTable table = getTableDefault();
        builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite()) {
            for (int i = 5; i < 10; i++) {
                write.write(
                        GenericRow.of(
                                i,
                                BinaryString.fromString("a" + i),
                                BinaryString.fromString("postR_" + i)));
            }
            builder.newCommit().commit(write.prepareCommit());
        }

        // WHERE f3 LIKE 'preR_%' -> rows from the pre-rename file (5 rows).
        PredicateBuilder pb = new PredicateBuilder(table.rowType());
        int f3Idx = table.rowType().getFieldIndex("f3");
        ReadBuilder rb =
                table.newReadBuilder()
                        .withFilter(pb.startsWith(f3Idx, BinaryString.fromString("preR_")));
        assertThat(countMatchingRows(rb)).isEqualTo(5);
    }

    /**
     * Columnar-split: two files cover the same row id range, each carrying a different subset of
     * columns. A query that projects only columns owned by one file should not read the other.
     */
    @Test
    public void testNoFilterProjectionPrunesColumnarSplitFiles() throws Exception {
        write(5);
        FileStoreTable table = getTableDefault();
        Schema schema = schemaDefault();
        assertThat(plannedFileCount(table, null, null)).isEqualTo(2);

        RowType readF0 = schema.rowType().project(Collections.singletonList("f0"));
        assertThat(plannedFileCount(table, readF0, null)).isEqualTo(1);

        RowType readF1 = schema.rowType().project(Collections.singletonList("f1"));
        assertThat(plannedFileCount(table, readF1, null)).isEqualTo(1);

        RowType readF2 = schema.rowType().project(Collections.singletonList("f2"));
        assertThat(plannedFileCount(table, readF2, null)).isEqualTo(1);

        RowType readF0F2 = schema.rowType().project(Arrays.asList("f0", "f2"));
        assertThat(plannedFileCount(table, readF0F2, null)).isEqualTo(2);

        assertThat(plannedFileCount(table, schema.rowType(), null)).isEqualTo(2);
    }

    /**
     * Row-disjoint pre-ALTER files must not be dropped by the column-pruning logic — the reader
     * needs them to emit rowCount NULL-filled rows for the projection.
     */
    @Test
    public void testNoFilterProjectionKeepsRowDisjointFiles() throws Exception {
        createTableDefault();
        Schema schema = schemaDefault();
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        RowType writeType = schema.rowType().project(Arrays.asList("f0", "f1"));
        try (BatchTableWrite write = builder.newWrite().withWriteType(writeType)) {
            for (int i = 0; i < 5; i++) {
                write.write(GenericRow.of(i, BinaryString.fromString("a" + i)));
            }
            builder.newCommit().commit(write.prepareCommit());
        }
        builder = getTableDefault().newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withWriteType(schema.rowType())) {
            for (int i = 5; i < 10; i++) {
                write.write(
                        GenericRow.of(
                                i,
                                BinaryString.fromString("a" + i),
                                BinaryString.fromString("b" + i)));
            }
            builder.newCommit().commit(write.prepareCommit());
        }
        FileStoreTable table = getTableDefault();

        assertThat(plannedFileCount(table, null, null)).isEqualTo(2);

        // Projecting f2 must still keep the pre-ALTER file as a row-count witness so
        // the reader emits 5 NULL-filled rows for the pre-ALTER range.
        RowType readF2 = schema.rowType().project(Collections.singletonList("f2"));
        assertThat(plannedFileCount(table, readF2, null)).isEqualTo(2);
    }

    /**
     * Columnar split + predicate on the file-A column: stats prune through file A's column, column
     * pruning then drops file B from the kept group.
     */
    @Test
    public void testColumnarSplitWithPredicateOnFileAColumn() throws Exception {
        write(10);
        FileStoreTable table = getTableDefault();
        Schema schema = schemaDefault();
        PredicateBuilder pb = new PredicateBuilder(table.rowType());
        int f0Idx = table.rowType().getFieldIndex("f0");
        RowType readF0 = schema.rowType().project(Collections.singletonList("f0"));
        assertThat(plannedFileCount(table, readF0, pb.greaterThan(f0Idx, 5))).isEqualTo(1);
        assertThat(plannedFileCount(table, readF0, pb.greaterThan(f0Idx, 1000))).isEqualTo(0);
    }

    /**
     * Columnar split + predicate on the file-B column: stats prune through file B's column, column
     * pruning then drops file A from the kept group.
     */
    @Test
    public void testColumnarSplitWithPredicateOnFileBColumn() throws Exception {
        write(10);
        FileStoreTable table = getTableDefault();
        Schema schema = schemaDefault();
        PredicateBuilder pb = new PredicateBuilder(table.rowType());
        int f2Idx = table.rowType().getFieldIndex("f2");
        RowType readF2 = schema.rowType().project(Collections.singletonList("f2"));
        assertThat(plannedFileCount(table, readF2, pb.equal(f2Idx, BinaryString.fromString("b5"))))
                .isEqualTo(1);
    }

    /**
     * Three-way columnar split: fileA{f0}, fileB{f1}, fileC{f2} share a row id range. A query that
     * touches one column should retain exactly that one file.
     */
    @Test
    public void testThreeWayColumnarSplitPruning() throws Exception {
        createTableDefault();
        Schema schema = schemaDefault();
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();

        RowType writeF0 = schema.rowType().project(Collections.singletonList("f0"));
        try (BatchTableWrite write = builder.newWrite().withWriteType(writeF0)) {
            for (int i = 0; i < 5; i++) {
                write.write(GenericRow.of(i));
            }
            builder.newCommit().commit(write.prepareCommit());
        }

        builder = getTableDefault().newBatchWriteBuilder();
        RowType writeF1 = schema.rowType().project(Collections.singletonList("f1"));
        try (BatchTableWrite write = builder.newWrite().withWriteType(writeF1)) {
            for (int i = 0; i < 5; i++) {
                write.write(GenericRow.of(BinaryString.fromString("f1_" + i)));
            }
            List<CommitMessage> msgs = write.prepareCommit();
            setFirstRowId(msgs, 0L);
            builder.newCommit().commit(msgs);
        }

        builder = getTableDefault().newBatchWriteBuilder();
        RowType writeF2 = schema.rowType().project(Collections.singletonList("f2"));
        try (BatchTableWrite write = builder.newWrite().withWriteType(writeF2)) {
            for (int i = 0; i < 5; i++) {
                write.write(GenericRow.of(BinaryString.fromString("f2_" + i)));
            }
            List<CommitMessage> msgs = write.prepareCommit();
            setFirstRowId(msgs, 0L);
            builder.newCommit().commit(msgs);
        }

        FileStoreTable table = getTableDefault();
        assertThat(plannedFileCount(table, null, null)).isEqualTo(3);
        assertThat(
                        plannedFileCount(
                                table,
                                schema.rowType().project(Collections.singletonList("f0")),
                                null))
                .isEqualTo(1);
        assertThat(
                        plannedFileCount(
                                table,
                                schema.rowType().project(Collections.singletonList("f1")),
                                null))
                .isEqualTo(1);
        assertThat(
                        plannedFileCount(
                                table,
                                schema.rowType().project(Collections.singletonList("f2")),
                                null))
                .isEqualTo(1);
        assertThat(
                        plannedFileCount(
                                table, schema.rowType().project(Arrays.asList("f0", "f2")), null))
                .isEqualTo(2);
        assertThat(
                        plannedFileCount(
                                table, schema.rowType().project(Arrays.asList("f1", "f2")), null))
                .isEqualTo(2);
    }

    /**
     * A columnar-split group covering rows 0..4 (file A {f0,f1} + file B {f2}), plus a row-disjoint
     * group at rows 5..9 (file C with the full schema). Per-group column pruning composes correctly
     * across the two topologies.
     */
    @Test
    public void testMixedColumnarSplitAndRowDisjoint() throws Exception {
        write(5);
        Schema schema = schemaDefault();
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withWriteType(schema.rowType())) {
            for (int i = 5; i < 10; i++) {
                write.write(
                        GenericRow.of(
                                i,
                                BinaryString.fromString("a" + i),
                                BinaryString.fromString("c" + i)));
            }
            builder.newCommit().commit(write.prepareCommit());
        }
        FileStoreTable table = getTableDefault();

        assertThat(plannedFileCount(table, null, null)).isEqualTo(3);
        RowType readF0 = schema.rowType().project(Collections.singletonList("f0"));
        assertThat(plannedFileCount(table, readF0, null)).isEqualTo(2);
        RowType readF2 = schema.rowType().project(Collections.singletonList("f2"));
        assertThat(plannedFileCount(table, readF2, null)).isEqualTo(2);
    }

    /**
     * System-field-only projection is filtered out of readType in
     * DataEvolutionFileStoreScan.withReadType — readType stays null and
     * postFilterManifestEntriesEnabled returns false. The column-pruning path is not entered, so
     * every file in every group flows through unchanged.
     */
    @Test
    public void testSystemFieldOnlyProjectionIsNotPruned() throws Exception {
        write(5);
        FileStoreTable table = getTableDefault();
        assertThat(plannedFileCount(table, null, null)).isEqualTo(2);
        assertThat(plannedFileCount(table, RowType.of(SpecialFields.ROW_ID), null)).isEqualTo(2);
    }

    private static int plannedFileCount(FileStoreTable table, RowType readType, Predicate filter) {
        ReadBuilder rb = table.newReadBuilder();
        if (readType != null) {
            rb = rb.withReadType(readType);
        }
        if (filter != null) {
            rb = rb.withFilter(filter);
        }
        return rb.newScan().plan().splits().stream()
                .mapToInt(
                        s ->
                                s instanceof DataSplit
                                        ? ((DataSplit) s).dataFiles().size()
                                        : ((IndexedSplit) s).dataSplit().dataFiles().size())
                .sum();
    }

    private static long countMatchingRows(ReadBuilder rb) throws Exception {
        RecordReader<InternalRow> reader = rb.newRead().createReader(rb.newScan().plan());
        AtomicInteger cnt = new AtomicInteger(0);
        reader.forEachRemaining(r -> cnt.incrementAndGet());
        reader.close();
        return cnt.get();
    }

    private Range assertContinuousRowIdRange(List<DataFileMeta> files) {
        files.sort(Comparator.comparingLong(DataFileMeta::nonNullFirstRowId));
        long start = files.get(0).nonNullFirstRowId();
        long expectedStart = start;
        for (DataFileMeta file : files) {
            assertThat(file.nonNullFirstRowId()).isEqualTo(expectedStart);
            expectedStart += file.rowCount();
        }
        return new Range(start, expectedStart - 1);
    }
}
