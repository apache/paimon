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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.globalindex.DataEvolutionGlobalIndexScanner;
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for bitmap indexed batch scan. */
public class BitmapGlobalIndexTableTest extends DataEvolutionTestBase {

    private static final String INDEX_TYPE = "bitmap";

    @Test
    public void testBitmapGlobalIndexWithCoreScanAcrossRanges() throws Exception {
        write(1000L);
        createIndex("f1", Collections.singletonList(new Range(0L, 499L)));
        createIndex("f1", Collections.singletonList(new Range(500L, 999L)));

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        Predicate predicate =
                new PredicateBuilder(table.rowType())
                        .in(
                                1,
                                Arrays.asList(
                                        BinaryString.fromString("a200"),
                                        BinaryString.fromString("a700")));

        RoaringNavigableMap64 rowIds = globalIndexScan(table, predicate);
        assertThat(rowIds.toRangeList())
                .containsExactlyInAnyOrder(new Range(200L, 200L), new Range(700L, 700L));

        ReadBuilder readBuilder = table.newReadBuilder().withFilter(predicate);
        List<String> readF1 = new ArrayList<>();
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(row -> readF1.add(row.getString(1).toString()));

        assertThat(readF1).containsExactly("a200", "a700");
    }

    @Test
    public void testBitmapGlobalIndexComplementsAndNulls() throws Exception {
        long oldRowCount = 10L;
        write(oldRowCount);

        catalog.alterTable(identifier(), SchemaChange.addColumn("f3", DataTypes.STRING()), false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite()) {
            write.write(
                    GenericRow.of(
                            100,
                            BinaryString.fromString("a-new"),
                            BinaryString.fromString("b-new"),
                            BinaryString.fromString("not-null")));
            try (BatchTableCommit commit = writeBuilder.newCommit()) {
                commit.commit(write.prepareCommit());
            }
        }

        createIndex("f3", null);

        table = (FileStoreTable) catalog.getTable(identifier());
        Predicate isNull = new PredicateBuilder(table.rowType()).isNull(3);
        RoaringNavigableMap64 rowIds = globalIndexScan(table, isNull);
        assertThat(rowIds.getLongCardinality()).isEqualTo(oldRowCount);
        assertThat(rowIds.toRangeList()).containsExactly(new Range(0L, oldRowCount - 1));

        Predicate isNotNull = new PredicateBuilder(table.rowType()).isNotNull(3);
        rowIds = globalIndexScan(table, isNotNull);
        assertThat(rowIds.toRangeList()).containsExactly(new Range(oldRowCount, oldRowCount));

        Predicate notEqual =
                new PredicateBuilder(table.rowType())
                        .notEqual(3, BinaryString.fromString("not-null"));
        rowIds = globalIndexScan(table, notEqual);
        assertThat(rowIds.isEmpty()).isTrue();
    }

    @Test
    public void testBitmapGlobalIndexStartsWith() throws Exception {
        write(1000L);
        createIndex("f1", null);

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        Predicate predicate =
                new PredicateBuilder(table.rowType()).startsWith(1, BinaryString.fromString("a20"));

        RoaringNavigableMap64 rowIds = globalIndexScan(table, predicate);
        assertThat(rowIds.toRangeList())
                .containsExactlyInAnyOrder(new Range(20L, 20L), new Range(200L, 209L));
    }

    @Test
    public void testBitmapGlobalIndexFallbackScan() throws Exception {
        write(1000L);
        createIndex("f1", null);

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        Predicate contains =
                new PredicateBuilder(table.rowType()).contains(1, BinaryString.fromString("20"));

        RoaringNavigableMap64 rowIds = globalIndexScan(table, contains);
        assertThat(rowIds.toRangeList())
                .containsExactlyInAnyOrder(
                        new Range(20L, 20L),
                        new Range(120L, 120L),
                        new Range(200L, 209L),
                        new Range(220L, 220L),
                        new Range(320L, 320L),
                        new Range(420L, 420L),
                        new Range(520L, 520L),
                        new Range(620L, 620L),
                        new Range(720L, 720L),
                        new Range(820L, 820L),
                        new Range(920L, 920L));

        Predicate range =
                new PredicateBuilder(table.rowType())
                        .between(
                                1,
                                BinaryString.fromString("a200"),
                                BinaryString.fromString("a209"));
        rowIds = globalIndexScan(table, range);
        assertThat(rowIds.toRangeList()).containsExactly(new Range(200L, 209L));
    }

    private void createIndex(String fieldName, List<Range> rowRanges) throws Exception {
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        DataField indexField = table.rowType().getField(fieldName);
        RowType readRowType =
                SpecialFields.rowTypeWithRowId(
                        table.rowType().project(Collections.singletonList(fieldName)));
        ReadBuilder readBuilder = table.newReadBuilder().withReadType(readRowType);

        List<Split> splits =
                rowRanges == null
                        ? readBuilder.newScan().plan().splits()
                        : readBuilder.withRowRanges(rowRanges).newScan().plan().splits();

        List<CommitMessage> commitMessages = new ArrayList<>();
        for (Split split : splits) {
            commitMessages.add(buildIndex(table, indexField, readRowType, split));
        }
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(commitMessages);
        }
    }

    private CommitMessage buildIndex(
            FileStoreTable table, DataField indexField, RowType readRowType, Split split)
            throws Exception {
        Range rowRange = rowRange(split);
        GlobalIndexWriter indexWriter =
                GlobalIndexBuilderUtils.createIndexWriter(
                        table, INDEX_TYPE, indexField, table.coreOptions().toConfiguration());
        GlobalIndexSingleColumnWriter writer = (GlobalIndexSingleColumnWriter) indexWriter;
        InternalRow.FieldGetter fieldGetter =
                InternalRow.createFieldGetter(
                        indexField.type(), readRowType.getFieldIndex(indexField.name()));
        int rowIdIndex = readRowType.getFieldIndex(SpecialFields.ROW_ID.name());
        List<Pair<Object, Long>> entries = new ArrayList<>();

        try (RecordReader<InternalRow> reader =
                        table.newReadBuilder()
                                .withReadType(readRowType)
                                .newRead()
                                .createReader(Collections.singletonList(split));
                CloseableIterator<InternalRow> iterator = reader.toCloseableIterator()) {
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                long rowId = row.getLong(rowIdIndex);
                if (rowId >= rowRange.from && rowId <= rowRange.to) {
                    entries.add(Pair.of(fieldGetter.getFieldOrNull(row), rowId - rowRange.from));
                }
            }
        }
        Comparator<Object> keyComparator =
                KeySerializer.create(indexField.type()).createComparator();
        entries.sort(
                (left, right) -> {
                    if (left.getKey() == null) {
                        return right.getKey() == null ? 0 : -1;
                    }
                    return right.getKey() == null
                            ? 1
                            : keyComparator.compare(left.getKey(), right.getKey());
                });
        for (Pair<Object, Long> entry : entries) {
            writer.write(entry.getKey(), entry.getValue());
        }

        List<ResultEntry> resultEntries = writer.finish();
        List<IndexFileMeta> indexFileMetas =
                GlobalIndexBuilderUtils.toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange,
                        indexField.id(),
                        INDEX_TYPE,
                        resultEntries);
        DataIncrement dataIncrement = DataIncrement.indexIncrement(indexFileMetas);
        return new CommitMessageImpl(
                partition(split), 0, null, dataIncrement, CompactIncrement.emptyIncrement());
    }

    private BinaryRow partition(Split split) {
        return dataSplit(split).partition();
    }

    private Range rowRange(Split split) {
        if (split instanceof IndexedSplit) {
            List<Range> rowRanges = ((IndexedSplit) split).rowRanges();
            assertThat(rowRanges).hasSize(1);
            return rowRanges.get(0);
        }

        List<Range> ranges =
                dataSplit(split).dataFiles().stream()
                        .map(DataFileMeta::nonNullRowIdRange)
                        .sorted(Comparator.comparingLong(range -> range.from))
                        .collect(Collectors.toList());
        return new Range(ranges.get(0).from, ranges.get(ranges.size() - 1).to);
    }

    private DataSplit dataSplit(Split split) {
        return split instanceof IndexedSplit
                ? ((IndexedSplit) split).dataSplit()
                : (DataSplit) split;
    }

    private RoaringNavigableMap64 globalIndexScan(FileStoreTable table, Predicate predicate)
            throws Exception {
        try (DataEvolutionGlobalIndexScanner scanner =
                DataEvolutionGlobalIndexScanner.create(
                                table, PartitionPredicate.ALWAYS_TRUE, predicate)
                        .get()) {
            return scanner.scan(predicate).get().results();
        }
    }
}
