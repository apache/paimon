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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.globalindex.DataEvolutionBatchScan;
import org.apache.paimon.globalindex.GlobalIndexFileReadWrite;
import org.apache.paimon.globalindex.GlobalIndexParallelWriter;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexScanBuilder;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.GlobalIndexerFactory;
import org.apache.paimon.globalindex.GlobalIndexerFactoryUtils;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.RowRangeGlobalIndexScanner;
import org.apache.paimon.globalindex.bitmap.BitmapGlobalIndexerFactory;
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexerFactory;
import org.apache.paimon.globalindex.btree.BTreeIndexOptions;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Test for BTree indexed batch scan. */
public class GlobalIndexTableTest extends DataEvolutionTestBase {

    private static final Logger log = LoggerFactory.getLogger(GlobalIndexTableTest.class);

    @Test
    public void testBitmapGlobalIndex() throws Exception {
        innerTestGlobalIndex(BitmapGlobalIndexerFactory.IDENTIFIER);
    }

    @Test
    public void testBTreeGlobalIndex() throws Exception {
        innerTestGlobalIndex(BTreeGlobalIndexerFactory.IDENTIFIER);
    }

    private void innerTestGlobalIndex(String indexType) throws Exception {
        write(100000L);
        createIndex(indexType, "f1");

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
    public void testBitmapGlobalIndexWithCoreScan() throws Exception {
        innerTestGlobalIndexWithCoreScan(BitmapGlobalIndexerFactory.IDENTIFIER);
    }

    @Test
    public void testBTreeGlobalIndexWithCoreScan() throws Exception {
        innerTestGlobalIndexWithCoreScan(BTreeGlobalIndexerFactory.IDENTIFIER);
    }

    private void innerTestGlobalIndexWithCoreScan(String indexType) throws Exception {
        write(100000L);
        createIndex(indexType, "f1");

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());

        Predicate predicate =
                new PredicateBuilder(table.rowType())
                        .in(
                                1,
                                Arrays.asList(
                                        BinaryString.fromString("a200"),
                                        BinaryString.fromString("a300"),
                                        BinaryString.fromString("a400"),
                                        BinaryString.fromString("a56789")));

        ReadBuilder readBuilder = table.newReadBuilder().withFilter(predicate);

        List<String> readF1 = new ArrayList<>();
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(
                        row -> {
                            readF1.add(row.getString(1).toString());
                        });

        Assertions.assertThat(readF1).containsExactly("a200", "a300", "a400", "a56789");
    }

    @Test
    public void testMultipleBitmapIndices() throws Exception {
        innerTestMultipleIndices(BitmapGlobalIndexerFactory.IDENTIFIER);
    }

    @Test
    public void testMultipleBTreeIndices() throws Exception {
        innerTestMultipleIndices(BTreeGlobalIndexerFactory.IDENTIFIER);
    }

    private void innerTestMultipleIndices(String indexType) throws Exception {
        write(100000L);
        createIndex(indexType, "f1");
        createIndex(indexType, "f2");

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        Predicate predicate1 =
                new PredicateBuilder(table.rowType())
                        .in(
                                1,
                                Arrays.asList(
                                        BinaryString.fromString("a200"),
                                        BinaryString.fromString("a300"),
                                        BinaryString.fromString("a56789")));

        Predicate predicate2 =
                new PredicateBuilder(table.rowType())
                        .in(
                                2,
                                Arrays.asList(
                                        BinaryString.fromString("b200"),
                                        BinaryString.fromString("b400"),
                                        BinaryString.fromString("b56789")));

        Predicate predicate = PredicateBuilder.and(predicate1, predicate2);
        ReadBuilder readBuilder = table.newReadBuilder().withFilter(predicate);

        List<String> result = new ArrayList<>();
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(
                        row -> {
                            result.add(row.getString(1).toString());
                        });

        Assertions.assertThat(result).containsExactly("a200", "a56789");
    }

    private void createIndex(String indexType, String fieldName) throws Exception {
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        FileIO fileIO = table.fileIO();
        RowType rowType = SpecialFields.rowTypeWithRowTracking(table.rowType().project(fieldName));
        ReadBuilder readBuilder = table.newReadBuilder().withReadType(rowType);
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());

        GlobalIndexFileReadWrite indexFileReadWrite =
                new GlobalIndexFileReadWrite(
                        fileIO,
                        table.store().pathFactory().indexFileFactory(BinaryRow.EMPTY_ROW, 0));

        DataField indexField = table.rowType().getField(fieldName);
        GlobalIndexerFactory globalIndexerFactory = GlobalIndexerFactoryUtils.load(indexType);

        List<IndexFileMeta> indexFileMetas;
        if (indexType.equals(BTreeGlobalIndexerFactory.IDENTIFIER)) {
            indexFileMetas =
                    createBTreeIndex(
                            fileIO,
                            globalIndexerFactory,
                            indexField,
                            rowType,
                            reader,
                            indexFileReadWrite);
        } else if (indexType.equals(BitmapGlobalIndexerFactory.IDENTIFIER)) {
            indexFileMetas =
                    createBitmapIndex(
                            fileIO, globalIndexerFactory, indexField, reader, indexFileReadWrite);
        } else {
            throw new Exception("Unsupported scalar index type: " + indexType);
        }

        DataIncrement dataIncrement = DataIncrement.indexIncrement(indexFileMetas);

        CommitMessage commitMessage =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        null,
                        dataIncrement,
                        CompactIncrement.emptyIncrement());

        table.newBatchWriteBuilder().newCommit().commit(Collections.singletonList(commitMessage));
    }

    private List<IndexFileMeta> createBitmapIndex(
            FileIO fileIO,
            GlobalIndexerFactory indexerFactory,
            DataField indexField,
            RecordReader<InternalRow> reader,
            GlobalIndexFileReadWrite indexFileReadWrite)
            throws Exception {
        GlobalIndexer globalIndexer = indexerFactory.create(indexField, new Options());
        GlobalIndexSingletonWriter writer =
                (GlobalIndexSingletonWriter) globalIndexer.createWriter(indexFileReadWrite);

        reader.forEachRemaining(r -> writer.write(r.getString(0)));

        List<ResultEntry> results = writer.finish();
        // bitmap index only generate one file for each writer
        Assertions.assertThat(results).hasSize(1);
        ResultEntry result = results.get(0);

        String fileName = result.fileName();
        long fileSize = fileIO.getFileSize(indexFileReadWrite.filePath(fileName));
        GlobalIndexMeta globalIndexMeta =
                new GlobalIndexMeta(0, result.rowCount() - 1, indexField.id(), null, result.meta());
        return Collections.singletonList(
                new IndexFileMeta(
                        BitmapGlobalIndexerFactory.IDENTIFIER,
                        fileName,
                        fileSize,
                        result.rowCount(),
                        globalIndexMeta));
    }

    private List<IndexFileMeta> createBTreeIndex(
            FileIO fileIO,
            GlobalIndexerFactory indexerFactory,
            DataField indexField,
            RowType rowType,
            RecordReader<InternalRow> reader,
            GlobalIndexFileReadWrite indexFileReadWrite)
            throws Exception {
        Options options = new Options();
        options.set(BTreeIndexOptions.BTREE_INDEX_CACHE_SIZE, MemorySize.ofMebiBytes(1));
        GlobalIndexer globalIndexer = indexerFactory.create(indexField, options);

        // collect all rows
        List<InternalRow> rows = new ArrayList<>();
        InternalRowSerializer rowSerializer = new InternalRowSerializer(rowType);
        reader.forEachRemaining(row -> rows.add(rowSerializer.copy(row)));
        // sort by row id
        rows.sort(Comparator.comparing(row -> row.getLong(1)));
        // intentionally split into two chunks, build index for each range
        int midRow = rows.size() / 2;
        Range firstRange = new Range(rows.get(0).getLong(1), rows.get(midRow).getLong(1));
        Range secondRange =
                new Range(rows.get(midRow + 1).getLong(1), rows.get(rows.size() - 1).getLong(1));

        List<IndexFileMeta> indexFileMetas = new ArrayList<>();
        indexFileMetas.addAll(
                createBTreeIndexForRange(
                        fileIO,
                        globalIndexer,
                        indexField,
                        rows.subList(0, midRow + 1),
                        indexFileReadWrite,
                        firstRange));
        indexFileMetas.addAll(
                createBTreeIndexForRange(
                        fileIO,
                        globalIndexer,
                        indexField,
                        rows.subList(midRow + 1, rows.size()),
                        indexFileReadWrite,
                        secondRange));

        return indexFileMetas;
    }

    private List<IndexFileMeta> createBTreeIndexForRange(
            FileIO fileIO,
            GlobalIndexer globalIndexer,
            DataField indexField,
            List<InternalRow> rowChunk,
            GlobalIndexFileReadWrite indexFileReadWrite,
            Range range)
            throws Exception {
        // collect all data and sort by index field.
        // this will be done distributedly by Spark/Flink in production.
        List<InternalRow> rows = new ArrayList<>(rowChunk);
        rows.sort(Comparator.comparing(row -> row.getString(0).toString()));

        int fileNum = 10;
        int targetFileSize = rows.size() / fileNum;
        int currentOffset = 0;

        List<IndexFileMeta> indexFileMetas = new ArrayList<>();
        for (int i = 0; i < fileNum; i++) {
            // write btree file for each data chunk.
            GlobalIndexParallelWriter writer =
                    (GlobalIndexParallelWriter) globalIndexer.createWriter(indexFileReadWrite);
            for (int j = currentOffset;
                    j < Math.min(currentOffset + targetFileSize, rows.size());
                    j++) {
                InternalRow row = rows.get(j);
                writer.write(row.getString(0), row.getLong(1));
            }
            currentOffset += targetFileSize;

            List<ResultEntry> entries = writer.finish();
            Assertions.assertThat(entries).hasSize(1);
            ResultEntry entry = entries.get(0);

            String fileName = entry.fileName();
            long fileSize = fileIO.getFileSize(indexFileReadWrite.filePath(fileName));
            GlobalIndexMeta globalIndexMeta =
                    new GlobalIndexMeta(range.from, range.to, indexField.id(), null, entry.meta());

            indexFileMetas.add(
                    new IndexFileMeta(
                            BTreeGlobalIndexerFactory.IDENTIFIER,
                            fileName,
                            fileSize,
                            entry.rowCount(),
                            globalIndexMeta));
        }

        return indexFileMetas;
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
}
