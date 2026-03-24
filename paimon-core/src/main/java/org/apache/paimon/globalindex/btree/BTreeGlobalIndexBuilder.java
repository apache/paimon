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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalRow.FieldGetter;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.globalindex.DataEvolutionBatchScan;
import org.apache.paimon.globalindex.GlobalIndexParallelWriter;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.MutableObjectIteratorAdapter;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RangeHelper;
import org.apache.paimon.utils.RowRangeIndex;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.createIndexWriter;
import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.toIndexFileMetas;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Builder to build btree global index. */
public class BTreeGlobalIndexBuilder implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final double FLOATING = 1.2;
    private static final String INDEX_TYPE = "btree";

    private final FileStoreTable table;
    private final RowType rowType;
    private final Options options;
    private final long recordsPerRange;

    private DataField indexField;

    // readRowType is composed by partition fields, indexed field and _ROW_ID field
    private RowType readRowType;
    @Nullable private Snapshot snapshot;

    @Nullable private PartitionPredicate partitionPredicate;

    public BTreeGlobalIndexBuilder(Table table) {
        this.table = (FileStoreTable) table;
        this.rowType = this.table.rowType();
        this.options = this.table.coreOptions().toConfiguration();
        this.recordsPerRange =
                (long) (options.get(BTreeIndexOptions.BTREE_INDEX_RECORDS_PER_RANGE) * FLOATING);
    }

    public BTreeGlobalIndexBuilder withIndexField(String indexField) {
        checkArgument(
                rowType.containsField(indexField),
                "Column '%s' does not exist in table '%s'.",
                indexField,
                table.fullName());
        this.indexField = rowType.getField(indexField);
        List<String> readColumns = new ArrayList<>();
        readColumns.add(this.indexField.name());
        readColumns.add(SpecialFields.ROW_ID.name());

        this.readRowType = SpecialFields.rowTypeWithRowId(table.rowType()).project(readColumns);
        return this;
    }

    public BTreeGlobalIndexBuilder withPartitionPredicate(PartitionPredicate partitionPredicate) {
        this.partitionPredicate = partitionPredicate;
        return this;
    }

    public BTreeGlobalIndexBuilder withSnapshot(Snapshot snapshot) {
        this.snapshot = snapshot;
        return this;
    }

    public Optional<Pair<RowRangeIndex, List<DataSplit>>> scan() {
        SnapshotReader snapshotReader = table.newSnapshotReader();
        if (partitionPredicate != null) {
            snapshotReader = snapshotReader.withPartitionFilter(partitionPredicate);
        }
        Snapshot snapshot =
                this.snapshot != null
                        ? this.snapshot
                        : snapshotReader.snapshotManager().latestSnapshot();
        if (snapshot == null) {
            return Optional.empty();
        }
        snapshotReader = snapshotReader.withSnapshot(snapshot);
        Range dataRange = new Range(0, snapshot.nextRowId() - 1);

        return Optional.of(
                Pair.of(
                        RowRangeIndex.create(Collections.singletonList(dataRange)),
                        snapshotReader.read().dataSplits()));
    }

    public Optional<Pair<RowRangeIndex, List<DataSplit>>> incrementalScan() {
        SnapshotReader snapshotReader = table.newSnapshotReader();
        if (partitionPredicate != null) {
            snapshotReader = snapshotReader.withPartitionFilter(partitionPredicate);
        }
        Snapshot snapshot =
                this.snapshot != null
                        ? this.snapshot
                        : snapshotReader.snapshotManager().latestSnapshot();
        if (snapshot == null) {
            return Optional.empty();
        }
        snapshotReader = snapshotReader.withSnapshot(snapshot);

        Preconditions.checkArgument(indexField != null, "indexField must be set before scan.");
        Range dataRange = new Range(0, snapshot.nextRowId() - 1);
        List<Range> indexedRanges = indexedRowRanges(snapshot);
        List<Range> nonIndexedRanges = dataRange.exclude(indexedRanges);
        if (nonIndexedRanges.isEmpty()) {
            return Optional.empty();
        }
        snapshotReader = snapshotReader.withRowRanges(nonIndexedRanges);
        return Optional.of(
                Pair.of(
                        RowRangeIndex.create(nonIndexedRanges),
                        snapshotReader.read().dataSplits()));
    }

    private List<Range> indexedRowRanges(Snapshot snapshot) {
        List<Range> ranges = new ArrayList<>();
        for (IndexManifestEntry entry :
                table.store().newIndexFileHandler().scan(snapshot, "btree")) {
            if (partitionPredicate != null && !partitionPredicate.test(entry.partition())) {
                continue;
            }
            if (entry.indexFile().globalIndexMeta() == null) {
                continue;
            }
            if (entry.indexFile().globalIndexMeta().indexFieldId() != indexField.id()) {
                continue;
            }
            ranges.add(
                    new Range(
                            entry.indexFile().globalIndexMeta().rowRangeStart(),
                            entry.indexFile().globalIndexMeta().rowRangeEnd()));
        }
        return Range.sortAndMergeOverlap(ranges, true);
    }

    @VisibleForTesting
    public List<CommitMessage> build(DataSplit split, IOManager ioManager) throws IOException {
        BinaryRow partition = split.partition();
        Range rowRange = calcRowRange(split);

        CoreOptions options = new CoreOptions(this.options);
        BinaryExternalSortBuffer buffer =
                BinaryExternalSortBuffer.create(
                        ioManager,
                        readRowType,
                        // sort by <partition, indexed_field>
                        IntStream.range(0, readRowType.getFieldCount() - 1).toArray(),
                        options.writeBufferSize(),
                        options.pageSize(),
                        options.localSortMaxNumFileHandles(),
                        options.spillCompressOptions(),
                        options.writeBufferSpillDiskSize(),
                        options.sequenceFieldSortOrderIsAscending());

        List<Split> splitList = Collections.singletonList(split);
        RecordReader<InternalRow> reader =
                table.newReadBuilder().withReadType(readRowType).newRead().createReader(splitList);
        try (CloseableIterator<InternalRow> iterator = reader.toCloseableIterator()) {
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                buffer.write(row);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Iterator<InternalRow> iterator =
                new MutableObjectIteratorAdapter<>(
                        buffer.sortedIterator(), new BinaryRow(readRowType.getFieldCount()));
        List<CommitMessage> result = buildForSinglePartition(rowRange, partition, iterator);
        buffer.clear();

        return result;
    }

    public long recordsPerRange() {
        return recordsPerRange;
    }

    public List<CommitMessage> buildForSinglePartition(
            Range rowRange, BinaryRow partition, Iterator<InternalRow> data) throws IOException {
        long counter = 0;
        GlobalIndexParallelWriter currentWriter = null;
        List<CommitMessage> commitMessages = new ArrayList<>();
        FieldGetter indexFieldGetter = InternalRow.createFieldGetter(indexField.type(), 0);

        while (data.hasNext()) {
            InternalRow row = data.next();

            if (currentWriter != null && counter >= recordsPerRange) {
                commitMessages.add(flushIndex(rowRange, currentWriter.finish(), partition));
                currentWriter = null;
                counter = 0;
            }

            counter++;
            if (currentWriter == null) {
                currentWriter = createWriter();
            }

            long localRowId = row.getLong(1) - rowRange.from;
            currentWriter.write(indexFieldGetter.getFieldOrNull(row), localRowId);
        }

        if (counter > 0) {
            commitMessages.add(flushIndex(rowRange, currentWriter.finish(), partition));
        }
        return commitMessages;
    }

    public GlobalIndexParallelWriter createWriter() throws IOException {
        GlobalIndexParallelWriter currentWriter;
        GlobalIndexWriter indexWriter = createIndexWriter(table, INDEX_TYPE, indexField, options);
        if (!(indexWriter instanceof GlobalIndexParallelWriter)) {
            throw new RuntimeException(
                    "Unexpected implementation, the index writer of BTree should be an instance of GlobalIndexParallelWriter, but found: "
                            + indexWriter.getClass().getName());
        }
        currentWriter = (GlobalIndexParallelWriter) indexWriter;
        return currentWriter;
    }

    public CommitMessage flushIndex(
            Range rowRange, List<ResultEntry> resultEntries, BinaryRow partition)
            throws IOException {
        List<IndexFileMeta> indexFileMetas =
                toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange,
                        indexField.id(),
                        INDEX_TYPE,
                        resultEntries);
        DataIncrement dataIncrement = DataIncrement.indexIncrement(indexFileMetas);
        return new CommitMessageImpl(
                partition, 0, null, dataIncrement, CompactIncrement.emptyIncrement());
    }

    public static Pair<Range, Split> calcRowRangeWithRowIndex(
            RowRangeIndex rowRangeIndex, DataSplit dataSplit) {
        if (rowRangeIndex != null) {
            IndexedSplit indexedSplit =
                    (IndexedSplit)
                            DataEvolutionBatchScan.wrapToIndexSplits(
                                            Arrays.asList(dataSplit), rowRangeIndex, null)
                                    .splits()
                                    .get(0);
            checkArgument(
                    indexedSplit.rowRanges().size() == 1,
                    "Expected exactly one row range for the split, but found: %s",
                    indexedSplit.rowRanges());
            return Pair.of(indexedSplit.rowRanges().get(0), indexedSplit.dataSplit());
        }

        return Pair.of(calcRowRange(dataSplit), dataSplit);
    }

    public static Range calcRowRange(DataSplit dataSplit) {
        List<Range> ranges = calcRowRanges(singletonList(dataSplit));
        if (ranges.isEmpty()) {
            return null;
        }
        return new Range(ranges.get(0).from, ranges.get(ranges.size() - 1).to);
    }

    public static List<Range> calcRowRanges(List<DataSplit> dataSplits) {
        List<Range> ranges = new ArrayList<>();
        for (DataSplit dataSplit : dataSplits) {
            for (DataFileMeta file : dataSplit.dataFiles()) {
                ranges.add(file.nonNullRowIdRange());
            }
        }
        return Range.sortAndMergeOverlap(ranges, true);
    }

    public static List<DataSplit> splitByContiguousRowRange(List<DataSplit> splits) {
        List<DataSplit> result = new ArrayList<>();
        for (DataSplit split : splits) {
            result.addAll(splitByContiguousRowRange(split));
        }
        return result;
    }

    public static Map<BinaryRow, Map<Range, List<Split>>> groupSplitsByRange(
            RowRangeIndex rowRangeIndex, List<DataSplit> splits) {
        Map<BinaryRow, List<Pair<Range, Split>>> partitionSplitRanges = new HashMap<>();
        for (DataSplit split : splits) {
            Pair<Range, Split> keyPair = calcRowRangeWithRowIndex(rowRangeIndex, split);
            Range splitRange = keyPair.getKey();
            Split splitWithRange = keyPair.getValue();
            if (splitRange == null) {
                continue;
            }
            BinaryRow partition = split.partition();
            partitionSplitRanges
                    .computeIfAbsent(partition, p -> new ArrayList<>())
                    .add(Pair.of(splitRange, splitWithRange));
        }

        Map<BinaryRow, Map<Range, List<Split>>> result = new HashMap<>();
        for (Map.Entry<BinaryRow, List<Pair<Range, Split>>> partitionEntry :
                partitionSplitRanges.entrySet()) {
            List<Pair<Range, Split>> splitRanges = partitionEntry.getValue();
            splitRanges.sort(
                    Comparator.comparingLong((Pair<Range, Split> e) -> e.getKey().from)
                            .thenComparingLong(e -> e.getKey().to));

            Map<Range, List<Split>> partitionRanges = new LinkedHashMap<>();
            Range current = null;
            List<Split> currentSplits = new ArrayList<>();
            for (Map.Entry<Range, Split> entry : splitRanges) {
                Range splitRange = entry.getKey();
                if (current == null) {
                    current = splitRange;
                    currentSplits.add(entry.getValue());
                    continue;
                }
                Range merged = Range.union(current, splitRange);
                if (merged != null) {
                    current = merged;
                    currentSplits.add(entry.getValue());
                } else {
                    partitionRanges.put(current, currentSplits);
                    current = splitRange;
                    currentSplits = new ArrayList<>();
                    currentSplits.add(entry.getValue());
                }
            }
            if (current != null) {
                partitionRanges.put(current, currentSplits);
            }
            result.put(partitionEntry.getKey(), partitionRanges);
        }

        return result;
    }

    private static List<DataSplit> splitByContiguousRowRange(DataSplit split) {
        List<DataFileMeta> input = split.dataFiles();
        RangeHelper<DataFileMeta> rangeHelper = new RangeHelper<>(DataFileMeta::nonNullRowIdRange);
        List<List<DataFileMeta>> ranges = rangeHelper.mergeOverlappingRanges(input);

        Supplier<DataSplit.Builder> builderSupplier =
                () ->
                        DataSplit.builder()
                                .withSnapshot(split.snapshotId())
                                .withPartition(split.partition())
                                .withBucket(split.bucket())
                                .withBucketPath(split.bucketPath())
                                .withTotalBuckets(split.totalBuckets())
                                .isStreaming(split.isStreaming())
                                .rawConvertible(split.rawConvertible());
        return packByContiguousRanges(builderSupplier, ranges);
    }

    private static List<DataSplit> packByContiguousRanges(
            Supplier<DataSplit.Builder> builderFactory, List<List<DataFileMeta>> ranges) {
        if (ranges.isEmpty()) {
            return new ArrayList<>();
        }

        List<DataSplit> result = new ArrayList<>();
        List<DataFileMeta> currentSegment = new ArrayList<>();
        long currentMaxRowId = Long.MIN_VALUE;

        for (List<DataFileMeta> rangeFiles : ranges) {
            long minRowId = minRowId(rangeFiles);
            long maxRowId = maxRowId(rangeFiles);
            if (currentSegment.isEmpty() || areContiguous(currentMaxRowId, minRowId)) {
                currentSegment.addAll(rangeFiles);
                currentMaxRowId = maxRowId;
            } else {
                DataSplit.Builder builder = builderFactory.get();
                builder.withDataFiles(currentSegment);
                result.add(builder.build());
                currentSegment = new ArrayList<>(rangeFiles);
                currentMaxRowId = maxRowId;
            }
        }

        DataSplit.Builder builder = builderFactory.get();
        builder.withDataFiles(currentSegment);
        result.add(builder.build());
        return result;
    }

    private static long minRowId(List<DataFileMeta> files) {
        return files.stream()
                .mapToLong(f -> f.nonNullRowIdRange().from)
                .min()
                .orElse(Long.MAX_VALUE);
    }

    private static long maxRowId(List<DataFileMeta> files) {
        return files.stream().mapToLong(f -> f.nonNullRowIdRange().to).max().orElse(Long.MIN_VALUE);
    }

    private static boolean areContiguous(long previousMaxRowId, long currentMinRowId) {
        // Contiguous means no gap between adjacent ranges.
        // e.g. previous max == current min (as requested) or previous max + 1 == current min.
        return previousMaxRowId >= currentMinRowId - 1;
    }
}
