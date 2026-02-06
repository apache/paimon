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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.globalindex.GlobalIndexParallelWriter;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.RowIdIndexFieldsExtractor;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
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
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.createIndexWriter;
import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.toIndexFileMetas;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Builder to build btree global index. */
public class BTreeGlobalIndexBuilder implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final double FLOATING = 1.2;

    private final FileStoreTable table;
    private final RowType rowType;
    private final Options options;
    private final long recordsPerRange;

    private String indexType;
    private DataField indexField;

    // readRowType is composed by partition fields, indexed field and _ROW_ID field
    private RowType readRowType;
    private RowIdIndexFieldsExtractor extractor;

    @Nullable private PartitionPredicate partitionPredicate;

    public BTreeGlobalIndexBuilder(Table table) {
        this.table = (FileStoreTable) table;
        this.rowType = table.rowType();
        this.options = this.table.coreOptions().toConfiguration();
        this.recordsPerRange =
                (long) (options.get(BTreeIndexOptions.BTREE_INDEX_RECORDS_PER_RANGE) * FLOATING);
    }

    public BTreeGlobalIndexBuilder withIndexType(String indexType) {
        this.indexType = indexType;
        Preconditions.checkArgument(
                BTreeGlobalIndexerFactory.IDENTIFIER.equals(indexType),
                "BTreeGlobalInderBuilder only supports %s index type",
                BTreeGlobalIndexerFactory.IDENTIFIER);
        return this;
    }

    public BTreeGlobalIndexBuilder withIndexField(String indexField) {
        checkArgument(
                rowType.containsField(indexField),
                "Column '%s' does not exist in table '%s'.",
                indexField,
                table.fullName());
        this.indexField = rowType.getField(indexField);
        List<String> readColumns = new ArrayList<>(table.partitionKeys());
        readColumns.addAll(
                SpecialFields.rowTypeWithRowId(new RowType(singletonList(this.indexField)))
                        .getFieldNames());
        this.readRowType = SpecialFields.rowTypeWithRowId(table.rowType()).project(readColumns);
        this.extractor =
                new RowIdIndexFieldsExtractor(
                        this.readRowType, table.partitionKeys(), this.indexField.name());
        return this;
    }

    public BTreeGlobalIndexBuilder withPartitionPredicate(PartitionPredicate partitionPredicate) {
        this.partitionPredicate = partitionPredicate;
        return this;
    }

    public List<DataSplit> scan() {
        // 1. read the whole dataset of target partitions
        SnapshotReader snapshotReader = table.newSnapshotReader();
        if (partitionPredicate != null) {
            snapshotReader = snapshotReader.withPartitionFilter(partitionPredicate);
        }

        return snapshotReader.read().dataSplits();
    }

    public List<CommitMessage> build(List<DataSplit> splits, IOManager ioManager)
            throws IOException {
        Range rowRange = calcRowRange(splits);
        if (splits.isEmpty() || rowRange == null) {
            return Collections.emptyList();
        }

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

        List<Split> splitList = new ArrayList<>(splits);
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
        List<CommitMessage> result = build(rowRange, iterator);
        buffer.clear();

        return result;
    }

    public List<CommitMessage> build(Range rowRange, Iterator<InternalRow> data)
            throws IOException {
        long counter = 0;
        BinaryRow currentPart = null;
        GlobalIndexParallelWriter currentWriter = null;
        List<CommitMessage> commitMessages = new ArrayList<>();

        while (data.hasNext()) {
            InternalRow row = data.next();
            BinaryRow partRow = extractor.extractPartition(row);

            // the input is sorted by <partition, indexedField>
            if (currentWriter != null) {
                if (!Objects.equals(partRow, currentPart) || counter >= recordsPerRange) {
                    commitMessages.add(flushIndex(rowRange, currentWriter.finish(), currentPart));
                    currentWriter = null;
                    counter = 0;
                }
            }

            // write <value, rowId> pair to index file
            currentPart = partRow;
            counter++;

            if (currentWriter == null) {
                currentWriter = createWriter();
            }

            // convert the original rowId to local rowId
            long localRowId = extractor.extractRowId(row) - rowRange.from;
            currentWriter.write(extractor.extractIndexField(row), localRowId);
        }

        if (counter > 0) {
            commitMessages.add(flushIndex(rowRange, currentWriter.finish(), currentPart));
        }

        return commitMessages;
    }

    private GlobalIndexParallelWriter createWriter() throws IOException {
        GlobalIndexParallelWriter currentWriter;
        GlobalIndexWriter indexWriter = createIndexWriter(table, indexType, indexField, options);
        if (!(indexWriter instanceof GlobalIndexParallelWriter)) {
            throw new RuntimeException(
                    "Unexpected implementation, the index writer of BTree should be an instance of GlobalIndexParallelWriter, but found: "
                            + indexWriter.getClass().getName());
        }
        currentWriter = (GlobalIndexParallelWriter) indexWriter;
        return currentWriter;
    }

    private CommitMessage flushIndex(
            Range rowRange, List<ResultEntry> resultEntries, BinaryRow partition)
            throws IOException {
        List<IndexFileMeta> indexFileMetas =
                toIndexFileMetas(table, rowRange, indexField.id(), indexType, resultEntries);
        DataIncrement dataIncrement = DataIncrement.indexIncrement(indexFileMetas);
        return new CommitMessageImpl(
                partition, 0, null, dataIncrement, CompactIncrement.emptyIncrement());
    }

    public static Range calcRowRange(List<DataSplit> dataSplits) {
        long start = Long.MAX_VALUE;
        long end = Long.MIN_VALUE;
        for (DataSplit dataSplit : dataSplits) {
            for (DataFileMeta file : dataSplit.dataFiles()) {
                long firstRowId = file.nonNullFirstRowId();
                start = Math.min(start, firstRowId);
                end = Math.max(end, firstRowId + file.rowCount());
            }
        }
        return start == Long.MAX_VALUE ? null : new Range(start, end);
    }
}
