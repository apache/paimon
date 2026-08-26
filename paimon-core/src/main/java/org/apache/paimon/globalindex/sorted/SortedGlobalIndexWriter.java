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

package org.apache.paimon.globalindex.sorted;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalRow.FieldGetter;
import org.apache.paimon.globalindex.GlobalIndexKeyExtractor;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.SortedGlobalIndexer;
import org.apache.paimon.index.DataEvolutionIndexSourceMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.createIndexWriter;
import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.toIndexFileMetas;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Writer for sorted global indexes. */
public class SortedGlobalIndexWriter implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final double FLOATING = 1.2;

    private final String indexType;
    private final FileStoreTable table;
    private final RowType rowType;
    private final Options options;
    private final long recordsPerRange;

    private DataField indexField;
    private GlobalIndexKeyExtractor keyExtractor;

    public SortedGlobalIndexWriter(Table table, String indexType) {
        this(table, indexType, ((FileStoreTable) table).coreOptions().toConfiguration());
    }

    public SortedGlobalIndexWriter(Table table, String indexType, Options options) {
        this.indexType = indexType;
        this.table = (FileStoreTable) table;
        this.rowType = this.table.rowType();
        this.options = options;
        this.recordsPerRange =
                (long) (options.get(SortedIndexOptions.SORTED_INDEX_RECORDS_PER_RANGE) * FLOATING);
    }

    public SortedGlobalIndexWriter withIndexField(String indexField) {
        checkArgument(
                rowType.containsField(indexField),
                "Column '%s' does not exist in table '%s'.",
                indexField,
                table.fullName());
        this.indexField = rowType.getField(indexField);
        GlobalIndexer indexer = GlobalIndexer.create(indexType, this.indexField, options);
        checkArgument(
                indexer instanceof SortedGlobalIndexer,
                "Index algorithm %s does not expose sorted index keys.",
                indexType);
        this.keyExtractor = ((SortedGlobalIndexer) indexer).keyExtractor();
        return this;
    }

    public GlobalIndexKeyExtractor keyExtractor() {
        return keyExtractor;
    }

    public long recordsPerRange() {
        return recordsPerRange;
    }

    public List<CommitMessage> buildForSinglePartition(
            Range rowRange, BinaryRow partition, Iterator<InternalRow> data, long scanSnapshotId)
            throws IOException {
        FieldGetter indexFieldGetter = InternalRow.createFieldGetter(keyExtractor.keyType(), 0);
        try (SortedSingleColumnIndexWriter writer = createTaskWriter(rowRange)) {
            while (data.hasNext()) {
                InternalRow row = data.next();
                long localRowId = row.getLong(1) - rowRange.from;
                writer.write(indexFieldGetter.getFieldOrNull(row), localRowId);
            }

            List<CommitMessage> commitMessages = new ArrayList<>();
            for (List<ResultEntry> resultEntries : writer.finish()) {
                commitMessages.add(flushIndex(rowRange, resultEntries, partition, scanSnapshotId));
            }
            return commitMessages;
        }
    }

    /** Creates a task writer which owns file rotation and source-row coverage semantics. */
    public SortedSingleColumnIndexWriter createTaskWriter(Range rowRange) throws IOException {
        if (keyExtractor.isIdentity()) {
            return new SortedSingleColumnIndexWriter(recordsPerRange, this::createWriter);
        }
        return SortedSingleColumnIndexWriter.forSourceRowCount(rowRange.count(), createWriter());
    }

    public GlobalIndexSingleColumnWriter createWriter() throws IOException {
        GlobalIndexWriter indexWriter = createIndexWriter(table, indexType, indexField, options);
        if (!(indexWriter instanceof GlobalIndexSingleColumnWriter)) {
            throw new RuntimeException(
                    "Unexpected implementation, the index writer of "
                            + indexType
                            + " should be an instance of GlobalIndexSingleColumnWriter, but found: "
                            + indexWriter.getClass().getName());
        }
        return (GlobalIndexSingleColumnWriter) indexWriter;
    }

    public CommitMessage flushIndex(
            Range rowRange,
            List<ResultEntry> resultEntries,
            BinaryRow partition,
            long scanSnapshotId)
            throws IOException {
        byte[] sourceMeta = new DataEvolutionIndexSourceMeta(scanSnapshotId).serialize();
        List<IndexFileMeta> indexFileMetas =
                toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange,
                        Collections.singletonList(indexField),
                        indexType,
                        resultEntries,
                        sourceMeta,
                        table.schema().id());
        DataIncrement dataIncrement = DataIncrement.indexIncrement(indexFileMetas);
        return new CommitMessageImpl(
                partition, 0, null, dataIncrement, CompactIncrement.emptyIncrement());
    }
}
