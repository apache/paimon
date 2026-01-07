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

package org.apache.paimon.spark.globalindex.btree;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.globalindex.GlobalIndexParallelWriter;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.btree.BTreeIndexOptions;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.options.Options;
import org.apache.paimon.spark.globalindex.RowIdIndexFieldsExtractor;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.Range;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.paimon.spark.globalindex.GlobalIndexBuilderUtils.createIndexWriter;
import static org.apache.paimon.spark.globalindex.GlobalIndexBuilderUtils.toIndexFileMetas;

/**
 * A global index builder implementation for BTree Index. The caller of {@link
 * #build(CloseableIterator) build} must ensure the input data is sorted by partitions and indexed
 * field.
 */
public class BTreeGlobalIndexBuilder implements Serializable {

    private static final double FLOATING = 1.2;

    private final FileStoreTable table;
    private final DataField indexField;
    private final String indexType;
    private final Range rowRange;
    private final Options options;
    private final RowIdIndexFieldsExtractor extractor;
    private final long recordsPerRange;

    public BTreeGlobalIndexBuilder(
            FileStoreTable table,
            RowType readType,
            DataField indexField,
            String indexType,
            Range rowRange,
            Options options) {
        this.table = table;
        this.indexField = indexField;
        this.indexType = indexType;
        this.rowRange = rowRange;
        this.options = options;
        List<String> readColumns = new ArrayList<>(table.partitionKeys());
        readColumns.addAll(readType.getFieldNames());
        this.extractor =
                new RowIdIndexFieldsExtractor(
                        SpecialFields.rowTypeWithRowId(table.rowType()).project(readColumns),
                        table.partitionKeys(),
                        indexField.name());

        // Each partition boundary is derived from sampling, so we introduce a slack factor
        // to avoid generating too many small files due to sampling variance.
        this.recordsPerRange =
                (long) (options.get(BTreeIndexOptions.BTREE_INDEX_RECORDS_PER_RANGE) * FLOATING);
    }

    public List<CommitMessage> build(CloseableIterator<InternalRow> data) throws IOException {
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
                    commitMessages.add(flushIndex(currentWriter.finish(), currentPart));
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
            currentWriter.write(extractor.extractIndexField(row), extractor.extractRowId(row));
        }

        if (counter > 0) {
            commitMessages.add(flushIndex(currentWriter.finish(), currentPart));
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

    private CommitMessage flushIndex(List<ResultEntry> resultEntries, BinaryRow partition)
            throws IOException {
        List<IndexFileMeta> indexFileMetas =
                toIndexFileMetas(table, rowRange, indexField.id(), indexType, resultEntries);
        DataIncrement dataIncrement = DataIncrement.indexIncrement(indexFileMetas);
        return new CommitMessageImpl(
                partition, 0, null, dataIncrement, CompactIncrement.emptyIncrement());
    }
}
