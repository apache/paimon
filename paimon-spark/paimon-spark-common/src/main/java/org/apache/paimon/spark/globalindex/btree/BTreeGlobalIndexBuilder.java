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
import org.apache.paimon.spark.globalindex.GlobalIndexBuilder;
import org.apache.paimon.spark.globalindex.GlobalIndexBuilderContext;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The {@link GlobalIndexBuilder} implementation for BTree Index. The caller of {@link
 * BTreeGlobalIndexBuilder#build(CloseableIterator) build} must ensure the input data is sorted by
 * partitions and indexed field.
 */
public class BTreeGlobalIndexBuilder extends GlobalIndexBuilder {
    private static final double FLOATING = 1.2;

    private final IndexFieldsExtractor extractor;
    private final long recordsPerRange;
    private BinaryRow currentPart = null;
    private GlobalIndexParallelWriter currentWriter = null;
    private LongCounter counter = new LongCounter();

    protected BTreeGlobalIndexBuilder(GlobalIndexBuilderContext context) {
        super(context);
        Preconditions.checkNotNull(
                context.fullRange(), "Full range cannot be null for BTreeGlobalIndexBuilder.");

        FileStoreTable table = context.table();
        List<String> readColumns = new ArrayList<>(table.partitionKeys());
        readColumns.addAll(context.readType().getFieldNames());
        this.extractor =
                new IndexFieldsExtractor(
                        SpecialFields.rowTypeWithRowId(table.rowType()).project(readColumns),
                        table.partitionKeys(),
                        context.indexField().name());

        // Each partition boundary is derived from sampling, so we introduce a slack factor
        // to avoid generating too many small files due to sampling variance.
        this.recordsPerRange =
                (long)
                        (context.options().get(BTreeIndexOptions.BTREE_INDEX_RECORDS_PER_RANGE)
                                * FLOATING);
    }

    @Override
    public List<CommitMessage> build(CloseableIterator<InternalRow> data) throws IOException {
        List<CommitMessage> commitMessages = new ArrayList<>();

        while (data.hasNext()) {
            InternalRow row = data.next();

            BinaryRow partRow = extractor.extractPartition(row);
            // may flush last part data
            // this is correct only if the input is sorted by <partition, indexedField>
            if (currentPart != null && !partRow.equals(currentPart)
                    || counter.getValue() >= recordsPerRange) {
                flushIndex(commitMessages);
            }

            createWriterIfNeeded();

            // write <value, rowId> pair to index file
            currentPart = partRow;
            counter.add(1);
            currentWriter.write(extractor.extractIndexField(row), extractor.extractRowId(row));
        }

        flushIndex(commitMessages);

        return commitMessages;
    }

    private void flushIndex(List<CommitMessage> resultMessages) throws IOException {
        if (counter.getValue() == 0 || currentWriter == null || currentPart == null) {
            return;
        }

        List<ResultEntry> resultEntries = currentWriter.finish();
        List<IndexFileMeta> fileMetas =
                convertToIndexMeta(context.fullRange().from, context.fullRange().to, resultEntries);
        DataIncrement dataIncrement = DataIncrement.indexIncrement(fileMetas);
        CommitMessage commitMessage =
                new CommitMessageImpl(
                        currentPart, 0, null, dataIncrement, CompactIncrement.emptyIncrement());

        // reset writer
        currentWriter = null;
        currentPart = null;
        counter.reset();

        resultMessages.add(commitMessage);
    }

    private void createWriterIfNeeded() throws IOException {
        if (currentWriter == null) {
            GlobalIndexWriter indexWriter = createIndexWriter();
            if (!(indexWriter instanceof GlobalIndexParallelWriter)) {
                throw new RuntimeException(
                        "Unexpected implementation, the index writer of BTree should be an instance of GlobalIndexParallelWriter, but found: "
                                + indexWriter.getClass().getName());
            }
            currentWriter = (GlobalIndexParallelWriter) indexWriter;
        }
    }
}
