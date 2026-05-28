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

package org.apache.paimon.spark.globalindex;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.globalindex.GlobalIndexMultiColumnWriter;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.Range;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.createIndexWriter;
import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.toIndexFileMetas;

/** Default global index builder. */
public class DefaultGlobalIndexBuilder implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultGlobalIndexBuilder.class);
    private static final long serialVersionUID = 1L;

    private final FileStoreTable table;
    private final BinaryRow partition;
    private final RowType readType;
    private final List<DataField> indexFields;
    private final String indexType;
    private final Range rowRange;
    private final Options options;

    public DefaultGlobalIndexBuilder(
            FileStoreTable table,
            BinaryRow partition,
            RowType readType,
            DataField indexField,
            String indexType,
            Range rowRange,
            Options options) {
        this(
                table,
                partition,
                readType,
                Collections.singletonList(indexField),
                indexType,
                rowRange,
                options);
    }

    public DefaultGlobalIndexBuilder(
            FileStoreTable table,
            BinaryRow partition,
            RowType readType,
            List<DataField> indexFields,
            String indexType,
            Range rowRange,
            Options options) {
        this.table = table;
        this.partition = partition;
        this.readType = readType;
        this.indexFields = indexFields;
        this.indexType = indexType;
        this.rowRange = rowRange;
        this.options = options;
    }

    public FileStoreTable table() {
        return table;
    }

    public RowType readType() {
        return readType;
    }

    public CommitMessage build(CloseableIterator<InternalRow> data) throws IOException {
        LongCounter rowCounter = new LongCounter(0);
        List<ResultEntry> resultEntries = writePaimonRows(data, rowCounter);
        List<IndexFileMeta> indexFileMetas =
                toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange,
                        indexFields,
                        indexType,
                        resultEntries);
        DataIncrement dataIncrement = DataIncrement.indexIncrement(indexFileMetas);
        return new CommitMessageImpl(
                partition, 0, null, dataIncrement, CompactIncrement.emptyIncrement());
    }

    private List<ResultEntry> writePaimonRows(
            CloseableIterator<InternalRow> rows, LongCounter rowCounter) throws IOException {
        GlobalIndexWriter indexWriter = createIndexWriter(table, indexType, indexFields, options);
        boolean multiColumn = indexFields.size() > 1;

        try {
            if (multiColumn) {
                GlobalIndexMultiColumnWriter multiWriter =
                        (GlobalIndexMultiColumnWriter) indexWriter;
                int[] projection = new int[indexFields.size()];
                InternalRow.FieldGetter[] getters = new InternalRow.FieldGetter[indexFields.size()];
                for (int i = 0; i < indexFields.size(); i++) {
                    DataField field = indexFields.get(i);
                    projection[i] = readType.getFieldIndex(field.name());
                    getters[i] =
                            InternalRow.createFieldGetter(
                                    field.type(), readType.getFieldIndex(field.name()));
                }
                ProjectedRow projectedRow = ProjectedRow.from(projection);
                while (rows.hasNext()) {
                    InternalRow row = rows.next();
                    boolean hasNull = false;
                    for (InternalRow.FieldGetter getter : getters) {
                        if (getter.getFieldOrNull(row) == null) {
                            hasNull = true;
                            break;
                        }
                    }
                    if (hasNull) {
                        LOG.info(
                                "Null value in indexed columns, stopping shard [{}, {}].",
                                rowRange.from,
                                rowRange.to);
                        break;
                    }
                    multiWriter.write(projectedRow.replaceRow(row));
                    rowCounter.add(1);
                }
            } else {
                DataField indexField = indexFields.get(0);
                GlobalIndexSingletonWriter singleWriter = (GlobalIndexSingletonWriter) indexWriter;
                InternalRow.FieldGetter getter =
                        InternalRow.createFieldGetter(
                                indexField.type(), readType.getFieldIndex(indexField.name()));
                rows.forEachRemaining(
                        row -> {
                            Object indexO = getter.getFieldOrNull(row);
                            singleWriter.write(indexO);
                            rowCounter.add(1);
                        });
            }
            return indexWriter.finish();
        } finally {
            closeWriterQuietly(indexWriter);
        }
    }

    private static void closeWriterQuietly(GlobalIndexWriter writer) {
        if (writer instanceof java.io.Closeable) {
            try {
                ((java.io.Closeable) writer).close();
            } catch (IOException ignored) {
            }
        }
    }
}
