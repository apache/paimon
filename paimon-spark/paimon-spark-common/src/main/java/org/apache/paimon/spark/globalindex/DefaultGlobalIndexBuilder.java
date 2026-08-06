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
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.createIndexWriter;
import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.toIndexFileMetas;

/** Default global index builder. */
public class DefaultGlobalIndexBuilder implements Serializable {

    private static final long serialVersionUID = 1L;

    private final FileStoreTable table;
    private final BinaryRow partition;
    private final RowType readType;
    private final DataField indexField;
    private final List<DataField> extraFields;
    private final String indexType;
    private final Range rowRange;
    private final Options options;
    private final @Nullable byte[] sourceMeta;

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
                indexField,
                Collections.emptyList(),
                indexType,
                rowRange,
                options,
                null);
    }

    public DefaultGlobalIndexBuilder(
            FileStoreTable table,
            BinaryRow partition,
            RowType readType,
            DataField indexField,
            List<DataField> extraFields,
            String indexType,
            Range rowRange,
            Options options) {
        this(
                table,
                partition,
                readType,
                indexField,
                extraFields,
                indexType,
                rowRange,
                options,
                null);
    }

    public DefaultGlobalIndexBuilder(
            FileStoreTable table,
            BinaryRow partition,
            RowType readType,
            DataField indexField,
            List<DataField> extraFields,
            String indexType,
            Range rowRange,
            Options options,
            @Nullable byte[] sourceMeta) {
        this.table = table;
        this.partition = partition;
        this.readType = readType;
        this.indexField = indexField;
        // Copy into a serializable ArrayList: callers may pass a List#subList view (e.g.
        // indexFields.subList(1, ...)), which is not Serializable, and this builder is serialized
        // and shipped to Spark executors. A null value means no extra columns.
        this.extraFields =
                extraFields == null ? Collections.emptyList() : new ArrayList<>(extraFields);
        this.indexType = indexType;
        this.rowRange = rowRange;
        this.options = options;
        this.sourceMeta = sourceMeta;
    }

    /** The primary index column followed by the extra columns, in index order. */
    private List<DataField> indexedFields() {
        List<DataField> fields = new ArrayList<>(1 + extraFields.size());
        fields.add(indexField);
        fields.addAll(extraFields);
        return fields;
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
                        indexedFields(),
                        indexType,
                        resultEntries,
                        sourceMeta);
        DataIncrement dataIncrement = DataIncrement.indexIncrement(indexFileMetas);
        return new CommitMessageImpl(
                partition, 0, null, dataIncrement, CompactIncrement.emptyIncrement());
    }

    private List<ResultEntry> writePaimonRows(
            CloseableIterator<InternalRow> rows, LongCounter rowCounter) throws IOException {
        GlobalIndexWriter indexWriter =
                createIndexWriter(table, indexType, indexField, extraFields, options);
        boolean multiColumn = !extraFields.isEmpty();

        try {
            if (multiColumn) {
                GlobalIndexMultiColumnWriter multiWriter =
                        (GlobalIndexMultiColumnWriter) indexWriter;
                List<DataField> indexedFields = indexedFields();
                int[] projection = new int[indexedFields.size()];
                for (int i = 0; i < indexedFields.size(); i++) {
                    DataField field = indexedFields.get(i);
                    projection[i] = readType.getFieldIndex(field.name());
                }
                ProjectedRow projectedRow = ProjectedRow.from(projection);
                int rowIdIndex = readType.getFieldIndex(SpecialFields.ROW_ID.name());
                while (rows.hasNext()) {
                    InternalRow row = rows.next();
                    long absRowId = row.getLong(rowIdIndex);
                    if (absRowId < rowRange.from || absRowId > rowRange.to) {
                        continue;
                    }
                    multiWriter.write(absRowId - rowRange.from, projectedRow.replaceRow(row));
                    rowCounter.add(1);
                }
            } else {
                GlobalIndexSingleColumnWriter singleWriter =
                        (GlobalIndexSingleColumnWriter) indexWriter;
                InternalRow.FieldGetter getter =
                        InternalRow.createFieldGetter(
                                indexField.type(), readType.getFieldIndex(indexField.name()));
                int rowIdIndex = readType.getFieldIndex(SpecialFields.ROW_ID.name());
                while (rows.hasNext()) {
                    InternalRow row = rows.next();
                    long absRowId = row.getLong(rowIdIndex);
                    if (absRowId < rowRange.from || absRowId > rowRange.to) {
                        continue;
                    }
                    Object indexO = getter.getFieldOrNull(row);
                    singleWriter.write(indexO, absRowId - rowRange.from);
                    rowCounter.add(1);
                }
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
