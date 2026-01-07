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
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
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
import org.apache.paimon.utils.Range;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import static org.apache.paimon.spark.globalindex.GlobalIndexBuilderUtils.createIndexWriter;
import static org.apache.paimon.spark.globalindex.GlobalIndexBuilderUtils.toIndexFileMetas;

/** Default global index builder. */
public class DefaultGlobalIndexBuilder implements Serializable {

    private static final long serialVersionUID = 1L;

    private final FileStoreTable table;
    private final BinaryRow partition;
    private final RowType readType;
    private final DataField indexField;
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
        this.table = table;
        this.partition = partition;
        this.readType = readType;
        this.indexField = indexField;
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
                toIndexFileMetas(table, rowRange, indexField.id(), indexType, resultEntries);
        DataIncrement dataIncrement = DataIncrement.indexIncrement(indexFileMetas);
        return new CommitMessageImpl(
                partition, 0, null, dataIncrement, CompactIncrement.emptyIncrement());
    }

    private List<ResultEntry> writePaimonRows(
            CloseableIterator<InternalRow> rows, LongCounter rowCounter) throws IOException {
        GlobalIndexSingletonWriter indexWriter =
                (GlobalIndexSingletonWriter)
                        createIndexWriter(table, indexType, indexField, options);

        InternalRow.FieldGetter getter =
                InternalRow.createFieldGetter(
                        indexField.type(), readType.getFieldIndex(indexField.name()));
        rows.forEachRemaining(
                row -> {
                    Object indexO = getter.getFieldOrNull(row);
                    indexWriter.write(indexO);
                    rowCounter.add(1);
                });
        return indexWriter.finish();
    }
}
