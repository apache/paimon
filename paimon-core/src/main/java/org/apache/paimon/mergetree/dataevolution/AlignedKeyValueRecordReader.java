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

package org.apache.paimon.mergetree.dataevolution;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.reader.DataEvolutionRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/**
 * Reads a base file and its column files in lockstep. The n-th row of each column file replaces the
 * columns it holds in the n-th record of the base file.
 *
 * <p>Every record gets its own {@link DataEvolutionRow} and copied column rows. The merge keeps
 * records of earlier calls to {@code next()}, so neither may be reused.
 */
public class AlignedKeyValueRecordReader implements RecordReader<KeyValue> {

    private final DataFileMeta baseFile;
    private final RecordReader<KeyValue> baseReader;
    private final List<DataFileMeta> columnFiles;
    private final List<RecordReader<InternalRow>> columnReaders;
    private final RecordIterator<InternalRow>[] columnBatches;
    private final InternalRowSerializer columnSerializer;
    private final int[] rowOffsets;
    private final int[] fieldOffsets;

    private long readRows;

    @SuppressWarnings("unchecked")
    public AlignedKeyValueRecordReader(
            DataFileMeta baseFile,
            RecordReader<KeyValue> baseReader,
            List<DataFileMeta> columnFiles,
            List<RecordReader<InternalRow>> columnReaders,
            RowType readValueType,
            int[] rowOffsets,
            int[] fieldOffsets) {
        this.baseFile = baseFile;
        this.baseReader = baseReader;
        this.columnFiles = columnFiles;
        this.columnReaders = columnReaders;
        this.columnBatches = new RecordIterator[columnReaders.size()];
        this.columnSerializer = new InternalRowSerializer(readValueType);
        this.rowOffsets = rowOffsets;
        this.fieldOffsets = fieldOffsets;
    }

    @Nullable
    @Override
    public RecordIterator<KeyValue> readBatch() throws IOException {
        RecordIterator<KeyValue> batch = baseReader.readBatch();
        if (batch == null) {
            checkColumnFilesExhausted();
            return null;
        }
        return new RecordIterator<KeyValue>() {
            @Nullable
            @Override
            public KeyValue next() throws IOException {
                KeyValue kv = batch.next();
                if (kv == null) {
                    return null;
                }
                DataEvolutionRow value =
                        new DataEvolutionRow(columnReaders.size() + 1, rowOffsets, fieldOffsets);
                value.setRow(0, kv.value());
                for (int i = 0; i < columnReaders.size(); i++) {
                    InternalRow row = nextColumnRow(i);
                    if (row == null) {
                        throw new IllegalStateException(
                                String.format(
                                        "Column file %s has fewer rows than base file %s, only %s rows are read.",
                                        columnFiles.get(i).fileName(),
                                        baseFile.fileName(),
                                        readRows));
                    }
                    value.setRow(i + 1, row);
                }
                readRows++;
                return kv.replaceValue(value);
            }

            @Override
            public void releaseBatch() {
                batch.releaseBatch();
            }
        };
    }

    /** Column rows are copied because a column batch can be released before the base batch. */
    @Nullable
    private InternalRow nextColumnRow(int i) throws IOException {
        while (true) {
            if (columnBatches[i] == null) {
                columnBatches[i] = columnReaders.get(i).readBatch();
                if (columnBatches[i] == null) {
                    return null;
                }
            }
            InternalRow row = columnBatches[i].next();
            if (row != null) {
                return columnSerializer.copy(row);
            }
            columnBatches[i].releaseBatch();
            columnBatches[i] = null;
        }
    }

    private void checkColumnFilesExhausted() throws IOException {
        for (int i = 0; i < columnReaders.size(); i++) {
            if (nextColumnRow(i) != null) {
                throw new IllegalStateException(
                        String.format(
                                "Column file %s has more rows than base file %s, which has %s rows.",
                                columnFiles.get(i).fileName(), baseFile.fileName(), readRows));
            }
        }
    }

    @Override
    public void close() throws IOException {
        IOException exception = null;
        for (int i = 0; i < columnReaders.size(); i++) {
            if (columnBatches[i] != null) {
                columnBatches[i].releaseBatch();
                columnBatches[i] = null;
            }
            try {
                columnReaders.get(i).close();
            } catch (IOException e) {
                exception = addSuppressed(exception, e);
            }
        }
        try {
            baseReader.close();
        } catch (IOException e) {
            exception = addSuppressed(exception, e);
        }
        if (exception != null) {
            throw exception;
        }
    }

    private static IOException addSuppressed(@Nullable IOException first, IOException next) {
        if (first == null) {
            return next;
        }
        first.addSuppressed(next);
        return first;
    }
}
