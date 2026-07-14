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

package org.apache.paimon.index.pksorted;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Reads one scalar value from every physical record in a data file. */
public final class PkSortedDataFileReader implements Closeable, PkSortedIndexBuilder.Reader {

    private final DataFileMeta dataFile;
    private final RecordReaderIterator<KeyValue> iterator;
    private final InternalRow.FieldGetter fieldGetter;
    private long rowsRead;

    public PkSortedDataFileReader(
            KeyValueFileReaderFactory readerFactory, DataFileMeta dataFile, DataField indexField)
            throws IOException {
        this.dataFile = dataFile;
        this.iterator = new RecordReaderIterator<>(readerFactory.createRecordReader(dataFile));
        this.fieldGetter = InternalRow.createFieldGetter(indexField.type(), 0);
    }

    public long rowCount() {
        return dataFile.rowCount();
    }

    @Nullable
    public Entry readNext() {
        if (rowsRead == rowCount()) {
            checkArgument(
                    !iterator.hasNext(),
                    "Data file %s contains more rows than declared.",
                    dataFile.fileName());
            return null;
        }
        checkArgument(
                iterator.hasNext(),
                "Data file %s ended before its declared row count.",
                dataFile.fileName());
        KeyValue keyValue = iterator.next();
        long rowPosition = rowsRead++;
        if (rowsRead == rowCount()) {
            checkArgument(
                    !iterator.hasNext(),
                    "Data file %s contains more rows than declared.",
                    dataFile.fileName());
        }
        return new Entry(fieldGetter.getFieldOrNull(keyValue.value()), rowPosition);
    }

    @Override
    public void close() throws IOException {
        try {
            iterator.close();
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(
                    "Failed to close sorted index reader for " + dataFile.fileName(), e);
        }
    }

    /** One projected value and its zero-based physical position. */
    public static final class Entry {

        @Nullable private final Object value;
        private final long rowPosition;

        public Entry(@Nullable Object value, long rowPosition) {
            this.value = value;
            this.rowPosition = rowPosition;
        }

        @Nullable
        public Object value() {
            return value;
        }

        public long rowPosition() {
            return rowPosition;
        }
    }

    /** Bucket-scoped factory with scalar-only projection and no deletion-vector filtering. */
    public static final class Factory {

        private final KeyValueFileReaderFactory readerFactory;
        private final DataField indexField;

        public Factory(
                KeyValueFileReaderFactory.Builder readerFactoryBuilder,
                BinaryRow partition,
                int bucket,
                DataField indexField) {
            this.readerFactory =
                    readerFactoryBuilder
                            .copyWithoutProjection()
                            .withReadKeyType(RowType.of())
                            .withReadValueType(RowType.of(indexField))
                            .buildWithoutDeletionVector(partition, bucket);
            this.indexField = indexField;
        }

        public PkSortedDataFileReader create(DataFileMeta dataFile) throws IOException {
            return new PkSortedDataFileReader(readerFactory, dataFile, indexField);
        }
    }
}
