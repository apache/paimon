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

package org.apache.flink.table.store.table;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.operation.FileStoreRead;
import org.apache.flink.table.store.file.utils.RecordReader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/** An abstraction layer above {@link FileStoreRead} to provide reading of {@link RowData}. */
public abstract class TableRead {

    protected final FileStoreRead read;

    protected TableRead(FileStoreRead read) {
        this.read = read;
    }

    public TableRead withProjection(int[][] projection) {
        withProjectionImpl(projection);
        return this;
    }

    protected abstract void withProjectionImpl(int[][] projection);

    public RecordReader<RowData> createReader(
            BinaryRowData partition, int bucket, List<DataFileMeta> files) throws IOException {
        return new RowDataRecordReader(read.createReader(partition, bucket, files));
    }

    protected abstract Iterator<RowData> rowDataIteratorFromKv(KeyValue kv);

    private class RowDataRecordReader implements RecordReader<RowData> {

        private final RecordReader<KeyValue> wrapped;

        private RowDataRecordReader(RecordReader<KeyValue> wrapped) {
            this.wrapped = wrapped;
        }

        @Nullable
        @Override
        public RecordIterator<RowData> readBatch() throws IOException {
            RecordIterator<KeyValue> batch = wrapped.readBatch();
            return batch == null ? null : new RowDataRecordIterator(batch);
        }

        @Override
        public void close() throws IOException {
            wrapped.close();
        }
    }

    private class RowDataRecordIterator implements RecordReader.RecordIterator<RowData> {

        private final RecordReader.RecordIterator<KeyValue> wrapped;
        private Iterator<RowData> iterator;

        private RowDataRecordIterator(RecordReader.RecordIterator<KeyValue> wrapped) {
            this.wrapped = wrapped;
            this.iterator = null;
        }

        @Override
        public RowData next() throws IOException {
            while (true) {
                if (iterator != null && iterator.hasNext()) {
                    return iterator.next();
                }
                KeyValue kv = wrapped.next();
                if (kv == null) {
                    return null;
                }
                iterator = rowDataIteratorFromKv(kv);
            }
        }

        @Override
        public void releaseBatch() {
            wrapped.releaseBatch();
        }
    }
}
