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

package org.apache.flink.table.store.table.source;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.operation.KeyValueFileStoreRead;
import org.apache.flink.table.store.file.utils.RecordReader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/**
 * An abstraction layer above {@link KeyValueFileStoreRead} to provide reading of {@link RowData}.
 */
public abstract class KeyValueTableRead implements TableRead {

    protected final KeyValueFileStoreRead read;

    protected KeyValueTableRead(KeyValueFileStoreRead read) {
        this.read = read;
    }

    @Override
    public RecordReader<RowData> createReader(
            BinaryRowData partition, int bucket, List<DataFileMeta> files) throws IOException {
        return new RowDataRecordReader(read.createReader(partition, bucket, files));
    }

    protected abstract RecordReader.RecordIterator<RowData> rowDataRecordIteratorFromKv(
            RecordReader.RecordIterator<KeyValue> kvRecordIterator);

    private class RowDataRecordReader implements RecordReader<RowData> {

        private final RecordReader<KeyValue> wrapped;

        private RowDataRecordReader(RecordReader<KeyValue> wrapped) {
            this.wrapped = wrapped;
        }

        @Nullable
        @Override
        public RecordIterator<RowData> readBatch() throws IOException {
            RecordIterator<KeyValue> batch = wrapped.readBatch();
            return batch == null ? null : rowDataRecordIteratorFromKv(batch);
        }

        @Override
        public void close() throws IOException {
            wrapped.close();
        }
    }
}
