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

package org.apache.paimon.table.source;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.operation.KeyValueFileStoreRead;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * An abstraction layer above {@link KeyValueFileStoreRead} to provide reading of {@link
 * InternalRow}.
 */
public abstract class KeyValueTableRead extends AbstractDataTableRead<KeyValue> {

    protected final KeyValueFileStoreRead read;

    protected KeyValueTableRead(KeyValueFileStoreRead read, TableSchema schema) {
        super(read, schema);
        this.read = read;
    }

    @Override
    public TableRead withIOManager(IOManager ioManager) {
        read.withIOManager(ioManager);
        return this;
    }

    @Override
    public final RecordReader<InternalRow> reader(Split split) throws IOException {
        return new RowDataRecordReader(read.createReader((DataSplit) split));
    }

    public final RecordReader<KeyValue> kvReader(Split split) throws IOException {
        return read.createReader((DataSplit) split);
    }

    protected abstract RecordReader.RecordIterator<InternalRow> rowDataRecordIteratorFromKv(
            RecordReader.RecordIterator<KeyValue> kvRecordIterator);

    private class RowDataRecordReader implements RecordReader<InternalRow> {

        private final RecordReader<KeyValue> wrapped;

        private RowDataRecordReader(RecordReader<KeyValue> wrapped) {
            this.wrapped = wrapped;
        }

        @Nullable
        @Override
        public RecordIterator<InternalRow> readBatch() throws IOException {
            RecordIterator<KeyValue> batch = wrapped.readBatch();
            return batch == null ? null : rowDataRecordIteratorFromKv(batch);
        }

        @Override
        public void close() throws IOException {
            wrapped.close();
        }
    }
}
