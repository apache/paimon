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
import org.apache.paimon.operation.MergeFileSplitRead;
import org.apache.paimon.operation.RawFileSplitRead;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.utils.LazyField;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * An abstraction layer above {@link MergeFileSplitRead} to provide reading of {@link InternalRow}.
 */
public final class KeyValueTableRead extends AbstractDataTableRead<KeyValue> {

    private final LazyField<MergeFileSplitRead> mergeRead;
    private final LazyField<RawFileSplitRead> batchRawRead;

    private int[][] projection = null;
    private boolean forceKeepDelete = false;
    private Predicate predicate = null;
    private IOManager ioManager = null;

    public KeyValueTableRead(
            Supplier<MergeFileSplitRead> mergeReadSupplier,
            Supplier<RawFileSplitRead> batchRawReadSupplier,
            TableSchema schema) {
        super(schema);
        this.mergeRead = new LazyField<>(() -> createMergeRead(mergeReadSupplier));
        this.batchRawRead = new LazyField<>(() -> createBatchRawRead(batchRawReadSupplier));
    }

    private MergeFileSplitRead createMergeRead(Supplier<MergeFileSplitRead> readSupplier) {
        MergeFileSplitRead read =
                readSupplier
                        .get()
                        .withKeyProjection(new int[0][])
                        .withValueProjection(projection)
                        .withFilter(predicate)
                        .withIOManager(ioManager);
        if (forceKeepDelete) {
            read = read.forceKeepDelete();
        }
        return read;
    }

    private RawFileSplitRead createBatchRawRead(Supplier<RawFileSplitRead> readSupplier) {
        return readSupplier.get().withProjection(projection).withFilter(predicate);
    }

    @Override
    public void projection(int[][] projection) {
        if (mergeRead.initialized()) {
            mergeRead.get().withValueProjection(projection);
        }
        if (batchRawRead.initialized()) {
            batchRawRead.get().withProjection(projection);
        }
        this.projection = projection;
    }

    @Override
    public InnerTableRead forceKeepDelete() {
        if (mergeRead.initialized()) {
            mergeRead.get().forceKeepDelete();
        }
        this.forceKeepDelete = true;
        return this;
    }

    @Override
    protected InnerTableRead innerWithFilter(Predicate predicate) {
        if (mergeRead.initialized()) {
            mergeRead.get().withFilter(predicate);
        }
        if (batchRawRead.initialized()) {
            batchRawRead.get().withFilter(predicate);
        }
        this.predicate = predicate;
        return this;
    }

    @Override
    public TableRead withIOManager(IOManager ioManager) {
        if (mergeRead.initialized()) {
            mergeRead.get().withIOManager(ioManager);
        }
        this.ioManager = ioManager;
        return this;
    }

    @Override
    public RecordReader<InternalRow> reader(Split split) throws IOException {
        DataSplit dataSplit = (DataSplit) split;
        if (!dataSplit.isStreaming() && split.convertToRawFiles().isPresent()) {
            return batchRawRead.get().createReader(dataSplit);
        }

        RecordReader<KeyValue> reader = mergeRead.get().createReader(dataSplit);
        return new RecordReader<InternalRow>() {

            @Nullable
            @Override
            public RecordIterator<InternalRow> readBatch() throws IOException {
                RecordIterator<KeyValue> batch = reader.readBatch();
                return batch == null ? null : new ValueContentRowDataRecordIterator(batch);
            }

            @Override
            public void close() throws IOException {
                reader.close();
            }
        };
    }
}
