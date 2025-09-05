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

package org.apache.paimon.table.format;

import org.apache.paimon.casting.DefaultValueRow;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.FileStoreWrite.State;
import org.apache.paimon.operation.WriteRestore;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.InnerTableWrite;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.table.sink.TableWrite;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Restorable;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** {@link TableWrite} implementation for format table. */
public class FormatTableWrite implements InnerTableWrite, Restorable<List<State<InternalRow>>> {

    private final RowType rowType;
    private final FormatTableFileWrite write;
    private final RowPartitionKeyExtractor partitionKeyExtractor;

    private boolean batchCommitted = false;

    private final int[] notNullFieldIndex;
    private final @Nullable DefaultValueRow defaultValueRow;

    public FormatTableWrite(
            RowType rowType,
            FormatTableFileWrite write,
            RowPartitionKeyExtractor partitionKeyExtractor,
            boolean ignoreDelete) {
        this.rowType = rowType;
        this.write = write;
        this.partitionKeyExtractor = partitionKeyExtractor;

        List<String> notNullColumnNames =
                rowType.getFields().stream()
                        .filter(field -> !field.type().isNullable())
                        .map(DataField::name)
                        .collect(Collectors.toList());
        this.notNullFieldIndex = rowType.getFieldIndices(notNullColumnNames);
        this.defaultValueRow = DefaultValueRow.create(rowType);
    }

    @Override
    public InnerTableWrite withWriteRestore(WriteRestore writeRestore) {
        this.write.withWriteRestore(writeRestore);
        return this;
    }

    @Override
    public FormatTableWrite withIgnorePreviousFiles(boolean ignorePreviousFiles) {
        write.withIgnorePreviousFiles(ignorePreviousFiles);
        return this;
    }

    @Override
    public FormatTableWrite withIOManager(IOManager ioManager) {
        write.withIOManager(ioManager);
        return this;
    }

    @Override
    public BatchTableWrite withWriteType(RowType writeType) {
        write.withWriteType(writeType);
        return this;
    }

    @Override
    public FormatTableWrite withMemoryPool(MemorySegmentPool memoryPool) {
        write.withMemoryPool(memoryPool);
        return this;
    }

    @Override
    public BinaryRow getPartition(InternalRow row) {
        return partitionKeyExtractor.partition(row);
    }

    @Override
    public int getBucket(InternalRow row) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(InternalRow row) throws Exception {
        checkNullability(row);
        row = wrapDefaultValue(row);
        BinaryRow partition = partitionKeyExtractor.partition(row);
        write.write(partition, row);
    }

    @Override
    public void write(InternalRow row, int bucket) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBundle(BinaryRow partition, int bucket, BundleRecords bundle)
            throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public FormatTableWrite withMetricRegistry(MetricRegistry metricRegistry) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws Exception {
        write.close();
    }

    @Override
    public List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier)
            throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CommitMessage> prepareCommit() throws Exception {
        write.flush();
        return new ArrayList<>();
    }

    @Override
    public List<State<InternalRow>> checkpoint() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void restore(List<State<InternalRow>> state) {
        throw new UnsupportedOperationException();
    }

    private void checkNullability(InternalRow row) {
        for (int idx : notNullFieldIndex) {
            if (row.isNullAt(idx)) {
                String columnName = rowType.getFields().get(idx).name();
                throw new RuntimeException(
                        String.format("Cannot write null to non-null column(%s)", columnName));
            }
        }
    }

    private InternalRow wrapDefaultValue(InternalRow row) {
        return defaultValueRow == null ? row : defaultValueRow.replaceRow(row);
    }
}
