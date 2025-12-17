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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.casting.DefaultValueRow;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.FormatTableRowPartitionKeyExtractor;
import org.apache.paimon.table.sink.TableWrite;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ProjectedRow;

import javax.annotation.Nullable;

import java.util.List;
import java.util.stream.Collectors;

/** {@link TableWrite} implementation for format table. */
public class FormatTableWrite implements BatchTableWrite {

    private RowType rowType;
    private final FormatTableFileWriter write;
    private final FormatTableRowPartitionKeyExtractor partitionKeyExtractor;

    private final int[] notNullFieldIndex;
    private final @Nullable DefaultValueRow defaultValueRow;
    private final ProjectedRow projectedRow;
    private final RowType writeRowType;

    public FormatTableWrite(
            FileIO fileIO,
            RowType rowType,
            CoreOptions options,
            RowType partitionType,
            List<String> partitionKeys) {
        this.rowType = rowType;
        this.partitionKeyExtractor =
                new FormatTableRowPartitionKeyExtractor(rowType, partitionKeys);
        List<String> notNullColumnNames =
                rowType.getFields().stream()
                        .filter(field -> !field.type().isNullable())
                        .map(DataField::name)
                        .collect(Collectors.toList());
        this.notNullFieldIndex = rowType.getFieldIndices(notNullColumnNames);
        this.defaultValueRow = DefaultValueRow.create(rowType);
        this.writeRowType =
                rowType.project(
                        rowType.getFieldNames().stream()
                                .filter(name -> !partitionType.getFieldNames().contains(name))
                                .collect(Collectors.toList()));
        this.projectedRow = ProjectedRow.from(writeRowType, rowType);
        this.write = new FormatTableFileWriter(fileIO, writeRowType, options, partitionType);
    }

    @Override
    public BinaryRow getPartition(InternalRow row) {
        return partitionKeyExtractor.partition(row);
    }

    @Override
    public void write(InternalRow row) throws Exception {
        // checkNullability
        for (int idx : notNullFieldIndex) {
            if (row.isNullAt(idx)) {
                String columnName = rowType.getFields().get(idx).name();
                throw new RuntimeException(
                        String.format("Cannot write null to non-null column(%s)", columnName));
            }
        }
        row = defaultValueRow == null ? row : defaultValueRow.replaceRow(row);
        BinaryRow partition = partitionKeyExtractor.partition(row);
        write.write(partition, projectedRow.replaceRow(row));
    }

    @Override
    public List<CommitMessage> prepareCommit() throws Exception {
        return write.prepareCommit();
    }

    @Override
    public void close() throws Exception {
        write.close();
    }

    @Override
    public int getBucket(InternalRow row) {
        return 0;
    }

    @Override
    public TableWrite withMemoryPoolFactory(MemoryPoolFactory memoryPoolFactory) {
        return this;
    }

    @Override
    public BatchTableWrite withIOManager(IOManager ioManager) {
        return this;
    }

    @Override
    public BatchTableWrite withWriteType(RowType writeType) {
        throw new UnsupportedOperationException();
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
    public TableWrite withMetricRegistry(MetricRegistry registry) {
        return this;
    }
}
