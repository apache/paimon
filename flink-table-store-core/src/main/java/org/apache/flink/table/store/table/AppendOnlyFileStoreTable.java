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

import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.file.AppendOnlyFileStore;
import org.apache.flink.table.store.file.WriteMode;
import org.apache.flink.table.store.file.operation.AppendOnlyFileStoreRead;
import org.apache.flink.table.store.file.operation.AppendOnlyFileStoreScan;
import org.apache.flink.table.store.file.operation.ReverseReader;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.fs.FileIO;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.reader.RecordReader;
import org.apache.flink.table.store.table.sink.SinkRecordConverter;
import org.apache.flink.table.store.table.sink.TableWriteImpl;
import org.apache.flink.table.store.table.source.AbstractDataTableScan;
import org.apache.flink.table.store.table.source.AppendOnlySplitGenerator;
import org.apache.flink.table.store.table.source.DataSplit;
import org.apache.flink.table.store.table.source.InnerTableRead;
import org.apache.flink.table.store.table.source.Split;
import org.apache.flink.table.store.table.source.SplitGenerator;
import org.apache.flink.table.store.types.RowKind;
import org.apache.flink.table.store.utils.Preconditions;

import java.io.IOException;

/** {@link FileStoreTable} for {@link WriteMode#APPEND_ONLY} write mode. */
public class AppendOnlyFileStoreTable extends AbstractFileStoreTable {

    private static final long serialVersionUID = 1L;

    private transient AppendOnlyFileStore lazyStore;

    AppendOnlyFileStoreTable(FileIO fileIO, Path path, TableSchema tableSchema) {
        super(fileIO, path, tableSchema);
    }

    @Override
    protected FileStoreTable copy(TableSchema newTableSchema) {
        return new AppendOnlyFileStoreTable(fileIO, path, newTableSchema);
    }

    @Override
    public AppendOnlyFileStore store() {
        if (lazyStore == null) {
            lazyStore =
                    new AppendOnlyFileStore(
                            fileIO,
                            schemaManager(),
                            tableSchema.id(),
                            new CoreOptions(tableSchema.options()),
                            tableSchema.logicalPartitionType(),
                            tableSchema.logicalBucketKeyType(),
                            tableSchema.logicalRowType());
        }
        return lazyStore;
    }

    @Override
    public AbstractDataTableScan newScan() {
        AppendOnlyFileStoreScan scan = store().newScan();
        return new AbstractDataTableScan(
                fileIO, scan, tableSchema, store().pathFactory(), options()) {
            /**
             * Currently, the streaming read of overwrite is implemented by reversing the {@link
             * RowKind} of overwrote records to {@link RowKind#DELETE}, so only tables that have
             * primary key support it.
             *
             * @see ReverseReader
             */
            @Override
            public boolean supportStreamingReadOverwrite() {
                return false;
            }

            @Override
            protected SplitGenerator splitGenerator(FileStorePathFactory pathFactory) {
                return new AppendOnlySplitGenerator(
                        store().options().splitTargetSize(), store().options().splitOpenFileCost());
            }

            @Override
            protected void withNonPartitionFilter(Predicate predicate) {
                scan.withFilter(predicate);
            }
        };
    }

    @Override
    public InnerTableRead newRead() {
        AppendOnlyFileStoreRead read = store().newRead();
        return new InnerTableRead() {
            @Override
            public InnerTableRead withFilter(Predicate predicate) {
                read.withFilter(predicate);
                return this;
            }

            @Override
            public InnerTableRead withProjection(int[][] projection) {
                read.withProjection(projection);
                return this;
            }

            @Override
            public RecordReader<InternalRow> createReader(Split split) throws IOException {
                return read.createReader((DataSplit) split);
            }
        };
    }

    @Override
    public TableWriteImpl<InternalRow> newWrite(String commitUser) {
        return new TableWriteImpl<>(
                store().newWrite(commitUser),
                new SinkRecordConverter(tableSchema),
                record -> {
                    Preconditions.checkState(
                            record.row().getRowKind() == RowKind.INSERT,
                            "Append only writer can not accept row with RowKind %s",
                            record.row().getRowKind());
                    return record.row();
                });
    }
}
