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

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.AppendOnlyFileStore;
import org.apache.flink.table.store.file.WriteMode;
import org.apache.flink.table.store.file.operation.AppendOnlyFileStoreRead;
import org.apache.flink.table.store.file.operation.AppendOnlyFileStoreScan;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.table.sink.SinkRecordConverter;
import org.apache.flink.table.store.table.sink.TableWrite;
import org.apache.flink.table.store.table.sink.TableWriteImpl;
import org.apache.flink.table.store.table.source.AppendOnlySplitGenerator;
import org.apache.flink.table.store.table.source.DataSplit;
import org.apache.flink.table.store.table.source.DataTableScan;
import org.apache.flink.table.store.table.source.Split;
import org.apache.flink.table.store.table.source.SplitGenerator;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/** {@link FileStoreTable} for {@link WriteMode#APPEND_ONLY} write mode. */
public class AppendOnlyFileStoreTable extends AbstractFileStoreTable {

    private static final long serialVersionUID = 1L;

    private final AppendOnlyFileStore store;

    AppendOnlyFileStoreTable(Path path, SchemaManager schemaManager, TableSchema tableSchema) {
        super(path, tableSchema);
        this.store =
                new AppendOnlyFileStore(
                        schemaManager,
                        tableSchema.id(),
                        new CoreOptions(tableSchema.options()),
                        tableSchema.logicalPartitionType(),
                        tableSchema.logicalBucketKeyType(),
                        tableSchema.logicalRowType());
    }

    @Override
    public DataTableScan newScan() {
        AppendOnlyFileStoreScan scan = store.newScan();
        return new DataTableScan(scan, tableSchema, store.pathFactory()) {
            @Override
            protected SplitGenerator splitGenerator(FileStorePathFactory pathFactory) {
                return new AppendOnlySplitGenerator(
                        store.options().splitTargetSize(), store.options().splitOpenFileCost());
            }

            @Override
            protected void withNonPartitionFilter(Predicate predicate) {
                scan.withFilter(predicate);
            }
        };
    }

    @Override
    public TableRead newRead() {
        AppendOnlyFileStoreRead read = store.newRead();
        return new TableRead() {
            @Override
            public TableRead withFilter(Predicate predicate) {
                read.withFilter(predicate);
                return this;
            }

            @Override
            public TableRead withProjection(int[][] projection) {
                read.withProjection(projection);
                return this;
            }

            @Override
            public RecordReader<RowData> createReader(Split split) throws IOException {
                return read.createReader((DataSplit) split);
            }
        };
    }

    @Override
    public TableWrite newWrite(String commitUser) {
        SinkRecordConverter recordConverter =
                new SinkRecordConverter(store.options().bucket(), tableSchema);
        return new TableWriteImpl<>(
                store.newWrite(commitUser),
                recordConverter,
                record -> {
                    Preconditions.checkState(
                            record.row().getRowKind() == RowKind.INSERT,
                            "Append only writer can not accept row with RowKind %s",
                            record.row().getRowKind());
                    return record.row();
                });
    }

    @Override
    public AppendOnlyFileStore store() {
        return store;
    }
}
