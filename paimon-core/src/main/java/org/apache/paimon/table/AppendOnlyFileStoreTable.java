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

package org.apache.paimon.table;

import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.iceberg.AppendOnlyIcebergCommitCallback;
import org.apache.paimon.iceberg.IcebergOptions;
import org.apache.paimon.io.TablePathProvider;
import org.apache.paimon.manifest.ManifestCacheFilter;
import org.apache.paimon.operation.AppendOnlyFileStoreScan;
import org.apache.paimon.operation.AppendOnlyFileStoreWrite;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.RawFileSplitRead;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.AbstractDataTableRead;
import org.apache.paimon.table.source.AppendOnlySplitGenerator;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import java.io.IOException;
import java.util.List;
import java.util.function.BiConsumer;

/** {@link FileStoreTable} for append table. */
class AppendOnlyFileStoreTable extends AbstractFileStoreTable {

    private static final long serialVersionUID = 1L;

    private transient AppendOnlyFileStore lazyStore;

    AppendOnlyFileStoreTable(
            FileIO fileIO, TablePathProvider tablePathProvider, TableSchema tableSchema) {
        this(fileIO, tablePathProvider, tableSchema, CatalogEnvironment.empty());
    }

    AppendOnlyFileStoreTable(
            FileIO fileIO,
            TablePathProvider tablePathProvider,
            TableSchema tableSchema,
            CatalogEnvironment catalogEnvironment) {
        super(fileIO, tablePathProvider, tableSchema, catalogEnvironment);
    }

    @Override
    public AppendOnlyFileStore store() {
        if (lazyStore == null) {
            lazyStore =
                    new AppendOnlyFileStore(
                            fileIO,
                            schemaManager(),
                            tableSchema,
                            new CoreOptions(tableSchema.options()),
                            tableSchema.logicalPartitionType(),
                            tableSchema.logicalBucketKeyType(),
                            tableSchema.logicalRowType(),
                            name(),
                            catalogEnvironment,
                            tablePathProvider);
        }
        return lazyStore;
    }

    @Override
    protected SplitGenerator splitGenerator() {
        return new AppendOnlySplitGenerator(
                store().options().splitTargetSize(),
                store().options().splitOpenFileCost(),
                bucketMode());
    }

    /**
     * Currently, the streaming read of overwrite is implemented by reversing the {@link RowKind} of
     * overwrote records to {@link RowKind#DELETE}, so only tables that have primary key support it.
     */
    @Override
    public boolean supportStreamingReadOverwrite() {
        return false;
    }

    @Override
    protected BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer() {
        return (scan, predicate) -> ((AppendOnlyFileStoreScan) scan).withFilter(predicate);
    }

    @Override
    public InnerTableRead newRead() {
        RawFileSplitRead read = store().newRead();
        return new AbstractDataTableRead<InternalRow>(schema()) {

            @Override
            protected InnerTableRead innerWithFilter(Predicate predicate) {
                read.withFilter(predicate);
                return this;
            }

            @Override
            public void applyReadType(RowType readType) {
                read.withReadType(readType);
            }

            @Override
            public RecordReader<InternalRow> reader(Split split) throws IOException {
                return read.createReader((DataSplit) split);
            }
        };
    }

    @Override
    public TableWriteImpl<InternalRow> newWrite(String commitUser) {
        return newWrite(commitUser, null);
    }

    @Override
    public TableWriteImpl<InternalRow> newWrite(
            String commitUser, ManifestCacheFilter manifestFilter) {
        AppendOnlyFileStoreWrite writer = store().newWrite(commitUser, manifestFilter);
        return new TableWriteImpl<>(
                rowType(),
                writer,
                createRowKeyExtractor(),
                (record, rowKind) -> {
                    Preconditions.checkState(
                            rowKind.isAdd(),
                            "Append only writer can not accept row with RowKind %s",
                            rowKind);
                    return record.row();
                },
                rowKindGenerator(),
                CoreOptions.fromMap(tableSchema.options()).ignoreDelete());
    }

    @Override
    public LocalTableQuery newLocalTableQuery() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected List<CommitCallback> createCommitCallbacks(String commitUser) {
        List<CommitCallback> callbacks = super.createCommitCallbacks(commitUser);
        CoreOptions options = coreOptions();

        if (options.toConfiguration().get(IcebergOptions.METADATA_ICEBERG_STORAGE)
                != IcebergOptions.StorageType.DISABLED) {
            callbacks.add(new AppendOnlyIcebergCommitCallback(this, commitUser));
        }

        return callbacks;
    }
}
