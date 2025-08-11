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
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.AppendOnlyFileStoreScan;
import org.apache.paimon.operation.BaseAppendFileStoreWrite;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.AppendOnlySplitGenerator;
import org.apache.paimon.table.source.AppendTableRead;
import org.apache.paimon.table.source.DataEvolutionSplitGenerator;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.table.source.splitread.AppendTableRawFileSplitReadProvider;
import org.apache.paimon.table.source.splitread.DataEvolutionSplitReadProvider;
import org.apache.paimon.table.source.splitread.SplitReadConfig;
import org.apache.paimon.table.source.splitread.SplitReadProvider;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

/** {@link FileStoreTable} for append table. */
public class AppendOnlyFileStoreTable extends AbstractFileStoreTable {

    private static final long serialVersionUID = 1L;

    private transient AppendOnlyFileStore lazyStore;

    AppendOnlyFileStoreTable(FileIO fileIO, Path path, TableSchema tableSchema) {
        this(fileIO, path, tableSchema, CatalogEnvironment.empty());
    }

    public AppendOnlyFileStoreTable(
            FileIO fileIO,
            Path path,
            TableSchema tableSchema,
            CatalogEnvironment catalogEnvironment) {
        super(fileIO, path, tableSchema, catalogEnvironment);
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
                            tableSchema.logicalRowType().notNull(),
                            name(),
                            catalogEnvironment);
        }
        return lazyStore;
    }

    @Override
    protected SplitGenerator splitGenerator() {
        long targetSplitSize = store().options().splitTargetSize();
        long openFileCost = store().options().splitOpenFileCost();
        return coreOptions().dataEvolutionEnabled()
                ? new DataEvolutionSplitGenerator(targetSplitSize, openFileCost)
                : new AppendOnlySplitGenerator(targetSplitSize, openFileCost, bucketMode());
    }

    @Override
    public boolean supportStreamingReadOverwrite() {
        return new CoreOptions(tableSchema.options()).streamingReadAppendOverwrite();
    }

    @Override
    protected BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer() {
        return (scan, predicate) -> ((AppendOnlyFileStoreScan) scan).withFilter(predicate);
    }

    @Override
    public InnerTableRead newRead() {
        List<Function<SplitReadConfig, SplitReadProvider>> providerFactories = new ArrayList<>();
        if (coreOptions().dataEvolutionEnabled()) {
            // add data evolution first
            providerFactories.add(
                    config ->
                            new DataEvolutionSplitReadProvider(
                                    () -> store().newDataEvolutionRead(), config));
        }
        providerFactories.add(
                config -> new AppendTableRawFileSplitReadProvider(() -> store().newRead(), config));
        return new AppendTableRead(providerFactories, schema());
    }

    @Override
    public TableWriteImpl<InternalRow> newWrite(String commitUser) {
        return newWrite(commitUser, null);
    }

    @Override
    public TableWriteImpl<InternalRow> newWrite(String commitUser, @Nullable Integer writeId) {
        BaseAppendFileStoreWrite writer = store().newWrite(commitUser, writeId);
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
}
