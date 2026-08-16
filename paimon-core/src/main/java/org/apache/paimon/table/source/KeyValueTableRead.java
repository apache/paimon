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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.operation.MergeFileSplitRead;
import org.apache.paimon.operation.RawFileSplitRead;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.reader.LimitRecordReader;
import org.apache.paimon.reader.ReadBatchSizer;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.splitread.IncrementalChangelogReadProvider;
import org.apache.paimon.table.source.splitread.IncrementalDiffReadProvider;
import org.apache.paimon.table.source.splitread.MergeFileSplitReadProvider;
import org.apache.paimon.table.source.splitread.PrimaryKeyIndexedSplitReadProvider;
import org.apache.paimon.table.source.splitread.PrimaryKeyTableRawFileSplitReadProvider;
import org.apache.paimon.table.source.splitread.SplitReadProvider;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.paimon.table.source.BlobViewTableReadSupport.blobViewFieldIndexes;

/**
 * An abstraction layer above {@link MergeFileSplitRead} to provide reading of {@link InternalRow}.
 */
public final class KeyValueTableRead extends AbstractDataTableRead {

    private final Supplier<MergeFileSplitRead> mergeReadSupplier;
    private final Supplier<RawFileSplitRead> batchRawReadSupplier;
    private final List<SplitReadProvider> readProviders;
    private final CoreOptions options;
    @Nullable private final CatalogContext catalogContext;

    @Nullable private RowType readType = null;
    private boolean forceKeepDelete = false;
    private Predicate predicate = null;
    private IOManager ioManager = null;
    @Nullable private TopN topN = null;
    @Nullable private Integer limit = null;
    @Nullable private ReadBatchSizer readBatchSizer;

    public KeyValueTableRead(
            Supplier<MergeFileSplitRead> mergeReadSupplier,
            Supplier<RawFileSplitRead> batchRawReadSupplier,
            TableSchema schema,
            CoreOptions options,
            @Nullable CatalogContext catalogContext) {
        super(schema);
        this.mergeReadSupplier = mergeReadSupplier;
        this.batchRawReadSupplier = batchRawReadSupplier;
        this.options = options;
        this.catalogContext = catalogContext;
        this.readProviders =
                Arrays.asList(
                        new PrimaryKeyIndexedSplitReadProvider(batchRawReadSupplier, this::config),
                        new PrimaryKeyTableRawFileSplitReadProvider(
                                batchRawReadSupplier, this::config),
                        new MergeFileSplitReadProvider(mergeReadSupplier, this::config),
                        new IncrementalChangelogReadProvider(mergeReadSupplier, this::config),
                        new IncrementalDiffReadProvider(mergeReadSupplier, this::config));
    }

    private List<SplitRead<InternalRow>> initialized() {
        List<SplitRead<InternalRow>> readers = new ArrayList<>();
        for (SplitReadProvider readProvider : readProviders) {
            if (readProvider.get().initialized()) {
                readers.add(readProvider.get().get());
            }
        }
        return readers;
    }

    private void config(SplitRead<InternalRow> read) {
        if (forceKeepDelete) {
            read = read.forceKeepDelete();
        }
        if (readType != null) {
            read = read.withReadType(readType);
        }
        if (topN != null) {
            read = read.withTopN(topN);
        }
        read.withFilter(predicate).withIOManager(ioManager);
        if (readBatchSizer != null) {
            read.withReadBatchSizer(readBatchSizer);
        }
    }

    @Override
    public void applyReadType(RowType readType) {
        initialized().forEach(r -> r.withReadType(readType));
        this.readType = readType;
    }

    @Override
    public InnerTableRead forceKeepDelete() {
        initialized().forEach(SplitRead::forceKeepDelete);
        this.forceKeepDelete = true;
        return this;
    }

    @Override
    protected InnerTableRead innerWithFilter(Predicate predicate) {
        initialized().forEach(r -> r.withFilter(predicate));
        this.predicate = predicate;
        return this;
    }

    @Override
    public InnerTableRead withTopN(TopN topN) {
        initialized().forEach(r -> r.withTopN(topN));
        this.topN = topN;
        return this;
    }

    @Override
    public InnerTableRead withLimit(int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public RecordReader<InternalRow> createReader(List<Split> splits) throws IOException {
        return LimitRecordReader.limit(super.createReader(splits), limit);
    }

    @Override
    public RecordReader<InternalRow> createReader(Split split) throws IOException {
        QueryAuthContext queryAuthContext = unwrapQueryAuthSplit(split);
        RecordReader<InternalRow> reader;
        int[] blobViewFields = blobViewFieldIndexes(currentReadType(), options);
        if (catalogContext != null && blobViewFields.length > 0) {
            reader = createReaderWithBlobView(queryAuthContext, blobViewFields);
        } else {
            reader = createDataReader(queryAuthContext.split(), queryAuthContext.authResult());
        }
        return LimitRecordReader.limit(reader, limit);
    }

    private RecordReader<InternalRow> createReaderWithBlobView(
            QueryAuthContext queryAuthContext, int[] blobViewFields) throws IOException {
        RecordReader<InternalRow> reader;
        reader =
                BlobViewTableReadSupport.createBlobViewReader(
                        catalogContext,
                        queryAuthContext.split(),
                        queryAuthContext.authResult(),
                        blobViewFields,
                        currentReadType(),
                        predicate(),
                        topN,
                        limit,
                        executeFilter,
                        () ->
                                createDataReader(
                                        queryAuthContext.split(), queryAuthContext.authResult()),
                        this::createBlobViewPrescanRead);
        return reader;
    }

    private InnerTableRead createBlobViewPrescanRead() {
        KeyValueTableRead read =
                new KeyValueTableRead(
                        mergeReadSupplier, batchRawReadSupplier, schema(), options, null);
        if (ioManager != null) {
            read.withIOManager(ioManager);
        }
        if (forceKeepDelete) {
            read.forceKeepDelete();
        }
        if (executeFilter) {
            read.executeFilter();
        }
        if (readBatchSizer != null) {
            read.withReadBatchSizer(readBatchSizer);
        }
        return read;
    }

    @Override
    public TableRead withIOManager(IOManager ioManager) {
        initialized().forEach(r -> r.withIOManager(ioManager));
        this.ioManager = ioManager;
        return this;
    }

    @Override
    public InnerTableRead withReadBatchSizer(ReadBatchSizer sizer) {
        initialized().forEach(r -> r.withReadBatchSizer(sizer));
        this.readBatchSizer = sizer;
        return this;
    }

    @Override
    public RecordReader<InternalRow> reader(Split split) throws IOException {
        for (SplitReadProvider readProvider : readProviders) {
            if (readProvider.match(split, new SplitReadProvider.Context(forceKeepDelete))) {
                return readProvider.get().get().createReader(split);
            }
        }

        throw new RuntimeException("Should not happen.");
    }

    public static RecordReader<InternalRow> unwrap(
            RecordReader<KeyValue> reader, Map<String, String> schemaOptions) {
        return new RecordReader<InternalRow>() {

            @Nullable
            @Override
            public RecordIterator<InternalRow> readBatch() throws IOException {
                boolean keyValueSequenceNumberEnabled =
                        Boolean.parseBoolean(
                                schemaOptions.getOrDefault(
                                        CoreOptions.KEY_VALUE_SEQUENCE_NUMBER_ENABLED.key(),
                                        "false"));

                RecordIterator<KeyValue> batch = reader.readBatch();
                return batch == null
                        ? null
                        : new ValueContentRowDataRecordIterator(
                                batch, keyValueSequenceNumberEnabled);
            }

            @Override
            public void close() throws IOException {
                reader.close();
            }
        };
    }

    @VisibleForTesting
    public IOManager ioManager() {
        return ioManager;
    }
}
