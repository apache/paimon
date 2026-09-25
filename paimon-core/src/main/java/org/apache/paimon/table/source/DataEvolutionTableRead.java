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
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.globalindex.IndexQuerySplit;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.reader.EmptyRecordReader;
import org.apache.paimon.reader.ReadBatchSizer;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.splitread.SplitReadConfig;
import org.apache.paimon.table.source.splitread.SplitReadProvider;
import org.apache.paimon.utils.ExceptionUtils;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/** A {@link TableRead} for data-evolution enabled append-only tables. */
public class DataEvolutionTableRead extends AppendTableRead {

    private final CoreOptions options;
    @Nullable private final CatalogContext catalogContext;
    @Nullable private final Supplier<InnerTableRead> readFactory;
    @Nullable private final FileIO fileIO;

    public DataEvolutionTableRead(
            List<Function<SplitReadConfig, SplitReadProvider>> providerFactories,
            TableSchema schema,
            CoreOptions options,
            @Nullable CatalogContext catalogContext,
            @Nullable Supplier<InnerTableRead> readFactory) {
        this(providerFactories, schema, options, catalogContext, readFactory, null);
    }

    public DataEvolutionTableRead(
            List<Function<SplitReadConfig, SplitReadProvider>> providerFactories,
            TableSchema schema,
            CoreOptions options,
            @Nullable CatalogContext catalogContext,
            @Nullable Supplier<InnerTableRead> readFactory,
            @Nullable FileIO fileIO) {
        super(providerFactories, schema);
        this.options = options;
        this.catalogContext = catalogContext;
        this.readFactory = readFactory;
        this.fileIO = fileIO;
    }

    @Override
    public RecordReader<InternalRow> createReader(Split split) throws IOException {
        QueryAuthContext queryAuthContext = unwrapQueryAuthSplit(split);
        final Split dataSplit;
        boolean filterOnRead = executeFilter;
        if (queryAuthContext.split() instanceof IndexQuerySplit) {
            if (fileIO == null) {
                throw new IllegalStateException("FileIO is required for index query evaluation.");
            }
            IndexQuerySplit indexQuerySplit = (IndexQuerySplit) queryAuthContext.split();
            Split selectedSplit;
            try {
                IndexedSplit indexedSplit = indexQuerySplit.evaluate(fileIO);
                if (indexedSplit.rowRanges().isEmpty()) {
                    return new EmptyRecordReader<>();
                }
                selectedSplit = indexedSplit;
            } catch (IOException e) {
                if (!ExceptionUtils.findThrowable(
                                        e,
                                        cause ->
                                                cause instanceof FileNotFoundException
                                                        || cause instanceof NoSuchFileException)
                                .isPresent()
                        || options.scalarIndexSearchMode()
                                == CoreOptions.GlobalIndexSearchMode.FAST) {
                    throw e;
                }
                if (predicate() == null) {
                    throw new IOException(
                            "Cannot scan a split without its index and query filter", e);
                }
                selectedSplit = indexQuerySplit.dataSplit();
                filterOnRead = true;
            }
            dataSplit = selectedSplit;
        } else {
            dataSplit = queryAuthContext.split();
        }
        final boolean applyFilter = filterOnRead;
        int[] blobViewFields =
                BlobViewTableReadSupport.blobViewFieldIndexes(currentReadType(), options);
        ReadBatchSizer sizer = readBatchSizer();
        if (catalogContext != null && blobViewFields.length > 0) {
            if (readFactory == null) {
                throw new IllegalStateException(
                        "Cannot read blob-view-field fields without a readFactory.");
            }
            return BlobViewTableReadSupport.createBlobViewReader(
                    catalogContext,
                    dataSplit,
                    queryAuthContext.authResult(),
                    blobViewFields,
                    currentReadType(),
                    predicate(),
                    topN,
                    limit,
                    applyFilter,
                    () -> createDataReader(dataSplit, queryAuthContext.authResult(), applyFilter),
                    () -> {
                        InnerTableRead prescanRead = readFactory.get();
                        if (sizer != null) {
                            // Blob-view prescan is a separate physical read under the same budget.
                            prescanRead.withReadBatchSizer(sizer);
                        }
                        if (applyFilter) {
                            prescanRead.executeFilter();
                        }
                        return prescanRead;
                    });
        }
        return createDataReader(dataSplit, queryAuthContext.authResult(), filterOnRead);
    }
}
