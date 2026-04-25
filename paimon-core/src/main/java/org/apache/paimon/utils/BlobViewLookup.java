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

package org.apache.paimon.utils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.BlobViewResolver;
import org.apache.paimon.data.BlobViewStruct;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Batch-preloads {@link BlobDescriptor}s for a set of {@link BlobViewStruct}s by scanning the
 * upstream tables in row-range chunks.
 */
public class BlobViewLookup {

    private static final int PRELOAD_DESCRIPTOR_THREAD_NUM = 100;
    private static final long MIN_ROW_PER_TASK = 100L;
    private static final ExecutorService PRELOAD_DESCRIPTOR_EXECUTOR =
            ThreadPoolUtils.createCachedThreadPool(
                    PRELOAD_DESCRIPTOR_THREAD_NUM, "blob-view-preload");

    public static BlobViewResolver createResolver(
            CatalogContext catalogContext, List<BlobViewStruct> viewStructs) {
        return createResolver(catalogContext, viewStructs, CatalogFactory::createCatalog);
    }

    @VisibleForTesting
    static BlobViewResolver createResolver(
            CatalogContext catalogContext,
            List<BlobViewStruct> viewStructs,
            CatalogLoader catalogLoader) {
        Map<BlobViewStruct, BlobDescriptor> cached =
                preloadDescriptors(catalogContext, viewStructs, catalogLoader);
        Map<String, UriReader> cache = new HashMap<>();
        return blobView -> {
            BlobViewStruct viewStruct = blobView.viewStruct();
            BlobDescriptor descriptor = cached.get(viewStruct);
            if (descriptor == null) {
                throw new IllegalStateException(
                        "BlobViewStruct not found in preloaded cache: "
                                + viewStruct
                                + ". Cache keys: "
                                + cached.keySet());
            }
            UriReader uriReader =
                    cache.computeIfAbsent(
                            viewStruct.tableName(),
                            tableName -> {
                                try (Catalog catalog = catalogLoader.create(catalogContext)) {
                                    return UriReader.fromFile(
                                            catalog.getTable(Identifier.fromString(tableName))
                                                    .fileIO());
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });
            blobView.resolve(uriReader, descriptor);
        };
    }

    private static Map<BlobViewStruct, BlobDescriptor> preloadDescriptors(
            CatalogContext catalogContext,
            List<BlobViewStruct> viewStructs,
            CatalogLoader catalogLoader) {
        if (viewStructs.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, TableReferences> grouped = groupReferencesByTable(viewStructs);
        try {
            return loadReferencedDescriptors(
                    catalogContext, grouped.values(), PRELOAD_DESCRIPTOR_EXECUTOR, catalogLoader);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to preload blob descriptors.", e);
        } catch (ExecutionException e) {
            throw new RuntimeException(
                    "Failed to preload blob descriptors.", e.getCause() == null ? e : e.getCause());
        } catch (Exception e) {
            throw new RuntimeException("Failed to preload blob descriptors.", e);
        }
    }

    private static long targetRowsPerTask(Collection<TableReadPlan> plans) {
        long totalRows = 0L;
        for (TableReadPlan plan : plans) {
            for (Range rowRange : plan.rowRanges) {
                totalRows += rowRange.count();
            }
        }

        if (totalRows <= 0L) {
            return MIN_ROW_PER_TASK;
        }

        return Math.max(
                MIN_ROW_PER_TASK,
                (totalRows + PRELOAD_DESCRIPTOR_THREAD_NUM - 1) / PRELOAD_DESCRIPTOR_THREAD_NUM);
    }

    private static Map<String, TableReferences> groupReferencesByTable(
            Collection<BlobViewStruct> viewStructs) {
        Map<String, TableReferences> grouped = new HashMap<>();
        for (BlobViewStruct viewStruct : viewStructs) {
            grouped.computeIfAbsent(viewStruct.tableName(), TableReferences::new).add(viewStruct);
        }
        return grouped;
    }

    private static Map<BlobViewStruct, BlobDescriptor> loadReferencedDescriptors(
            CatalogContext catalogContext,
            Collection<TableReferences> grouped,
            ExecutorService executor,
            CatalogLoader catalogLoader)
            throws Exception {
        List<TableReadPlan> plans = new ArrayList<>(grouped.size());
        for (TableReferences tableReferences : grouped) {
            plans.add(createTableReadPlan(catalogContext, tableReferences, catalogLoader));
        }
        long targetRowsPerTask = targetRowsPerTask(plans);

        CompletionService<Map<BlobViewStruct, BlobDescriptor>> completionService =
                new ExecutorCompletionService<>(executor);
        List<Future<Map<BlobViewStruct, BlobDescriptor>>> futures = new ArrayList<>();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        for (TableReadPlan plan : plans) {
            for (List<Range> rangeChunk : splitRowRanges(plan.rowRanges, targetRowsPerTask)) {
                futures.add(
                        completionService.submit(
                                () -> {
                                    ClassLoader originalClassLoader =
                                            Thread.currentThread().getContextClassLoader();
                                    Thread.currentThread()
                                            .setContextClassLoader(contextClassLoader);
                                    try {
                                        return loadTableDescriptorChunk(
                                                catalogContext,
                                                plan.tableName,
                                                plan.fields,
                                                plan.readType,
                                                rangeChunk,
                                                catalogLoader);
                                    } finally {
                                        Thread.currentThread()
                                                .setContextClassLoader(originalClassLoader);
                                    }
                                }));
            }
        }

        Map<BlobViewStruct, BlobDescriptor> resolved = new HashMap<>();
        try {
            for (int i = 0; i < futures.size(); i++) {
                resolved.putAll(completionService.take().get());
            }
        } catch (Exception e) {
            for (Future<Map<BlobViewStruct, BlobDescriptor>> future : futures) {
                future.cancel(true);
            }
            throw e;
        }
        return resolved;
    }

    private static TableReadPlan createTableReadPlan(
            CatalogContext catalogContext,
            TableReferences tableReferences,
            CatalogLoader catalogLoader)
            throws Exception {
        try (Catalog catalog = catalogLoader.create(catalogContext)) {
            List<FieldRead> fields = new ArrayList<>(tableReferences.referencesByField.size());
            Table table = catalog.getTable(Identifier.fromString(tableReferences.tableName));
            for (Map.Entry<Integer, List<BlobViewStruct>> entry :
                    tableReferences.referencesByField.entrySet()) {
                int fieldId = entry.getKey();
                if (!table.rowType().containsField(fieldId)) {
                    throw new IllegalArgumentException(
                            "Cannot find blob fieldId "
                                    + fieldId
                                    + " in upstream table "
                                    + tableReferences.tableName
                                    + ".");
                }
                int fieldPos = table.rowType().getFieldIndexByFieldId(fieldId);
                fields.add(
                        new FieldRead(
                                fieldId, fieldPos, table.rowType().getFields().get(fieldPos)));
            }

            Collections.sort(fields, Comparator.comparingInt(left -> left.fieldPos));

            List<DataField> readFields = new ArrayList<>(fields.size());
            for (FieldRead field : fields) {
                readFields.add(field.field);
            }

            return new TableReadPlan(
                    tableReferences.tableName,
                    fields,
                    SpecialFields.rowTypeWithRowId(new RowType(readFields)),
                    toSortedDistinctRanges(tableReferences.rowIds));
        }
    }

    private static Map<BlobViewStruct, BlobDescriptor> loadTableDescriptorChunk(
            CatalogContext catalogContext,
            String tableName,
            List<FieldRead> fields,
            RowType readType,
            List<Range> rowRanges,
            CatalogLoader catalogLoader)
            throws Exception {
        try (Catalog catalog = catalogLoader.create(catalogContext)) {
            Map<BlobViewStruct, BlobDescriptor> resolved = new HashMap<>();
            Table table =
                    catalog.getTable(Identifier.fromString(tableName))
                            .copy(
                                    Collections.singletonMap(
                                            CoreOptions.BLOB_AS_DESCRIPTOR.key(), "true"));

            ReadBuilder readBuilder =
                    table.newReadBuilder().withReadType(readType).withRowRanges(rowRanges);
            Iterator<Long> rowIdIterator = rowIds(rowRanges).iterator();

            try (RecordReader<InternalRow> reader =
                    readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
                RecordReader.RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    try {
                        InternalRow row;
                        while ((row = batch.next()) != null) {
                            if (!rowIdIterator.hasNext()) {
                                throw new IllegalStateException(
                                        "Read more rows than requested row ranges.");
                            }
                            long rowId = rowIdIterator.next();
                            for (int i = 0; i < fields.size(); i++) {
                                Blob blob = row.getBlob(i);
                                if (blob != null) {
                                    resolved.put(
                                            new BlobViewStruct(
                                                    tableName, fields.get(i).fieldId, rowId),
                                            blob.toDescriptor());
                                }
                            }
                        }
                    } finally {
                        batch.releaseBatch();
                    }
                }
            }

            return resolved;
        }
    }

    private static List<Long> rowIds(List<Range> rowRanges) {
        List<Long> rowIds = new ArrayList<>();
        for (Range rowRange : rowRanges) {
            for (long rowId = rowRange.from; rowId <= rowRange.to; rowId++) {
                rowIds.add(rowId);
            }
        }
        return rowIds;
    }

    private static List<List<Range>> splitRowRanges(List<Range> rowRanges, long targetRowsPerTask) {
        if (rowRanges.isEmpty()) {
            return Collections.emptyList();
        }

        List<List<Range>> chunks = new ArrayList<>();
        List<Range> currentChunk = new ArrayList<>();
        long currentChunkRows = 0L;
        for (Range rowRange : rowRanges) {
            long nextFrom = rowRange.from;
            while (nextFrom <= rowRange.to) {
                if (currentChunkRows == targetRowsPerTask) {
                    chunks.add(currentChunk);
                    currentChunk = new ArrayList<>();
                    currentChunkRows = 0L;
                }

                long remainingRows = targetRowsPerTask - currentChunkRows;
                long nextTo = Math.min(rowRange.to, nextFrom + remainingRows - 1);
                currentChunk.add(new Range(nextFrom, nextTo));
                currentChunkRows += nextTo - nextFrom + 1;
                nextFrom = nextTo + 1;
            }
        }

        if (!currentChunk.isEmpty()) {
            chunks.add(currentChunk);
        }
        return chunks;
    }

    private static List<Range> toSortedDistinctRanges(List<Long> rowIds) {
        if (rowIds.isEmpty()) {
            return Collections.emptyList();
        }

        Collections.sort(rowIds);

        List<Range> ranges = new ArrayList<>();
        long rangeStart = rowIds.get(0);
        long rangeEnd = rangeStart;
        for (int i = 1; i < rowIds.size(); i++) {
            long rowId = rowIds.get(i);
            if (rowId == rangeEnd) {
                continue;
            }
            if (rowId != rangeEnd + 1) {
                ranges.add(new Range(rangeStart, rangeEnd));
                rangeStart = rowId;
            }
            rangeEnd = rowId;
        }
        ranges.add(new Range(rangeStart, rangeEnd));
        return ranges;
    }

    @VisibleForTesting
    @FunctionalInterface
    interface CatalogLoader {

        Catalog create(CatalogContext catalogContext);
    }

    private static class TableReferences {
        private final String tableName;
        private final Map<Integer, List<BlobViewStruct>> referencesByField = new HashMap<>();
        private final List<Long> rowIds = new ArrayList<>();

        private TableReferences(String tableName) {
            this.tableName = tableName;
        }

        private void add(BlobViewStruct viewStruct) {
            referencesByField
                    .computeIfAbsent(viewStruct.fieldId(), unused -> new ArrayList<>())
                    .add(viewStruct);
            rowIds.add(viewStruct.rowId());
        }
    }

    private static class FieldRead {
        private final int fieldId;
        private final int fieldPos;
        private final DataField field;

        private FieldRead(int fieldId, int fieldPos, DataField field) {
            this.fieldId = fieldId;
            this.fieldPos = fieldPos;
            this.field = field;
        }
    }

    private static class TableReadPlan {
        private final String tableName;
        private final List<FieldRead> fields;
        private final RowType readType;
        private final List<Range> rowRanges;

        private TableReadPlan(
                String tableName, List<FieldRead> fields, RowType readType, List<Range> rowRanges) {
            this.tableName = tableName;
            this.fields = fields;
            this.readType = readType;
            this.rowRanges = rowRanges;
        }
    }

    private BlobViewLookup() {}
}
