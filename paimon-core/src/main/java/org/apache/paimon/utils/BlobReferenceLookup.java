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
import org.apache.paimon.data.BlobRef;
import org.apache.paimon.data.BlobReference;
import org.apache.paimon.data.BlobReferenceResolver;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Batch-preloads {@link BlobDescriptor}s for a set of {@link BlobReference}s by scanning the
 * upstream tables in row-range chunks. The preloaded descriptors are lightweight (uri + offset +
 * length) so memory stays small even for large numbers of references.
 */
public class BlobReferenceLookup {

    private static final int PRELOAD_DESCRIPTOR_THREAD_NUM = 100;
    private static final long MIN_ROW_PER_TASK = 100L;
    private static final ExecutorService PRELOAD_DESCRIPTOR_EXECUTOR =
            ThreadPoolUtils.createCachedThreadPool(
                    PRELOAD_DESCRIPTOR_THREAD_NUM, "blob-reference-preload");

    /**
     * Creates a resolver that resolves {@link BlobRef}s using a preloaded descriptor cache. All
     * given references are batch-scanned from the upstream tables upfront.
     */
    public static BlobReferenceResolver createResolver(
            CatalogContext catalogContext, List<BlobReference> references) {
        return createResolver(catalogContext, references, CatalogFactory::createCatalog);
    }

    @VisibleForTesting
    static BlobReferenceResolver createResolver(
            CatalogContext catalogContext,
            List<BlobReference> references,
            CatalogLoader catalogLoader) {
        Map<BlobReference, BlobDescriptor> cached =
                preloadDescriptors(catalogContext, references, catalogLoader);
        Map<String, UriReader> cache = new HashMap<>();
        return blobRef -> {
            BlobDescriptor descriptor = cached.get(blobRef.reference());
            if (descriptor == null) {
                throw new IllegalStateException(
                        "BlobReference not found in preloaded cache: "
                                + blobRef.reference()
                                + ". Cache keys: "
                                + cached.keySet());
            }
            UriReader uriReader =
                    cache.computeIfAbsent(
                            blobRef.reference().tableName(),
                            tableName -> {
                                try (Catalog catalog = catalogLoader.create(catalogContext)) {
                                    return UriReader.fromFile(
                                            catalog.getTable(Identifier.fromString(tableName))
                                                    .fileIO());
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });
            blobRef.resolve(uriReader, descriptor);
        };
    }

    private static Map<BlobReference, BlobDescriptor> preloadDescriptors(
            CatalogContext catalogContext,
            List<BlobReference> references,
            CatalogLoader catalogLoader) {
        if (references.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, TableReferences> grouped = groupReferencesByTable(references);
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
            Collection<BlobReference> references) {
        Map<String, TableReferences> grouped = new HashMap<>();
        for (BlobReference reference : references) {
            grouped.computeIfAbsent(reference.tableName(), TableReferences::new).add(reference);
        }
        return grouped;
    }

    private static Map<BlobReference, BlobDescriptor> loadReferencedDescriptors(
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

        CompletionService<Map<BlobReference, BlobDescriptor>> completionService =
                new ExecutorCompletionService<>(executor);
        List<Future<Map<BlobReference, BlobDescriptor>>> futures = new ArrayList<>();
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

        Map<BlobReference, BlobDescriptor> resolved = new HashMap<>();
        try {
            for (int i = 0; i < futures.size(); i++) {
                resolved.putAll(completionService.take().get());
            }
        } catch (Exception e) {
            for (Future<Map<BlobReference, BlobDescriptor>> future : futures) {
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
            for (Map.Entry<Integer, List<BlobReference>> entry :
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

    private static Map<BlobReference, BlobDescriptor> loadTableDescriptorChunk(
            CatalogContext catalogContext,
            String tableName,
            List<FieldRead> fields,
            RowType readType,
            List<Range> rowRanges,
            CatalogLoader catalogLoader)
            throws Exception {
        try (Catalog catalog = catalogLoader.create(catalogContext)) {
            Map<BlobReference, BlobDescriptor> resolved = new HashMap<>();
            Table table =
                    catalog.getTable(Identifier.fromString(tableName))
                            .copy(
                                    Collections.singletonMap(
                                            CoreOptions.BLOB_AS_DESCRIPTOR.key(), "true"));

            ReadBuilder readBuilder =
                    table.newReadBuilder().withReadType(readType).withRowRanges(rowRanges);

            try (RecordReader<InternalRow> reader =
                    readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
                RecordReader.RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    try {
                        InternalRow row;
                        while ((row = batch.next()) != null) {
                            long rowId = row.getLong(fields.size());
                            for (int i = 0; i < fields.size(); i++) {
                                Blob blob = row.getBlob(i);
                                if (blob != null) {
                                    resolved.put(
                                            new BlobReference(
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
        private final Map<Integer, List<BlobReference>> referencesByField = new HashMap<>();
        private final List<Long> rowIds = new ArrayList<>();

        private TableReferences(String tableName) {
            this.tableName = tableName;
        }

        private void add(BlobReference reference) {
            referencesByField
                    .computeIfAbsent(reference.fieldId(), unused -> new ArrayList<>())
                    .add(reference);
            rowIds.add(reference.rowId());
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

    private BlobReferenceLookup() {}
}
