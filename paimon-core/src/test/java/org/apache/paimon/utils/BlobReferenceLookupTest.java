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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.BlobRef;
import org.apache.paimon.data.BlobReference;
import org.apache.paimon.data.BlobReferenceResolver;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link BlobReferenceLookup}. */
public class BlobReferenceLookupTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testCreateResolverPreloadsDescriptors() throws Exception {
        CatalogContext context = CatalogContext.create(new Path(tempDir.toUri().toString()));
        byte[] leftPayload = new byte[] {1, 2, 3};
        byte[] rightPayload = new byte[] {4, 5, 6};
        Blob leftBlob = createBlob("left.blob", leftPayload);
        Blob rightBlob = createBlob("right.blob", rightPayload);

        BlobReference leftReference = new BlobReference("default.source", 7, 12L);
        BlobReference rightReference = new BlobReference("default.source", 8, 12L);

        Split readerSplit = new TestSplit();
        InternalRow preloadRow = GenericRow.of(leftBlob, rightBlob, 12L);
        Table table =
                createBlobTable(
                        7,
                        "blob_left",
                        8,
                        "blob_right",
                        readerSplit,
                        Collections.singletonList(preloadRow),
                        null);

        Map<String, Table> tables = new HashMap<>();
        tables.put(leftReference.tableName(), table);

        BlobReferenceResolver resolver =
                BlobReferenceLookup.createResolver(
                        context,
                        Arrays.asList(leftReference, rightReference),
                        catalogLoader(tables));

        BlobRef leftBlobRef = new BlobRef(leftReference);
        BlobRef rightBlobRef = new BlobRef(rightReference);
        resolver.resolve(leftBlobRef);
        resolver.resolve(rightBlobRef);
        assertThat(leftBlobRef.isResolved()).isTrue();
        assertThat(rightBlobRef.isResolved()).isTrue();
        assertThat(leftBlobRef.toData()).isEqualTo(leftPayload);
        assertThat(rightBlobRef.toData()).isEqualTo(rightPayload);

        // Same coordinates → same result
        BlobRef anotherLeft = new BlobRef(new BlobReference("default.source", 7, 12L));
        resolver.resolve(anotherLeft);
        assertThat(anotherLeft.toData()).isEqualTo(leftPayload);

        // Only one readBuilder should have been created (batch preload)
        verify(table, times(1)).newReadBuilder();
    }

    @Test
    public void testCreateResolverPreloadsDescriptorsInParallelWithinSingleTable()
            throws Exception {
        CatalogContext context = CatalogContext.create(new Path(tempDir.toUri().toString()));
        Split readerSplit = new TestSplit();
        CountDownLatch started = new CountDownLatch(2);
        AtomicBoolean firstChunkOverlapped = new AtomicBoolean(false);
        AtomicBoolean secondChunkOverlapped = new AtomicBoolean(false);
        List<List<Range>> observedRanges = new CopyOnWriteArrayList<>();

        byte[] firstPayload = new byte[] {1, 2, 3};
        byte[] secondPayload = new byte[] {4, 5, 6};
        Blob firstBlob = createBlob("parallel-first.blob", firstPayload);
        Blob secondBlob = createBlob("parallel-second.blob", secondPayload);

        BlobReference firstReference = new BlobReference("default.source", 7, 12L);
        BlobReference secondReference = new BlobReference("default.source", 7, 112L);
        List<BlobReference> references = new java.util.ArrayList<>();
        references.add(firstReference);
        for (long rowId = 13L; rowId < 112L; rowId++) {
            references.add(new BlobReference("default.source", 7, rowId));
        }
        references.add(secondReference);

        List<InternalRow> rows = new java.util.ArrayList<>();
        rows.add(GenericRow.of(firstBlob, 12L));
        for (long rowId = 13L; rowId < 112L; rowId++) {
            rows.add(GenericRow.of((Blob) null, rowId));
        }
        rows.add(GenericRow.of(secondBlob, 112L));

        Table table =
                createBlobTable(
                        7,
                        "blob",
                        readerSplit,
                        rows,
                        rowRanges -> {
                            observedRanges.add(copyRanges(rowRanges));
                            long rowId = rowRanges.get(0).from;
                            if (rowId == 12L) {
                                return () -> awaitConcurrentRead(started, firstChunkOverlapped);
                            }
                            if (rowId == 112L) {
                                return () -> awaitConcurrentRead(started, secondChunkOverlapped);
                            }
                            return null;
                        });

        Map<String, Table> tables = new HashMap<>();
        tables.put(firstReference.tableName(), table);

        BlobReferenceResolver resolver =
                BlobReferenceLookup.createResolver(context, references, catalogLoader(tables));

        BlobRef firstBlobRef = new BlobRef(firstReference);
        BlobRef secondBlobRef = new BlobRef(secondReference);
        resolver.resolve(firstBlobRef);
        resolver.resolve(secondBlobRef);

        assertThat(firstBlobRef.toData()).isEqualTo(firstPayload);
        assertThat(secondBlobRef.toData()).isEqualTo(secondPayload);
        assertThat(firstChunkOverlapped.get()).isTrue();
        assertThat(secondChunkOverlapped.get()).isTrue();
        assertThat(observedRanges)
                .containsExactlyInAnyOrder(
                        Collections.singletonList(new Range(12L, 111L)),
                        Collections.singletonList(new Range(112L, 112L)));

        verify(table, times(2)).newReadBuilder();
    }

    @Test
    public void testCreateResolverPreloadsDescriptorsInParallelAcrossTables() throws Exception {
        CatalogContext context = CatalogContext.create(new Path(tempDir.toUri().toString()));
        Split readerSplit = new TestSplit();
        CountDownLatch started = new CountDownLatch(2);
        AtomicBoolean leftOverlapped = new AtomicBoolean(false);
        AtomicBoolean rightOverlapped = new AtomicBoolean(false);

        byte[] leftPayload = new byte[] {1, 2, 3};
        byte[] rightPayload = new byte[] {4, 5, 6};
        Blob leftBlob = createBlob("parallel-left.blob", leftPayload);
        Blob rightBlob = createBlob("parallel-right.blob", rightPayload);

        BlobReference leftReference = new BlobReference("default.left_source", 7, 12L);
        BlobReference rightReference = new BlobReference("default.right_source", 8, 24L);

        Table leftTable =
                createBlobTable(
                        7,
                        "blob_left",
                        readerSplit,
                        Collections.singletonList(GenericRow.of(leftBlob, 12L)),
                        ignored -> () -> awaitConcurrentRead(started, leftOverlapped));
        Table rightTable =
                createBlobTable(
                        8,
                        "blob_right",
                        readerSplit,
                        Collections.singletonList(GenericRow.of(rightBlob, 24L)),
                        ignored -> () -> awaitConcurrentRead(started, rightOverlapped));

        Map<String, Table> tables = new HashMap<>();
        tables.put(leftReference.tableName(), leftTable);
        tables.put(rightReference.tableName(), rightTable);

        BlobReferenceResolver resolver =
                BlobReferenceLookup.createResolver(
                        context,
                        Arrays.asList(leftReference, rightReference),
                        catalogLoader(tables));

        BlobRef leftBlobRef = new BlobRef(leftReference);
        BlobRef rightBlobRef = new BlobRef(rightReference);
        resolver.resolve(leftBlobRef);
        resolver.resolve(rightBlobRef);

        assertThat(leftBlobRef.toData()).isEqualTo(leftPayload);
        assertThat(rightBlobRef.toData()).isEqualTo(rightPayload);
        assertThat(leftOverlapped.get()).isTrue();
        assertThat(rightOverlapped.get()).isTrue();

        verify(leftTable, times(1)).newReadBuilder();
        verify(rightTable, times(1)).newReadBuilder();
    }

    private Blob createBlob(String fileName, byte[] payload) throws IOException {
        Path blobPath = new Path(tempDir.toUri().toString(), fileName);
        LocalFileIO fileIO = LocalFileIO.create();
        try (org.apache.paimon.fs.PositionOutputStream out =
                fileIO.newOutputStream(blobPath, false)) {
            out.write(payload);
        }

        BlobDescriptor descriptor = new BlobDescriptor(blobPath.toString(), 0L, payload.length);
        return Blob.fromDescriptor(UriReader.fromFile(LocalFileIO.create()), descriptor);
    }

    private BlobReferenceLookup.CatalogLoader catalogLoader(Map<String, Table> tables) {
        return ignored -> {
            try {
                return createCatalog(tables);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    private Catalog createCatalog(Map<String, Table> tables) throws Exception {
        Catalog catalog = mock(Catalog.class);
        when(catalog.getTable(any()))
                .thenAnswer(
                        invocation -> {
                            Identifier identifier = invocation.getArgument(0);
                            return tables.get(identifier.getFullName());
                        });
        return catalog;
    }

    private Table createBlobTable(
            int fieldId,
            String fieldName,
            Split readerSplit,
            List<InternalRow> rows,
            @Nullable Function<List<Range>, Runnable> beforeFirstBatchFactory) {
        return createBlobTable(
                fieldId, fieldName, -1, null, readerSplit, rows, beforeFirstBatchFactory);
    }

    private Table createBlobTable(
            int firstFieldId,
            String firstFieldName,
            int secondFieldId,
            @Nullable String secondFieldName,
            Split readerSplit,
            List<InternalRow> rows,
            @Nullable Function<List<Range>, Runnable> beforeFirstBatchFactory) {
        Table table = mock(Table.class);

        List<DataField> fields = new java.util.ArrayList<>();
        fields.add(new DataField(firstFieldId, firstFieldName, DataTypes.BLOB()));
        if (secondFieldName != null) {
            fields.add(new DataField(secondFieldId, secondFieldName, DataTypes.BLOB()));
        }

        when(table.copy(anyMap())).thenReturn(table);
        when(table.rowType()).thenReturn(new RowType(fields));
        when(table.fileIO()).thenReturn(LocalFileIO.create());
        when(table.newReadBuilder())
                .thenAnswer(
                        ignored ->
                                createReadBuilder(
                                        fields.size(), readerSplit, rows, beforeFirstBatchFactory));
        return table;
    }

    private ReadBuilder createReadBuilder(
            int blobFieldCount,
            Split readerSplit,
            List<InternalRow> rows,
            @Nullable Function<List<Range>, Runnable> beforeFirstBatchFactory) {
        ReadBuilder readBuilder = mock(ReadBuilder.class);
        TableScan scan = mock(TableScan.class);
        TableScan.Plan plan = mock(TableScan.Plan.class);
        AtomicReference<List<Range>> rowRangesRef =
                new AtomicReference<List<Range>>(Collections.<Range>emptyList());

        when(readBuilder.withReadType(any(RowType.class))).thenReturn(readBuilder);
        when(readBuilder.withRowRanges(anyList()))
                .thenAnswer(
                        invocation -> {
                            List<Range> rowRanges = copyRanges(invocation.getArgument(0));
                            rowRangesRef.set(rowRanges);
                            return readBuilder;
                        });
        when(readBuilder.newRead())
                .thenAnswer(
                        invocation -> {
                            List<Range> rowRanges = rowRangesRef.get();
                            Runnable beforeFirstBatch =
                                    beforeFirstBatchFactory == null
                                            ? null
                                            : beforeFirstBatchFactory.apply(rowRanges);
                            return new ListRowTableRead(
                                    readerSplit,
                                    filterRows(rows, blobFieldCount, rowRanges),
                                    beforeFirstBatch);
                        });
        when(readBuilder.newScan()).thenReturn(scan);
        when(scan.plan()).thenReturn(plan);
        when(plan.splits()).thenReturn(Collections.singletonList(readerSplit));
        return readBuilder;
    }

    private List<InternalRow> filterRows(
            List<InternalRow> rows, int blobFieldCount, List<Range> rowRanges) {
        if (rowRanges.isEmpty()) {
            return rows;
        }

        List<InternalRow> filtered = new java.util.ArrayList<>();
        for (InternalRow row : rows) {
            long rowId = row.getLong(blobFieldCount);
            if (containsRowId(rowRanges, rowId)) {
                filtered.add(row);
            }
        }
        return filtered;
    }

    private boolean containsRowId(List<Range> rowRanges, long rowId) {
        for (Range rowRange : rowRanges) {
            if (rowId >= rowRange.from && rowId <= rowRange.to) {
                return true;
            }
        }
        return false;
    }

    private static List<Range> copyRanges(List<Range> rowRanges) {
        List<Range> copied = new java.util.ArrayList<>(rowRanges.size());
        for (Range rowRange : rowRanges) {
            copied.add(new Range(rowRange.from, rowRange.to));
        }
        return copied;
    }

    private static void awaitConcurrentRead(CountDownLatch started, AtomicBoolean overlapped) {
        started.countDown();
        try {
            overlapped.set(started.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static class ListRowTableRead implements TableRead {

        private final List<InternalRow> rows;
        private final Runnable beforeFirstBatch;

        private ListRowTableRead(Split split, List<InternalRow> rows) {
            this(split, rows, null);
        }

        private ListRowTableRead(
                Split split, List<InternalRow> rows, @Nullable Runnable beforeFirstBatch) {
            this.rows = rows;
            this.beforeFirstBatch = beforeFirstBatch;
        }

        @Override
        public TableRead withMetricRegistry(MetricRegistry registry) {
            return this;
        }

        @Override
        public TableRead executeFilter() {
            return this;
        }

        @Override
        public TableRead withIOManager(IOManager ioManager) {
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) {
            return new RecordReader<InternalRow>() {

                private boolean emitted = false;

                @Nullable
                @Override
                public RecordIterator<InternalRow> readBatch() {
                    if (emitted) {
                        return null;
                    }
                    emitted = true;
                    if (beforeFirstBatch != null) {
                        beforeFirstBatch.run();
                    }
                    return new RecordIterator<InternalRow>() {

                        private int next = 0;

                        @Nullable
                        @Override
                        public InternalRow next() {
                            return next < rows.size() ? rows.get(next++) : null;
                        }

                        @Override
                        public void releaseBatch() {}
                    };
                }

                @Override
                public void close() throws IOException {}
            };
        }
    }

    private static class TestSplit implements Split {

        @Override
        public long rowCount() {
            return 1L;
        }

        @Override
        public OptionalLong mergedRowCount() {
            return OptionalLong.of(1L);
        }
    }
}
