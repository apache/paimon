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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarRow;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.data.columnar.VectorizedRowIterator;
import org.apache.paimon.data.columnar.heap.HeapIntVector;
import org.apache.paimon.data.columnar.heap.HeapLongVector;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.RowDataRollingFileWriter;
import org.apache.paimon.io.VectorizedBundleRecords;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.BundleRecordIterator;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.IOExceptionSupplier;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link BaseAppendFileStoreWrite}. */
class BaseAppendFileStoreWriteTest {

    @Test
    void testCompactRewriteWritesBatchAsBundle() throws Exception {
        TestingBundleReader reader = new TestingBundleReader(2);
        RawFileSplitRead rawRead = mock(RawFileSplitRead.class);
        RowDataRollingFileWriter writer = mock(RowDataRollingFileWriter.class);
        List<DataFileMeta> inputFiles = Collections.singletonList(mock(DataFileMeta.class));
        List<DataFileMeta> outputFiles = Collections.singletonList(mock(DataFileMeta.class));
        when(rawRead.createReader(
                        BinaryRow.EMPTY_ROW,
                        0,
                        inputFiles,
                        (Map<String, IOExceptionSupplier<DeletionVector>>) null))
                .thenReturn(reader);
        doAnswer(
                        invocation -> {
                            assertThat(reader.batchReleased).isFalse();
                            assertThat(reader.closed).isFalse();
                            return null;
                        })
                .when(writer)
                .writeBundle(any());
        when(writer.result()).thenReturn(outputFiles);
        TestingAppendWrite write = new TestingAppendWrite(rawRead, writer);

        assertThat(write.compactRewrite(BinaryRow.EMPTY_ROW, 0, null, inputFiles))
                .isSameAs(outputFiles);

        ArgumentCaptor<BundleRecords> bundle = ArgumentCaptor.forClass(BundleRecords.class);
        verify(writer).writeBundle(bundle.capture());
        assertThat(bundle.getValue().rowCount()).isEqualTo(2);
        verify(writer, never()).write(any(InternalRow.class));
        verify(writer).close();
        assertThat(reader.nextCalls).isZero();
        assertThat(reader.batchReleased).isTrue();
        assertThat(reader.closed).isTrue();
    }

    @Test
    void testCompactRewriteReleasesBorrowedBatchAfterBundleFailure() throws Exception {
        IOException failure = new IOException("bundle failure");
        TestingBundleReader reader = new TestingBundleReader(2);
        RawFileSplitRead rawRead = mock(RawFileSplitRead.class);
        RowDataRollingFileWriter writer = mock(RowDataRollingFileWriter.class);
        List<DataFileMeta> inputFiles = Collections.singletonList(mock(DataFileMeta.class));
        when(rawRead.createReader(
                        BinaryRow.EMPTY_ROW,
                        0,
                        inputFiles,
                        (Map<String, IOExceptionSupplier<DeletionVector>>) null))
                .thenReturn(reader);
        doThrow(failure).when(writer).writeBundle(any());
        TestingAppendWrite write = new TestingAppendWrite(rawRead, writer);

        assertThatThrownBy(() -> write.compactRewrite(BinaryRow.EMPTY_ROW, 0, null, inputFiles))
                .isSameAs(failure);
        verify(writer).close();
        assertThat(reader.batchReleased).isTrue();
        assertThat(reader.closed).isTrue();
    }

    @Test
    void testCompactRewriteConsumesStatefulVectorizedIteratorAsRows() throws Exception {
        HeapIntVector idVector = new HeapIntVector(2);
        idVector.setInt(0, 10);
        idVector.setInt(1, 20);
        HeapLongVector rowIdVector = new HeapLongVector(2);
        rowIdVector.fillWithNulls();
        VectorizedColumnBatch batch =
                new VectorizedColumnBatch(new ColumnVector[] {idVector, rowIdVector});
        batch.setNumRows(2);
        VectorizedRowIterator tracked =
                new VectorizedRowIterator(
                        new Path("file:///tracked.data"), new ColumnarRow(batch), null);
        tracked.reset(5L);
        tracked.assignRowTracking(
                100L, 200L, Collections.singletonMap(SpecialFields.ROW_ID.name(), 1));

        RecordReader<InternalRow> reader = singleBatchReader(tracked);
        RawFileSplitRead rawRead = mock(RawFileSplitRead.class);
        RowDataRollingFileWriter writer = mock(RowDataRollingFileWriter.class);
        List<DataFileMeta> inputFiles = Collections.singletonList(mock(DataFileMeta.class));
        List<DataFileMeta> outputFiles = Collections.singletonList(mock(DataFileMeta.class));
        when(rawRead.createReader(
                        BinaryRow.EMPTY_ROW,
                        0,
                        inputFiles,
                        (Map<String, IOExceptionSupplier<DeletionVector>>) null))
                .thenReturn(reader);
        when(writer.result()).thenReturn(outputFiles);
        List<List<Long>> written = new ArrayList<>();
        doAnswer(
                        invocation -> {
                            InternalRow row = invocation.getArgument(0);
                            written.add(Arrays.asList((long) row.getInt(0), row.getLong(1)));
                            return null;
                        })
                .when(writer)
                .write(any(InternalRow.class));

        TestingAppendWrite write = new TestingAppendWrite(rawRead, writer);
        assertThat(write.compactRewrite(BinaryRow.EMPTY_ROW, 0, null, inputFiles))
                .isSameAs(outputFiles);

        assertThat(written).containsExactly(Arrays.asList(10L, 105L), Arrays.asList(20L, 106L));
        verify(writer, never()).writeBundle(any());
        verify(writer).close();
    }

    private static RecordReader<InternalRow> singleBatchReader(
            RecordReader.RecordIterator<InternalRow> iterator) {
        return new RecordReader<InternalRow>() {
            private boolean returned;

            @Override
            public RecordIterator<InternalRow> readBatch() {
                if (returned) {
                    return null;
                }
                returned = true;
                return iterator;
            }

            @Override
            public void close() {}
        };
    }

    private static class TestingBundleReader implements RecordReader<InternalRow> {

        private final VectorizedColumnBatch batch;
        private boolean returned;
        private boolean batchReleased;
        private boolean closed;
        private int nextCalls;

        private TestingBundleReader(int rowCount) {
            HeapIntVector vector = new HeapIntVector(rowCount);
            for (int i = 0; i < rowCount; i++) {
                vector.setInt(i, i);
            }
            batch = new VectorizedColumnBatch(new ColumnVector[] {vector});
            batch.setNumRows(rowCount);
        }

        @Override
        public RecordIterator<InternalRow> readBatch() {
            if (returned) {
                return null;
            }
            returned = true;
            return new BundleRecordIterator() {
                @Override
                public BundleRecords bundleRecords() {
                    return new VectorizedBundleRecords(batch, null);
                }

                @Override
                public InternalRow next() {
                    nextCalls++;
                    throw new AssertionError("Bundle compaction must not consume rows");
                }

                @Override
                public void releaseBatch() {
                    batchReleased = true;
                }
            };
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static class TestingAppendWrite extends BaseAppendFileStoreWrite {

        private final RowDataRollingFileWriter writer;

        private TestingAppendWrite(
                RawFileSplitRead readForCompact, RowDataRollingFileWriter writer) {
            super(
                    mock(FileIO.class),
                    readForCompact,
                    0,
                    RowType.of(DataTypes.INT()),
                    RowType.of(),
                    mock(FileStorePathFactory.class),
                    mock(SnapshotManager.class),
                    mock(FileStoreScan.class),
                    new CoreOptions(new Options()),
                    null,
                    "test");
            this.writer = writer;
        }

        @Override
        RowDataRollingFileWriter createRollingFileWriter(
                BinaryRow partition, int bucket, Supplier<LongCounter> seqNumCounterSupplier) {
            return writer;
        }

        @Override
        protected CompactManager getCompactManager(
                BinaryRow partition,
                int bucket,
                List<DataFileMeta> restoredFiles,
                ExecutorService compactExecutor,
                BucketedDvMaintainer dvMaintainer) {
            return null;
        }

        @Override
        protected Function<WriterContainer<InternalRow>, Boolean> createWriterCleanChecker() {
            return null;
        }
    }
}
