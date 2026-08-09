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

package org.apache.paimon.format.mosaic;

import org.apache.paimon.arrow.ArrowBundleRecords;
import org.apache.paimon.arrow.ArrowUtils;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.mosaic.MosaicWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;

/** Test for {@link MosaicRecordsWriter}. */
class MosaicRecordsWriterTest {

    private static final FileFormatFactory.FormatContext FORMAT_CONTEXT =
            new FileFormatFactory.FormatContext(new Options(), 1024, 1024);

    @Test
    void testConstructorFailureClosesCreatedResources() {
        RowType rowType = DataTypes.ROW(DataTypes.INT(), DataTypes.STRING());
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        RuntimeException failure = new RuntimeException("native writer failed");

        assertThatThrownBy(
                        () ->
                                new MosaicRecordsWriter(
                                        new ByteArrayOutputStream(),
                                        rowType,
                                        FORMAT_CONTEXT,
                                        Collections.emptyList(),
                                        null,
                                        allocator,
                                        (outputStream, arrowSchema, options, bufferAllocator) -> {
                                            throw failure;
                                        }))
                .isSameAs(failure);

        assertThat(allocator.closeCount()).isEqualTo(1);
    }

    @Test
    void testCompatibleSameRootArrowBundleUsesDirectWrite() throws Exception {
        RowType rowType = RowType.builder().field("value", DataTypes.INT()).build();
        RootAllocator writerAllocator = new RootAllocator();
        MosaicWriter nativeWriter = mock(MosaicWriter.class);
        MosaicRecordsWriter writer = createWriter(rowType, writerAllocator, nativeWriter);

        try (BufferAllocator sourceAllocator =
                        writerAllocator.newChildAllocator("mosaic-direct-test", 0, Long.MAX_VALUE);
                VectorSchemaRoot root =
                        ArrowUtils.createVectorSchemaRoot(rowType, sourceAllocator)) {
            setInt((IntVector) root.getVector("value"), 1);
            root.setRowCount(1);

            writer.writeBundle(new ArrowBundleRecords(root, rowType, true));

            verify(nativeWriter).write(same(root));
        }
        writer.close();
    }

    @Test
    void testDifferentAllocatorRootFallsBackToRows() throws Exception {
        RowType rowType = RowType.builder().field("value", DataTypes.INT()).build();
        RootAllocator writerAllocator = new RootAllocator();
        MosaicWriter nativeWriter = mock(MosaicWriter.class);
        MosaicRecordsWriter writer = createWriter(rowType, writerAllocator, nativeWriter);

        try (RootAllocator sourceAllocator = new RootAllocator();
                VectorSchemaRoot root =
                        ArrowUtils.createVectorSchemaRoot(rowType, sourceAllocator)) {
            setInt((IntVector) root.getVector("value"), 1);
            root.setRowCount(1);

            writer.writeBundle(new ArrowBundleRecords(root, rowType, true));

            verify(nativeWriter, never()).write(same(root));
        }
        writer.close();

        verify(nativeWriter).write(any(VectorSchemaRoot.class));
    }

    @Test
    void testReorderedArrowBundleFallsBackToRows() throws Exception {
        RowType writerType =
                RowType.builder().field("a", DataTypes.INT()).field("b", DataTypes.INT()).build();
        RowType sourceType =
                RowType.builder().field("b", DataTypes.INT()).field("a", DataTypes.INT()).build();
        RootAllocator writerAllocator = new RootAllocator();
        MosaicWriter nativeWriter = mock(MosaicWriter.class);
        MosaicRecordsWriter writer = createWriter(writerType, writerAllocator, nativeWriter);
        doAnswer(
                        invocation -> {
                            VectorSchemaRoot written = invocation.getArgument(0);
                            assertThat(written.getSchema().getFields().get(0).getName())
                                    .isEqualTo("a");
                            assertThat(written.getSchema().getFields().get(1).getName())
                                    .isEqualTo("b");
                            assertThat(((IntVector) written.getVector("a")).get(0)).isEqualTo(10);
                            assertThat(((IntVector) written.getVector("b")).get(0)).isEqualTo(20);
                            return null;
                        })
                .when(nativeWriter)
                .write(any(VectorSchemaRoot.class));

        try (BufferAllocator sourceAllocator =
                        writerAllocator.newChildAllocator(
                                "mosaic-reordered-test", 0, Long.MAX_VALUE);
                VectorSchemaRoot root =
                        ArrowUtils.createVectorSchemaRoot(sourceType, sourceAllocator)) {
            setInt((IntVector) root.getVector("b"), 20);
            setInt((IntVector) root.getVector("a"), 10);
            root.setRowCount(1);

            writer.writeBundle(new ArrowBundleRecords(root, writerType, true));

            verify(nativeWriter, never()).write(same(root));
        }
        writer.close();

        verify(nativeWriter).write(any(VectorSchemaRoot.class));
    }

    private static MosaicRecordsWriter createWriter(
            RowType rowType, RootAllocator allocator, MosaicWriter nativeWriter) {
        return new MosaicRecordsWriter(
                new ByteArrayOutputStream(),
                rowType,
                FORMAT_CONTEXT,
                Collections.emptyList(),
                null,
                allocator,
                (outputStream, arrowSchema, options, bufferAllocator) -> nativeWriter);
    }

    private static void setInt(IntVector vector, int value) {
        vector.allocateNew(1);
        vector.setSafe(0, value);
        vector.setValueCount(1);
    }

    private static class CloseCountingRootAllocator extends RootAllocator {

        private int closeCount;

        @Override
        public void close() {
            closeCount++;
            super.close();
        }

        int closeCount() {
            return closeCount;
        }
    }
}
