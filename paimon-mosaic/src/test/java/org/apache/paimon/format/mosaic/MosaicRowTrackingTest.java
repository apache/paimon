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

import org.apache.paimon.arrow.ArrowUtils;
import org.apache.paimon.arrow.converter.ArrowBatchConverter;
import org.apache.paimon.arrow.converter.ArrowPerRowBatchConverter;
import org.apache.paimon.arrow.converter.ArrowVectorizedBatchConverter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnarRowIterator;
import org.apache.paimon.deletionvectors.ApplyDeletionFileRecordIterator;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileRecordReader;
import org.apache.paimon.mosaic.MosaicReader;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.VectorizedRecordIterator;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MosaicRowTrackingTest {
    @Test
    void synthesizedRowIdsRequireRowFallback() throws Exception {
        checkRowIds(false, false, false);
    }

    @Test
    void synthesizedRowIdsRemainDistinctAfterDeletionSelection() throws Exception {
        checkRowIds(true, false, false);
    }

    @Test
    void synthesizedRowIdsRemainCorrectThroughRowFallback() throws Exception {
        checkRowIds(true, true, false);
    }

    @Test
    void directDecorationDoesNotExposePositionDependentVectors() throws Exception {
        checkRowIds(false, false, true);
        checkRowIds(true, false, true);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void synthesizedRowIdsRemainCorrectAfterReorderedProjection(int rowGroupIndex)
            throws Exception {
        checkRowIds(true, false, false, true, rowGroupIndex);
    }

    private void checkRowIds(boolean deletion, boolean forceRowFallback, boolean directDecoration)
            throws Exception {
        checkRowIds(deletion, forceRowFallback, directDecoration, false, 0);
    }

    private void checkRowIds(
            boolean deletion,
            boolean forceRowFallback,
            boolean directDecoration,
            boolean reorderedProjection,
            int rowGroupIndex)
            throws Exception {
        RowType type =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field(SpecialFields.ROW_ID.name(), DataTypes.BIGINT())
                        .build();
        RootAllocator allocator = new RootAllocator();
        MosaicReader nativeReader = mock(MosaicReader.class);
        when(nativeReader.numRowGroups()).thenReturn(rowGroupIndex + 1);
        for (int group = 0; group <= rowGroupIndex; group++) {
            VectorSchemaRoot root = ArrowUtils.createVectorSchemaRoot(type, allocator);
            root.allocateNew();
            for (int row = 0; row < 3; row++) {
                ((IntVector) root.getVector(0)).setSafe(row, 7 + group * 3 + row);
                ((BigIntVector) root.getVector(1)).setNull(row);
            }
            root.setRowCount(3);
            when(nativeReader.getSchema()).thenReturn(root.getSchema());
            when(nativeReader.rowGroupNumRows(group)).thenReturn(3);
            when(nativeReader.readRowGroup(group, allocator)).thenReturn(root);
        }
        Path path = new Path("file:/tmp/pr9-review");
        MosaicRecordsReader reader =
                new MosaicRecordsReader(
                        mock(MosaicInputFileAdapter.class),
                        0,
                        type,
                        type,
                        null,
                        path,
                        allocator,
                        (file, size, alloc) -> nativeReader);
        int[] indexMapping = reorderedProjection ? new int[] {1, 0} : new int[] {0, 1};
        int rowIdIndex = reorderedProjection ? 0 : 1;
        RowType outputType = reorderedProjection ? type.project(indexMapping) : type;
        try (DataFileRecordReader dataReader =
                        new DataFileRecordReader(
                                outputType,
                                reader,
                                false,
                                false,
                                indexMapping,
                                null,
                                null,
                                true,
                                1000L,
                                123L,
                                Collections.singletonMap(SpecialFields.ROW_ID.name(), rowIdIndex),
                                null,
                                path);
                RootAllocator outputAllocator = new RootAllocator();
                VectorSchemaRoot output =
                        ArrowUtils.createVectorSchemaRoot(outputType, outputAllocator)) {
            for (int group = 0; group < rowGroupIndex; group++) {
                FileRecordIterator<InternalRow> previousBatch = dataReader.readBatch();
                try {
                    for (int row = 0; row < 3; row++) {
                        assertThat(previousBatch.next().getLong(rowIdIndex))
                                .isEqualTo(1000L + group * 3 + row);
                    }
                    assertThat(previousBatch.next()).isNull();
                } finally {
                    previousBatch.releaseBatch();
                }
            }
            long startPosition = rowGroupIndex * 3L;
            FileRecordIterator<InternalRow> batch =
                    directDecoration
                            ? ((ColumnarRowIterator) reader.readBatch())
                                    .assignRowTracking(
                                            1000L,
                                            123L,
                                            Collections.singletonMap(
                                                    SpecialFields.ROW_ID.name(), 1))
                            : dataReader.readBatch();
            assertThat(batch).isNotInstanceOf(VectorizedRecordIterator.class);
            ArrowBatchConverter converter;
            if (!forceRowFallback && batch instanceof VectorizedRecordIterator) {
                ArrowVectorizedBatchConverter vectorConverter =
                        new ArrowVectorizedBatchConverter(
                                output, ArrowUtils.createArrowFieldWriters(output, outputType));
                if (deletion) {
                    vectorConverter.reset(
                            new ApplyDeletionFileRecordIterator(
                                    batch, pos -> pos == startPosition + 1));
                } else {
                    vectorConverter.reset((VectorizedRecordIterator) batch);
                }
                converter = vectorConverter;
            } else {
                ArrowPerRowBatchConverter rowConverter =
                        new ArrowPerRowBatchConverter(
                                output, ArrowUtils.createArrowFieldWriters(output, outputType));
                rowConverter.reset(
                        deletion
                                ? new ApplyDeletionFileRecordIterator(
                                        batch, pos -> pos == startPosition + 1)
                                : batch);
                converter = rowConverter;
            }
            converter.next(1024);
            long[] observed = new long[output.getRowCount()];
            for (int i = 0; i < observed.length; i++) {
                observed[i] = ((BigIntVector) output.getVector(rowIdIndex)).get(i);
            }
            batch.releaseBatch();
            assertThat(observed)
                    .containsExactly(
                            deletion
                                    ? new long[] {1000L + startPosition, 1002L + startPosition}
                                    : new long[] {
                                        1000L + startPosition,
                                        1001L + startPosition,
                                        1002L + startPosition
                                    });
        }
    }
}
