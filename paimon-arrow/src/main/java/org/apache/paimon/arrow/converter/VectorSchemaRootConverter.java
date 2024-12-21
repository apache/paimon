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

package org.apache.paimon.arrow.converter;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.arrow.ArrowUtils;
import org.apache.paimon.arrow.writer.ArrowFieldWriter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.ApplyDeletionFileRecordIterator;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.VectorizedRecordIterator;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Use Paimon API to read records from {@link DataSplit}s and convert to {@link VectorSchemaRoot}.
 */
public class VectorSchemaRootConverter {

    private final RecordReader<InternalRow> recordReader;

    // for users to override the #init()
    protected final RowType readType;
    protected BufferAllocator allocator;
    protected final boolean vsrCaseSensitive;
    protected ArrowVectorizedBatchConverter batchConverter;
    protected ArrowPerRowBatchConverter perRowConverter;

    private final boolean schemaContainsConstructedType;

    private ArrowBatchConverter currentConverter;

    private long readBytes = 0;

    private boolean inited = false;

    public VectorSchemaRootConverter(
            ReadBuilder readBuilder,
            RowType rowType,
            @Nullable int[] projection,
            @Nullable Predicate predicate,
            List<DataSplit> splits,
            boolean vsrCaseSensitive) {
        this.readType = projection == null ? rowType : rowType.project(projection);

        readBuilder.withProjection(projection);
        if (predicate != null) {
            readBuilder.withFilter(predicate);
        }
        TableRead read = readBuilder.newRead();
        List<ReaderSupplier<InternalRow>> readerSuppliers = new ArrayList<>();
        for (DataSplit split : splits) {
            readerSuppliers.add(() -> read.createReader(split));
        }
        try {
            this.recordReader = ConcatRecordReader.create(readerSuppliers);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        this.vsrCaseSensitive = vsrCaseSensitive;
        this.schemaContainsConstructedType =
                rowType.getFieldTypes().stream()
                        .anyMatch(dataType -> dataType.is(DataTypeFamily.CONSTRUCTED));
    }

    public void init() {
        allocator = new RootAllocator();
        VectorSchemaRoot vsr =
                ArrowUtils.createVectorSchemaRoot(readType, allocator, vsrCaseSensitive);
        ArrowFieldWriter[] fieldWriters = ArrowUtils.createArrowFieldWriters(vsr, readType);
        batchConverter = new ArrowVectorizedBatchConverter(vsr, fieldWriters);
        perRowConverter = new ArrowPerRowBatchConverter(vsr, fieldWriters);
    }

    /**
     * Get {@link VectorSchemaRoot} of which row count is at most {@code maxBatchRows}. Return null
     * if there is no more data.
     */
    @Nullable
    public VectorSchemaRoot next(int maxBatchRows) {
        if (!inited) {
            init();
            inited = true;
        }

        if (currentConverter == null) {
            resetCurrentConverter();
            if (currentConverter == null) {
                // no more data
                return null;
            }
        }

        VectorSchemaRoot vsr = currentConverter.next(maxBatchRows);
        if (vsr == null) {
            resetCurrentConverter();
            if (currentConverter == null) {
                // no more data
                return null;
            }
            vsr = currentConverter.next(maxBatchRows);
        }
        if (vsr != null) {
            readBytes += vsr.getFieldVectors().stream().mapToLong(ValueVector::getBufferSize).sum();
        }
        return vsr;
    }

    private void resetCurrentConverter() {
        RecordReader.RecordIterator<InternalRow> iterator;
        try {
            iterator = recordReader.readBatch();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (iterator == null) {
            // no more data
            currentConverter = null;
            return;
        }

        // TODO: delete this after #3883 is fixed completely.
        boolean vectorizedAndCompactly =
                ((FileRecordIterator<InternalRow>) iterator).vectorizedAndCompactly();
        boolean useVectorizedSafely = vectorizedAndCompactly || !schemaContainsConstructedType;

        if (isVectorizedWithDv(iterator) && useVectorizedSafely) {
            batchConverter.reset((ApplyDeletionFileRecordIterator) iterator);
            currentConverter = batchConverter;
        } else if (iterator instanceof VectorizedRecordIterator && useVectorizedSafely) {
            batchConverter.reset((VectorizedRecordIterator) iterator);
            currentConverter = batchConverter;
        } else {
            perRowConverter.reset(iterator);
            currentConverter = perRowConverter;
        }
    }

    public void close() throws IOException {
        recordReader.close();
        batchConverter.close();
        perRowConverter.close();
        allocator.close();
    }

    @VisibleForTesting
    static boolean isVectorizedWithDv(RecordReader.RecordIterator<InternalRow> iterator) {
        if (iterator instanceof ApplyDeletionFileRecordIterator) {
            ApplyDeletionFileRecordIterator deletionIterator =
                    (ApplyDeletionFileRecordIterator) iterator;
            FileRecordIterator<InternalRow> innerIterator = deletionIterator.iterator();
            return innerIterator instanceof VectorizedRecordIterator;
        }
        return false;
    }

    public long getReadBytes() {
        return readBytes;
    }
}
