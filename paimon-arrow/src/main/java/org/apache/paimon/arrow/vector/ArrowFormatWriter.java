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

package org.apache.paimon.arrow.vector;

import org.apache.paimon.arrow.ArrowFieldTypeConversion;
import org.apache.paimon.arrow.ArrowUtils;
import org.apache.paimon.arrow.writer.ArrowFieldWriter;
import org.apache.paimon.arrow.writer.ArrowFieldWriterFactoryVisitor;
import org.apache.paimon.arrow.writer.ArrowFieldWriters;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VariantType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/** Write from {@link InternalRow} to {@link VectorSchemaRoot}. */
public class ArrowFormatWriter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ArrowFormatWriter.class);

    private final VectorSchemaRoot vectorSchemaRoot;
    private final ArrowFieldWriter[] fieldWriters;
    private final int batchSize;
    private final BufferAllocator allocator;
    @Nullable private final Long memoryUsedMaxInBytes;
    private int rowId;

    public ArrowFormatWriter(RowType rowType, int writeBatchSize, boolean caseSensitive) {
        this(rowType, writeBatchSize, caseSensitive, new RootAllocator(), null, null);
    }

    public ArrowFormatWriter(
            RowType rowType,
            int writeBatchSize,
            boolean caseSensitive,
            @Nullable Long memoryUsedMaxInBytes) {
        this(
                rowType,
                writeBatchSize,
                caseSensitive,
                new RootAllocator(),
                memoryUsedMaxInBytes,
                null);
    }

    public ArrowFormatWriter(
            RowType rowType,
            int writeBatchSize,
            boolean caseSensitive,
            BufferAllocator allocator,
            @Nullable Long memoryUsedMaxInBytes) {
        this(rowType, writeBatchSize, caseSensitive, allocator, memoryUsedMaxInBytes, null);
    }

    public ArrowFormatWriter(
            RowType rowType,
            int writeBatchSize,
            boolean caseSensitive,
            @Nullable Long memoryUsedMaxInBytes,
            @Nullable RowType shreddingSchemas) {
        this(
                rowType,
                writeBatchSize,
                caseSensitive,
                new RootAllocator(),
                memoryUsedMaxInBytes,
                shreddingSchemas);
    }

    public ArrowFormatWriter(
            RowType rowType,
            int writeBatchSize,
            boolean caseSensitive,
            BufferAllocator allocator,
            @Nullable Long memoryUsedMaxInBytes,
            @Nullable RowType shreddingSchemas) {
        this(
                rowType,
                writeBatchSize,
                caseSensitive,
                allocator,
                memoryUsedMaxInBytes,
                shreddingSchemas,
                ArrowFieldTypeConversion.ARROW_FIELD_TYPE_VISITOR,
                ArrowFieldWriterFactoryVisitor.INSTANCE);
    }

    public ArrowFormatWriter(
            RowType rowType,
            int writeBatchSize,
            boolean caseSensitive,
            BufferAllocator allocator,
            @Nullable Long memoryUsedMaxInBytes,
            @Nullable RowType shreddingSchemas,
            ArrowFieldTypeConversion.ArrowFieldTypeVisitor fieldTypeVisitor,
            ArrowFieldWriterFactoryVisitor fieldWriterFactory) {
        this.allocator = allocator;

        RowType outputRowType = replaceWithShreddingType(rowType, shreddingSchemas);
        vectorSchemaRoot =
                ArrowUtils.createVectorSchemaRoot(
                        outputRowType, allocator, caseSensitive, fieldTypeVisitor);

        fieldWriters = new ArrowFieldWriter[rowType.getFieldCount()];

        for (int i = 0; i < fieldWriters.length; i++) {
            DataField field = rowType.getFields().get(i);
            DataType type = field.type();
            if (type instanceof VariantType) {
                RowType shreddingSchema =
                        shreddingSchemas != null && shreddingSchemas.containsField(field.name())
                                ? (RowType) shreddingSchemas.getField(field.name()).type()
                                : null;
                fieldWriters[i] =
                        new ArrowFieldWriters.VariantWriter(
                                vectorSchemaRoot.getVector(i), type.isNullable(), shreddingSchema);
            } else {
                fieldWriters[i] =
                        type.accept(fieldWriterFactory)
                                .create(vectorSchemaRoot.getVector(i), type.isNullable());
            }
        }

        this.batchSize = writeBatchSize;
        this.memoryUsedMaxInBytes = memoryUsedMaxInBytes;
    }

    public void flush() {
        vectorSchemaRoot.setRowCount(rowId);
    }

    public boolean write(InternalRow currentRow) {
        if (rowId >= batchSize) {
            return false;
        }
        if (memoryUsedMaxInBytes != null && rowId % 32 == 0) {
            long memoryUsed = memoryUsed();
            if (memoryUsed > memoryUsedMaxInBytes) {
                LOG.debug(
                        "Memory used by ArrowFormatCWriter exceeds the limit: {} > {} while writing record row id: {}",
                        memoryUsed,
                        memoryUsedMaxInBytes,
                        rowId);
                return false;
            }
        }
        for (int i = 0; i < currentRow.getFieldCount(); i++) {
            try {
                fieldWriters[i].write(rowId, currentRow, i);
            } catch (OversizedAllocationException | IndexOutOfBoundsException e) {
                // maybe out of memory
                LOG.warn("Arrow field writer failed while writing", e);
                return false;
            }
        }

        rowId++;
        return true;
    }

    public long memoryUsed() {
        vectorSchemaRoot.setRowCount(rowId);
        long memoryUsed = 0;
        for (FieldVector fieldVector : vectorSchemaRoot.getFieldVectors()) {
            memoryUsed += fieldVector.getBufferSize();
        }
        return memoryUsed;
    }

    public boolean empty() {
        return rowId == 0;
    }

    public void reset() {
        for (ArrowFieldWriter fieldWriter : fieldWriters) {
            fieldWriter.reset();
        }
        rowId = 0;
    }

    @Override
    public void close() {
        vectorSchemaRoot.close();
        allocator.close();
    }

    public VectorSchemaRoot getVectorSchemaRoot() {
        return vectorSchemaRoot;
    }

    public BufferAllocator getAllocator() {
        return allocator;
    }

    private static RowType replaceWithShreddingType(
            RowType rowType, @Nullable RowType shreddingSchemas) {
        if (shreddingSchemas == null) {
            return rowType;
        }

        List<DataField> newFields = new ArrayList<>();
        for (DataField field : rowType.getFields()) {
            if (field.type() instanceof VariantType
                    && shreddingSchemas.containsField(field.name())) {
                RowType shreddingSchema = (RowType) shreddingSchemas.getField(field.name()).type();
                newFields.add(field.newType(shreddingSchema));
            } else {
                newFields.add(field);
            }
        }
        return new RowType(rowType.isNullable(), newFields);
    }
}
