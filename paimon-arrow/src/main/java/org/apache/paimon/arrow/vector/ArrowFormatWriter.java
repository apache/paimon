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

import org.apache.paimon.arrow.ArrowUtils;
import org.apache.paimon.arrow.writer.ArrowFieldWriter;
import org.apache.paimon.arrow.writer.ArrowFieldWriterFactoryVisitor;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowType;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.OversizedAllocationException;

/** Write from {@link InternalRow} to {@link VectorSchemaRoot}. */
public class ArrowFormatWriter implements AutoCloseable {

    private final VectorSchemaRoot vectorSchemaRoot;
    private final ArrowFieldWriter[] fieldWriters;
    private final ArrowArray array;
    private final ArrowSchema schema;

    private final int batchSize;

    private final RootAllocator allocator;
    private int rowId;

    public ArrowFormatWriter(RowType rowType, int writeBatchSize) {
        allocator = new RootAllocator();
        array = ArrowArray.allocateNew(allocator);
        schema = ArrowSchema.allocateNew(allocator);

        vectorSchemaRoot = ArrowUtils.createVectorSchemaRoot(rowType, allocator, false);

        fieldWriters = new ArrowFieldWriter[rowType.getFieldCount()];

        for (int i = 0; i < fieldWriters.length; i++) {
            fieldWriters[i] =
                    rowType.getFields()
                            .get(i)
                            .type()
                            .accept(ArrowFieldWriterFactoryVisitor.INSTANCE)
                            .create(vectorSchemaRoot.getVector(i));
        }

        this.batchSize = writeBatchSize;
    }

    public ArrowCStruct flush() {
        vectorSchemaRoot.setRowCount(rowId);
        ArrowCStruct arrowCStruct = ArrowUtils.serializeToCStruct(vectorSchemaRoot, array, schema);
        rowId = 0;
        return arrowCStruct;
    }

    public boolean write(InternalRow currentRow) {
        if (rowId >= batchSize) {
            return false;
        }
        for (int i = 0; i < currentRow.getFieldCount(); i++) {
            try {
                fieldWriters[i].write(rowId, currentRow, i);
            } catch (OversizedAllocationException | IndexOutOfBoundsException e) {
                // maybe out of memory
                return false;
            }
        }

        rowId++;
        return true;
    }

    public boolean empty() {
        return rowId == 0;
    }

    @Override
    public void close() {
        array.release();
        schema.release();
        array.close();
        schema.close();
        vectorSchemaRoot.close();
        allocator.close();
    }

    public VectorSchemaRoot getVectorSchemaRoot() {
        return vectorSchemaRoot;
    }
}
