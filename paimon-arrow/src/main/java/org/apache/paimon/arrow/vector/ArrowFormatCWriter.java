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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowType;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * This writer could flush to c struct, but you need to release it, except it has been released in c
 * code.
 */
public class ArrowFormatCWriter implements AutoCloseable {

    private final ArrowArray array;
    private final ArrowSchema schema;
    private final ArrowFormatWriter realWriter;

    public ArrowFormatCWriter(RowType rowType, int writeBatchSize) {
        this.realWriter = new ArrowFormatWriter(rowType, writeBatchSize);
        RootAllocator allocator = realWriter.getAllocator();
        array = ArrowArray.allocateNew(allocator);
        schema = ArrowSchema.allocateNew(allocator);
    }

    public boolean write(InternalRow currentRow) {
        return realWriter.write(currentRow);
    }

    public ArrowCStruct flush() {
        realWriter.flush();
        VectorSchemaRoot vectorSchemaRoot = realWriter.getVectorSchemaRoot();
        return ArrowUtils.serializeToCStruct(vectorSchemaRoot, array, schema);
    }

    public boolean empty() {
        return realWriter.empty();
    }

    // if c++ code release this, we don't need to release again (release twice may cause a problem)
    public void release() {
        array.release();
        schema.release();
    }

    @Override
    public void close() {
        array.close();
        schema.close();
        realWriter.close();
    }

    public VectorSchemaRoot getVectorSchemaRoot() {
        return realWriter.getVectorSchemaRoot();
    }

    public RootAllocator getAllocator() {
        return realWriter.getAllocator();
    }
}
