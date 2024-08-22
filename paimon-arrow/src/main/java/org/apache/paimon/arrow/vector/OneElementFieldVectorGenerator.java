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

import org.apache.paimon.arrow.writer.ArrowFieldWriter;
import org.apache.paimon.arrow.writer.ArrowFieldWriterFactoryVisitor;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.DataField;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;

import static org.apache.paimon.arrow.ArrowUtils.createVector;

/** Convert a static value to a FieldVector. */
public class OneElementFieldVectorGenerator implements AutoCloseable {

    private final GenericRow row;
    private final FieldVector fieldVector;
    private final ArrowFieldWriter writer;

    private int pos = 0;

    public OneElementFieldVectorGenerator(
            BufferAllocator bufferAllocator, DataField dataField, Object value) {
        fieldVector = createVector(dataField, bufferAllocator, false);
        writer =
                dataField
                        .type()
                        .accept(ArrowFieldWriterFactoryVisitor.INSTANCE)
                        .create(fieldVector);
        this.row = new GenericRow(1);
        row.setField(0, value);
    }

    FieldVector get(int rowCount) {
        if (rowCount > pos) {
            for (int i = pos; i < rowCount; i++) {
                writer.write(i, row, 0);
            }
            pos = rowCount;
        }
        fieldVector.setValueCount(rowCount);
        return fieldVector;
    }

    @Override
    public void close() {
        fieldVector.close();
    }
}
