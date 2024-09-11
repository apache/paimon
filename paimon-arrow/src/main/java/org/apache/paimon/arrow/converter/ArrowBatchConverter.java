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

import org.apache.paimon.arrow.writer.ArrowFieldWriter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;

import org.apache.arrow.vector.VectorSchemaRoot;

import javax.annotation.Nullable;

/**
 * A reusable converter to convert Paimon data in {@link RecordReader.RecordIterator} into Arrow
 * format.
 */
public abstract class ArrowBatchConverter {

    // reusable
    protected final VectorSchemaRoot root;
    protected final ArrowFieldWriter[] fieldWriters;

    protected RecordReader.RecordIterator<InternalRow> iterator;

    ArrowBatchConverter(VectorSchemaRoot root, ArrowFieldWriter[] fieldWriters) {
        this.root = root;
        this.fieldWriters = fieldWriters;
    }

    /**
     * Write and get at most {@code maxBatchRows} data. Return null when finishing writing current
     * iterator.
     *
     * <p>NOTE: the returned value will be reused, and it's lifecycle is managed by this writer.
     */
    @Nullable
    public VectorSchemaRoot next(int maxBatchRows) {
        if (iterator == null) {
            return null;
        }

        for (ArrowFieldWriter fieldWriter : fieldWriters) {
            fieldWriter.reset();
        }
        doWrite(maxBatchRows);
        return root;
    }

    protected abstract void doWrite(int maxBatchRows);

    public void close() {
        root.close();
    }

    protected void releaseIterator() {
        iterator.releaseBatch();
        iterator = null;
    }
}
