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

package dev.vortex.arrow;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * One record batch of a Vortex scan. Top-level string and binary columns are {@link ViewColumn}s
 * over native memory and are not in {@link #root()}, which holds the other columns. Closing the
 * batch releases the root and the native memory.
 */
public final class ArrowBatch implements AutoCloseable {

    private final VectorSchemaRoot root;
    private final ViewColumn[] viewColumns;
    private final int rowCount;
    private ArrowArray nativeArray;

    ArrowBatch(
            VectorSchemaRoot root, ViewColumn[] viewColumns, ArrowArray nativeArray, int rowCount) {
        this.root = root;
        this.rowCount = rowCount;
        this.viewColumns = viewColumns;
        this.nativeArray = nativeArray;
    }

    public VectorSchemaRoot root() {
        return root;
    }

    public int rowCount() {
        return rowCount;
    }

    /** The view column at schema position {@code index}, or null if the root holds that field. */
    public ViewColumn viewColumn(int index) {
        return index < viewColumns.length ? viewColumns[index] : null;
    }

    @Override
    public void close() {
        try {
            root.close();
        } finally {
            if (nativeArray != null) {
                ArrowArray array = nativeArray;
                nativeArray = null;
                if (array.snapshot().release != 0) {
                    array.release();
                }
                array.close();
            }
        }
    }
}
