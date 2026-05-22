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

package org.apache.paimon.format.row;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.utils.NestedProjectedRow;

import javax.annotation.Nullable;

import java.io.IOException;

/** Iterator over rows within a single decompressed block. */
class RowFileRecordIterator implements FileRecordIterator<InternalRow> {

    private final Path filePath;
    private final RowBlockReader blockReader;
    @Nullable private final NestedProjectedRow projection;
    private final long blockStartRow;
    @Nullable private final int[] selectedLocalIndices;

    private int cursor;
    private long currentPosition;

    RowFileRecordIterator(
            Path filePath,
            RowBlockReader blockReader,
            @Nullable NestedProjectedRow projection,
            long blockStartRow) {
        this(filePath, blockReader, projection, blockStartRow, null);
    }

    RowFileRecordIterator(
            Path filePath,
            RowBlockReader blockReader,
            @Nullable NestedProjectedRow projection,
            long blockStartRow,
            @Nullable int[] selectedLocalIndices) {
        this.filePath = filePath;
        this.blockReader = blockReader;
        this.projection = projection;
        this.blockStartRow = blockStartRow;
        this.selectedLocalIndices = selectedLocalIndices;
        this.cursor = 0;
        this.currentPosition = blockStartRow;
    }

    @Override
    public long returnedPosition() {
        return currentPosition;
    }

    @Override
    public Path filePath() {
        return filePath;
    }

    @Nullable
    @Override
    public InternalRow next() throws IOException {
        if (selectedLocalIndices != null) {
            return nextSelected(selectedLocalIndices);
        } else {
            return nextSequential();
        }
    }

    @Nullable
    private InternalRow nextSequential() {
        if (cursor >= blockReader.rowCount()) {
            return null;
        }
        currentPosition = blockStartRow + cursor;
        InternalRow row = blockReader.readRow(cursor);
        cursor++;
        return applyProjection(row);
    }

    @Nullable
    private InternalRow nextSelected(int[] selectedLocalIndices) {
        if (cursor >= selectedLocalIndices.length) {
            return null;
        }
        int localIdx = selectedLocalIndices[cursor];
        currentPosition = blockStartRow + localIdx;
        InternalRow row = blockReader.readRow(localIdx);
        cursor++;
        return applyProjection(row);
    }

    private InternalRow applyProjection(InternalRow row) {
        if (projection != null) {
            return projection.replaceRow(row);
        }
        return row;
    }

    @Override
    public void releaseBatch() {}
}
