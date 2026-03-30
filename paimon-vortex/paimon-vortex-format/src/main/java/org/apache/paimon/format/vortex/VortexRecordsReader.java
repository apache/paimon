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

package org.apache.paimon.format.vortex;

import org.apache.paimon.arrow.reader.ArrowBatchReader;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongIterator;

import dev.vortex.api.Array;
import dev.vortex.api.ArrayIterator;
import dev.vortex.api.Expression;
import dev.vortex.api.File;
import dev.vortex.api.Files;
import dev.vortex.api.ImmutableScanOptions;
import dev.vortex.arrow.ArrowAllocation;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/** File reader for Vortex format. */
public class VortexRecordsReader implements FileRecordReader<InternalRow> {

    private final ArrowBatchReader arrowBatchReader;
    private final Path filePath;
    private final BufferAllocator allocator;
    private final ArrayIterator arrayIterator;
    private final File vortexFile;
    private final LongIterator positionIterator;
    private VectorSchemaRoot reuse;
    private Array currentArray;
    private long returnedPosition = -1;

    public VortexRecordsReader(
            Path path,
            RowType projectedRowType,
            @Nullable long[] rowIndices,
            @Nullable Expression predicate,
            Map<String, String> storageOptions) {
        this.filePath = path;
        this.allocator =
                ArrowAllocation.rootAllocator()
                        .newChildAllocator("vortex-reader", 0, Long.MAX_VALUE);
        try {
            this.vortexFile = Files.open(path.toUri().toString(), storageOptions);
            try {
                ImmutableScanOptions.Builder scanBuilder = ImmutableScanOptions.builder();
                scanBuilder.addAllColumns(projectedRowType.getFieldNames());
                if (rowIndices != null) {
                    scanBuilder.rowIndices(rowIndices);
                }
                if (predicate != null) {
                    scanBuilder.predicate(predicate);
                }
                this.arrayIterator = vortexFile.newScan(scanBuilder.build());
            } catch (Exception e) {
                vortexFile.close();
                throw e;
            }
        } catch (Exception e) {
            allocator.close();
            throw e;
        }
        this.arrowBatchReader = new ArrowBatchReader(projectedRowType, true);
        this.positionIterator =
                rowIndices != null
                        ? LongIterator.fromArray(rowIndices)
                        : LongIterator.fromRange(0, vortexFile.rowCount());
    }

    @Nullable
    @Override
    public FileRecordIterator<InternalRow> readBatch() throws IOException {
        if (!arrayIterator.hasNext()) {
            return null;
        }

        releaseCurrentArray();
        Array array = arrayIterator.next();
        this.currentArray = array;
        VectorSchemaRoot vsr = array.exportToArrow(allocator, reuse);
        this.reuse = vsr;
        Iterator<InternalRow> rows = arrowBatchReader.readBatch(vsr).iterator();

        return new FileRecordIterator<InternalRow>() {
            @Override
            public long returnedPosition() {
                return returnedPosition;
            }

            @Override
            public Path filePath() {
                return filePath;
            }

            @Nullable
            @Override
            public InternalRow next() {
                if (!rows.hasNext()) {
                    return null;
                }
                returnedPosition = positionIterator.next();
                return rows.next();
            }

            @Override
            public void releaseBatch() {
                releaseCurrentArray();
            }
        };
    }

    private void releaseCurrentArray() {
        if (currentArray != null) {
            currentArray.close();
            currentArray = null;
        }
    }

    @Override
    public void close() {
        releaseCurrentArray();
        if (reuse != null) {
            reuse.close();
        }
        arrayIterator.close();
        vortexFile.close();
        allocator.close();
    }
}
