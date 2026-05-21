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

import org.apache.paimon.arrow.ArrowBundleRecords;
import org.apache.paimon.arrow.vector.ArrowFormatWriter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.BundleFormatWriter;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.mosaic.ColumnStatistics;
import org.apache.paimon.mosaic.MosaicWriter;
import org.apache.paimon.mosaic.WriterOptions;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Mosaic records writer. */
public class MosaicRecordsWriter implements BundleFormatWriter {

    private final ArrowFormatWriter arrowFormatWriter;
    private final MosaicWriter nativeWriter;
    private final BufferAllocator allocator;
    private final List<String> statsColumnNames;
    @Nullable private MosaicWriterMetadata metadata;

    public MosaicRecordsWriter(
            OutputStream outputStream,
            RowType rowType,
            FileFormatFactory.FormatContext formatContext,
            List<String> statsColumnNames,
            @Nullable Integer numBuckets) {
        this.statsColumnNames = statsColumnNames;
        this.allocator = new RootAllocator();

        int writeBatchSize = formatContext.writeBatchSize();
        long writeBatchMemory = formatContext.writeBatchMemory().getBytes();

        this.arrowFormatWriter =
                new ArrowFormatWriter(rowType, writeBatchSize, true, allocator, writeBatchMemory);

        WriterOptions options = new WriterOptions().zstdLevel(formatContext.zstdLevel());
        if (numBuckets != null) {
            options = options.numBuckets(numBuckets);
        }
        MemorySize blockSize = formatContext.blockSize();
        if (blockSize != null) {
            options = options.rowGroupMaxSize(blockSize.getBytes());
        }
        if (!statsColumnNames.isEmpty()) {
            options.statsColumns(statsColumnNames.toArray(new String[0]));
        }

        Schema arrowSchema = arrowFormatWriter.getVectorSchemaRoot().getSchema();
        this.nativeWriter = new MosaicWriter(outputStream, arrowSchema, options, allocator);
    }

    @Override
    public void addElement(InternalRow internalRow) {
        if (!arrowFormatWriter.write(internalRow)) {
            flush();
            if (!arrowFormatWriter.write(internalRow)) {
                throw new RuntimeException("Failed to write row to Mosaic file");
            }
        }
    }

    @Override
    public void writeBundle(BundleRecords bundleRecords) {
        if (bundleRecords instanceof ArrowBundleRecords) {
            flush();
            nativeWriter.write(((ArrowBundleRecords) bundleRecords).getVectorSchemaRoot());
        } else {
            for (InternalRow row : bundleRecords) {
                addElement(row);
            }
        }
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) {
        if (!suggestedCheck) {
            return false;
        }
        return nativeWriter.estimatedFileSize() >= targetSize;
    }

    @Override
    public void close() throws IOException {
        Throwable throwable = null;

        try {
            flush();
        } catch (Throwable t) {
            throwable = t;
        }

        try {
            nativeWriter.close();
        } catch (Throwable t) {
            throwable = addSuppressed(throwable, t);
        }

        try {
            collectMetadata();
        } catch (Throwable t) {
            throwable = addSuppressed(throwable, t);
        }

        try {
            arrowFormatWriter.close();
        } catch (Throwable t) {
            throwable = addSuppressed(throwable, t);
        }

        try {
            allocator.close();
        } catch (Throwable t) {
            throwable = addSuppressed(throwable, t);
        }

        if (throwable != null) {
            rethrow(throwable);
        }
    }

    @Nullable
    @Override
    public Object writerMetadata() {
        return metadata;
    }

    private void collectMetadata() {
        int numRowGroups = nativeWriter.numRowGroups();
        List<Map<String, ColumnStatistics>> allStats = new ArrayList<>(numRowGroups);
        for (int i = 0; i < numRowGroups; i++) {
            allStats.add(nativeWriter.getRowGroupStatistics(i));
        }
        this.metadata = new MosaicWriterMetadata(numRowGroups, allStats, statsColumnNames);
    }

    private void flush() {
        arrowFormatWriter.flush();
        if (!arrowFormatWriter.empty()) {
            VectorSchemaRoot vsr = arrowFormatWriter.getVectorSchemaRoot();
            nativeWriter.write(vsr);
        }
        arrowFormatWriter.reset();
    }

    private static Throwable addSuppressed(Throwable throwable, Throwable suppressed) {
        if (throwable == null) {
            return suppressed;
        }
        throwable.addSuppressed(suppressed);
        return throwable;
    }

    private static void rethrow(Throwable throwable) throws IOException {
        if (throwable instanceof IOException) {
            throw (IOException) throwable;
        }
        if (throwable instanceof RuntimeException) {
            throw (RuntimeException) throwable;
        }
        if (throwable instanceof Error) {
            throw (Error) throwable;
        }
        throw new IOException(throwable);
    }
}
