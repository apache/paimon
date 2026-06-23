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

package org.apache.paimon.format.orc.writer;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatMetadataUtils;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.SupportsWriterMetadata;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.MemorySize;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.Writer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkNotNull;
import static org.apache.paimon.utils.Preconditions.checkState;

/** A {@link FormatWriter} implementation that writes data in ORC format. */
public class OrcBulkWriter implements FormatWriter, SupportsWriterMetadata {

    private final Writer writer;
    private final Vectorizer<InternalRow> vectorizer;
    private final VectorizedRowBatch rowBatch;
    private final PositionOutputStream underlyingStream;
    private final Map<String, byte[]> metadata;

    private long currentBatchMemoryUsage = 0;
    private final long memoryLimit;
    private boolean closed = false;

    public OrcBulkWriter(
            Vectorizer<InternalRow> vectorizer,
            Writer writer,
            PositionOutputStream underlyingStream,
            int batchSize,
            MemorySize memoryLimit) {
        this.vectorizer = checkNotNull(vectorizer);
        this.writer = checkNotNull(writer);

        this.rowBatch = vectorizer.getSchema().createRowBatch(batchSize);
        this.underlyingStream = underlyingStream;
        this.memoryLimit = memoryLimit.getBytes();
        this.metadata = new HashMap<>();
    }

    @Override
    public void addMetadata(Map<String, byte[]> metadata) {
        checkState(!closed, "Cannot add metadata after writer is closed.");
        for (Map.Entry<String, byte[]> entry : metadata.entrySet()) {
            this.metadata.put(
                    entry.getKey(), Arrays.copyOf(entry.getValue(), entry.getValue().length));
        }
    }

    @Override
    public void addElement(InternalRow element) throws IOException {
        currentBatchMemoryUsage += vectorizer.vectorize(element, rowBatch);
        if (rowBatch.size == rowBatch.getMaxSize() || currentBatchMemoryUsage >= this.memoryLimit) {
            flush();
        }
    }

    private void flush() throws IOException {
        if (rowBatch.size != 0) {
            writer.addRowBatch(rowBatch);
            rowBatch.reset();
            currentBatchMemoryUsage = 0;
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        for (Map.Entry<String, String> entry :
                FormatMetadataUtils.encodeMetadata(metadata).entrySet()) {
            writer.addUserMetadata(
                    entry.getKey(),
                    ByteBuffer.wrap(entry.getValue().getBytes(StandardCharsets.UTF_8)));
        }
        writer.close();
        this.closed = true;
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        return rowBatch.size == 0 && length() >= targetSize;
    }

    private long length() throws IOException {
        long estimateMemory = writer.estimateMemory();
        long fileLength = underlyingStream.getPos();

        // This value is estimated, not actual.
        return (long) Math.ceil(fileLength + estimateMemory * 0.2);
    }

    @VisibleForTesting
    VectorizedRowBatch getRowBatch() {
        return rowBatch;
    }

    @VisibleForTesting
    long getMemoryLimit() {
        return memoryLimit;
    }
}
