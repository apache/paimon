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
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.PositionOutputStream;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.Writer;

import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** A {@link FormatWriter} implementation that writes data in ORC format. */
public class OrcBulkWriter implements FormatWriter {

    private final Writer writer;
    private final Vectorizer<InternalRow> vectorizer;
    private final VectorizedRowBatch rowBatch;
    private final PositionOutputStream underlyingStream;

    public OrcBulkWriter(
            Vectorizer<InternalRow> vectorizer,
            Writer writer,
            PositionOutputStream underlyingStream,
            int batchSize) {
        this.vectorizer = checkNotNull(vectorizer);
        this.writer = checkNotNull(writer);

        this.rowBatch = vectorizer.getSchema().createRowBatch(batchSize);
        this.underlyingStream = underlyingStream;
    }

    @Override
    public void addElement(InternalRow element) throws IOException {
        vectorizer.vectorize(element, rowBatch);
        if (rowBatch.size == rowBatch.getMaxSize()) {
            flush();
        }
    }

    private void flush() throws IOException {
        if (rowBatch.size != 0) {
            writer.addRowBatch(rowBatch);
            rowBatch.reset();
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        writer.close();
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
}
