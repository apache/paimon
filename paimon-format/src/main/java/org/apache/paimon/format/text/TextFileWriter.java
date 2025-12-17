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

package org.apache.paimon.format.text;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.HadoopCompressionType;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.types.RowType;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

/** Base class for text-based format writers that provides common functionality. */
public abstract class TextFileWriter implements FormatWriter {

    protected final PositionOutputStream outputStream;
    protected final BufferedWriter writer;
    protected final RowType rowType;

    protected TextFileWriter(PositionOutputStream outputStream, RowType rowType, String compression)
            throws IOException {
        this.outputStream = outputStream;
        OutputStream compressedStream =
                HadoopCompressionUtils.createCompressedOutputStream(outputStream, compression);
        this.writer =
                new BufferedWriter(
                        new OutputStreamWriter(compressedStream, StandardCharsets.UTF_8),
                        getOptimalBufferSize(compression));
        this.rowType = rowType;
    }

    /**
     * Writes a single row element to the output stream. Subclasses must implement this method to
     * handle their specific format serialization.
     */
    @Override
    public abstract void addElement(InternalRow element) throws IOException;

    @Override
    public void close() throws IOException {
        writer.flush();
        writer.close();
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        if (suggestedCheck) {
            return outputStream.getPos() >= targetSize;
        }
        return false;
    }

    private int getOptimalBufferSize(String compression) {
        HadoopCompressionType type =
                HadoopCompressionType.fromValue(compression)
                        .orElseThrow(IllegalArgumentException::new);
        switch (type) {
            case GZIP:
            case DEFLATE:
                return 65536; // 64KB for deflate-based compression
            case SNAPPY:
            case LZ4:
                return 131072; // 128KB for fast compression
            case ZSTD:
                return 262144; // 256KB for high compression ratio
            case BZIP2:
                return 65536; // 64KB for bzip2
            default:
                return 65536; // Default 64KB buffer size
        }
    }
}
