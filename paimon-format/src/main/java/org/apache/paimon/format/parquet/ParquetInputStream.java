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

package org.apache.paimon.format.parquet;

import org.apache.paimon.fs.FileRange;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadable;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.ParquetFileRange;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link SeekableInputStream} adapter for Paimon's {@link SeekableInputStream}.
 *
 * <p>This class bridges Paimon's FileIO abstraction with Parquet's input stream requirements,
 * including support for vectored reads when the underlying stream implements {@link
 * VectoredReadable}.
 *
 * <p><b>Thread Safety:</b> This class is <b>NOT thread-safe</b>. The same instance should not be
 * used concurrently by multiple threads. Each thread should create its own instance if parallel
 * reading is required. This is consistent with the general contract of {@link java.io.InputStream}
 * and Parquet's {@link org.apache.parquet.io.SeekableInputStream}.
 *
 * <p>When the underlying stream supports {@link VectoredReadable}, position-based reads (pread) are
 * used which are inherently thread-safe per stream instance. When vectored reads are not supported,
 * the implementation falls back to a seek-read-seek pattern which is not thread-safe and may
 * produce incorrect results if accessed concurrently.
 */
public class ParquetInputStream extends DelegatingSeekableInputStream {

    private final SeekableInputStream in;

    public ParquetInputStream(SeekableInputStream in) {
        super(in);
        this.in = in;
    }

    public SeekableInputStream in() {
        return in;
    }

    @Override
    public long getPos() throws IOException {
        return in.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
        in.seek(newPos);
    }

    @Override
    public void readVectored(List<ParquetFileRange> ranges, ByteBufferAllocator allocator)
            throws IOException {
        if (ranges.isEmpty()) {
            return;
        }

        // Check if underlying stream supports vectored reads
        if (in instanceof VectoredReadable) {
            VectoredReadable vectoredReadable = (VectoredReadable) in;

            // Convert Parquet ranges to Paimon ranges
            List<FileRange> paimonRanges = new ArrayList<>(ranges.size());
            for (ParquetFileRange parquetRange : ranges) {
                paimonRanges.add(
                        FileRange.createFileRange(
                                parquetRange.getOffset(), parquetRange.getLength()));
            }

            // Perform vectored read using Paimon's implementation
            vectoredReadable.readVectored(paimonRanges);

            // Convert results from byte[] to ByteBuffer and set to Parquet ranges
            for (int i = 0; i < ranges.size(); i++) {
                ParquetFileRange parquetRange = ranges.get(i);
                FileRange paimonRange = paimonRanges.get(i);

                // Chain the futures to convert byte[] to ByteBuffer
                parquetRange.setDataReadFuture(
                        paimonRange
                                .getData()
                                .thenApply(
                                        bytes -> {
                                            ByteBuffer buffer = allocator.allocate(bytes.length);
                                            buffer.put(bytes);
                                            buffer.flip();
                                            return buffer;
                                        }));
            }
        } else {
            // Fallback: use serial reads when vectored reads are not supported
            for (ParquetFileRange range : ranges) {
                byte[] data = new byte[range.getLength()];
                readFully(range.getOffset(), data);
                ByteBuffer buffer = allocator.allocate(data.length);
                buffer.put(data);
                buffer.flip();
                range.setDataReadFuture(
                        java.util.concurrent.CompletableFuture.completedFuture(buffer));
            }
        }
    }

    @Override
    public boolean readVectoredAvailable(ByteBufferAllocator allocator) {
        // Return true if underlying stream supports vectored reads
        return in instanceof VectoredReadable;
    }

    /**
     * Read fully from a given position without changing the current stream offset.
     *
     * <p>When the underlying stream supports {@link VectoredReadable}, this uses position-based
     * read (pread) which does not modify the stream position. When vectored reads are not
     * supported, this falls back to a seek-read-seek pattern which temporarily modifies the stream
     * position and is not thread-safe.
     *
     * @param position the position to read from
     * @param buffer the buffer to read into
     * @throws IOException if an I/O error occurs
     */
    private void readFully(long position, byte[] buffer) throws IOException {
        if (in instanceof VectoredReadable) {
            // Use position-based read which doesn't affect stream position
            ((VectoredReadable) in).preadFully(position, buffer, 0, buffer.length);
        } else {
            // Fallback: temporarily change stream position
            // NOTE: This is not thread-safe - concurrent calls may interfere with each other
            long oldPos = in.getPos();
            try {
                in.seek(position);
                readFully(buffer);
            } finally {
                in.seek(oldPos);
            }
        }
    }
}
