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

package org.apache.paimon.diskann.index;

import org.apache.paimon.fs.SeekableInputStream;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Fetches vectors from a DiskANN data file through a Paimon {@link SeekableInputStream}.
 *
 * <p>The underlying stream can be backed by any Paimon FileIO provider — local, HDFS, S3, OSS, etc.
 *
 * <p>The Rust JNI layer uses two access modes:
 *
 * <ul>
 *   <li><b>Single-vector zero-copy</b>: {@link #loadVector(long)} reads a vector into a
 *       pre-allocated {@link ByteBuffer#allocateDirect DirectByteBuffer}. The Rust side reads
 *       floats directly from the native memory address — no {@code float[]} allocation and no JNI
 *       array copy.
 *   <li><b>Batch prefetch</b>: {@link #readVectorsBatch(long[], int)} reads multiple vectors into a
 *       larger DirectByteBuffer in a single JNI call, reducing per-vector JNI round-trip overhead.
 * </ul>
 *
 * <h3>Data file layout</h3>
 *
 * <p>Vectors are stored contiguously in sequential order. Each vector occupies {@code dimension *
 * 4} bytes (native-order floats). The vector at position {@code i} is at byte offset {@code i *
 * dimension * 4}. The sequential position IS the ID.
 *
 * <p>The start point vector is NOT stored in the data file; it is handled in memory by the Rust
 * native layer.
 */
public class FileIOVectorReader implements Closeable {

    /** Source stream — must support seek(). */
    private final SeekableInputStream input;

    /** Vector dimension. */
    private final int dimension;

    /** Byte size of a single vector: {@code dimension * Float.BYTES}. */
    private final int vectorBytes;

    /** Reusable heap byte buffer for stream I/O (stream API requires {@code byte[]}). */
    private final byte[] readBuf;

    /**
     * Pre-allocated DirectByteBuffer for single-vector reads. Rust reads directly from its native
     * address via {@code GetDirectBufferAddress} — zero JNI array copy.
     */
    private final ByteBuffer directBuf;

    /**
     * Pre-allocated DirectByteBuffer for batch reads. Holds up to {@code maxBatchSize} vectors
     * packed sequentially.
     */
    private final ByteBuffer batchBuf;

    /** Maximum number of vectors that fit in {@link #batchBuf}. */
    private final int maxBatchSize;

    /**
     * Create a reader.
     *
     * @param input seekable input stream for the data file
     * @param dimension vector dimension
     * @param maxBatchSize maximum number of vectors in a batch read (typically max_degree)
     */
    public FileIOVectorReader(SeekableInputStream input, int dimension, int maxBatchSize) {
        this.input = input;
        this.dimension = dimension;
        this.vectorBytes = dimension * Float.BYTES;
        this.readBuf = new byte[vectorBytes];
        this.maxBatchSize = Math.max(maxBatchSize, 1);

        // Single-vector DirectByteBuffer — Rust gets its native address once at init.
        this.directBuf = ByteBuffer.allocateDirect(vectorBytes).order(ByteOrder.nativeOrder());

        // Batch DirectByteBuffer — sized for maxBatchSize vectors.
        this.batchBuf =
                ByteBuffer.allocateDirect(this.maxBatchSize * vectorBytes)
                        .order(ByteOrder.nativeOrder());
    }

    // ------------------------------------------------------------------
    // DirectByteBuffer accessors (called by Rust JNI during init)
    // ------------------------------------------------------------------

    /** Return the single-vector DirectByteBuffer. Rust caches its native address. */
    public ByteBuffer getDirectBuffer() {
        return directBuf;
    }

    /** Return the batch DirectByteBuffer. Rust caches its native address. */
    public ByteBuffer getBatchBuffer() {
        return batchBuf;
    }

    /** Return the maximum batch size supported by {@link #batchBuf}. */
    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    // ------------------------------------------------------------------
    // Single-vector zero-copy read (hot path during beam search)
    // ------------------------------------------------------------------

    /**
     * Read a single vector into the pre-allocated {@link #directBuf}.
     *
     * <p>After this call returns {@code true}, the vector data is available in the DirectByteBuffer
     * at offset 0. The Rust side reads floats directly from the native memory address.
     *
     * @param position 0-based position in the data file (int_id − 1)
     * @return {@code true} if the vector was read successfully, {@code false} if position is
     *     invalid
     */
    public boolean loadVector(long position) {
        if (position < 0) {
            return false;
        }
        long byteOffset = position * vectorBytes;
        try {
            input.seek(byteOffset);
            readFully(input, readBuf);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to read vector at position " + position + " offset " + byteOffset, e);
        }
        // Copy from heap byte[] into DirectByteBuffer (single memcpy, no float[] allocation).
        directBuf.clear();
        directBuf.put(readBuf, 0, vectorBytes);
        return true;
    }

    // ------------------------------------------------------------------
    // Batch prefetch (reduces JNI call count)
    // ------------------------------------------------------------------

    /**
     * Read multiple vectors into the batch DirectByteBuffer in one JNI call.
     *
     * <p>Vectors are packed sequentially in the batch buffer: vector i occupies bytes {@code [i *
     * vectorBytes, (i+1) * vectorBytes)}. The Rust side reads all vectors from the native address
     * after a single JNI round-trip.
     *
     * @param positions array of 0-based positions (int_id − 1 for each vector)
     * @param count number of positions to read (must be ≤ {@link #maxBatchSize})
     * @return number of vectors successfully read (always equals {@code count} on success)
     */
    public int readVectorsBatch(long[] positions, int count) {
        int n = Math.min(count, maxBatchSize);
        batchBuf.clear();
        for (int i = 0; i < n; i++) {
            long byteOffset = positions[i] * vectorBytes;
            try {
                input.seek(byteOffset);
                readFully(input, readBuf);
                batchBuf.put(readBuf, 0, vectorBytes);
            } catch (IOException e) {
                throw new RuntimeException(
                        "Failed to batch-read vector at position "
                                + positions[i]
                                + " offset "
                                + byteOffset,
                        e);
            }
        }
        return n;
    }

    // ------------------------------------------------------------------
    // Legacy read (kept for backward compatibility)
    // ------------------------------------------------------------------

    /**
     * Read a vector and return as {@code float[]}. This is the legacy path — prefer {@link
     * #loadVector(long)} for the zero-copy hot path.
     *
     * @param position 0-based position in the data file
     * @return the float vector, or {@code null} if position is negative
     */
    public float[] readVector(long position) {
        if (position < 0) {
            return null;
        }
        long byteOffset = position * vectorBytes;
        try {
            input.seek(byteOffset);
            readFully(input, readBuf);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to read vector at position " + position + " offset " + byteOffset, e);
        }
        float[] vector = new float[dimension];
        ByteBuffer bb = ByteBuffer.wrap(readBuf).order(ByteOrder.nativeOrder());
        bb.asFloatBuffer().get(vector);
        return vector;
    }

    @Override
    public void close() throws IOException {
        input.close();
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static void readFully(SeekableInputStream in, byte[] buf) throws IOException {
        int off = 0;
        while (off < buf.length) {
            int n = in.read(buf, off, buf.length - off);
            if (n < 0) {
                throw new IOException(
                        "Unexpected end of stream at offset " + off + " of " + buf.length);
            }
            off += n;
        }
    }
}
