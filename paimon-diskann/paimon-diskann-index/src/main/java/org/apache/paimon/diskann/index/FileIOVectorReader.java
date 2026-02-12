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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Fetches vectors from a DiskANN data file through a Paimon {@link SeekableInputStream}.
 *
 * <p>The underlying stream can be backed by any Paimon FileIO provider — local, HDFS, S3, OSS, etc.
 * This class adds an LRU cache so that repeated reads for the same vector (common during DiskANN's
 * beam search) do not trigger redundant I/O.
 *
 * <p>The Rust JNI layer invokes {@link #readVector(long)} via reflection during DiskANN's native
 * beam search — no specific Java interface is required.
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

    /** LRU cache: position → float[]. */
    private final LinkedHashMap<Long, float[]> cache;

    /** Reusable byte buffer for reading a single vector. */
    private final byte[] readBuf;

    /**
     * Create a reader.
     *
     * @param input seekable input stream for the data file
     * @param dimension vector dimension
     * @param cacheSize maximum number of cached vectors (0 disables caching)
     */
    public FileIOVectorReader(SeekableInputStream input, int dimension, int cacheSize) {
        this.input = input;
        this.dimension = dimension;
        this.readBuf = new byte[dimension * Float.BYTES];

        final int cap = Math.max(cacheSize, 16);
        this.cache =
                new LinkedHashMap<Long, float[]>(cap, 0.75f, true) {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<Long, float[]> eldest) {
                        return size() > cap;
                    }
                };
    }

    /**
     * Read the vector associated with the given <em>external</em> ID.
     *
     * <p>Called by the Rust JNI layer during DiskANN's native beam search. Returns a <b>defensive
     * copy</b> — callers may freely modify the returned array without corrupting the cache.
     *
     * <p>The byte offset is computed as {@code position * dimension * Float.BYTES}.
     *
     * @param position the 0-based position in the data file (int_id - 1 for user vectors)
     * @return the float vector (a fresh copy), or {@code null} if position is negative
     */
    public float[] readVector(long position) {
        // Start point (position = -1) is not in the data file.
        if (position < 0) {
            return null;
        }

        // 1. LRU cache hit — return a defensive copy.
        float[] cached = cache.get(position);
        if (cached != null) {
            return Arrays.copyOf(cached, cached.length);
        }

        // 2. Compute byte offset: sequential position.
        long byteOffset = position * dimension * Float.BYTES;
        try {
            input.seek(byteOffset);
            readFully(input, readBuf);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to read vector at position " + position + " offset " + byteOffset, e);
        }

        // 3. Decode floats.
        float[] vector = new float[dimension];
        ByteBuffer bb = ByteBuffer.wrap(readBuf).order(ByteOrder.nativeOrder());
        bb.asFloatBuffer().get(vector);

        // 4. Store a separate copy in the cache so the returned array is independent.
        cache.put(position, Arrays.copyOf(vector, vector.length));
        return vector;
    }

    @Override
    public void close() throws IOException {
        cache.clear();
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
