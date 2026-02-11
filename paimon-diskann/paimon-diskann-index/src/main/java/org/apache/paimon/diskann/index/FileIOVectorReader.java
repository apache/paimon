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
 * <p>The underlying stream can be backed by any Paimon FileIO provider — local, HDFS, S3, OSS (via
 * Jindo SDK), etc. This class adds an LRU cache so that repeated reads for the same vector (common
 * during DiskANN's beam search) do not trigger redundant I/O.
 *
 * <p>The Rust JNI layer invokes {@link #readVector(long)} via reflection during DiskANN's native
 * beam search — no specific Java interface is required.
 *
 * <h3>File layout (vector section)</h3>
 *
 * <p>Vectors are stored contiguously. Each vector occupies {@code dimension * 4} bytes
 * (native-order floats). The position of a vector with entry-index {@code i} is: {@code
 * vectorSectionOffset + i * dimension * 4}.
 */
public class FileIOVectorReader implements Closeable {

    /** Source stream — must support seek(). */
    private final SeekableInputStream input;

    /** Vector dimension. */
    private final int dimension;

    /** Byte offset of the vector section within the file. Vectors start after the graph section. */
    private final long vectorSectionOffset;

    /**
     * Mapping from external (user-facing) vector ID to entry index in the file. Entry index
     * determines the byte offset: {@code vectorSectionOffset + entryIndex * dimension * 4}.
     */
    private final Map<Long, Integer> extIdToEntryIndex;

    /** LRU cache: external vector ID → float[]. */
    private final LinkedHashMap<Long, float[]> cache;

    /** Reusable byte buffer for reading a single vector. */
    private final byte[] readBuf;

    /**
     * Create a reader.
     *
     * @param input seekable input stream for the data file
     * @param dimension vector dimension
     * @param vectorSectionOffset byte offset where the vector section starts in the file
     * @param extIdToEntryIndex mapping from external vector ID to sequential entry index
     * @param cacheSize maximum number of cached vectors (0 disables caching)
     */
    public FileIOVectorReader(
            SeekableInputStream input,
            int dimension,
            long vectorSectionOffset,
            Map<Long, Integer> extIdToEntryIndex,
            int cacheSize) {
        this.input = input;
        this.dimension = dimension;
        this.vectorSectionOffset = vectorSectionOffset;
        this.extIdToEntryIndex = extIdToEntryIndex;
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
     * @param vectorId the external (user-facing) vector ID
     * @return the float vector (a fresh copy), or {@code null} if unavailable
     */
    public float[] readVector(long vectorId) {
        // 1. LRU cache hit — return a defensive copy.
        float[] cached = cache.get(vectorId);
        if (cached != null) {
            return Arrays.copyOf(cached, cached.length);
        }

        // 2. Look up entry index.
        Integer entryIndex = extIdToEntryIndex.get(vectorId);
        if (entryIndex == null) {
            return null; // unknown vector
        }

        // 3. Seek & read from the underlying stream.
        long byteOffset = vectorSectionOffset + (long) entryIndex * dimension * Float.BYTES;
        try {
            input.seek(byteOffset);
            readFully(input, readBuf);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to read vector " + vectorId + " at offset " + byteOffset, e);
        }

        // 4. Decode floats.
        float[] vector = new float[dimension];
        ByteBuffer bb = ByteBuffer.wrap(readBuf).order(ByteOrder.nativeOrder());
        bb.asFloatBuffer().get(vector);

        // 5. Store a separate copy in the cache so the returned array is independent.
        cache.put(vectorId, Arrays.copyOf(vector, vector.length));
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

    // ------------------------------------------------------------------
    // Factory helper — parse header to obtain vectorSectionOffset
    //                   and extIdToEntryIndex.
    // ------------------------------------------------------------------

    /**
     * Parse a serialized byte array (header + graph section) to extract the mapping and vector
     * section offset needed by this reader.
     *
     * <p>This is typically called once when opening the index for search. The byte array only needs
     * to contain the header and graph section — the vector section is not accessed.
     *
     * @param data byte array containing at least the header and graph section
     * @return a {@link IndexLayout} containing the parsed metadata
     */
    public static IndexLayout parseIndexLayout(byte[] data) {
        // Header: 9 × i32 = 36 bytes.
        int off = 0;
        // skip magic(4), version(4), dimension(4), metricType(4), indexType(4),
        // maxDegree(4), buildListSize(4)
        off += 28;
        int count = readInt(data, off);
        off += 4;
        // skip startId(4)
        off += 4;

        // Scan graph section to find the vector section offset.
        for (int i = 0; i < count; i++) {
            // skip ext_id(8) + int_id(4)
            off += 12;
            int neighborCount = readInt(data, off);
            off += 4;
            // skip neighbor IDs
            off += neighborCount * 4;
        }

        return new IndexLayout(off);
    }

    /** Parsed layout metadata. */
    public static class IndexLayout {
        private final long vectorSectionOffset;

        IndexLayout(long vectorSectionOffset) {
            this.vectorSectionOffset = vectorSectionOffset;
        }

        public long vectorSectionOffset() {
            return vectorSectionOffset;
        }
    }

    private static int readInt(byte[] buf, int off) {
        return (buf[off] & 0xFF)
                | ((buf[off + 1] & 0xFF) << 8)
                | ((buf[off + 2] & 0xFF) << 16)
                | ((buf[off + 3] & 0xFF) << 24);
    }
}
