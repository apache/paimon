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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Reads graph structure (neighbors) from a DiskANN index file on demand via a Paimon {@link
 * SeekableInputStream}.
 *
 * <p>The underlying stream can be backed by any Paimon FileIO provider — local, HDFS, S3, OSS, etc.
 * This class adds an LRU cache so that repeated reads for the same node (common during DiskANN's
 * beam search) do not trigger redundant I/O.
 *
 * <p>The Rust JNI layer invokes {@link #readNeighbors(int)} via reflection during DiskANN's native
 * beam search. It also calls getter methods ({@link #getDimension()}, {@link #getCount()}, etc.)
 * during searcher initialization.
 *
 * <h3>Index file layout</h3>
 *
 * <pre>
 *   Header (36 bytes): 9 × i32
 *     magic, version, dimension, metricType, indexType,
 *     maxDegree, buildListSize, count, startId
 *
 *   Graph section: for each node (count nodes):
 *     ext_id       : i64
 *     int_id       : i32
 *     neighbor_cnt : i32
 *     neighbors    : neighbor_cnt × i32
 * </pre>
 *
 * <p>On construction, the header is read and the graph section is scanned once sequentially to
 * build an offset index (mapping internal node ID → file byte offset for its neighbor data). After
 * that, individual neighbor lists are read on demand by seeking to the stored offset.
 */
public class FileIOGraphReader implements Closeable {

    /** Header size: 9 × i32 = 36 bytes. */
    private static final int HEADER_SIZE = 36;

    /** Source stream — must support seek(). */
    private final SeekableInputStream input;

    // ---- Header fields ----
    private final int dimension;
    private final int metricValue;
    private final int indexTypeValue;
    private final int maxDegree;
    private final int buildListSize;
    private final int count;
    private final int startId;

    // ---- Offset index built during initial scan ----

    /** Mapping from internal node ID → byte offset of the node's neighbor_cnt field in the file. */
    private final Map<Integer, Long> nodeNeighborOffsets;

    /** All internal node IDs (in file order). */
    private final int[] allInternalIds;

    /** Corresponding external IDs (same order as {@link #allInternalIds}). */
    private final long[] allExternalIds;

    /** LRU cache: internal node ID → neighbor list (int[]). */
    private final LinkedHashMap<Integer, int[]> cache;

    /**
     * Create a reader by parsing the header and scanning the graph section to build the offset
     * index.
     *
     * @param input seekable input stream for the index file (header + graph)
     * @param cacheSize maximum number of cached neighbor lists (0 uses default 4096)
     * @throws IOException if reading or parsing fails
     */
    public FileIOGraphReader(SeekableInputStream input, int cacheSize) throws IOException {
        this.input = input;

        // 1. Read header.
        byte[] headerBuf = new byte[HEADER_SIZE];
        input.seek(0);
        readFully(input, headerBuf);

        int off = 0;
        // magic(4) + version(4) — skip validation here; Rust validates during search.
        off += 8;
        this.dimension = readInt(headerBuf, off);
        off += 4;
        this.metricValue = readInt(headerBuf, off);
        off += 4;
        this.indexTypeValue = readInt(headerBuf, off);
        off += 4;
        this.maxDegree = readInt(headerBuf, off);
        off += 4;
        this.buildListSize = readInt(headerBuf, off);
        off += 4;
        this.count = readInt(headerBuf, off);
        off += 4;
        this.startId = readInt(headerBuf, off);

        // 2. Scan graph section to build offset index.
        this.nodeNeighborOffsets = new HashMap<>(count);
        this.allInternalIds = new int[count];
        this.allExternalIds = new long[count];

        // Reusable buffer for reading ext_id(8) + int_id(4) + neighbor_cnt(4) = 16 bytes per node.
        byte[] nodeBuf = new byte[16];
        long filePos = HEADER_SIZE;

        for (int i = 0; i < count; i++) {
            input.seek(filePos);
            readFully(input, nodeBuf);

            long extId = readLong(nodeBuf, 0);
            int intId = readInt(nodeBuf, 8);
            int neighborCount = readInt(nodeBuf, 12);

            allInternalIds[i] = intId;
            allExternalIds[i] = extId;

            // Store file offset pointing to the neighbor_cnt field (so readNeighbors can re-read
            // count + data).
            nodeNeighborOffsets.put(intId, filePos + 12);

            // Advance past: ext_id(8) + int_id(4) + neighbor_cnt(4) + neighbors(cnt*4).
            filePos += 16 + (long) neighborCount * 4;
        }

        // 3. Create LRU cache.
        final int cap = cacheSize > 0 ? cacheSize : 4096;
        this.cache =
                new LinkedHashMap<Integer, int[]>(cap, 0.75f, true) {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<Integer, int[]> eldest) {
                        return size() > cap;
                    }
                };
    }

    // ---- Header accessors (called by Rust JNI during initialization) ----

    public int getDimension() {
        return dimension;
    }

    public int getMetricValue() {
        return metricValue;
    }

    public int getIndexTypeValue() {
        return indexTypeValue;
    }

    public int getMaxDegree() {
        return maxDegree;
    }

    public int getBuildListSize() {
        return buildListSize;
    }

    public int getCount() {
        return count;
    }

    public int getStartId() {
        return startId;
    }

    /** Return all internal node IDs (in file order). */
    public int[] getAllInternalIds() {
        return allInternalIds;
    }

    /** Return all external node IDs (same order as {@link #getAllInternalIds()}). */
    public long[] getAllExternalIds() {
        return allExternalIds;
    }

    // ---- On-demand neighbor reading (called by Rust JNI during beam search) ----

    /**
     * Read the neighbor list of the given internal node.
     *
     * <p>Called by the Rust JNI layer during DiskANN's native beam search. Returns a <b>defensive
     * copy</b> — callers may freely modify the returned array without corrupting the cache.
     *
     * @param internalNodeId the internal (graph) node ID
     * @return the neighbor internal IDs, or an empty array if the node is unknown
     */
    public int[] readNeighbors(int internalNodeId) {
        // 1. LRU cache hit — return a defensive copy.
        int[] cached = cache.get(internalNodeId);
        if (cached != null) {
            return Arrays.copyOf(cached, cached.length);
        }

        // 2. Look up file offset.
        Long offset = nodeNeighborOffsets.get(internalNodeId);
        if (offset == null) {
            return new int[0]; // unknown node
        }

        // 3. Seek & read neighbor_cnt + neighbor_ids.
        try {
            input.seek(offset);

            // Read neighbor count (4 bytes).
            byte[] cntBuf = new byte[4];
            readFully(input, cntBuf);
            int neighborCount = readInt(cntBuf, 0);

            // Read neighbor IDs.
            byte[] neighborBuf = new byte[neighborCount * 4];
            readFully(input, neighborBuf);

            int[] neighbors = new int[neighborCount];
            ByteBuffer bb = ByteBuffer.wrap(neighborBuf).order(ByteOrder.nativeOrder());
            bb.asIntBuffer().get(neighbors);

            // 4. Cache a copy.
            cache.put(internalNodeId, Arrays.copyOf(neighbors, neighbors.length));
            return neighbors;
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to read neighbors for node " + internalNodeId + " at offset " + offset,
                    e);
        }
    }

    @Override
    public void close() throws IOException {
        cache.clear();
        input.close();
    }

    // ---- Helpers ----

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

    private static int readInt(byte[] buf, int off) {
        return (buf[off] & 0xFF)
                | ((buf[off + 1] & 0xFF) << 8)
                | ((buf[off + 2] & 0xFF) << 16)
                | ((buf[off + 3] & 0xFF) << 24);
    }

    private static long readLong(byte[] buf, int off) {
        return (buf[off] & 0xFFL)
                | ((buf[off + 1] & 0xFFL) << 8)
                | ((buf[off + 2] & 0xFFL) << 16)
                | ((buf[off + 3] & 0xFFL) << 24)
                | ((buf[off + 4] & 0xFFL) << 32)
                | ((buf[off + 5] & 0xFFL) << 40)
                | ((buf[off + 6] & 0xFFL) << 48)
                | ((buf[off + 7] & 0xFFL) << 56);
    }
}
