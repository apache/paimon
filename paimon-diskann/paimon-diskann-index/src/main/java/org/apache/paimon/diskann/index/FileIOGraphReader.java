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
 * <h3>Index file layout (graph only, no header)</h3>
 *
 * <pre>
 *   Graph section: for each node (count nodes):
 *     int_id       : i32
 *     neighbor_cnt : i32
 *     neighbors    : neighbor_cnt × i32
 * </pre>
 *
 * <p>DiskANN stores vectors sequentially — the position IS the ID. Internal IDs map to positions
 * via {@code position = int_id - 1} for user vectors. The start point ({@code int_id == startId})
 * is not a user vector.
 *
 * <p>All metadata (dimension, metric, max_degree, etc.) is provided externally via {@link
 * DiskAnnIndexMeta} — the file contains only graph data.
 *
 * <p>On construction, the graph section is scanned once sequentially to build an offset index
 * (mapping internal node ID → file byte offset for its neighbor data). After that, individual
 * neighbor lists are read on demand by seeking to the stored offset.
 */
public class FileIOGraphReader implements Closeable {

    /** Source stream — must support seek(). */
    private final SeekableInputStream input;

    // ---- Metadata fields (from DiskAnnIndexMeta) ----
    private final int dimension;
    private final int metricValue;
    private final int maxDegree;
    private final int buildListSize;
    private final int count;
    private final int startId;

    // ---- Offset index built during initial scan ----

    /** Mapping from internal node ID → byte offset of the node's neighbor_cnt field in the file. */
    private final Map<Integer, Long> nodeNeighborOffsets;

    /** LRU cache: internal node ID → neighbor list (int[]). */
    private final LinkedHashMap<Integer, int[]> cache;

    /**
     * Create a reader from metadata and a seekable input stream.
     *
     * <p>The stream should point to a file that contains ONLY the graph section (no header, no
     * IDs). All metadata is supplied via parameters (originally from {@link DiskAnnIndexMeta}).
     *
     * @param input seekable input stream for the index file (graph only)
     * @param dimension vector dimension
     * @param metricValue metric type value (0=L2, 1=IP, 2=Cosine)
     * @param maxDegree maximum adjacency list size
     * @param buildListSize search list size used during construction
     * @param count total number of graph nodes (including start point)
     * @param startId internal ID of the graph start/entry point
     * @param cacheSize maximum number of cached neighbor lists (0 uses default 4096)
     * @throws IOException if reading or parsing fails
     */
    public FileIOGraphReader(
            SeekableInputStream input,
            int dimension,
            int metricValue,
            int maxDegree,
            int buildListSize,
            int count,
            int startId,
            int cacheSize)
            throws IOException {
        this.input = input;
        this.dimension = dimension;
        this.metricValue = metricValue;
        this.maxDegree = maxDegree;
        this.buildListSize = buildListSize;
        this.count = count;
        this.startId = startId;

        // Scan graph section to build offset index.
        // The file starts directly with graph entries (no header).
        // Each entry: int_id(4) + neighbor_cnt(4) + neighbors(cnt*4).
        this.nodeNeighborOffsets = new HashMap<>(count);

        // Reusable buffer for reading int_id(4) + neighbor_cnt(4) = 8 bytes per node.
        byte[] nodeBuf = new byte[8];
        long filePos = 0;

        for (int i = 0; i < count; i++) {
            input.seek(filePos);
            readFully(input, nodeBuf);

            int intId = readInt(nodeBuf, 0);
            int neighborCount = readInt(nodeBuf, 4);

            // Store file offset pointing to the neighbor_cnt field (so readNeighbors can re-read
            // count + data).
            nodeNeighborOffsets.put(intId, filePos + 4);

            // Advance past: int_id(4) + neighbor_cnt(4) + neighbors(cnt*4).
            filePos += 8 + (long) neighborCount * 4;
        }

        // Create LRU cache.
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
}
