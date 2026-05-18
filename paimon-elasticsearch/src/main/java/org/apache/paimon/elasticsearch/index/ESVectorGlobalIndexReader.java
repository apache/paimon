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

package org.apache.paimon.elasticsearch.index;

import org.apache.paimon.elasticsearch.index.model.ESIndexEntryMeta;
import org.apache.paimon.elasticsearch.index.model.ESScoredGlobalIndexResult;
import org.apache.paimon.elasticsearch.index.model.ESVectorIndexOptions;
import org.apache.paimon.elasticsearch.index.util.ArchiveFlatVectorReader;
import org.apache.paimon.elasticsearch.index.util.Slf4jBridgeESLoggerFactory;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.VectorType;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.elasticsearch.vectorindex.api.VectorIndexClusterSearcher;
import org.elasticsearch.vectorindex.impl.DiskBBQCentroidReader;
import org.elasticsearch.vectorindex.impl.DiskBBQClusterSearcher;
import org.elasticsearch.vectorindex.io.VectorSearchSession;
import org.elasticsearch.vectorindex.model.ClusterReference;
import org.elasticsearch.vectorindex.model.RescoreConfig;
import org.elasticsearch.vectorindex.model.SearchResult;
import org.elasticsearch.vectorindex.model.VectorIndexConfig;
import org.elasticsearch.vectorindex.model.VectorIndexMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Vector global index reader using ES DiskBBQ.
 *
 * <p>Reads an archive file (all Lucene segment files packed) plus an offset table file. On first
 * search, Range Reads .mivf/.cenivf from the archive for centroid lookup. Per query, Range Reads
 * only the matched cluster posting lists using {@link DiskBBQCentroidReader} and {@link
 * DiskBBQClusterSearcher}.
 */
public class ESVectorGlobalIndexReader implements GlobalIndexReader {

    private static final Logger LOG = LoggerFactory.getLogger(ESVectorGlobalIndexReader.class);

    private final GlobalIndexFileReader fileReader;
    private final DataType fieldType;
    private final ESVectorIndexOptions options;

    private final GlobalIndexIOMeta archiveIOMeta;
    private final GlobalIndexIOMeta offsetsIOMeta;

    private volatile VectorIndexMeta indexMeta;
    private volatile Map<String, long[]> fileOffsetsInArchive;
    private volatile long clivfBaseOffset;
    private volatile byte[] clivfData;
    private volatile DiskBBQCentroidReader centroidReader;
    private volatile DiskBBQClusterSearcher clusterSearcher;
    private volatile ExecutorService parallelSearchExecutor;
    private SeekableInputStream cachedArchiveStream;
    private volatile ArchiveFlatVectorReader rawVectorReader;

    private long totalBytesRead;
    private long totalReadCount;
    private long totalReadTimeNanos;

    public ESVectorGlobalIndexReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> ioMetas,
            DataType fieldType,
            ESVectorIndexOptions options) {
        this.fileReader = fileReader;
        this.fieldType = fieldType;
        this.options = options;

        if (ioMetas.size() != 2) {
            throw new IllegalArgumentException(
                    "Expected 2 index files (archive + offsets), got: " + ioMetas.size());
        }

        GlobalIndexIOMeta tmpArchive = null;
        GlobalIndexIOMeta tmpOffsets = null;
        try {
            for (GlobalIndexIOMeta m : ioMetas) {
                ESIndexEntryMeta entryMeta = ESIndexEntryMeta.deserialize(m.metadata());
                if (entryMeta.fileRole() == ESIndexEntryMeta.FILE_ROLE_ARCHIVE) {
                    tmpArchive = m;
                } else if (entryMeta.fileRole() == ESIndexEntryMeta.FILE_ROLE_OFFSETS) {
                    tmpOffsets = m;
                }
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to deserialize ESIndexEntryMeta", e);
        }
        if (tmpArchive == null || tmpOffsets == null) {
            throw new IllegalArgumentException(
                    "Expected archive and offset table files but could not identify both roles");
        }
        this.archiveIOMeta = tmpArchive;
        this.offsetsIOMeta = tmpOffsets;
    }

    @Override
    public Optional<ScoredGlobalIndexResult> visitVectorSearch(VectorSearch vectorSearch) {
        try {
            ensureLoaded();
            return Optional.ofNullable(search(vectorSearch));
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to search ES vector index with fieldName=%s, limit=%d",
                            vectorSearch.fieldName(), vectorSearch.limit()),
                    e);
        }
    }

    private ScoredGlobalIndexResult search(VectorSearch vectorSearch) throws IOException {
        validateSearchVector(vectorSearch.vector());
        float[] queryVector = vectorSearch.vector().clone();
        int limit = vectorSearch.limit();

        int effectiveK = (int) Math.min(limit, indexMeta.vectorCount);
        if (effectiveK <= 0) {
            return null;
        }

        int nprobe = Math.min(options.nprobe(), centroidReader.numCentroids());
        long centroidStart = System.nanoTime();
        List<ClusterReference> clusters = centroidReader.findNearestClusters(queryVector, nprobe);
        long centroidMs = (System.nanoTime() - centroidStart) / 1_000_000;

        RoaringNavigableMap64 includeRowIds = vectorSearch.includeRowIds();
        long[] candidateIds = null;
        if (includeRowIds != null) {
            long cardinality = includeRowIds.getLongCardinality();
            if (cardinality == 0) {
                return null;
            }
            effectiveK = (int) Math.min(effectiveK, cardinality);
            candidateIds = new long[(int) cardinality];
            int idx = 0;
            for (long id : includeRowIds) {
                candidateIds[idx++] = id;
            }
        }

        // Phase-1 candidate pool: oversample when rescore is on.
        boolean rescoreOn = options.rescoreEnabled() && rawVectorReader != null;
        int phase1K =
                rescoreOn
                        ? Math.min(
                                effectiveK * options.rescoreOversample(),
                                (int) indexMeta.vectorCount)
                        : effectiveK;

        long clusterSearchStart = System.nanoTime();
        List<SearchResult> clusterResults;
        if (options.searchBulk() && clivfData != null) {
            clusterResults = searchClustersBulk(clusters, queryVector, phase1K, candidateIds);
        } else if (options.searchParallel() && clusters.size() > 1) {
            clusterResults = searchClustersParallel(clusters, queryVector, phase1K, candidateIds);
        } else {
            clusterResults = searchClustersSerial(clusters, queryVector, phase1K, candidateIds);
        }

        long clusterSearchMs = (System.nanoTime() - clusterSearchStart) / 1_000_000;
        long mergeStart = System.nanoTime();
        SearchResult merged = VectorIndexClusterSearcher.mergeResults(clusterResults, phase1K);
        if (merged == null || merged.count == 0) {
            return null;
        }
        long mergeMs = (System.nanoTime() - mergeStart) / 1_000_000;

        // Phase-2: rescore using raw vectors from the archived .vec file.
        long rescoreMs = 0;
        if (rescoreOn) {
            long rescoreStart = System.nanoTime();
            RescoreConfig rescore = new RescoreConfig(options.rescoreOversample(), rawVectorReader);
            merged =
                    VectorSearchSession.rerankWithRawVectors(
                            merged,
                            queryVector,
                            effectiveK,
                            rescore,
                            options.metric().toLuceneFunction());
            rescoreMs = (System.nanoTime() - rescoreStart) / 1_000_000;
        }

        RoaringNavigableMap64 resultBitmap = new RoaringNavigableMap64();
        HashMap<Long, Float> id2scores = new HashMap<>(merged.count);
        for (int i = 0; i < merged.count; i++) {
            resultBitmap.add(merged.ids[i]);
            id2scores.put(merged.ids[i], merged.scores[i]);
        }
        String mode =
                options.searchBulk() ? "bulk" : (options.searchParallel() ? "parallel" : "serial");
        LOG.info(
                "DiskBBQ search [{}]: centroid={}ms, clusterSearch={}ms (nprobe={}), "
                        + "merge={}ms, rescore={}ms (on={}, phase1K={}), results={}",
                mode,
                centroidMs,
                clusterSearchMs,
                clusters.size(),
                mergeMs,
                rescoreMs,
                rescoreOn,
                phase1K,
                merged.count);
        return new ESScoredGlobalIndexResult(resultBitmap, id2scores);
    }

    private List<SearchResult> searchClustersSerial(
            List<ClusterReference> clusters,
            float[] queryVector,
            int effectiveK,
            long[] candidateIds)
            throws IOException {
        List<SearchResult> results = new ArrayList<>(clusters.size());
        for (ClusterReference cluster : clusters) {
            byte[] clusterData =
                    readRangeFromArchive(
                            clivfBaseOffset + cluster.offset(), (int) cluster.length());
            SearchResult result =
                    clusterSearcher.searchCluster(
                            clusterData, queryVector, effectiveK, candidateIds);
            if (result.count > 0) {
                results.add(result);
            }
        }
        return results;
    }

    private ExecutorService ensureParallelExecutor() {
        if (parallelSearchExecutor == null) {
            synchronized (this) {
                if (parallelSearchExecutor == null) {
                    int poolSize =
                            Math.min(options.nprobe(), Runtime.getRuntime().availableProcessors());
                    parallelSearchExecutor =
                            new ThreadPoolExecutor(
                                    poolSize,
                                    poolSize,
                                    60L,
                                    TimeUnit.SECONDS,
                                    new LinkedBlockingQueue<>());
                    ((ThreadPoolExecutor) parallelSearchExecutor).allowCoreThreadTimeOut(true);
                }
            }
        }
        return parallelSearchExecutor;
    }

    private List<SearchResult> searchClustersParallel(
            List<ClusterReference> clusters,
            float[] queryVector,
            int effectiveK,
            long[] candidateIds)
            throws IOException {
        VectorIndexConfig config = options.toVectorIndexConfig();
        ExecutorService executor = ensureParallelExecutor();
        List<Future<SearchResult>> futures = new ArrayList<>(clusters.size());
        for (ClusterReference cluster : clusters) {
            futures.add(
                    executor.submit(
                            () -> {
                                DiskBBQClusterSearcher localSearcher = new DiskBBQClusterSearcher();
                                localSearcher.init(config);
                                try {
                                    byte[] clusterData =
                                            readRangeFromArchive(
                                                    clivfBaseOffset + cluster.offset(),
                                                    (int) cluster.length());
                                    return localSearcher.searchCluster(
                                            clusterData, queryVector, effectiveK, candidateIds);
                                } finally {
                                    localSearcher.close();
                                }
                            }));
        }

        List<SearchResult> results = new ArrayList<>(clusters.size());
        for (Future<SearchResult> future : futures) {
            try {
                SearchResult result = future.get();
                if (result.count > 0) {
                    results.add(result);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Parallel cluster search interrupted", e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof IOException) {
                    throw (IOException) cause;
                }
                throw new IOException("Parallel cluster search failed", cause);
            }
        }
        return results;
    }

    private List<SearchResult> searchClustersBulk(
            List<ClusterReference> clusters,
            float[] queryVector,
            int effectiveK,
            long[] candidateIds)
            throws IOException {
        List<SearchResult> results = new ArrayList<>(clusters.size());
        for (ClusterReference cluster : clusters) {
            int offset = (int) cluster.offset();
            int length = (int) cluster.length();
            byte[] clusterBytes = new byte[length];
            System.arraycopy(clivfData, offset, clusterBytes, 0, length);
            SearchResult result =
                    clusterSearcher.searchCluster(
                            clusterBytes, queryVector, effectiveK, candidateIds);
            if (result.count > 0) {
                results.add(result);
            }
        }
        return results;
    }

    private void ensureLoaded() throws IOException {
        Slf4jBridgeESLoggerFactory.ensureInitialized();
        if (centroidReader == null) {
            synchronized (this) {
                if (centroidReader == null) {
                    long loadStart = System.nanoTime();
                    ESIndexEntryMeta archiveEntryMeta =
                            ESIndexEntryMeta.deserialize(archiveIOMeta.metadata());
                    indexMeta = archiveEntryMeta.toVectorIndexMeta();

                    loadOffsetTable();

                    long[] mivfRange = findFileByExtension(".mivf");
                    long[] cenivfRange = findFileByExtension(".cenivf");
                    long[] clivfRange = findFileByExtension(".clivf");

                    byte[] mivfBytes = readRangeFromArchive(mivfRange[0], (int) mivfRange[1]);
                    byte[] cenivfBytes = readRangeFromArchive(cenivfRange[0], (int) cenivfRange[1]);
                    clivfBaseOffset = clivfRange[0];

                    if (options.searchBulk()) {
                        clivfData = readRangeFromArchive(clivfRange[0], (int) clivfRange[1]);
                    }

                    DiskBBQCentroidReader cr = new DiskBBQCentroidReader();
                    cr.load(mivfBytes, cenivfBytes, options.toVectorIndexConfig());
                    centroidReader = cr;

                    DiskBBQClusterSearcher cs = new DiskBBQClusterSearcher();
                    cs.init(options.toVectorIndexConfig());
                    clusterSearcher = cs;

                    // Lazy raw vector reader for rescore. If the archive doesn't contain a .vec
                    // file (older indices, or non-flat formats), we silently skip it; rescore
                    // will then no-op even when configured.
                    if (options.rescoreEnabled()) {
                        try {
                            String fieldName =
                                    indexMeta.fieldName != null ? indexMeta.fieldName : "vector";
                            rawVectorReader =
                                    new ArchiveFlatVectorReader(
                                            this::readRangeFromArchive,
                                            fileOffsetsInArchive,
                                            fieldName);
                            LOG.info(
                                    "Rescore enabled: ArchiveFlatVectorReader ready with {} raw vectors for field '{}' (dim={})",
                                    rawVectorReader.numVectors(),
                                    fieldName,
                                    indexMeta.dimension);
                        } catch (IOException | RuntimeException ex) {
                            LOG.warn(
                                    "Rescore configured but raw vector reader init failed: {}. "
                                            + "Rescore will be skipped.",
                                    ex.getMessage());
                        }
                    }

                    LOG.info(
                            "DiskBBQ ensureLoaded: {}ms, bulk={}, rescore={}",
                            (System.nanoTime() - loadStart) / 1_000_000,
                            options.searchBulk(),
                            options.rescoreEnabled());
                }
            }
        }
    }

    private void loadOffsetTable() throws IOException {
        byte[] tableBytes = readAllBytes(offsetsIOMeta);
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(tableBytes));
        int version = dis.readInt();
        if (version != 1) {
            throw new IOException("Unsupported offset table version: " + version);
        }
        int fileCount = dis.readInt();
        Map<String, long[]> offsets = new LinkedHashMap<>(fileCount);
        for (int i = 0; i < fileCount; i++) {
            String name = dis.readUTF();
            long offset = dis.readLong();
            long length = dis.readLong();
            offsets.put(name, new long[] {offset, length});
        }
        fileOffsetsInArchive = offsets;
    }

    private long[] findFileByExtension(String extension) throws IOException {
        for (Map.Entry<String, long[]> entry : fileOffsetsInArchive.entrySet()) {
            if (entry.getKey().endsWith(extension)) {
                return entry.getValue();
            }
        }
        throw new IOException("File with extension " + extension + " not found in offset table");
    }

    private byte[] readRangeFromArchive(long offset, int length) throws IOException {
        byte[] data = new byte[length];
        long start = System.nanoTime();
        SeekableInputStream in = getOrCreateArchiveStream();
        synchronized (in) {
            in.seek(offset);
            int pos = 0;
            while (pos < data.length) {
                int read = in.read(data, pos, data.length - pos);
                if (read == -1) {
                    throw new IOException("Unexpected end of archive at offset " + (offset + pos));
                }
                pos += read;
            }
        }
        totalReadTimeNanos += System.nanoTime() - start;
        totalBytesRead += length;
        totalReadCount++;
        return data;
    }

    private SeekableInputStream getOrCreateArchiveStream() throws IOException {
        if (cachedArchiveStream == null) {
            cachedArchiveStream = fileReader.getInputStream(archiveIOMeta);
        }
        return cachedArchiveStream;
    }

    private byte[] readAllBytes(GlobalIndexIOMeta meta) throws IOException {
        byte[] bytes = new byte[(int) meta.fileSize()];
        try (SeekableInputStream in = fileReader.getInputStream(meta)) {
            int offset = 0;
            while (offset < bytes.length) {
                int read = in.read(bytes, offset, bytes.length - offset);
                if (read == -1) {
                    break;
                }
                offset += read;
            }
        }
        return bytes;
    }

    private void validateSearchVector(Object vector) {
        if (!(vector instanceof float[])) {
            throw new IllegalArgumentException(
                    "Expected float[] vector but got: " + vector.getClass());
        }
        boolean validFieldType = false;
        if (fieldType instanceof VectorType) {
            validFieldType = ((VectorType) fieldType).getElementType() instanceof FloatType;
        } else if (fieldType instanceof ArrayType) {
            validFieldType = ((ArrayType) fieldType).getElementType() instanceof FloatType;
        }
        if (!validFieldType) {
            throw new IllegalArgumentException(
                    "ES requires VectorType<FLOAT> or ArrayType<FLOAT>, but field type is: "
                            + fieldType);
        }
        int queryDim = ((float[]) vector).length;
        if (queryDim != indexMeta.dimension) {
            throw new IllegalArgumentException(
                    String.format(
                            "Query vector dimension mismatch: index expects %d, but got %d",
                            indexMeta.dimension, queryDim));
        }
    }

    public long getTotalBytesRead() {
        return totalBytesRead;
    }

    public long getTotalReadCount() {
        return totalReadCount;
    }

    public long getTotalReadTimeNanos() {
        return totalReadTimeNanos;
    }

    @Override
    public void close() throws IOException {
        if (centroidReader != null) {
            try {
                centroidReader.close();
            } catch (IOException ignored) {
            }
            centroidReader = null;
        }
        if (clusterSearcher != null) {
            clusterSearcher.close();
            clusterSearcher = null;
        }
        if (parallelSearchExecutor != null) {
            parallelSearchExecutor.shutdownNow();
            parallelSearchExecutor = null;
        }
        if (cachedArchiveStream != null) {
            try {
                cachedArchiveStream.close();
            } catch (IOException ignored) {
            }
            cachedArchiveStream = null;
        }
    }

    // =================== unsupported visitors =====================

    @Override
    public Optional<GlobalIndexResult> visitIsNotNull(FieldRef fieldRef) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitIsNull(FieldRef fieldRef) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitStartsWith(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitEndsWith(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitContains(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitLike(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitLessThan(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitNotEqual(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitEqual(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitGreaterThan(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitIn(FieldRef fieldRef, List<Object> literals) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitNotIn(FieldRef fieldRef, List<Object> literals) {
        return Optional.empty();
    }
}
