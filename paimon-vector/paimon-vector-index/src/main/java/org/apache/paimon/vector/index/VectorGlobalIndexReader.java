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

package org.apache.paimon.vector.index;

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.index.ivfpq.VectorIndexInput;
import org.apache.paimon.index.ivfpq.VectorIndexReader;
import org.apache.paimon.index.ivfpq.VectorSearchResult;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.VectorType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Vector global index reader using paimon-vector-index.
 *
 * <p>Each shard has exactly one vector index file. The reader lazily opens the index and performs
 * vector similarity search.
 */
public class VectorGlobalIndexReader implements GlobalIndexReader {

    private final GlobalIndexIOMeta ioMeta;
    private final GlobalIndexFileReader fileReader;
    private final DataType fieldType;
    private final VectorIndexOptions options;
    private final ExecutorService executor;

    private volatile VectorIndexMeta indexMeta;
    private volatile VectorIndexReader vectorReader;
    private SeekableInputStream openStream;

    public VectorGlobalIndexReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> ioMetas,
            DataType fieldType,
            VectorIndexOptions options,
            ExecutorService executor) {
        checkArgument(ioMetas.size() == 1, "Expected exactly one index file per shard");
        this.executor = executor;
        this.fileReader = fileReader;
        this.ioMeta = ioMetas.get(0);
        this.fieldType = fieldType;
        this.options = options;
    }

    @Override
    public CompletableFuture<Optional<ScoredGlobalIndexResult>> visitVectorSearch(
            VectorSearch vectorSearch) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        ensureLoaded();
                        return Optional.ofNullable(search(vectorSearch));
                    } catch (IOException e) {
                        throw new RuntimeException(
                                String.format(
                                        "Failed vector index search: field=%s, limit=%d",
                                        vectorSearch.fieldName(), vectorSearch.limit()),
                                e);
                    }
                },
                executor);
    }

    private ScoredGlobalIndexResult search(VectorSearch vectorSearch) throws IOException {
        validateSearchVector(vectorSearch.vector());
        float[] queryVector = vectorSearch.vector().clone();
        int limit = vectorSearch.limit();
        int nprobe = indexMeta.nprobe();
        VectorMetric metric = indexMeta.metric();

        RoaringNavigableMap64 includeRowIds = vectorSearch.includeRowIds();
        VectorSearchResult result;

        if (includeRowIds != null) {
            long cardinality = includeRowIds.getLongCardinality();
            if (cardinality == 0) {
                return null;
            }
            byte[] filterBytes = includeRowIds.serialize();
            int effectiveK = (int) Math.min(limit, cardinality);
            result =
                    vectorReader.search(
                            queryVector, effectiveK, nprobe, indexMeta.efSearch(), filterBytes);
        } else {
            result = vectorReader.search(queryVector, limit, nprobe, indexMeta.efSearch());
        }

        long[] ids = result.ids();
        float[] distances = result.distances();

        if (ids.length == 0) {
            return null;
        }

        RoaringNavigableMap64 resultBitmap = new RoaringNavigableMap64();
        HashMap<Long, Float> id2scores = new HashMap<>(ids.length);

        for (int i = 0; i < ids.length; i++) {
            long rowId = ids[i];
            if (rowId < 0) {
                continue;
            }
            float score = convertDistanceToScore(distances[i], metric);
            resultBitmap.add(rowId);
            id2scores.put(rowId, score);
        }

        if (resultBitmap.isEmpty()) {
            return null;
        }

        return ScoredGlobalIndexResult.create(
                resultBitmap,
                rowId -> {
                    Float score = id2scores.get(rowId);
                    if (score == null) {
                        throw new IllegalArgumentException(
                                "No score found for rowId: "
                                        + rowId
                                        + ". Only rowIds present in results() are valid.");
                    }
                    return score;
                });
    }

    private static float convertDistanceToScore(float distance, VectorMetric metric) {
        switch (metric) {
            case L2:
                return 1.0f / (1.0f + distance);
            case COSINE:
                return 1.0f - distance;
            case INNER_PRODUCT:
                return distance;
            default:
                throw new IllegalArgumentException("Unknown metric: " + metric);
        }
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
                    "Vector index requires VectorType<FLOAT> or ArrayType<FLOAT>, but field type is: "
                            + fieldType);
        }
        int queryDim = ((float[]) vector).length;
        if (queryDim != indexMeta.dimension()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Query vector dimension mismatch: index expects %d, but got %d",
                            indexMeta.dimension(), queryDim));
        }
    }

    private void ensureLoaded() throws IOException {
        if (vectorReader == null) {
            synchronized (this) {
                if (vectorReader == null) {
                    indexMeta = VectorIndexMeta.deserialize(ioMeta.metadata());
                    SeekableInputStream in = fileReader.getInputStream(ioMeta);
                    try {
                        vectorReader =
                                new VectorIndexReader(new SeekableStreamVectorIndexInput(in));
                        openStream = in;
                    } catch (Exception e) {
                        IOUtils.closeQuietly(in);
                        throw e;
                    }
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        Throwable firstException = null;

        if (vectorReader != null) {
            try {
                vectorReader.close();
            } catch (Throwable t) {
                firstException = t;
            }
            vectorReader = null;
        }

        if (openStream != null) {
            try {
                openStream.close();
            } catch (Throwable t) {
                if (firstException == null) {
                    firstException = t;
                } else {
                    firstException.addSuppressed(t);
                }
            }
            openStream = null;
        }

        if (firstException != null) {
            if (firstException instanceof IOException) {
                throw (IOException) firstException;
            } else if (firstException instanceof RuntimeException) {
                throw (RuntimeException) firstException;
            } else {
                throw new RuntimeException(
                        "Failed to close vector global index reader", firstException);
            }
        }
    }

    private static class SeekableStreamVectorIndexInput implements VectorIndexInput {

        private final SeekableInputStream input;

        private SeekableStreamVectorIndexInput(SeekableInputStream input) {
            this.input = input;
        }

        @Override
        public synchronized void pread(long[] positions, byte[][] buffers) {
            if (positions.length != buffers.length) {
                throw new IllegalArgumentException(
                        "positions length "
                                + positions.length
                                + " != buffers length "
                                + buffers.length);
            }
            try {
                for (int i = 0; i < positions.length; i++) {
                    input.seek(positions[i]);
                    readFully(input, buffers[i]);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to read vector index", e);
            }
        }

        private static void readFully(SeekableInputStream input, byte[] buffer) throws IOException {
            int offset = 0;
            while (offset < buffer.length) {
                int read = input.read(buffer, offset, buffer.length - offset);
                if (read < 0) {
                    throw new IOException("Unexpected end of vector index file");
                }
                offset += read;
            }
        }
    }

    // =================== unsupported =====================

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIsNotNull(FieldRef fieldRef) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIsNull(FieldRef fieldRef) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitStartsWith(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitEndsWith(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitContains(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLike(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLessThan(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterOrEqual(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotEqual(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLessOrEqual(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitEqual(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterThan(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIn(
            FieldRef fieldRef, List<Object> literals) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotIn(
            FieldRef fieldRef, List<Object> literals) {
        return CompletableFuture.completedFuture(Optional.empty());
    }
}
