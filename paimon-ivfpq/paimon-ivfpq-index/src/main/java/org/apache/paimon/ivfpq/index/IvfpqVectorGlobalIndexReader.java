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

package org.apache.paimon.ivfpq.index;

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.index.ivfpq.IVFPQReader;
import org.apache.paimon.index.ivfpq.IVFPQResult;
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
 * Vector global index reader using IVF-PQ.
 *
 * <p>Each shard has exactly one IVF-PQ index file. The reader lazily opens the index and performs
 * vector similarity search. The native Rust JNI layer calls {@code seek(long)} and {@code
 * read(byte[], int, int)} directly on the {@link SeekableInputStream}, so no adapter is needed.
 */
public class IvfpqVectorGlobalIndexReader implements GlobalIndexReader {

    private final GlobalIndexIOMeta ioMeta;
    private final GlobalIndexFileReader fileReader;
    private final DataType fieldType;
    private final IvfpqVectorIndexOptions options;
    private final ExecutorService executor;

    private volatile IvfpqIndexMeta indexMeta;
    private volatile IVFPQReader ivfpqReader;
    private SeekableInputStream openStream;

    public IvfpqVectorGlobalIndexReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> ioMetas,
            DataType fieldType,
            IvfpqVectorIndexOptions options,
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
                                        "Failed IVF-PQ search: field=%s, limit=%d",
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
        IvfpqVectorMetric metric = indexMeta.metric();

        RoaringNavigableMap64 includeRowIds = vectorSearch.includeRowIds();
        IVFPQResult result;

        if (includeRowIds != null) {
            long cardinality = includeRowIds.getLongCardinality();
            if (cardinality == 0) {
                return null;
            }
            byte[] filterBytes = includeRowIds.serialize();
            int effectiveK = (int) Math.min(limit, cardinality);
            result = ivfpqReader.search(queryVector, effectiveK, nprobe, filterBytes);
        } else {
            result = ivfpqReader.search(queryVector, limit, nprobe);
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

    private static float convertDistanceToScore(float distance, IvfpqVectorMetric metric) {
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
                    "IVF-PQ requires VectorType<FLOAT> or ArrayType<FLOAT>, but field type is: "
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
        if (ivfpqReader == null) {
            synchronized (this) {
                if (ivfpqReader == null) {
                    indexMeta = IvfpqIndexMeta.deserialize(ioMeta.metadata());
                    SeekableInputStream in = fileReader.getInputStream(ioMeta);
                    try {
                        ivfpqReader = new IVFPQReader(in);
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

        if (ivfpqReader != null) {
            try {
                ivfpqReader.close();
            } catch (Throwable t) {
                firstException = t;
            }
            ivfpqReader = null;
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
                        "Failed to close IVF-PQ vector global index reader", firstException);
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
