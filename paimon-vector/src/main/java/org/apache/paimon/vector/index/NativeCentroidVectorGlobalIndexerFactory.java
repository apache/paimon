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

import org.apache.paimon.globalindex.CentroidVectorIndexFactory;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.Range;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Registered centroid index types share the router and select a local native algorithm. */
public abstract class NativeCentroidVectorGlobalIndexerFactory
        extends NativeVectorGlobalIndexerFactory implements CentroidVectorIndexFactory {
    private final String algorithm;

    NativeCentroidVectorGlobalIndexerFactory(String algorithm) {
        this.algorithm = algorithm;
    }

    @Override
    public String identifier() {
        return algorithm + "-centroid";
    }

    @Override
    String nativeAlgorithm() {
        return algorithm;
    }

    @Override
    NativeVectorGlobalIndexer createIndexer(
            DataType fieldType, Map<String, String> options, String algorithm, double ratio) {
        return new NativeCentroidVectorGlobalIndexer(fieldType, options, algorithm, ratio);
    }

    @Override
    public List<Range> sourceRowRanges(byte[] indexMeta) {
        VectorIndexShardMeta meta = VectorIndexShardMeta.deserialize(indexMeta);
        return meta.routingModel() ? meta.sourceRowRanges() : Collections.emptyList();
    }

    @Override
    public List<Range> validateGeneration(List<byte[]> indexMetas) {
        List<Range> coverage = null;
        String modelId = null;
        int nlist = 0;
        Set<Integer> centroids = new HashSet<>();
        for (byte[] bytes : indexMetas) {
            VectorIndexShardMeta meta = VectorIndexShardMeta.deserialize(bytes);
            String id = meta.modelId();
            checkArgument(
                    modelId == null || modelId.equals(id),
                    "Centroid shards belong to different models.");
            modelId = id;
            if (meta.routingModel()) {
                checkArgument(
                        coverage == null,
                        "Multiple routing models for the same partition and index.");
                coverage = meta.sourceRowRanges();
                nlist = meta.nlist();
            } else {
                checkArgument(centroids.add(meta.centroid()), "Duplicate centroid shard.");
            }
        }
        checkArgument(coverage != null, "Missing centroid routing model.");
        for (int centroid : centroids) {
            checkArgument(centroid < nlist, "Invalid centroid shard.");
        }
        checkArgument(centroids.size() == nlist, "Incomplete centroid model shard set.");
        return coverage;
    }

    /** Centroid routing over IVF-FLAT shards. */
    public static final class IvfFlat extends NativeCentroidVectorGlobalIndexerFactory {
        public IvfFlat() {
            super(IvfFlatVectorGlobalIndexerFactory.IDENTIFIER);
        }
    }

    /** Centroid routing over IVF-PQ shards. */
    public static final class IvfPq extends NativeCentroidVectorGlobalIndexerFactory {
        public IvfPq() {
            super(IvfPqAlgorithmVectorGlobalIndexerFactory.IDENTIFIER);
        }
    }

    /** Centroid routing over IVF-SQ shards. */
    public static final class IvfSq extends NativeCentroidVectorGlobalIndexerFactory {
        public IvfSq() {
            super(IvfSqVectorGlobalIndexerFactory.IDENTIFIER);
        }
    }

    /** Centroid routing over IVF-RQ shards. */
    public static final class IvfRq extends NativeCentroidVectorGlobalIndexerFactory {
        public IvfRq() {
            super(IvfRqVectorGlobalIndexerFactory.IDENTIFIER);
        }
    }

    /** Centroid routing over DiskANN shards. */
    public static final class DiskAnn extends NativeCentroidVectorGlobalIndexerFactory {
        public DiskAnn() {
            super(DiskAnnVectorGlobalIndexerFactory.IDENTIFIER);
        }
    }
}
