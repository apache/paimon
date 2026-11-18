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

package org.apache.paimon.globalindex;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.utils.Range;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Validation and coverage of an atomically committed centroid model and its shards. */
public final class CentroidVectorIndexUtils {
    private CentroidVectorIndexUtils() {}

    public static boolean centroidBased(IndexFileMeta file) {
        return file.globalIndexMeta() != null
                && VectorIndexShardMeta.isWrapped(file.globalIndexMeta().indexMeta())
                && CoreOptions.isVectorIndexType(file.indexType());
    }

    public static boolean isRoutingModel(IndexFileMeta file) {
        return centroidBased(file) && metadata(file).routingModel();
    }

    public static VectorIndexShardMeta metadata(IndexFileMeta file) {
        return VectorIndexShardMeta.deserialize(file.globalIndexMeta().indexMeta());
    }

    public static IndexFileMeta validateModel(List<IndexFileMeta> files) {
        IndexFileMeta routing = null;
        String modelId = null;
        int nlist = 0;
        Set<Integer> centroids = new HashSet<>();
        for (IndexFileMeta file : files) {
            checkArgument(
                    centroidBased(file),
                    "Cannot mix centroid-based and range-based vector shards.");
            VectorIndexShardMeta meta = metadata(file);
            String id = meta.modelId();
            checkArgument(
                    modelId == null || modelId.equals(id),
                    "Centroid shards belong to different models.");
            modelId = id;
            if (meta.routingModel()) {
                checkArgument(
                        routing == null,
                        "Multiple routing models for the same partition and index.");
                routing = file;
                nlist = meta.nlist();
                meta.sourceRowRanges();
            } else {
                checkArgument(centroids.add(meta.centroid()), "Duplicate centroid shard.");
            }
        }
        checkArgument(routing != null, "Missing centroid routing model.");
        for (int centroid : centroids) {
            checkArgument(centroid < nlist, "Invalid centroid shard.");
        }
        checkArgument(centroids.size() == nlist, "Incomplete centroid model shard set.");
        return routing;
    }

    public static List<Range> coverage(IndexFileMeta file) {
        if (!centroidBased(file)) {
            return Collections.singletonList(file.globalIndexMeta().rowRange());
        }
        VectorIndexShardMeta meta = metadata(file);
        return meta.routingModel() ? meta.sourceRowRanges() : Collections.emptyList();
    }
}
