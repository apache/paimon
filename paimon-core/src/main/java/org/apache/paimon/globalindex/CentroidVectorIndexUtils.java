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

import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.utils.Range;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Validation and coverage of an atomically committed centroid model and its shards. */
public final class CentroidVectorIndexUtils {
    private CentroidVectorIndexUtils() {}

    public static boolean centroidBased(IndexFileMeta file) {
        return VectorIndexUtils.isVectorIndexType(file.indexType())
                && file.globalIndexMeta() != null
                && VectorIndexShardMeta.deserialize(file.globalIndexMeta().indexMeta())
                        .centroidBased();
    }

    public static VectorIndexShardMeta metadata(IndexFileMeta file) {
        return VectorIndexShardMeta.deserialize(file.globalIndexMeta().indexMeta());
    }

    public static IndexFileMeta validateModel(List<IndexFileMeta> files) {
        IndexFileMeta routing = null;
        for (IndexFileMeta file : files) {
            checkArgument(
                    centroidBased(file),
                    "Cannot mix centroid-based and range-based vector shards.");
            if (file.isRoutingModel()) {
                checkArgument(
                        routing == null,
                        "Multiple routing models for the same partition and index.");
                routing = file;
            }
        }
        checkArgument(routing != null, "Missing centroid routing model.");
        VectorIndexShardMeta model = metadata(routing);
        Set<Integer> centroids = new HashSet<>();
        for (IndexFileMeta file : files) {
            checkArgument(
                    model.modelId().equals(metadata(file).modelId()),
                    "Centroid shards belong to different models.");
            if (!file.isRoutingModel()) {
                int centroid = metadata(file).centroid();
                checkArgument(
                        centroid < model.nlist() && centroids.add(centroid),
                        "Duplicate or invalid centroid shard.");
            }
        }
        checkArgument(centroids.size() == model.nlist(), "Incomplete centroid model shard set.");
        model.sourceRowRanges();
        return routing;
    }

    public static List<Range> coverage(IndexFileMeta file) {
        if (!centroidBased(file)) {
            return java.util.Collections.singletonList(file.globalIndexMeta().rowRange());
        }
        return file.isRoutingModel()
                ? metadata(file).sourceRowRanges()
                : java.util.Collections.emptyList();
    }
}
