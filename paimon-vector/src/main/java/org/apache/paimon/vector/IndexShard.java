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

package org.apache.paimon.vector;

import io.github.jbellis.jvector.graph.OnHeapGraphIndex;

import java.util.List;

/** A shard of the vector index containing row IDs, vectors, metadata, and the graph index. */
public class IndexShard {
    final long[] rowIds;
    final List<float[]> vectors;
    final VectorIndexMetadata metadata;
    final long rowRangeStart;
    final OnHeapGraphIndex graphIndex;

    IndexShard(
            long[] rowIds,
            List<float[]> vectors,
            VectorIndexMetadata metadata,
            long rowRangeStart,
            OnHeapGraphIndex graphIndex) {
        this.rowIds = rowIds;
        this.vectors = vectors;
        this.metadata = metadata;
        this.rowRangeStart = rowRangeStart;
        this.graphIndex = graphIndex;
    }
}
