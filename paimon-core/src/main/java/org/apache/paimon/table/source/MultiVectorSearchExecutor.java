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

package org.apache.paimon.table.source;

import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.MultiVectorSearchFusion;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.MultiVectorSearch;
import org.apache.paimon.predicate.MultiVectorSearchRoute;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.InnerTable;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/** Executes multi-vector search through existing per-column vector search builders. */
public class MultiVectorSearchExecutor {

    private final InnerTable table;
    private final MultiVectorSearch multiVectorSearch;
    private final @Nullable PartitionPredicate partitionFilter;
    private final @Nullable Predicate filter;

    public MultiVectorSearchExecutor(
            InnerTable table,
            MultiVectorSearch multiVectorSearch,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable Predicate filter) {
        this.table = table;
        this.multiVectorSearch = multiVectorSearch;
        this.partitionFilter = partitionFilter;
        this.filter = filter;
    }

    public ScoredGlobalIndexResult execute() {
        List<ScoredGlobalIndexResult> results = new ArrayList<>();
        List<Float> weights = new ArrayList<>();
        for (MultiVectorSearchRoute route : multiVectorSearch.routes()) {
            GlobalIndexResult result = executeRoute(route);
            if (result instanceof ScoredGlobalIndexResult) {
                ScoredGlobalIndexResult scored = (ScoredGlobalIndexResult) result;
                if (!scored.results().isEmpty()) {
                    results.add(scored);
                    weights.add(route.weight());
                }
            } else if (!result.results().isEmpty()) {
                throw new UnsupportedOperationException(
                        "Multi-vector search requires scored vector index results, but got: "
                                + result.getClass().getName());
            }
        }
        return MultiVectorSearchFusion.fuse(
                multiVectorSearch.fusion(),
                results,
                toFloatArray(weights),
                multiVectorSearch.limit());
    }

    private GlobalIndexResult executeRoute(MultiVectorSearchRoute route) {
        VectorSearchBuilder vectorSearchBuilder =
                table.newVectorSearchBuilder()
                        .withVector(route.vector())
                        .withVectorColumn(route.fieldName())
                        .withLimit(route.limit())
                        .withOptions(route.options());
        if (partitionFilter != null) {
            vectorSearchBuilder.withPartitionFilter(partitionFilter);
        }
        if (filter != null) {
            vectorSearchBuilder.withFilter(filter);
        }
        return vectorSearchBuilder.executeLocal();
    }

    private static float[] toFloatArray(List<Float> values) {
        float[] array = new float[values.size()];
        for (int i = 0; i < values.size(); i++) {
            array[i] = values.get(i);
        }
        return array;
    }
}
