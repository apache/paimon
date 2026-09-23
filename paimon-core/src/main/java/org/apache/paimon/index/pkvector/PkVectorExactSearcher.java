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

package org.apache.paimon.index.pkvector;

import org.apache.paimon.globalindex.VectorSearchMetric;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.LongPredicate;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Exact Top-K over one sequential physical-row vector source. */
public final class PkVectorExactSearcher {

    private PkVectorExactSearcher() {}

    public static List<PkVectorSearchResult> search(
            String dataFileName,
            PkVectorReader reader,
            float[] query,
            String metric,
            int limit,
            LongPredicate excludedPosition)
            throws IOException {
        return searchBatch(
                        dataFileName,
                        reader,
                        new float[][] {query},
                        metric,
                        limit,
                        excludedPosition)
                .get(0);
    }

    public static List<List<PkVectorSearchResult>> searchBatch(
            String dataFileName,
            PkVectorReader reader,
            float[][] queries,
            String metric,
            int limit,
            LongPredicate excludedPosition)
            throws IOException {
        checkArgument(queries != null && queries.length > 0, "Query vectors cannot be empty.");
        checkArgument(limit > 0, "Vector search limit must be positive.");
        checkArgument(
                VectorSearchMetric.isSupported(metric),
                "Unsupported vector distance metric: %s.",
                metric);
        metric = VectorSearchMetric.normalize(metric);

        Comparator<PkVectorSearchResult> bestFirst =
                Comparator.comparingDouble(PkVectorSearchResult::distance)
                        .thenComparingLong(PkVectorSearchResult::rowPosition);
        List<PriorityQueue<PkVectorSearchResult>> nearest = new ArrayList<>(queries.length);
        for (float[] query : queries) {
            validateQuery(query, reader.dimension());
            nearest.add(new PriorityQueue<>(limit, bestFirst.reversed()));
        }

        float[] vector = new float[reader.dimension()];
        for (long position = 0; position < reader.rowCount(); position++) {
            if (!reader.readNextVector(vector) || excludedPosition.test(position)) {
                continue;
            }
            for (int i = 0; i < queries.length; i++) {
                PkVectorSearchResult candidate =
                        new PkVectorSearchResult(
                                dataFileName,
                                position,
                                VectorSearchMetric.computeDistance(queries[i], vector, metric));
                PriorityQueue<PkVectorSearchResult> queryNearest = nearest.get(i);
                if (queryNearest.size() < limit) {
                    queryNearest.add(candidate);
                } else if (bestFirst.compare(candidate, queryNearest.peek()) < 0) {
                    queryNearest.poll();
                    queryNearest.add(candidate);
                }
            }
        }

        List<List<PkVectorSearchResult>> results = new ArrayList<>(queries.length);
        for (PriorityQueue<PkVectorSearchResult> queryNearest : nearest) {
            List<PkVectorSearchResult> result = new ArrayList<>(queryNearest);
            Collections.sort(result, bestFirst);
            results.add(Collections.unmodifiableList(result));
        }
        return Collections.unmodifiableList(results);
    }

    private static void validateQuery(float[] query, int dimension) {
        checkArgument(query != null, "Query vector cannot be null.");
        checkArgument(query.length == dimension, "Query vector dimension does not match.");
        for (int i = 0; i < query.length; i++) {
            checkArgument(
                    Float.isFinite(query[i]),
                    "Query vector element at position %s must be finite.",
                    i);
        }
    }
}
