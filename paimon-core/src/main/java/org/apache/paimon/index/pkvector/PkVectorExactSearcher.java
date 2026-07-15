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
        checkArgument(query.length == reader.dimension(), "Query vector dimension does not match.");
        checkArgument(limit > 0, "Vector search limit must be positive.");
        checkArgument(
                VectorSearchMetric.isSupported(metric),
                "Unsupported vector distance metric: %s.",
                metric);
        metric = VectorSearchMetric.normalize(metric);
        for (int i = 0; i < query.length; i++) {
            checkArgument(
                    Float.isFinite(query[i]),
                    "Query vector element at position %s must be finite.",
                    i);
        }

        Comparator<PkVectorSearchResult> bestFirst =
                Comparator.comparingDouble(PkVectorSearchResult::distance)
                        .thenComparingLong(PkVectorSearchResult::rowPosition);
        PriorityQueue<PkVectorSearchResult> nearest =
                new PriorityQueue<>(limit, bestFirst.reversed());
        float[] vector = new float[reader.dimension()];
        for (long position = 0; position < reader.rowCount(); position++) {
            if (!reader.readNextVector(vector) || excludedPosition.test(position)) {
                continue;
            }
            PkVectorSearchResult candidate =
                    new PkVectorSearchResult(
                            dataFileName,
                            position,
                            VectorSearchMetric.computeDistance(query, vector, metric));
            if (nearest.size() < limit) {
                nearest.add(candidate);
            } else if (bestFirst.compare(candidate, nearest.peek()) < 0) {
                nearest.poll();
                nearest.add(candidate);
            }
        }
        List<PkVectorSearchResult> result = new ArrayList<>(nearest);
        Collections.sort(result, bestFirst);
        return Collections.unmodifiableList(result);
    }
}
