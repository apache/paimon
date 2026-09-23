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

import java.util.Locale;

/** Common numerical semantics for vector search metrics. */
public final class VectorSearchMetric {

    private VectorSearchMetric() {}

    public static String normalize(String metric) {
        return metric.toLowerCase(Locale.ROOT).replace('-', '_');
    }

    public static boolean isSupported(String metric) {
        if (metric == null) {
            return false;
        }
        String normalized = normalize(metric);
        return "l2".equals(normalized)
                || "cosine".equals(normalized)
                || "inner_product".equals(normalized);
    }

    /** Returns a higher-is-better score for exact vector search. */
    public static float computeScore(float[] query, float[] stored, String metric) {
        if ("l2".equals(metric)) {
            return 1.0f / (1.0f + squaredL2(query, stored));
        } else if ("cosine".equals(metric)) {
            return cosineSimilarity(query, stored);
        } else if ("inner_product".equals(metric)) {
            return innerProduct(query, stored);
        }
        throw unknownMetric(metric);
    }

    /** Returns a lower-is-better distance for exact vector search. */
    public static float computeDistance(float[] query, float[] stored, String metric) {
        if ("l2".equals(metric)) {
            return squaredL2(query, stored);
        } else if ("cosine".equals(metric)) {
            return cosineDistance(cosineSimilarity(query, stored));
        } else if ("inner_product".equals(metric)) {
            return -innerProduct(query, stored);
        }
        throw unknownMetric(metric);
    }

    /** Converts a standardized higher-is-better index score to a lower-is-better distance. */
    public static float scoreToDistance(float score, String metric) {
        if ("l2".equals(metric)) {
            return 1.0f / score - 1.0f;
        } else if ("cosine".equals(metric)) {
            return cosineDistance(score);
        } else if ("inner_product".equals(metric)) {
            return -score;
        }
        throw unknownMetric(metric);
    }

    private static float squaredL2(float[] query, float[] stored) {
        float squared = 0;
        for (int i = 0; i < query.length; i++) {
            float delta = query[i] - stored[i];
            squared += delta * delta;
        }
        return squared;
    }

    private static float cosineSimilarity(float[] query, float[] stored) {
        float dot = 0;
        float queryNorm = 0;
        float storedNorm = 0;
        for (int i = 0; i < query.length; i++) {
            dot += query[i] * stored[i];
            queryNorm += query[i] * query[i];
            storedNorm += stored[i] * stored[i];
        }
        float denominator = (float) (Math.sqrt(queryNorm) * Math.sqrt(storedNorm));
        return denominator == 0 ? 0 : dot / denominator;
    }

    private static float innerProduct(float[] query, float[] stored) {
        float dot = 0;
        for (int i = 0; i < query.length; i++) {
            dot += query[i] * stored[i];
        }
        return dot;
    }

    private static float cosineDistance(float similarity) {
        return 1.0f - Math.max(-1.0f, Math.min(1.0f, similarity));
    }

    private static IllegalArgumentException unknownMetric(String metric) {
        return new IllegalArgumentException("Unknown vector search metric: " + metric);
    }
}
