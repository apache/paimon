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

import org.apache.paimon.utils.RoaringNavigableMap64;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Ranker utilities for hybrid search results. */
public class HybridSearchRanker {

    public static final String RRF_RANKER = "rrf";
    public static final String WEIGHTED_SCORE_RANKER = "weighted_score";

    private static final float RRF_K = 60.0f;

    private HybridSearchRanker() {}

    public static ScoredGlobalIndexResult rank(
            String ranker, List<ScoredGlobalIndexResult> results, float[] weights, int limit) {
        List<WeightedResult> weightedResults = new ArrayList<>(results.size());
        for (int i = 0; i < results.size(); i++) {
            weightedResults.add(new WeightedResult(results.get(i), weightAt(weights, i)));
        }
        return rank(ranker, weightedResults, limit);
    }

    public static ScoredGlobalIndexResult rank(
            String ranker, List<WeightedResult> results, int limit) {
        if (WEIGHTED_SCORE_RANKER.equals(normalizeRanker(ranker))) {
            return weightedScore(results, limit);
        }
        return rrf(results, limit);
    }

    public static String normalizeRanker(String ranker) {
        if (ranker == null || ranker.trim().isEmpty()) {
            return RRF_RANKER;
        }
        String normalized = ranker.trim().toLowerCase();
        if (!RRF_RANKER.equals(normalized) && !WEIGHTED_SCORE_RANKER.equals(normalized)) {
            throw new IllegalArgumentException("Unsupported hybrid ranker: " + ranker);
        }
        return normalized;
    }

    public static ScoredGlobalIndexResult rrf(
            List<ScoredGlobalIndexResult> results, float[] weights, int limit) {
        List<WeightedResult> weightedResults = new ArrayList<>(results.size());
        for (int i = 0; i < results.size(); i++) {
            weightedResults.add(new WeightedResult(results.get(i), weightAt(weights, i)));
        }
        return rrf(weightedResults, limit);
    }

    public static ScoredGlobalIndexResult rrf(List<WeightedResult> results, int limit) {
        Map<Long, Float> scores = new HashMap<>();
        for (WeightedResult weightedResult : results) {
            ScoredGlobalIndexResult result = weightedResult.result();
            float weight = weightedResult.weight();
            List<Long> ranked = rankedRowIds(result);
            for (int rank = 0; rank < ranked.size(); rank++) {
                Long rowId = ranked.get(rank);
                float contribution = weight / (RRF_K + rank + 1.0f);
                scores.compute(
                        rowId,
                        (k, oldScore) -> oldScore == null ? contribution : oldScore + contribution);
            }
        }
        return topK(scores, limit);
    }

    public static ScoredGlobalIndexResult weightedScore(
            List<ScoredGlobalIndexResult> results, float[] weights, int limit) {
        List<WeightedResult> weightedResults = new ArrayList<>(results.size());
        for (int i = 0; i < results.size(); i++) {
            weightedResults.add(new WeightedResult(results.get(i), weightAt(weights, i)));
        }
        return weightedScore(weightedResults, limit);
    }

    public static ScoredGlobalIndexResult weightedScore(List<WeightedResult> results, int limit) {
        Map<Long, Float> scores = new HashMap<>();
        for (WeightedResult weightedResult : results) {
            ScoredGlobalIndexResult result = weightedResult.result();
            float weight = weightedResult.weight();
            ScoreGetter scoreGetter = result.scoreGetter();
            for (long rowId : result.results()) {
                float contribution = weight * scoreGetter.score(rowId);
                scores.compute(
                        rowId,
                        (k, oldScore) -> oldScore == null ? contribution : oldScore + contribution);
            }
        }
        return topK(scores, limit);
    }

    private static List<Long> rankedRowIds(ScoredGlobalIndexResult result) {
        List<Long> rowIds = new ArrayList<>();
        for (long rowId : result.results()) {
            rowIds.add(rowId);
        }
        final ScoreGetter scoreGetter = result.scoreGetter();
        rowIds.sort(
                (left, right) -> {
                    int scoreCompare =
                            Float.compare(scoreGetter.score(right), scoreGetter.score(left));
                    if (scoreCompare != 0) {
                        return scoreCompare;
                    }
                    return Long.compare(left, right);
                });
        return rowIds;
    }

    private static ScoredGlobalIndexResult topK(Map<Long, Float> scores, int limit) {
        if (scores.isEmpty()) {
            return ScoredGlobalIndexResult.createEmpty();
        }
        List<Map.Entry<Long, Float>> ranked = new ArrayList<>(scores.entrySet());
        ranked.sort(
                (left, right) -> {
                    int scoreCompare = Float.compare(right.getValue(), left.getValue());
                    if (scoreCompare != 0) {
                        return scoreCompare;
                    }
                    return Long.compare(left.getKey(), right.getKey());
                });

        int size = Math.min(limit, ranked.size());
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        Map<Long, Float> topScores = new HashMap<>();
        for (int i = 0; i < size; i++) {
            Map.Entry<Long, Float> entry = ranked.get(i);
            bitmap.add(entry.getKey());
            topScores.put(entry.getKey(), entry.getValue());
        }
        return ScoredGlobalIndexResult.create(bitmap, topScores::get);
    }

    private static float weightAt(float[] weights, int index) {
        if (weights == null || index >= weights.length) {
            return 1.0f;
        }
        return weights[index];
    }

    /** Weighted result from one search route. */
    public static class WeightedResult implements Serializable {

        private static final long serialVersionUID = 1L;

        private final ScoredGlobalIndexResult result;
        private final float weight;

        public WeightedResult(ScoredGlobalIndexResult result, float weight) {
            this.result = result;
            this.weight = weight;
        }

        public ScoredGlobalIndexResult result() {
            return result;
        }

        public float weight() {
            return weight;
        }
    }
}
