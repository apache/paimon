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

import org.apache.paimon.predicate.MultiVectorSearch;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Fusion utilities for multi-vector search results. */
public class MultiVectorSearchFusion {

    private static final float RRF_K = 60.0f;

    private MultiVectorSearchFusion() {}

    public static ScoredGlobalIndexResult fuse(
            String fusion, List<ScoredGlobalIndexResult> results, float[] weights, int limit) {
        if (MultiVectorSearch.FUSION_WEIGHTED_SCORE.equals(fusion)) {
            return weightedScore(results, weights, limit);
        }
        return rrf(results, weights, limit);
    }

    public static ScoredGlobalIndexResult rrf(
            List<ScoredGlobalIndexResult> results, float[] weights, int limit) {
        Map<Long, Float> scores = new HashMap<>();
        for (int i = 0; i < results.size(); i++) {
            ScoredGlobalIndexResult result = results.get(i);
            float weight = weightAt(weights, i);
            List<Long> ranked = rankedRowIds(result);
            for (int rank = 0; rank < ranked.size(); rank++) {
                Long rowId = ranked.get(rank);
                float contribution = weight / (RRF_K + rank + 1.0f);
                Float oldScore = scores.get(rowId);
                scores.put(rowId, oldScore == null ? contribution : oldScore + contribution);
            }
        }
        return topK(scores, limit);
    }

    public static ScoredGlobalIndexResult weightedScore(
            List<ScoredGlobalIndexResult> results, float[] weights, int limit) {
        Map<Long, Float> scores = new HashMap<>();
        for (int i = 0; i < results.size(); i++) {
            ScoredGlobalIndexResult result = results.get(i);
            float weight = weightAt(weights, i);
            ScoreGetter scoreGetter = result.scoreGetter();
            for (long rowId : result.results()) {
                float contribution = weight * scoreGetter.score(rowId);
                Float oldScore = scores.get(rowId);
                scores.put(rowId, oldScore == null ? contribution : oldScore + contribution);
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
        Collections.sort(
                rowIds,
                new Comparator<Long>() {
                    @Override
                    public int compare(Long left, Long right) {
                        return Float.compare(scoreGetter.score(right), scoreGetter.score(left));
                    }
                });
        return rowIds;
    }

    private static ScoredGlobalIndexResult topK(Map<Long, Float> scores, int limit) {
        if (scores.isEmpty()) {
            return ScoredGlobalIndexResult.createEmpty();
        }
        List<Map.Entry<Long, Float>> ranked = new ArrayList<>(scores.entrySet());
        Collections.sort(
                ranked,
                new Comparator<Map.Entry<Long, Float>>() {
                    @Override
                    public int compare(Map.Entry<Long, Float> left, Map.Entry<Long, Float> right) {
                        int scoreCompare = Float.compare(right.getValue(), left.getValue());
                        if (scoreCompare != 0) {
                            return scoreCompare;
                        }
                        return Long.compare(left.getKey(), right.getKey());
                    }
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
}
