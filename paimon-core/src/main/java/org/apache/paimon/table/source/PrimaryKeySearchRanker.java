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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Deterministic reciprocal-rank fusion for physical primary-key search positions. */
public final class PrimaryKeySearchRanker {

    public static final int DEFAULT_RRF_K = 60;

    private static final Comparator<PrimaryKeySearchPosition> LOCAL_BEST_FIRST =
            (left, right) -> {
                int score = Float.compare(right.score(), left.score());
                return score != 0 ? score : left.compareTo(right);
            };

    private PrimaryKeySearchRanker() {}

    public static List<PrimaryKeySearchPosition> rrf(
            List<List<PrimaryKeySearchPosition>> rankings, int limit) {
        List<Ranking> weighted = new ArrayList<>(rankings.size());
        for (List<PrimaryKeySearchPosition> ranking : rankings) {
            weighted.add(new Ranking(ranking, 1D));
        }
        return weightedRrf(weighted, limit);
    }

    public static List<PrimaryKeySearchPosition> weightedRrf(List<Ranking> rankings, int limit) {
        checkArgument(limit > 0, "RRF result limit must be positive: %s.", limit);
        Map<PrimaryKeySearchPosition, Double> fusedScores = new HashMap<>();
        for (Ranking ranking : rankings) {
            addRanking(fusedScores, ranking);
        }

        return topK(fusedScores, limit);
    }

    /** Fuses heterogeneous route scores after independently normalizing each route to [0, 1]. */
    public static List<PrimaryKeySearchPosition> weightedScore(List<Ranking> rankings, int limit) {
        checkArgument(limit > 0, "Weighted-score result limit must be positive: %s.", limit);
        Map<PrimaryKeySearchPosition, Double> fusedScores = new HashMap<>();
        for (Ranking ranking : rankings) {
            Set<PrimaryKeySearchPosition> unique = new HashSet<>();
            float min = Float.POSITIVE_INFINITY;
            float max = Float.NEGATIVE_INFINITY;
            for (PrimaryKeySearchPosition position : ranking.positions) {
                checkArgument(
                        unique.add(position),
                        "One weighted-score ranking contains duplicate physical position %s.",
                        position);
                min = Math.min(min, position.score());
                max = Math.max(max, position.score());
            }
            float range = max - min;
            for (PrimaryKeySearchPosition position : ranking.positions) {
                double normalized = range > 0F ? (position.score() - min) / range : 1D;
                fusedScores.merge(position, ranking.weight * normalized, Double::sum);
            }
        }
        return topK(fusedScores, limit);
    }

    /** Fuses routes using weighted reciprocal rank without the RRF smoothing constant. */
    public static List<PrimaryKeySearchPosition> weightedMrr(List<Ranking> rankings, int limit) {
        checkArgument(limit > 0, "MRR result limit must be positive: %s.", limit);
        Map<PrimaryKeySearchPosition, Double> fusedScores = new HashMap<>();
        for (Ranking ranking : rankings) {
            List<PrimaryKeySearchPosition> sorted = new ArrayList<>(ranking.positions);
            sorted.sort(LOCAL_BEST_FIRST);
            Set<PrimaryKeySearchPosition> unique = new HashSet<>();
            for (int i = 0; i < sorted.size(); i++) {
                PrimaryKeySearchPosition position = sorted.get(i);
                checkArgument(
                        unique.add(position),
                        "One MRR ranking contains duplicate physical position %s.",
                        position);
                fusedScores.merge(position, ranking.weight / (i + 1D), Double::sum);
            }
        }
        return topK(fusedScores, limit);
    }

    private static List<PrimaryKeySearchPosition> topK(
            Map<PrimaryKeySearchPosition, Double> fusedScores, int limit) {

        Comparator<PrimaryKeySearchPosition> bestFirst =
                (left, right) -> {
                    int score = Float.compare(right.score(), left.score());
                    return score != 0 ? score : left.compareTo(right);
                };
        PriorityQueue<PrimaryKeySearchPosition> topK =
                new PriorityQueue<>(limit, bestFirst.reversed());
        for (Map.Entry<PrimaryKeySearchPosition, Double> entry : fusedScores.entrySet()) {
            PrimaryKeySearchPosition position =
                    entry.getKey().withScore(entry.getValue().floatValue());
            if (topK.size() < limit) {
                topK.add(position);
            } else if (bestFirst.compare(position, topK.peek()) < 0) {
                topK.poll();
                topK.add(position);
            }
        }
        List<PrimaryKeySearchPosition> result = new ArrayList<>(topK);
        result.sort(bestFirst);
        return Collections.unmodifiableList(result);
    }

    private static void addRanking(
            Map<PrimaryKeySearchPosition, Double> fusedScores, Ranking ranking) {
        List<PrimaryKeySearchPosition> sorted = new ArrayList<>(ranking.positions);
        sorted.sort(LOCAL_BEST_FIRST);
        Set<PrimaryKeySearchPosition> unique = new HashSet<>();
        int rank = 0;
        float previousScore = Float.NaN;
        for (int i = 0; i < sorted.size(); i++) {
            PrimaryKeySearchPosition position = sorted.get(i);
            checkArgument(
                    unique.add(position),
                    "One RRF ranking contains duplicate physical position %s.",
                    position);
            if (i == 0 || Float.compare(position.score(), previousScore) != 0) {
                rank = i + 1;
                previousScore = position.score();
            }
            double contribution = ranking.weight / (DEFAULT_RRF_K + rank);
            fusedScores.merge(position, contribution, Double::sum);
        }
    }

    /** One locally scored ranking and its optional route weight. */
    public static class Ranking implements Serializable {

        private static final long serialVersionUID = 1L;

        private final List<PrimaryKeySearchPosition> positions;
        private final double weight;

        public Ranking(List<PrimaryKeySearchPosition> positions, double weight) {
            checkArgument(
                    weight > 0D && !Double.isNaN(weight) && !Double.isInfinite(weight),
                    "Search route weight must be finite and positive: %s.",
                    weight);
            this.positions = Collections.unmodifiableList(new ArrayList<>(positions));
            this.weight = weight;
        }

        public List<PrimaryKeySearchPosition> positions() {
            return positions;
        }

        public double weight() {
            return weight;
        }
    }
}
