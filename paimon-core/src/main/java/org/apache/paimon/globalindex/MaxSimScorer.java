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

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.utils.IntArrayList;

import javax.annotation.Nullable;

import java.util.Arrays;

/** Scores complete embedding lists in one raw-data scan. */
public final class MaxSimScorer {

    private final InternalArray.ElementGetter childGetter;
    private final float[][] queries;
    private final double[] queryNorms;
    private final double[] maxima;

    public MaxSimScorer(ArrayType column, float[][] queries) {
        VectorIndexUtils.validateQuery(column, queries);
        this.childGetter = InternalArray.createElementGetter(column.getElementType());
        this.queries = queries;
        this.queryNorms = new double[queries.length];
        this.maxima = new double[queries.length];
        for (int i = 0; i < queries.length; i++) {
            queryNorms[i] = norm(queries[i]);
        }
    }

    /** Returns negative infinity when the row contains no embeddings. */
    public float score(InternalArray children) {
        bestMatches(children, null, maxima);
        double sum = 0;
        for (double value : maxima) {
            sum += value;
        }
        return (float) sum;
    }

    /** Writes each selected query's best cosine in selection order. Null selects all queries. */
    public void bestMatches(
            InternalArray children, @Nullable IntArrayList selected, double[] scores) {
        int count = selected == null ? queries.length : selected.size();
        Arrays.fill(scores, 0, count, Double.NEGATIVE_INFINITY);
        for (int j = 0; j < children.size(); j++) {
            if (children.isNullAt(j)) {
                continue;
            }
            float[] stored =
                    VectorIndexUtils.toFloatArray(childGetter.getElementOrNull(children, j));
            if (stored.length != queries[0].length) {
                throw new IllegalArgumentException(
                        "Query vector dimension mismatch: expected "
                                + stored.length
                                + ", got "
                                + queries[0].length);
            }
            double storedNorm = norm(stored);
            for (int pos = 0; pos < count; pos++) {
                int i = selected == null ? pos : selected.get(pos);
                double dot = 0;
                for (int d = 0; d < stored.length; d++) {
                    dot += (double) queries[i][d] * stored[d];
                }
                double denominator = queryNorms[i] * storedNorm;
                scores[pos] = Math.max(scores[pos], denominator == 0 ? 0 : dot / denominator);
            }
        }
    }

    private static double norm(float[] vector) {
        double squared = 0;
        for (float value : vector) {
            if (!Float.isFinite(value)) {
                throw new IllegalArgumentException("Multivector coordinates must be finite.");
            }
            squared += (double) value * value;
        }
        return Math.sqrt(squared);
    }
}
