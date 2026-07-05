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

package org.apache.paimon.tantivy.index;

import org.apache.paimon.globalindex.ScoreGetter;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.util.Map;

/** Full-text search global index result for Tantivy. */
public class TantivyScoredGlobalIndexResult implements ScoredGlobalIndexResult {

    private final RoaringNavigableMap64 results;
    private final Map<Long, Float> id2scores;

    public TantivyScoredGlobalIndexResult(
            RoaringNavigableMap64 results, Map<Long, Float> id2scores) {
        this.results = results;
        this.id2scores = id2scores;
    }

    @Override
    public ScoreGetter scoreGetter() {
        return rowId -> {
            Float score = id2scores.get(rowId);
            if (score == null) {
                throw new IllegalArgumentException(
                        "No score found for rowId: "
                                + rowId
                                + ". Only rowIds present in results() are valid.");
            }
            return score;
        };
    }

    @Override
    public RoaringNavigableMap64 results() {
        return results;
    }
}
