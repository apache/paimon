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

import org.apache.paimon.utils.LazyField;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.util.function.Supplier;

/** Vector search global index result for scored index. */
public interface ScoredGlobalIndexResult extends GlobalIndexResult {

    ScoreGetter scoreGetter();

    default GlobalIndexResult and(GlobalIndexResult other) {
        throw new UnsupportedOperationException("Please realize this by specified global index");
    }

    default ScoredGlobalIndexResult offset(long offset) {
        if (offset == 0) {
            return this;
        }
        RoaringNavigableMap64 roaringNavigableMap64 = results();
        final RoaringNavigableMap64 roaringNavigableMap64Offset = new RoaringNavigableMap64();
        final ScoreGetter thisScoreGetter = scoreGetter();

        for (long rowId : roaringNavigableMap64) {
            roaringNavigableMap64Offset.add(rowId + offset);
        }

        return create(
                () -> roaringNavigableMap64Offset, rowId -> thisScoreGetter.score(rowId - offset));
    }

    @Override
    default GlobalIndexResult or(GlobalIndexResult other) {
        if (!(other instanceof ScoredGlobalIndexResult)) {
            return GlobalIndexResult.super.or(other);
        }
        RoaringNavigableMap64 thisRowIds = results();
        ScoreGetter thisScoreGetter = scoreGetter();

        RoaringNavigableMap64 otherRowIds = other.results();
        ScoreGetter otherScoreGetter = ((ScoredGlobalIndexResult) other).scoreGetter();

        final RoaringNavigableMap64 resultOr = RoaringNavigableMap64.or(thisRowIds, otherRowIds);
        return new ScoredGlobalIndexResult() {
            @Override
            public ScoreGetter scoreGetter() {
                return rowId -> {
                    if (thisRowIds.contains(rowId)) {
                        return thisScoreGetter.score(rowId);
                    }
                    return otherScoreGetter.score(rowId);
                };
            }

            @Override
            public RoaringNavigableMap64 results() {
                return resultOr;
            }
        };
    }

    /** Returns a new {@link ScoredGlobalIndexResult} from supplier. */
    static ScoredGlobalIndexResult create(
            Supplier<RoaringNavigableMap64> supplier, ScoreGetter scoreGetter) {
        LazyField<RoaringNavigableMap64> lazyField = new LazyField<>(supplier);
        return new ScoredGlobalIndexResult() {
            @Override
            public ScoreGetter scoreGetter() {
                return scoreGetter;
            }

            @Override
            public RoaringNavigableMap64 results() {
                return lazyField.get();
            }
        };
    }
}
