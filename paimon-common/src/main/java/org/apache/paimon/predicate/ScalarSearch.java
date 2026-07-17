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

package org.apache.paimon.predicate;

import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** A scalar TopN search optionally restricted to a set of candidate row IDs. */
public class ScalarSearch implements Serializable {

    private static final long serialVersionUID = 1L;

    private final TopN topN;

    @Nullable private RoaringNavigableMap64 includeRowIds;

    public ScalarSearch(TopN topN) {
        this.topN = checkNotNull(topN);
        checkArgument(topN.limit() > 0, "Limit must be positive, got: %s", topN.limit());
    }

    public TopN topN() {
        return topN;
    }

    @Nullable
    public RoaringNavigableMap64 includeRowIds() {
        return includeRowIds;
    }

    public ScalarSearch withIncludeRowIds(@Nullable RoaringNavigableMap64 includeRowIds) {
        this.includeRowIds = includeRowIds;
        return this;
    }

    public ScalarSearch offsetRange(long from, long to) {
        if (includeRowIds == null) {
            return this;
        }
        return new ScalarSearch(topN)
                .withIncludeRowIds(VectorSearchUtils.offsetRowIds(includeRowIds, from, to));
    }

    @Override
    public String toString() {
        return topN.toString();
    }
}
