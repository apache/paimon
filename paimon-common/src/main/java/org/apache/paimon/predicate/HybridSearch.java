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

import org.apache.paimon.globalindex.HybridSearchRanker;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Hybrid search over vector and full-text routes.
 *
 * <p>This is an internal pushdown representation. Use {@code Table.newHybridSearchBuilder()} to
 * configure hybrid search from Java.
 */
public class HybridSearch implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<HybridSearchRoute> routes;
    private final int limit;
    private final String ranker;

    public HybridSearch(List<HybridSearchRoute> routes, int limit, String ranker) {
        if (routes == null || routes.isEmpty()) {
            throw new IllegalArgumentException("Routes cannot be null or empty");
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive, got: " + limit);
        }
        this.routes = Collections.unmodifiableList(new ArrayList<>(routes));
        this.limit = limit;
        this.ranker = HybridSearchRanker.normalizeRanker(ranker);
    }

    public List<HybridSearchRoute> routes() {
        return routes;
    }

    public int limit() {
        return limit;
    }

    public String ranker() {
        return ranker;
    }

    @Override
    public String toString() {
        return "Ranker(" + ranker + "), Limit(" + limit + "), Routes(" + routes + ")";
    }
}
