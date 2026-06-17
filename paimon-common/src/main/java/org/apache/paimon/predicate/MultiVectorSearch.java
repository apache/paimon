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

import org.apache.paimon.globalindex.MultiVectorSearchRanker;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Multi-vector search over multiple vector columns.
 *
 * <p>This is an internal pushdown representation. Use {@code
 * Table.newMultiVectorSearchBuilder()} to configure multi-vector search from Java.
 */
public class MultiVectorSearch implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<MultiVectorSearchRoute> routes;
    private final int limit;
    private final String ranker;

    public MultiVectorSearch(List<MultiVectorSearchRoute> routes, int limit, String ranker) {
        if (routes == null || routes.isEmpty()) {
            throw new IllegalArgumentException("Routes cannot be null or empty");
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive, got: " + limit);
        }
        this.routes = Collections.unmodifiableList(new ArrayList<>(routes));
        this.limit = limit;
        this.ranker = MultiVectorSearchRanker.normalizeRanker(ranker);
    }

    public List<MultiVectorSearchRoute> routes() {
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
