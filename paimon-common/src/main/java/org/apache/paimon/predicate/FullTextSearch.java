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

import java.io.Serializable;
import java.util.List;

/** FullTextSearch to perform full-text search with a structured query. */
public class FullTextSearch implements Serializable {

    private static final long serialVersionUID = 1L;

    private final FullTextQuery query;
    private final int limit;

    public FullTextSearch(FullTextQuery query, int limit) {
        if (query == null) {
            throw new IllegalArgumentException("Query cannot be null");
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive, got: " + limit);
        }
        this.query = query;
        this.limit = limit;
    }

    public int limit() {
        return limit;
    }

    public List<String> columns() {
        return query.columns();
    }

    public String fieldName() {
        return query.singleColumn();
    }

    public FullTextQuery query() {
        return query;
    }

    public String queryJson() {
        return query.toJson();
    }

    @Override
    public String toString() {
        return String.format(
                "FullTextSearch{columns=%s, limit=%d, queryJson=%s}",
                columns(), limit, queryJson());
    }
}
