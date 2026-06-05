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

/** FullTextSearch to perform full-text search on a text column. */
public class FullTextSearch implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String queryText;
    private final String fieldName;
    private final int limit;
    private final String queryOperator;

    public FullTextSearch(String queryText, int limit, String fieldName) {
        this(queryText, limit, fieldName, "or");
    }

    public FullTextSearch(String queryText, int limit, String fieldName, String queryOperator) {
        if (queryText == null || queryText.isEmpty()) {
            throw new IllegalArgumentException("Query text cannot be null or empty");
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive, got: " + limit);
        }
        if (fieldName == null || fieldName.isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        this.queryText = queryText;
        this.limit = limit;
        this.fieldName = fieldName;
        String normalizedOperator =
                queryOperator == null ? "or" : queryOperator.trim().toLowerCase();
        if (!"or".equals(normalizedOperator) && !"and".equals(normalizedOperator)) {
            throw new IllegalArgumentException(
                    "Query operator must be 'or' or 'and', got: " + queryOperator);
        }
        this.queryOperator = normalizedOperator;
    }

    public String queryText() {
        return queryText;
    }

    public int limit() {
        return limit;
    }

    public String fieldName() {
        return fieldName;
    }

    public String queryOperator() {
        return queryOperator;
    }

    @Override
    public String toString() {
        return String.format(
                "FullTextSearch{field=%s, query='%s', limit=%d, operator=%s}",
                fieldName, queryText, limit, queryOperator);
    }
}
