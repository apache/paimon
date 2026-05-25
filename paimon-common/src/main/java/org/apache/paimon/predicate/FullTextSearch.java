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

import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;

import java.io.Serializable;
import java.util.Optional;

/** FullTextSearch to perform full-text search on a text column. */
public class FullTextSearch implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String queryText;
    private final String fieldName;
    private final int limit;

    public FullTextSearch(String queryText, int limit, String fieldName) {
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

    public Optional<ScoredGlobalIndexResult> visit(GlobalIndexReader visitor) {
        return visitor.visitFullTextSearch(this);
    }

    @Override
    public String toString() {
        return String.format(
                "FullTextSearch{field=%s, query='%s', limit=%d}", fieldName, queryText, limit);
    }
}
