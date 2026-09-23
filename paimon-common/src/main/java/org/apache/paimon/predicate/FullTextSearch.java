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

/** FullTextSearch to perform full-text search with a query string. */
public class FullTextSearch implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String fieldName;
    private final String query;
    private final int limit;

    @Nullable private RoaringNavigableMap64 includeRowIds;

    public FullTextSearch(String fieldName, String query, int limit) {
        if (fieldName == null || fieldName.isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        if (query == null) {
            throw new IllegalArgumentException("Query cannot be null");
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive, got: " + limit);
        }
        this.fieldName = fieldName;
        this.query = query;
        this.limit = limit;
    }

    public int limit() {
        return limit;
    }

    public String column() {
        return fieldName;
    }

    public String fieldName() {
        return fieldName;
    }

    public String query() {
        return query;
    }

    public RoaringNavigableMap64 includeRowIds() {
        return includeRowIds;
    }

    public FullTextSearch withIncludeRowIds(RoaringNavigableMap64 includeRowIds) {
        this.includeRowIds = includeRowIds;
        return this;
    }

    public FullTextSearch offsetRange(long from, long to) {
        if (includeRowIds != null) {
            FullTextSearch target = new FullTextSearch(fieldName, query, limit);
            target.withIncludeRowIds(VectorSearchUtils.offsetRowIds(includeRowIds, from, to));
            return target;
        }
        return this;
    }

    @Override
    public String toString() {
        return String.format(
                "FullTextSearch{fieldName=%s, limit=%d, query=%s}", fieldName, limit, query);
    }
}
