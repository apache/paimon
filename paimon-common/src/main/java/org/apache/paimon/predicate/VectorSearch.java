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

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Optional;

/** VectorSearch to perform vector similarity search. * */
public class VectorSearch implements Serializable {
    private static final long serialVersionUID = 1L;

    private Object search;
    private String fieldName;
    private Optional<String> similarityFunction;
    private int limit;
    private Iterator<Long> includeRowIds;

    public VectorSearch(
            Object search,
            int limit,
            String fieldName,
            Iterator<Long> includeRowIds,
            @Nullable String similarityFunction) {
        if (search == null) {
            throw new IllegalArgumentException("Search cannot be null");
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive, got: " + limit);
        }
        this.search = search;
        this.limit = limit;
        this.fieldName = fieldName;
        this.similarityFunction = Optional.ofNullable(similarityFunction);
        this.includeRowIds = includeRowIds;
    }

    public VectorSearch(Object search, int limit, String fieldName) {
        this(search, limit, fieldName, null, null);
    }

    public VectorSearch(Object search, int limit, String fieldName, Iterator<Long> includeRowIds) {
        this(search, limit, fieldName, includeRowIds, null);
    }

    public Object search() {
        return search;
    }

    public int limit() {
        return limit;
    }

    public String fieldName() {
        return fieldName;
    }

    public Optional<String> similarityFunction() {
        return similarityFunction;
    }

    public Iterator<Long> includeRowIds() {
        return includeRowIds;
    }

    public VectorSearch withIncludeRowIds(Iterator<Long> includeRowIds) {
        this.includeRowIds = includeRowIds;
        return this;
    }

    public <T> T visit(FunctionVisitor<T> visitor) {
        return visitor.visitVectorSearch(this);
    }

    @Override
    public String toString() {
        return String.format("SimilarityFunction(%s), K(%s)", similarityFunction, limit);
    }
}
