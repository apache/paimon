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

/** VectorSearch. */
public class VectorSearch implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String fieldName;
    private final Object vector;
    private final Optional<String> similarityFunction;
    private final int limit;
    private final Iterator<Long> includeRowIds;

    public VectorSearch(
            String fieldName,
            Object vector,
            int limit,
            Iterator<Long> includeRowIds,
            @Nullable String similarityFunction) {
        if (fieldName == null) {
            throw new IllegalArgumentException("Field name cannot be null");
        }
        if (vector == null) {
            throw new IllegalArgumentException("Vector cannot be null");
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive, got: " + limit);
        }
        if (similarityFunction == null || similarityFunction.isEmpty()) {
            throw new IllegalArgumentException("Similarity function cannot be null or empty");
        }
        this.fieldName = fieldName;
        this.vector = vector;
        this.similarityFunction = Optional.ofNullable(similarityFunction);
        this.limit = limit;
        this.includeRowIds = includeRowIds;
    }

    public String fieldName() {
        return fieldName;
    }

    public Object vector() {
        return vector;
    }

    public Optional<String> similarityFunction() {
        return similarityFunction;
    }

    public int limit() {
        return limit;
    }

    public Iterator<Long> includeRowIds() {
        return includeRowIds;
    }

    public <T> T visit(FunctionVisitor<T> visitor) {
        return visitor.visitVectorSearch(this);
    }

    @Override
    public String toString() {
        return String.format(
                "FileName(%s), SimilarityFunction(%s), K(%s)",
                fieldName, similarityFunction, limit);
    }
}
