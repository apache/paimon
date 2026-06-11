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

/** Batch vector similarity search. */
public class BatchVectorSearch implements Serializable {

    private static final long serialVersionUID = 1L;

    private final float[][] vectors;
    private final String fieldName;
    private final int limit;

    @Nullable private RoaringNavigableMap64 includeRowIds;

    public BatchVectorSearch(float[][] vectors, int limit, String fieldName) {
        if (vectors == null || vectors.length == 0) {
            throw new IllegalArgumentException("Search vectors cannot be null or empty");
        }
        for (float[] vector : vectors) {
            if (vector == null) {
                throw new IllegalArgumentException("Search vector element cannot be null");
            }
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive, got: " + limit);
        }
        if (fieldName == null || fieldName.isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        this.vectors = vectors;
        this.limit = limit;
        this.fieldName = fieldName;
    }

    /** Query vectors in input order. */
    public float[][] vectors() {
        return vectors;
    }

    public int vectorCount() {
        return vectors.length;
    }

    public VectorSearch forIndex(int i) {
        VectorSearch vectorSearch = new VectorSearch(vectors[i], limit, fieldName);
        if (includeRowIds != null) {
            vectorSearch.withIncludeRowIds(includeRowIds);
        }
        return vectorSearch;
    }

    public int limit() {
        return limit;
    }

    public String fieldName() {
        return fieldName;
    }

    public RoaringNavigableMap64 includeRowIds() {
        return includeRowIds;
    }

    public BatchVectorSearch withIncludeRowIds(RoaringNavigableMap64 includeRowIds) {
        this.includeRowIds = includeRowIds;
        return this;
    }

    public BatchVectorSearch offsetRange(long from, long to) {
        if (includeRowIds != null) {
            BatchVectorSearch target = new BatchVectorSearch(vectors, limit, fieldName);
            target.withIncludeRowIds(VectorSearchUtils.offsetRowIds(includeRowIds, from, to));
            return target;
        }
        return this;
    }

    @Override
    public String toString() {
        return String.format(
                "FieldName(%s), Limit(%s), VectorCount(%s)", fieldName, limit, vectors.length);
    }
}
