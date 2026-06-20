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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * VectorSearch to perform vector similarity search.
 *
 * <p>This is an internal pushdown representation. Use {@code Table.newVectorSearchBuilder()} to
 * configure vector search from Java.
 */
public class VectorSearch implements Serializable {

    private static final long serialVersionUID = 1L;

    private final float[] vector;
    private final String fieldName;
    private final int limit;
    private final Map<String, String> options;

    @Nullable private RoaringNavigableMap64 includeRowIds;

    public VectorSearch(float[] vector, int limit, String fieldName) {
        this(vector, limit, fieldName, Collections.emptyMap());
    }

    public VectorSearch(float[] vector, int limit, String fieldName, Map<String, String> options) {
        if (vector == null) {
            throw new IllegalArgumentException("Search vector cannot be null");
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive, got: " + limit);
        }
        if (fieldName == null || fieldName.isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        this.vector = vector;
        this.limit = limit;
        this.fieldName = fieldName;
        this.options =
                options == null
                        ? Collections.emptyMap()
                        : Collections.unmodifiableMap(new HashMap<>(options));
    }

    public float[] vector() {
        return vector;
    }

    public int limit() {
        return limit;
    }

    public String fieldName() {
        return fieldName;
    }

    public Map<String, String> options() {
        return options == null ? Collections.emptyMap() : options;
    }

    public RoaringNavigableMap64 includeRowIds() {
        return includeRowIds;
    }

    public VectorSearch withIncludeRowIds(RoaringNavigableMap64 includeRowIds) {
        this.includeRowIds = includeRowIds;
        return this;
    }

    public VectorSearch offsetRange(long from, long to) {
        if (includeRowIds != null) {
            VectorSearch target = new VectorSearch(vector, limit, fieldName, options());
            target.withIncludeRowIds(VectorSearchUtils.offsetRowIds(includeRowIds, from, to));
            return target;
        }
        return this;
    }

    @Override
    public String toString() {
        return String.format("FieldName(%s), Limit(%s)", fieldName, limit);
    }
}
