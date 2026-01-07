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
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Optional;

/** VectorSearch to perform vector similarity search. * */
public class VectorSearch implements Serializable {

    private static final long serialVersionUID = 1L;

    // float[] or byte[]
    private final Object vector;
    private final String fieldName;
    private final int limit;

    @Nullable private RoaringNavigableMap64 includeRowIds;

    public VectorSearch(Object vector, int limit, String fieldName) {
        if (vector == null) {
            throw new IllegalArgumentException("Search cannot be null");
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
    }

    // float[] or byte[]
    public Object vector() {
        return vector;
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

    public VectorSearch withIncludeRowIds(RoaringNavigableMap64 includeRowIds) {
        this.includeRowIds = includeRowIds;
        return this;
    }

    public Optional<GlobalIndexResult> visit(GlobalIndexReader visitor) {
        return visitor.visitVectorSearch(this);
    }

    @Override
    public String toString() {
        return String.format("FieldName(%s), Limit(%s)", fieldName, limit);
    }
}
