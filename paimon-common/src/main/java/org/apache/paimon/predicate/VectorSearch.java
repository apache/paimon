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
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/** VectorSearch to perform vector similarity search. * */
public class VectorSearch implements Serializable {

    private static final long serialVersionUID = 1L;

    private final float[][] vectors;
    private final String fieldName;
    private final int limit;

    @Nullable private RoaringNavigableMap64 includeRowIds;

    public VectorSearch(float[] vector, int limit, String fieldName) {
        this(new float[][] {vector}, limit, fieldName);
    }

    public VectorSearch(float[][] vectors, int limit, String fieldName) {
        if (vectors == null || vectors.length == 0) {
            throw new IllegalArgumentException("Search vectors cannot be null or empty");
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

    /** Returns the first (or only) query vector. */
    public float[] vector() {
        return vectors[0];
    }

    /** Returns all query vectors. */
    public float[][] vectors() {
        return vectors;
    }

    /** Returns the number of query vectors. */
    public int vectorCount() {
        return vectors.length;
    }

    /** Returns a single-vector VectorSearch for the i-th query vector. */
    public VectorSearch forIndex(int i) {
        VectorSearch single = new VectorSearch(vectors[i], limit, fieldName);
        if (includeRowIds != null) {
            single.withIncludeRowIds(includeRowIds);
        }
        return single;
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

    public VectorSearch offsetRange(long from, long to) {
        if (includeRowIds != null) {
            RoaringNavigableMap64 range = new RoaringNavigableMap64();
            range.addRange(new Range(from, to));
            RoaringNavigableMap64 and64 = RoaringNavigableMap64.and(range, includeRowIds);
            final RoaringNavigableMap64 roaringNavigableMap64Offset = new RoaringNavigableMap64();
            for (long rowId : and64) {
                roaringNavigableMap64Offset.add(rowId - from);
            }
            VectorSearch target = new VectorSearch(vectors, limit, fieldName);
            target.withIncludeRowIds(roaringNavigableMap64Offset);
            return target;
        }
        return this;
    }

    public Optional<ScoredGlobalIndexResult> visit(GlobalIndexReader visitor) {
        return visitor.visitVectorSearch(this);
    }

    public List<Optional<ScoredGlobalIndexResult>> visitBatch(GlobalIndexReader visitor) {
        return visitor.visitBatchVectorSearch(this);
    }

    @Override
    public String toString() {
        return String.format(
                "FieldName(%s), Limit(%s), VectorCount(%s)", fieldName, limit, vectors.length);
    }
}
