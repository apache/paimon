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

import org.apache.paimon.types.DataType;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/** {@link LeafFunction} to eval TopK. */
public class TopKFunction extends LeafFunction {
    private static final long serialVersionUID = 1L;

    private final TopK topK;
    private final TopKRowIdFilter filter;

    public TopKFunction(TopK topK, TopKRowIdFilter filter) {
        this.topK = topK;
        this.filter = filter;
    }

    @Override
    public boolean test(DataType type, Object field, List<Object> literals) {
        return false;
    }

    @Override
    public boolean test(
            DataType type,
            long rowCount,
            Object min,
            Object max,
            Long nullCount,
            List<Object> literals) {
        return false;
    }

    @Override
    public Optional<LeafFunction> negate() {
        return Optional.empty();
    }

    @Override
    public <T> T visit(FunctionVisitor<T> visitor, FieldRef fieldRef, List<Object> literals) {
        return visitor.visitTopK(topK, filter);
    }

    /** Represents the TopK predicate. */
    public static class TopK implements Serializable {

        private static final long serialVersionUID = 1L;

        private Object vector;
        private final String similarityFunction;
        private final int k;

        public TopK(Object vector, String similarityFunction, int k) {
            if (vector == null) {
                throw new IllegalArgumentException("Vector cannot be null");
            }
            if (k <= 0) {
                throw new IllegalArgumentException("K must be positive, got: " + k);
            }
            if (similarityFunction == null || similarityFunction.isEmpty()) {
                throw new IllegalArgumentException("Similarity function cannot be null or empty");
            }
            this.vector = vector;
            this.similarityFunction = similarityFunction;
            this.k = k;
        }

        public Object vector() {
            return vector;
        }

        public String similarityFunction() {
            return similarityFunction;
        }

        public int k() {
            return k;
        }

        @Override
        public String toString() {
            return String.format("SimilarityFunction(%s), K(%s)", similarityFunction, k);
        }
    }

    /** Represents the TopK row id filter. */
    public static class TopKRowIdFilter implements Serializable {

        private static final long serialVersionUID = 1L;

        private Iterator<Long> includeRowIds;

        public TopKRowIdFilter(Iterator<Long> includeRowIds) {
            this.includeRowIds = includeRowIds;
        }

        public Iterator<Long> includeRowIds() {
            return includeRowIds;
        }
    }
}
