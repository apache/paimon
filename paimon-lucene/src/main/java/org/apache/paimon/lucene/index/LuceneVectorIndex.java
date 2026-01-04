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

package org.apache.paimon.lucene.index;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.VectorSimilarityFunction;

/** Lucene vector index interface. */
public abstract class LuceneVectorIndex<T> {

    public static final String VECTOR_FIELD = "vector";
    public static final String ROW_ID_FIELD = "id";

    public abstract long id();

    public abstract long dimension();

    public abstract T vector();

    public abstract IndexableField indexableField(VectorSimilarityFunction similarityFunction);

    public StoredField rowIdStoredField() {
        return new StoredField(ROW_ID_FIELD, id());
    }

    public LongPoint rowIdLongPoint() {
        return new LongPoint(ROW_ID_FIELD, id());
    }

    public void checkDimension(int dimension) {
        if (dimension() != dimension) {
            throw new IllegalArgumentException(
                    String.format(
                            "Vector dimension mismatch: expected %d, but got %d",
                            dimension(), dimension));
        }
    }
}
