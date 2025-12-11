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

import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.VectorSimilarityFunction;

/** Lucene vector index for float vector. */
public class LuceneFloatVectorIndex extends LuceneVectorIndex<float[]> {
    private final long rowId;
    private final float[] vector;

    public LuceneFloatVectorIndex(long rowId, float[] vector) {
        this.rowId = rowId;
        this.vector = vector;
    }

    @Override
    public float[] vector() {
        return vector;
    }

    @Override
    public long id() {
        return rowId;
    }

    @Override
    public long dimension() {
        return vector().length;
    }

    @Override
    public IndexableField indexableField(VectorSimilarityFunction similarityFunction) {
        return new KnnFloatVectorField(VECTOR_FIELD, this.vector(), similarityFunction);
    }
}
