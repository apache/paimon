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

import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.VectorSimilarityFunction;

/** Lucene vector index for byte vector. */
public class LuceneByteVectorIndex extends LuceneVectorIndex<byte[]> {
    private final long rowId;
    private final byte[] vector;

    public LuceneByteVectorIndex(long rowId, byte[] vector) {
        this.rowId = rowId;
        this.vector = vector;
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
        return new KnnByteVectorField(VECTOR_FIELD, this.vector, similarityFunction);
    }

    @Override
    public byte[] vector() {
        return vector;
    }
}
