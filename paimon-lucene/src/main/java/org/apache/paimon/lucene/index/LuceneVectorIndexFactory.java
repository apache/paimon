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

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.TinyIntType;

/** Factory for creating Lucene vector index instances based on data type. */
public abstract class LuceneVectorIndexFactory {

    public static LuceneVectorIndexFactory init(DataType dataType) {
        if (dataType instanceof ArrayType
                && ((ArrayType) dataType).getElementType() instanceof FloatType) {
            return new LuceneFloatVectorIndexFactory();
        } else if (dataType instanceof ArrayType
                && ((ArrayType) dataType).getElementType() instanceof TinyIntType) {
            return new LuceneByteVectorIndexFactory();
        } else {
            throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }

    public abstract LuceneVectorIndex create(long rowId, Object vector);

    /** Factory for creating LuceneFloatVectorIndex instances. */
    public static class LuceneFloatVectorIndexFactory extends LuceneVectorIndexFactory {
        @Override
        public LuceneVectorIndex create(long rowId, Object fieldData) {
            float[] vector;
            if (fieldData instanceof float[]) {
                vector = (float[]) fieldData;
            } else if (fieldData instanceof InternalArray) {
                vector = ((InternalArray) fieldData).toFloatArray();
            } else {
                throw new RuntimeException(
                        "Unsupported vector type: " + fieldData.getClass().getName());
            }
            return new LuceneFloatVectorIndex(rowId, vector);
        }
    }

    /** Factory for creating LuceneByteVectorIndex instances. */
    public static class LuceneByteVectorIndexFactory extends LuceneVectorIndexFactory {
        @Override
        public LuceneVectorIndex create(long rowId, Object fieldData) {
            byte[] vector;
            if (fieldData instanceof byte[]) {
                vector = (byte[]) fieldData;
            } else if (fieldData instanceof InternalArray) {
                vector = ((InternalArray) fieldData).toByteArray();
            } else {
                throw new RuntimeException(
                        "Unsupported vector type: " + fieldData.getClass().getName());
            }
            return new LuceneByteVectorIndex(rowId, (byte[]) vector);
        }
    }
}
