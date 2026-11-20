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

package org.apache.paimon.globalindex;

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.VectorType;

/** Materialization at the portable vector index boundary. */
public final class VectorIndexUtils {
    private VectorIndexUtils() {}

    public static boolean isMultiVector(DataType type) {
        if (!(type instanceof ArrayType)) {
            return false;
        }
        DataType child = ((ArrayType) type).getElementType();
        return (child instanceof VectorType
                        && ((VectorType) child).getElementType() instanceof FloatType)
                || (child instanceof ArrayType
                        && ((ArrayType) child).getElementType() instanceof FloatType);
    }

    public static void validateQuery(float[][] vectors) {
        if (vectors == null
                || vectors.length == 0
                || vectors[0] == null
                || vectors[0].length == 0) {
            throw new IllegalArgumentException(
                    "A multivector query requires at least one non-empty vector.");
        }
        for (float[] vector : vectors) {
            if (vector == null || vector.length != vectors[0].length) {
                throw new IllegalArgumentException("Multivector query dimensions must match.");
            }
            for (float value : vector) {
                if (!Float.isFinite(value)) {
                    throw new IllegalArgumentException(
                            "Multivector query coordinates must be finite.");
                }
            }
        }
    }

    public static void validateQuery(ArrayType columnType, float[][] vectors) {
        validateQuery(vectors);
        DataType child = columnType.getElementType();
        if (child instanceof VectorType) {
            int dimension = ((VectorType) child).getLength();
            if (vectors[0].length != dimension) {
                throw new IllegalArgumentException(
                        "Query vector dimension mismatch: expected "
                                + dimension
                                + ", got "
                                + vectors[0].length);
            }
        }
    }

    public static float[] toFloatArray(Object value) {
        if (value instanceof float[]) {
            return ((float[]) value).clone();
        }
        int size =
                value instanceof InternalVector
                        ? ((InternalVector) value).size()
                        : ((InternalArray) value).size();
        float[] result = new float[size];
        for (int i = 0; i < size; i++) {
            if (value instanceof InternalVector) {
                result[i] = ((InternalVector) value).getFloat(i);
            } else {
                InternalArray array = (InternalArray) value;
                if (array.isNullAt(i)) {
                    throw new IllegalArgumentException("Vector element cannot be null.");
                }
                result[i] = array.getFloat(i);
            }
        }
        return result;
    }
}
