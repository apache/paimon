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

/** Materialization at the portable vector index boundary. */
public final class VectorIndexUtils {
    private VectorIndexUtils() {}

    public static boolean isVectorIndexType(String type) {
        return "ivf-flat".equals(type)
                || "ivf-pq".equals(type)
                || "ivf-sq".equals(type)
                || "ivf-rq".equals(type)
                || "diskann".equals(type);
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
